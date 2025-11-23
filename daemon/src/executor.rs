use std::cmp::{Ordering, Reverse};
use std::collections::{HashMap, HashSet, VecDeque};
use std::f64::consts::{E as E_CONST, PI};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use common::attr::{AttributeContainer, AttributeValue};
use common::value::{FieldValue, QueryPath};
use graphdb_core::query::{
    AggregateExpression, AggregateFunction, BinaryOperator, ComparisonOperator, CreatePattern,
    CreateRelationship, ExistsFunction, Expression, FieldReference, FunctionExpression,
    GraphDbProcedure, ListExpression, ListPredicate, ListPredicateFunction, ListPredicateKind,
    MatchPattern, NodePattern, PathLength, PathMatchQuery, PathPattern, PathQueryMode, PathReturn,
    PredicateFilter, Procedure, Projection, Properties, Query, RelationshipDirection,
    RelationshipMatch, RelationshipPattern, ScalarFunction, SelectMatchClause, SelectQuery,
    UserProcedureCall, Value, ValueOperand, WithClause, parse_queries,
};
use graphdb_core::{
    AuthMethod, Database, Edge, EdgeClassEntry, EdgeId, InMemoryBackend, Node, NodeClassEntry,
    NodeId, RoleEntry, SimpleStorage, StorageBackend, UserEntry,
};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};

use crate::error::DaemonError;
use crate::path::{
    astar, dijkstra,
    traversal::{self, PathState, edge_matches_pattern},
};
use function_api::{
    self as functions, ExpressionHandle, FunctionContext, FunctionError, ProcedureContext,
};
#[cfg(test)]
use function_api::{FunctionArity, FunctionSpec, ProcedureRow, ProcedureSpec};

#[derive(Debug, Default, Serialize)]
pub struct ExecutionReport {
    pub messages: Vec<String>,
    pub selected_nodes: Vec<Node>,
    pub procedures: Vec<ProcedureResult>,
    pub paths: Vec<PathResult>,
    pub path_pairs: Vec<PathPairResult>,
    pub result_rows: Vec<JsonValue>,
    pub plan_summary: Option<JsonValue>,
}

#[derive(Clone, Default)]
struct QueryRow {
    nodes: HashMap<String, Option<Node>>,
    relationships: HashMap<String, Option<Edge>>,
    paths: HashMap<String, Option<QueryPath>>,
    lists: HashMap<String, Vec<FieldValue>>,
    scalars: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct PlanSummary {
    clauses: Vec<PlannedClause>,
    total_clauses: usize,
    optional_clauses: usize,
    filter_count: usize,
    has_with_clause: bool,
    return_count: usize,
}

#[derive(Debug, Serialize)]
struct PlannedClause {
    order: usize,
    optional: bool,
    pattern_count: usize,
    aliases: Vec<String>,
    selectivity_score: usize,
}

struct LogicalPlan {
    clauses: Vec<SelectMatchClause>,
    summary: PlanSummary,
}

struct GroupState {
    key_values: HashMap<String, Value>,
    row_indices: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcedureResult {
    pub name: String,
    pub rows: Vec<JsonValue>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathResult {
    pub alias: String,
    pub nodes: Vec<Node>,
    pub edge_ids: Vec<EdgeId>,
    pub length: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathPairResult {
    pub start_alias: String,
    pub end_alias: String,
    pub start: Node,
    pub end: Node,
    pub length: usize,
}
pub fn execute_script_with_memory(
    db: &Database<InMemoryBackend>,
    script: &str,
) -> Result<ExecutionReport, DaemonError> {
    execute_script(db, script)
}

pub fn execute_script_with_simple(
    db: &Database<SimpleStorage>,
    script: &str,
) -> Result<ExecutionReport, DaemonError> {
    execute_script(db, script)
}

fn execute_script<B: StorageBackend>(
    db: &Database<B>,
    script: &str,
) -> Result<ExecutionReport, DaemonError> {
    let queries = parse_queries(script)?;
    let mut ctx = ExecutionReport::default();
    for query in queries {
        execute_query(db, query, &mut ctx)?;
    }
    Ok(ctx)
}

fn execute_query<B: StorageBackend>(
    db: &Database<B>,
    query: Query,
    ctx: &mut ExecutionReport,
) -> Result<(), DaemonError> {
    ctx.result_rows.clear();
    match query {
        Query::InsertNode { pattern } => {
            let node_id = insert_node(db, pattern)?;
            ctx.messages.push(format!("inserted node {}", node_id));
            Ok(())
        }
        Query::InsertEdge {
            source,
            edge,
            target,
        } => {
            let edge_id = insert_edge(db, source, edge, target)?;
            ctx.messages.push(format!("inserted edge {}", edge_id));
            Ok(())
        }
        Query::DeleteNode { id } => {
            let node_id = parse_node_id(&Value::String(id))?;
            db.remove_node(node_id)?;
            ctx.messages.push("deleted node".into());
            Ok(())
        }
        Query::DeleteEdge { id } => {
            let edge_id = parse_edge_id(&Value::String(id))?;
            db.remove_edge(edge_id)?;
            ctx.messages.push("deleted edge".into());
            Ok(())
        }
        Query::Select(query) => execute_select(db, query, ctx),
        Query::Create { pattern } => execute_create(db, pattern, ctx),
        Query::UpdateNode { .. } | Query::UpdateEdge { .. } => {
            Err(DaemonError::Query("UPDATE not yet supported".into()))
        }
        Query::CallProcedure { procedure } => {
            let result = execute_procedure(db, procedure)?;
            ctx.messages.push(format!(
                "call {} returned {} row(s)",
                result.name,
                result.rows.len()
            ));
            ctx.procedures.push(result);
            Ok(())
        }
        Query::PathMatch(spec) => execute_path_query(db, spec, ctx),
    }
}

fn execute_select<B: StorageBackend>(
    db: &Database<B>,
    query: SelectQuery,
    ctx: &mut ExecutionReport,
) -> Result<(), DaemonError> {
    if query.match_clauses.is_empty() && query.with.is_none() && query.initial_with.is_none() {
        return Err(DaemonError::Query(
            "MATCH clause or WITH clause is required".into(),
        ));
    }

    let plan = build_logical_plan(&query);
    let query_timestamp = current_timestamp_millis();
    let plan_json = serde_json::to_value(&plan.summary).unwrap_or(JsonValue::Null);
    if plan_json != JsonValue::Null {
        tracing::debug!(target = "graphdb::planner", event = "query.plan", summary = %plan_json);
        ctx.plan_summary = Some(plan_json.clone());
    } else {
        ctx.plan_summary = None;
    }

    if query.explain {
        if plan_json != JsonValue::Null {
            ctx.result_rows = vec![plan_json];
        }
        ctx.selected_nodes.clear();
        ctx.messages.push("query plan only (EXPLAIN)".into());
        return Ok(());
    }

    let mut rows = vec![QueryRow::default()];
    if let Some(initial_with) = query.initial_with.as_ref() {
        rows = apply_with_clause(db, rows, initial_with, query_timestamp)?;
        if rows.is_empty() {
            ctx.result_rows.clear();
            ctx.selected_nodes.clear();
            ctx.messages.push("WITH clause produced 0 rows".into());
            return Ok(());
        }
    }
    let conditions_by_alias = group_conditions_by_alias(&query.conditions);

    for clause in &plan.clauses {
        rows = apply_match_clause(db, rows, clause, &conditions_by_alias)?;
        if rows.is_empty() {
            break;
        }
    }

    if !query.predicates.is_empty() {
        rows = apply_predicates(db, rows, &query.predicates, query_timestamp)?;
    }

    if rows.is_empty() {
        ctx.result_rows.clear();
        ctx.selected_nodes.clear();
        ctx.messages.push("match returned 0 rows".into());
        return Ok(());
    }

    if let Some(with_clause) = query.with.as_ref() {
        rows = apply_with_clause(db, rows, with_clause, query_timestamp)?;
    }

    let has_aggregate = query
        .returns
        .iter()
        .any(|projection| matches!(projection.expression, Expression::Aggregate(_)));
    let has_non_aggregate = query
        .returns
        .iter()
        .any(|projection| !matches!(projection.expression, Expression::Aggregate(_)));

    if has_aggregate {
        if has_non_aggregate {
            ctx.result_rows = grouped_projection(db, &rows, &query.returns, query_timestamp)?;
            ctx.selected_nodes.clear();
        } else {
            let row = evaluate_aggregates(&rows, &query.returns)?;
            ctx.result_rows = vec![row];
            ctx.selected_nodes.clear();
        }
    } else {
        ctx.result_rows = project_rows(db, &rows, &query.returns, query_timestamp)?;
        if query.returns.len() == 1 {
            if let Expression::Field(field) = &query.returns[0].expression {
                if field.property.is_none() {
                    ctx.selected_nodes = collect_nodes(&rows, &field.alias);
                } else {
                    ctx.selected_nodes.clear();
                }
            } else {
                ctx.selected_nodes.clear();
            }
        } else {
            ctx.selected_nodes.clear();
        }
    }

    ctx.messages.push("match completed".into());
    Ok(())
}

fn group_conditions_by_alias(
    conditions: &[graphdb_core::query::Condition],
) -> HashMap<String, Vec<graphdb_core::query::Condition>> {
    let mut map: HashMap<String, Vec<graphdb_core::query::Condition>> = HashMap::new();
    for condition in conditions {
        map.entry(condition.alias.clone())
            .or_default()
            .push(condition.clone());
    }
    map
}

fn build_logical_plan(query: &SelectQuery) -> LogicalPlan {
    let mut clauses = query.match_clauses.clone();
    clauses.sort_by_key(|clause| {
        (
            clause.optional,
            Reverse(clause_selectivity(clause)),
            clause.patterns.len(),
        )
    });

    let mut planned = Vec::new();
    for (idx, clause) in clauses.iter().enumerate() {
        let selectivity = clause_selectivity(clause);
        planned.push(PlannedClause {
            order: idx,
            optional: clause.optional,
            pattern_count: clause.patterns.len(),
            aliases: clause_aliases(clause),
            selectivity_score: selectivity,
        });
    }

    let summary = PlanSummary {
        clauses: planned,
        total_clauses: clauses.len(),
        optional_clauses: clauses.iter().filter(|c| c.optional).count(),
        filter_count: query.conditions.len() + query.predicates.len(),
        has_with_clause: query.with.is_some() || query.initial_with.is_some(),
        return_count: query.returns.len(),
    };

    LogicalPlan { clauses, summary }
}

fn clause_selectivity(clause: &SelectMatchClause) -> usize {
    clause.patterns.iter().map(pattern_selectivity).sum()
}

fn pattern_selectivity(pattern: &MatchPattern) -> usize {
    match pattern {
        MatchPattern::Node(node) => {
            let label_score = if node.label.is_some() { 3 } else { 1 };
            label_score + node.properties.0.len()
        }
        MatchPattern::Relationship(rel) => {
            let label_score = if rel.relationship.label.is_some() {
                3
            } else {
                1
            };
            let property_score = rel.relationship.properties.0.len();
            let length_score = match &rel.relationship.length {
                PathLength::Exact(1) => 4,
                PathLength::Exact(n) => (4 + (*n as usize)).max(1),
                PathLength::Range { min, max } => {
                    let upper = max.unwrap_or(*min);
                    (4 + (*min as usize) + (upper as usize)).max(1)
                }
            };
            label_score + property_score + length_score
        }
        MatchPattern::Path(path) => {
            let rel = &path.pattern;
            let label_score = if rel.relationship.label.is_some() {
                3
            } else {
                1
            };
            let property_score = rel.relationship.properties.0.len();
            let length_score = match &rel.relationship.length {
                PathLength::Exact(1) => 4,
                PathLength::Exact(n) => (4 + (*n as usize)).max(1),
                PathLength::Range { min, max } => {
                    let upper = max.unwrap_or(*min);
                    (4 + (*min as usize) + (upper as usize)).max(1)
                }
            };
            label_score + property_score + length_score
        }
    }
}

fn apply_match_clause<B: StorageBackend>(
    db: &Database<B>,
    rows: Vec<QueryRow>,
    clause: &SelectMatchClause,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    let mut result = Vec::new();
    for row in rows {
        let mut expanded = vec![row.clone()];
        for pattern in &clause.patterns {
            let mut next = Vec::new();
            for partial in expanded {
                let mut produced = match pattern {
                    MatchPattern::Node(node_pattern) => {
                        extend_with_node_pattern(db, partial, node_pattern, conditions_by_alias)?
                    }
                    MatchPattern::Relationship(rel) => {
                        extend_with_relationship_pattern(db, partial, rel, conditions_by_alias)?
                    }
                    MatchPattern::Path(path_pattern) => {
                        extend_with_path_pattern(db, partial, path_pattern, conditions_by_alias)?
                    }
                };
                next.append(&mut produced);
            }
            expanded = next;
            if expanded.is_empty() {
                break;
            }
        }

        if expanded.is_empty() {
            if clause.optional {
                let mut fallback = row.clone();
                register_missing_aliases(&mut fallback, clause);
                result.push(fallback);
            }
        } else {
            result.extend(expanded);
        }
    }

    Ok(result)
}

fn apply_with_clause<B: StorageBackend>(
    db: &Database<B>,
    rows: Vec<QueryRow>,
    clause: &WithClause,
    query_timestamp: i64,
) -> Result<Vec<QueryRow>, DaemonError> {
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let mut next = QueryRow::default();
        for projection in &clause.projections {
            let label = projection_label(projection);
            match &projection.expression {
                Expression::Field(field) => match resolve_field(&row, field)? {
                    FieldValue::Node(node) => {
                        next.nodes.insert(label, Some(node));
                    }
                    FieldValue::Relationship(edge) => {
                        next.relationships.insert(label, Some(edge));
                    }
                    FieldValue::Path(path) => {
                        next.paths.insert(label, Some(path));
                    }
                    FieldValue::List(items) => {
                        next.lists.insert(label, items);
                    }
                    FieldValue::Scalar(value) => {
                        next.scalars.insert(label, value);
                    }
                },
                Expression::Function(func) => {
                    match evaluate_function(db, &row, func.as_ref(), query_timestamp)? {
                        FieldValue::Node(node) => {
                            next.nodes.insert(label, Some(node));
                        }
                        FieldValue::Relationship(edge) => {
                            next.relationships.insert(label, Some(edge));
                        }
                        FieldValue::Path(path) => {
                            next.paths.insert(label, Some(path));
                        }
                        FieldValue::List(items) => {
                            next.lists.insert(label, items);
                        }
                        FieldValue::Scalar(value) => {
                            next.scalars.insert(label, value);
                        }
                    }
                }
                Expression::BinaryOp { .. } => {
                    match evaluate_expression(db, &row, &projection.expression, query_timestamp)? {
                        FieldValue::Node(node) => {
                            next.nodes.insert(label, Some(node));
                        }
                        FieldValue::Relationship(edge) => {
                            next.relationships.insert(label, Some(edge));
                        }
                        FieldValue::Path(path) => {
                            next.paths.insert(label, Some(path));
                        }
                        FieldValue::List(items) => {
                            next.lists.insert(label, items);
                        }
                        FieldValue::Scalar(value) => {
                            next.scalars.insert(label, value);
                        }
                    }
                }
                Expression::Literal(value) => {
                    next.scalars.insert(label, value.clone());
                }
                Expression::Aggregate(_) => {
                    return Err(DaemonError::Query(
                        "aggregates are not supported in WITH clauses".into(),
                    ));
                }
            }
        }
        results.push(next);
    }
    Ok(results)
}

fn apply_predicates<B: StorageBackend>(
    db: &Database<B>,
    rows: Vec<QueryRow>,
    predicates: &[PredicateFilter],
    query_timestamp: i64,
) -> Result<Vec<QueryRow>, DaemonError> {
    if predicates.is_empty() {
        return Ok(rows);
    }

    let mut filtered = Vec::new();
    'rows: for row in rows {
        for predicate in predicates {
            let mut value = field_value_to_scalar(evaluate_function(
                db,
                &row,
                &predicate.function,
                query_timestamp,
            )?)?;
            if predicate.negated {
                value = match value {
                    Value::Boolean(flag) => Value::Boolean(!flag),
                    Value::Null => Value::Null,
                    other => {
                        return Err(DaemonError::Query(format!(
                            "predicate returned non-boolean value: {:?}",
                            other
                        )));
                    }
                };
            }

            match value {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => continue 'rows,
                other => {
                    return Err(DaemonError::Query(format!(
                        "predicate returned non-boolean value: {:?}",
                        other
                    )));
                }
            }
        }
        filtered.push(row);
    }

    Ok(filtered)
}

fn select_nodes<B: StorageBackend>(
    db: &Database<B>,
    pattern: &graphdb_core::query::NodePattern,
    conditions: &[graphdb_core::query::Condition],
) -> Result<Vec<Node>, DaemonError> {
    let mut matches = Vec::new();
    let node_ids = db.node_ids()?;
    for node_id in node_ids {
        if let Some(node_arc) = db.get_node(node_id)? {
            if node_matches_pattern(&node_arc, pattern)
                && conditions
                    .iter()
                    .all(|cond| node_satisfies_condition(&node_arc, cond))
            {
                matches.push((*node_arc).clone());
            }
        }
    }
    Ok(matches)
}

pub(crate) fn node_matches_pattern(
    node: &Arc<Node>,
    pattern: &graphdb_core::query::NodePattern,
) -> bool {
    if let Some(label) = &pattern.label {
        if !node.labels().iter().any(|l| l == label) {
            return false;
        }
    }

    for (key, value) in pattern.properties.0.iter() {
        let attribute = node.attribute(key);
        if attribute
            .map(|attr| value_equals_attribute(value, attr))
            .unwrap_or(false)
            == false
        {
            return false;
        }
    }

    true
}

fn node_satisfies_condition(node: &Arc<Node>, condition: &graphdb_core::query::Condition) -> bool {
    let attr_value = node
        .attribute(&condition.property)
        .map(attribute_to_query_value)
        .unwrap_or(Value::Null);

    match condition.operator {
        ComparisonOperator::Equals => condition
            .value
            .as_ref()
            .map(|expected| values_equal(expected, &attr_value))
            .unwrap_or(false),
        ComparisonOperator::NotEquals => condition
            .value
            .as_ref()
            .map(|expected| !values_equal(expected, &attr_value))
            .unwrap_or(false),
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            if let Some(expected) = condition.value.as_ref() {
                if let Some(ordering) = compare_query_values(&attr_value, expected) {
                    match condition.operator {
                        ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
                        ComparisonOperator::GreaterThanOrEqual => {
                            ordering == Ordering::Greater || ordering == Ordering::Equal
                        }
                        ComparisonOperator::LessThan => ordering == Ordering::Less,
                        ComparisonOperator::LessThanOrEqual => {
                            ordering == Ordering::Less || ordering == Ordering::Equal
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
        ComparisonOperator::IsNull => matches!(attr_value, Value::Null),
        ComparisonOperator::IsNotNull => !matches!(attr_value, Value::Null),
    }
}

pub(crate) fn value_equals_attribute(value: &Value, attribute: &AttributeValue) -> bool {
    match (value, attribute) {
        (Value::String(a), AttributeValue::String(b)) => a == b,
        (Value::Integer(a), AttributeValue::Integer(b)) => *a == *b,
        (Value::Float(a), AttributeValue::Float(b)) => (*a - *b).abs() < f64::EPSILON,
        (Value::Boolean(a), AttributeValue::Boolean(b)) => *a == *b,
        (Value::Null, AttributeValue::Null) => true,
        (Value::List(values), AttributeValue::List(attrs)) => {
            values.len() == attrs.len()
                && values
                    .iter()
                    .zip(attrs)
                    .all(|(v, a)| value_equals_attribute(v, a))
        }
        _ => false,
    }
}

fn collect_nodes(rows: &[QueryRow], alias: &str) -> Vec<Node> {
    rows.iter()
        .filter_map(|row| row.nodes.get(alias).and_then(|entry| entry.clone()))
        .collect()
}

fn insert_node<B: StorageBackend>(
    db: &Database<B>,
    pattern: graphdb_core::query::NodePattern,
) -> Result<NodeId, DaemonError> {
    let node = materialize_node(pattern)?;
    let node_id = node.id();
    db.insert_node(node)?;
    Ok(node_id)
}

fn insert_edge<B: StorageBackend>(
    db: &Database<B>,
    source_pattern: graphdb_core::query::NodePattern,
    edge_pattern: graphdb_core::query::EdgePattern,
    target_pattern: graphdb_core::query::NodePattern,
) -> Result<EdgeId, DaemonError> {
    let source_id = extract_node_id(source_pattern, "source id missing")?;
    let target_id = extract_node_id(target_pattern, "target id missing")?;

    let edge = materialize_edge(edge_pattern, source_id, target_id)?;
    let edge_id = edge.id();
    db.insert_edge(edge)?;
    Ok(edge_id)
}

fn project_rows<B: StorageBackend>(
    db: &Database<B>,
    rows: &[QueryRow],
    projections: &[Projection],
    query_timestamp: i64,
) -> Result<Vec<JsonValue>, DaemonError> {
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let mut map = serde_json::Map::new();
        for projection in projections {
            let value = match &projection.expression {
                Expression::Field(field) => resolve_field(row, field)?,
                Expression::Function(func) => {
                    evaluate_function(db, row, func.as_ref(), query_timestamp)?
                }
                Expression::BinaryOp { .. } => {
                    evaluate_expression(db, row, &projection.expression, query_timestamp)?
                }
                Expression::Literal(value) => FieldValue::Scalar(value.clone()),
                Expression::Aggregate(_) => {
                    return Err(DaemonError::Query(
                        "unexpected aggregate in projection".into(),
                    ));
                }
            };
            let json_value = field_value_to_json(&value)?;
            map.insert(projection_label(projection), json_value);
        }
        results.push(JsonValue::Object(map));
    }
    Ok(results)
}

fn evaluate_aggregates(
    rows: &[QueryRow],
    projections: &[Projection],
) -> Result<JsonValue, DaemonError> {
    let mut map = serde_json::Map::new();
    for projection in projections {
        let agg = match &projection.expression {
            Expression::Aggregate(expr) => expr,
            Expression::Field(_) | Expression::Function(_) | Expression::Literal(_) => {
                return Err(DaemonError::Query(
                    "non-aggregate projection encountered in aggregate evaluation".into(),
                ));
            }
            Expression::BinaryOp { .. } => {
                return Err(DaemonError::Query(
                    "non-aggregate projection encountered in aggregate evaluation".into(),
                ));
            }
        };
        let value = compute_aggregate(rows, agg)?;
        map.insert(projection_label(projection), value_to_json(&value));
    }
    Ok(JsonValue::Object(map))
}

fn grouped_projection<B: StorageBackend>(
    db: &Database<B>,
    rows: &[QueryRow],
    projections: &[Projection],
    query_timestamp: i64,
) -> Result<Vec<JsonValue>, DaemonError> {
    let mut group_fields = Vec::new();
    let mut aggregate_fields = Vec::new();
    for projection in projections {
        match projection.expression {
            Expression::Field(_) | Expression::Function(_) | Expression::Literal(_) => {
                group_fields.push(projection)
            }
            Expression::BinaryOp { .. } => group_fields.push(projection),
            Expression::Aggregate(_) => aggregate_fields.push(projection),
        }
    }

    if group_fields.is_empty() {
        let row = evaluate_aggregates(rows, projections)?;
        return Ok(vec![row]);
    }

    let mut groups: HashMap<String, GroupState> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        let mut key_components = Vec::new();
        let mut key_values = HashMap::new();
        for projection in &group_fields {
            let label = projection_label(projection);
            let value = match &projection.expression {
                Expression::Field(field) => {
                    let field_value = resolve_field(row, field)?;
                    field_value_to_scalar(field_value)?
                }
                Expression::Function(func) => {
                    let value = evaluate_function(db, row, func.as_ref(), query_timestamp)?;
                    field_value_to_scalar(value)?
                }
                Expression::Literal(value) => value.clone(),
                Expression::BinaryOp { .. } => {
                    let value =
                        evaluate_expression(db, row, &projection.expression, query_timestamp)?;
                    field_value_to_scalar(value)?
                }
                Expression::Aggregate(_) => unreachable!(),
            };
            key_components.push(value_to_json(&value));
            key_values.insert(label, value);
        }
        let key = JsonValue::Array(key_components).to_string();
        let entry = groups.entry(key).or_insert_with(|| GroupState {
            key_values: key_values.clone(),
            row_indices: Vec::new(),
        });
        entry.row_indices.push(index);
    }

    let mut results = Vec::new();
    for state in groups.values() {
        let subset: Vec<QueryRow> = state.row_indices.iter().map(|&i| rows[i].clone()).collect();
        let mut map = serde_json::Map::new();
        for projection in projections {
            let label = projection_label(projection);
            match &projection.expression {
                Expression::Field(_) | Expression::Function(_) | Expression::Literal(_) => {
                    let value = state
                        .key_values
                        .get(&label)
                        .cloned()
                        .ok_or_else(|| DaemonError::Query("group key missing".into()))?;
                    map.insert(label, value_to_json(&value));
                }
                Expression::Aggregate(expr) => {
                    let value = compute_aggregate(&subset, expr)?;
                    map.insert(label, value_to_json(&value));
                }
                Expression::BinaryOp { .. } => {
                    let value = state
                        .key_values
                        .get(&label)
                        .cloned()
                        .ok_or_else(|| DaemonError::Query("group key missing".into()))?;
                    map.insert(label, value_to_json(&value));
                }
            }
        }
        results.push(JsonValue::Object(map));
    }

    Ok(results)
}

fn projection_label(projection: &Projection) -> String {
    if let Some(alias) = &projection.alias {
        return alias.clone();
    }

    match &projection.expression {
        Expression::Field(field) => field_label(field),
        Expression::Aggregate(agg) => aggregate_label(agg),
        Expression::Function(func) => function_label(func.as_ref()),
        Expression::Literal(value) => literal_label(value),
        Expression::BinaryOp { .. } => expression_label(&projection.expression),
    }
}

fn field_label(field: &FieldReference) -> String {
    match &field.property {
        Some(property) => format!("{}.{}", field.alias, property),
        None => field.alias.clone(),
    }
}

fn aggregate_label(expr: &AggregateExpression) -> String {
    let func = match expr.function {
        AggregateFunction::Avg => "avg",
        AggregateFunction::Collect => "collect",
        AggregateFunction::Count => "count",
        AggregateFunction::CountAll => "count",
        AggregateFunction::Max => "max",
        AggregateFunction::Min => "min",
        AggregateFunction::PercentileCont => "percentileCont",
        AggregateFunction::PercentileDisc => "percentileDisc",
        AggregateFunction::StDev => "stDev",
        AggregateFunction::StDevP => "stDevP",
        AggregateFunction::Sum => "sum",
    };

    let target = expr
        .target
        .as_ref()
        .map(field_label)
        .unwrap_or_else(|| "*".into());

    if let Some(p) = expr.percentile {
        format!("{}({}, {:.4})", func, target, p)
    } else {
        format!("{}({})", func, target)
    }
}

fn function_label(func: &FunctionExpression) -> String {
    match func {
        FunctionExpression::ListPredicate(spec) => {
            let func_name = match spec.kind {
                ListPredicateKind::All => "all",
                ListPredicateKind::Any => "any",
                ListPredicateKind::None => "none",
                ListPredicateKind::Single => "single",
            };
            format!(
                "{}({} IN {} WHERE {})",
                func_name,
                spec.variable,
                list_expression_label(&spec.list),
                list_predicate_label(&spec.variable, &spec.predicate)
            )
        }
        FunctionExpression::IsEmpty(operand) => {
            format!("isEmpty({})", value_operand_label(operand))
        }
        FunctionExpression::Exists(spec) => {
            format!("exists({})", exists_pattern_label(&spec.pattern))
        }
        FunctionExpression::Scalar(spec) => scalar_function_label(spec),
    }
}

fn scalar_function_label(func: &ScalarFunction) -> String {
    match func {
        ScalarFunction::Nodes(expr) => format!("nodes({})", expression_label(expr)),
        ScalarFunction::Relationships(expr) => {
            format!("relationships({})", expression_label(expr))
        }
        ScalarFunction::Keys(expr) => format!("keys({})", expression_label(expr)),
        ScalarFunction::Labels(expr) => format!("labels({})", expression_label(expr)),
        ScalarFunction::Range { start, end, step } => {
            if let Some(step_expr) = step {
                format!(
                    "range({},{},{})",
                    expression_label(start),
                    expression_label(end),
                    expression_label(step_expr)
                )
            } else {
                format!(
                    "range({},{})",
                    expression_label(start),
                    expression_label(end)
                )
            }
        }
        ScalarFunction::Reverse(expr) => format!("reverse({})", expression_label(expr)),
        ScalarFunction::Tail(expr) => format!("tail({})", expression_label(expr)),
        ScalarFunction::ToBooleanList(expr) => {
            format!("toBooleanList({})", expression_label(expr))
        }
        ScalarFunction::ToFloatList(expr) => {
            format!("toFloatList({})", expression_label(expr))
        }
        ScalarFunction::ToIntegerList(expr) => {
            format!("toIntegerList({})", expression_label(expr))
        }
        ScalarFunction::ToStringList(expr) => {
            format!("toStringList({})", expression_label(expr))
        }
        ScalarFunction::Coalesce(args) => {
            let inner = args
                .iter()
                .map(expression_label)
                .collect::<Vec<_>>()
                .join(", ");
            format!("coalesce({inner})")
        }
        ScalarFunction::StartNode(field) => format!("startNode({})", field_label(field)),
        ScalarFunction::EndNode(field) => format!("endNode({})", field_label(field)),
        ScalarFunction::Head(expr) => format!("head({})", expression_label(expr)),
        ScalarFunction::Last(expr) => format!("last({})", expression_label(expr)),
        ScalarFunction::Id(field) => format!("id({})", field_label(field)),
        ScalarFunction::Properties(expr) => format!("properties({})", expression_label(expr)),
        ScalarFunction::RandomUuid => "randomUUID()".into(),
        ScalarFunction::Size(expr) => format!("size({})", expression_label(expr)),
        ScalarFunction::Length(expr) => format!("length({})", expression_label(expr)),
        ScalarFunction::Timestamp => "timestamp()".into(),
        ScalarFunction::UserDefined(call) => {
            let args = call
                .arguments
                .iter()
                .map(expression_label)
                .collect::<Vec<_>>();
            format!("{}({})", call.name, args.join(", "))
        }
        ScalarFunction::Reduce {
            accumulator,
            variable,
            ..
        } => format!("reduce({} = …, {} IN …)", accumulator, variable),
        ScalarFunction::ToBoolean {
            expr,
            null_on_unsupported,
        } => {
            if *null_on_unsupported {
                format!("toBooleanOrNull({})", expression_label(expr))
            } else {
                format!("toBoolean({})", expression_label(expr))
            }
        }
        ScalarFunction::ToFloat {
            expr,
            null_on_unsupported,
        } => {
            if *null_on_unsupported {
                format!("toFloatOrNull({})", expression_label(expr))
            } else {
                format!("toFloat({})", expression_label(expr))
            }
        }
        ScalarFunction::ToInteger {
            expr,
            null_on_unsupported,
        } => {
            if *null_on_unsupported {
                format!("toIntegerOrNull({})", expression_label(expr))
            } else {
                format!("toInteger({})", expression_label(expr))
            }
        }
        ScalarFunction::ToString {
            expr,
            null_on_unsupported,
        } => {
            if *null_on_unsupported {
                format!("toStringOrNull({})", expression_label(expr))
            } else {
                format!("toString({})", expression_label(expr))
            }
        }
        ScalarFunction::Type(field) => format!("type({})", field_label(field)),
    }
}

fn expression_label(expr: &Expression) -> String {
    match expr {
        Expression::Field(field) => field_label(field),
        Expression::Function(func) => function_label(func.as_ref()),
        Expression::Literal(value) => value_to_json(value).to_string(),
        Expression::BinaryOp {
            left,
            operator,
            right,
        } => {
            let op_str = match operator {
                BinaryOperator::Add => "+",
                BinaryOperator::Subtract => "-",
            };
            format!(
                "({} {} {})",
                expression_label(left),
                op_str,
                expression_label(right)
            )
        }
        Expression::Aggregate(_) => "<agg>".into(),
    }
}

fn literal_label(value: &Value) -> String {
    value_to_json(value).to_string()
}

fn list_expression_label(expr: &ListExpression) -> String {
    match expr {
        ListExpression::Field(field) => field_label(field),
        ListExpression::Literal(value) => value_to_json(value).to_string(),
    }
}

fn value_operand_label(operand: &ValueOperand) -> String {
    match operand {
        ValueOperand::Field(field) => field_label(field),
        ValueOperand::Literal(value) => value_to_json(value).to_string(),
    }
}

fn list_predicate_label(variable: &str, predicate: &ListPredicate) -> String {
    match predicate {
        ListPredicate::Comparison { operator, value } => format!(
            "{} {} {}",
            variable,
            operator_symbol(operator),
            value_to_json(value)
        ),
        ListPredicate::IsNull { negated } => {
            if *negated {
                format!("{} IS NOT NULL", variable)
            } else {
                format!("{} IS NULL", variable)
            }
        }
    }
}

fn operator_symbol(op: &ComparisonOperator) -> &'static str {
    match op {
        ComparisonOperator::Equals => "=",
        ComparisonOperator::NotEquals => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::IsNull => "IS",
        ComparisonOperator::IsNotNull => "IS NOT",
    }
}

fn exists_pattern_label(pattern: &RelationshipMatch) -> String {
    format!(
        "{}{}{}",
        node_pattern_label(&pattern.left),
        relationship_pattern_label(&pattern.relationship),
        node_pattern_label(&pattern.right)
    )
}

fn node_pattern_label(node: &NodePattern) -> String {
    let mut parts = String::from("(");
    if let Some(alias) = &node.alias {
        parts.push_str(alias);
    }
    if let Some(label) = &node.label {
        parts.push(':');
        parts.push_str(label);
    }
    if !node.properties.0.is_empty() {
        if node.alias.is_some() || node.label.is_some() {
            parts.push(' ');
        }
        parts.push_str(&properties_label(&node.properties));
    }
    parts.push(')');
    parts
}

fn relationship_pattern_label(pattern: &RelationshipPattern) -> String {
    let mut inner = String::new();
    if let Some(alias) = &pattern.alias {
        inner.push_str(alias);
    }
    if let Some(label) = &pattern.label {
        if !inner.is_empty() {
            inner.push(':');
        } else {
            inner.push(':');
        }
        inner.push_str(label);
    }
    if !pattern.properties.0.is_empty() {
        if !inner.is_empty() {
            inner.push(' ');
        }
        inner.push_str(&properties_label(&pattern.properties));
    }
    let length = path_length_label(&pattern.length);
    if !length.is_empty() {
        if !inner.is_empty() {
            inner.push_str(&length);
        } else {
            inner.push_str(&length);
        }
    }

    match pattern.direction {
        RelationshipDirection::Outbound => format!("-[{}]->", inner),
        RelationshipDirection::Inbound => format!("<-[{}]-", inner),
        RelationshipDirection::Undirected => format!("-[{}]-", inner),
    }
}

fn path_length_label(length: &PathLength) -> String {
    match length {
        PathLength::Exact(1) => String::new(),
        PathLength::Exact(n) => format!("*{n}"),
        PathLength::Range { min, max } => match max {
            Some(value) => format!("*{min}..{value}"),
            None => format!("*{min}.."),
        },
    }
}

fn properties_label(props: &Properties) -> String {
    if props.0.is_empty() {
        return String::new();
    }
    let mut entries: Vec<_> = props.0.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let body = entries
        .into_iter()
        .map(|(key, value)| format!("{}: {}", key, value_to_json(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{}}}", body)
}

fn resolve_field(row: &QueryRow, field: &FieldReference) -> Result<FieldValue, DaemonError> {
    if let Some(value) = row.scalars.get(&field.alias) {
        if field.property.is_some() {
            return Err(DaemonError::Query(format!(
                "alias '{}' does not support property access",
                field.alias
            )));
        }
        return Ok(FieldValue::Scalar(value.clone()));
    }

    if let Some(items) = row.lists.get(&field.alias) {
        if field.property.is_some() {
            return Err(DaemonError::Query(
                "list aliases do not support property access".into(),
            ));
        }
        return Ok(FieldValue::List(items.clone()));
    }

    if let Some(entry) = row.nodes.get(&field.alias) {
        return match entry {
            Some(node) => {
                if let Some(property) = &field.property {
                    let attr = node.attribute(property);
                    let value = attr.map(attribute_to_query_value).unwrap_or(Value::Null);
                    Ok(FieldValue::Scalar(value))
                } else {
                    Ok(FieldValue::Node(node.clone()))
                }
            }
            None => Ok(FieldValue::Scalar(Value::Null)),
        };
    }

    if let Some(entry) = row.paths.get(&field.alias) {
        return match entry {
            Some(path) => {
                if field.property.is_some() {
                    Err(DaemonError::Query(
                        "path aliases do not support property access".into(),
                    ))
                } else {
                    Ok(FieldValue::Path(path.clone()))
                }
            }
            None => Ok(FieldValue::Scalar(Value::Null)),
        };
    }

    match row.relationships.get(&field.alias) {
        Some(Some(edge)) => {
            if let Some(property) = &field.property {
                let attr = edge.attribute(property);
                let value = attr.map(attribute_to_query_value).unwrap_or(Value::Null);
                Ok(FieldValue::Scalar(value))
            } else {
                Ok(FieldValue::Relationship(edge.clone()))
            }
        }
        Some(None) => Ok(FieldValue::Scalar(Value::Null)),
        None => Err(DaemonError::Query(format!(
            "unknown alias '{}'",
            field.alias
        ))),
    }
}

pub(crate) fn field_value_to_scalar(value: FieldValue) -> Result<Value, DaemonError> {
    match value {
        FieldValue::Scalar(val) => Ok(val),
        FieldValue::Node(_) => Err(DaemonError::Query(
            "grouping by node aliases is not supported".into(),
        )),
        FieldValue::Relationship(_) => Err(DaemonError::Query(
            "grouping by relationship aliases is not supported".into(),
        )),
        FieldValue::Path(_) => Err(DaemonError::Query(
            "grouping by path aliases is not supported".into(),
        )),
        FieldValue::List(_) => Err(DaemonError::Query(
            "grouping by list aliases is not supported".into(),
        )),
    }
}

fn evaluate_function<B: StorageBackend>(
    db: &Database<B>,
    row: &QueryRow,
    func: &FunctionExpression,
    query_timestamp: i64,
) -> Result<FieldValue, DaemonError> {
    match func {
        FunctionExpression::ListPredicate(spec) => {
            let value = evaluate_list_predicate_function(row, spec)?;
            Ok(FieldValue::Scalar(value))
        }
        FunctionExpression::IsEmpty(operand) => {
            let value = value_operand_value(row, operand)?;
            let result = match value {
                Value::Null => Value::Null,
                Value::String(s) => Value::Boolean(s.is_empty()),
                Value::List(values) => Value::Boolean(values.is_empty()),
                _ => {
                    return Err(DaemonError::Query(
                        "isEmpty expects STRING or LIST input".into(),
                    ));
                }
            };
            Ok(FieldValue::Scalar(result))
        }
        FunctionExpression::Exists(spec) => {
            let value = evaluate_exists_function(db, row, spec)?;
            Ok(FieldValue::Scalar(value))
        }
        FunctionExpression::Scalar(spec) => {
            evaluate_scalar_function(db, row, spec, query_timestamp)
        }
    }
}

fn evaluate_expression<B: StorageBackend>(
    db: &Database<B>,
    row: &QueryRow,
    expr: &Expression,
    query_timestamp: i64,
) -> Result<FieldValue, DaemonError> {
    match expr {
        Expression::Field(field) => resolve_field(row, field),
        Expression::Function(func) => evaluate_function(db, row, func.as_ref(), query_timestamp),
        Expression::Literal(value) => Ok(FieldValue::Scalar(value.clone())),
        Expression::BinaryOp {
            left,
            operator,
            right,
        } => {
            let left_value = evaluate_expression_to_scalar(db, row, left, query_timestamp)?;
            let right_value = evaluate_expression_to_scalar(db, row, right, query_timestamp)?;
            let result = apply_binary_operator(*operator, left_value, right_value)?;
            Ok(FieldValue::Scalar(result))
        }
        Expression::Aggregate(_) => Err(DaemonError::Query(
            "aggregates are not allowed in this context".into(),
        )),
    }
}

fn evaluate_expression_to_scalar<B: StorageBackend>(
    db: &Database<B>,
    row: &QueryRow,
    expr: &Expression,
    query_timestamp: i64,
) -> Result<Value, DaemonError> {
    let value = evaluate_expression(db, row, expr, query_timestamp)?;
    field_value_to_scalar(value)
}

fn field_value_is_null(value: &FieldValue) -> bool {
    matches!(value, FieldValue::Scalar(Value::Null))
}

fn resolve_relationship_from_field(
    row: &QueryRow,
    field: &FieldReference,
) -> Result<Option<Edge>, DaemonError> {
    if field.property.is_some() {
        return Err(DaemonError::Query(
            "relationship alias does not support property dereference here".into(),
        ));
    }

    match row.relationships.get(&field.alias) {
        Some(Some(edge)) => Ok(Some(edge.clone())),
        Some(None) => Ok(None),
        None => Err(DaemonError::Query(format!(
            "unknown relationship alias '{}'",
            field.alias
        ))),
    }
}

fn map_from_attributes(attrs: &HashMap<String, AttributeValue>) -> Value {
    let mut map = HashMap::new();
    for (key, value) in attrs.iter() {
        map.insert(key.clone(), attribute_to_query_value(value));
    }
    Value::Map(map)
}

fn apply_binary_operator(
    operator: BinaryOperator,
    left: Value,
    right: Value,
) -> Result<Value, DaemonError> {
    match operator {
        BinaryOperator::Add => add_values(left, right),
        BinaryOperator::Subtract => subtract_values(left, right),
    }
}

fn add_values(left: Value, right: Value) -> Result<Value, DaemonError> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => match a.checked_add(b) {
            Some(sum) => Ok(Value::Integer(sum)),
            None => Ok(Value::Float(a as f64 + b as f64)),
        },
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + b as f64)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::String(mut a), Value::String(b)) => {
            a.push_str(&b);
            Ok(Value::String(a))
        }
        _ => Err(DaemonError::Query(
            "addition requires numeric operands".into(),
        )),
    }
}

fn subtract_values(left: Value, right: Value) -> Result<Value, DaemonError> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => match a.checked_sub(b) {
            Some(diff) => Ok(Value::Integer(diff)),
            None => Ok(Value::Float(a as f64 - b as f64)),
        },
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - b as f64)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        _ => Err(DaemonError::Query(
            "subtraction requires numeric operands".into(),
        )),
    }
}

fn call_standard_function(name: &str, args: Vec<FieldValue>) -> Result<FieldValue, DaemonError> {
    functions::registry()
        .call(name, &args)
        .map_err(DaemonError::from)
}

fn call_standard_function_scalar(name: &str, args: Vec<FieldValue>) -> Result<Value, DaemonError> {
    let value = call_standard_function(name, args)?;
    field_value_to_scalar(value)
}

fn call_standard_function_scalar_unary(name: &str, arg: Value) -> Result<Value, DaemonError> {
    call_standard_function_scalar(name, vec![FieldValue::Scalar(arg)])
}

fn call_standard_function_with_context<B: StorageBackend>(
    name: &str,
    args: Vec<FieldValue>,
    ctx: &mut StandardFunctionContext<'_, B>,
) -> Result<FieldValue, DaemonError> {
    functions::registry()
        .call_with_context(name, &args, Some(ctx))
        .map_err(DaemonError::from)
}

struct StandardFunctionContext<'a, B: StorageBackend> {
    db: &'a Database<B>,
    row: QueryRow,
    query_timestamp: i64,
    expressions: HashMap<ExpressionHandle, Expression>,
    next_handle: u32,
}

impl<'a, B: StorageBackend> StandardFunctionContext<'a, B> {
    fn new(db: &'a Database<B>, row: &QueryRow, query_timestamp: i64) -> Self {
        Self {
            db,
            row: row.clone(),
            query_timestamp,
            expressions: HashMap::new(),
            next_handle: 1,
        }
    }

    fn register_expression(&mut self, expr: &Expression) -> ExpressionHandle {
        let handle = ExpressionHandle(self.next_handle);
        self.next_handle = self.next_handle.wrapping_add(1);
        self.expressions.insert(handle, expr.clone());
        handle
    }
}

impl<'a, B: StorageBackend> FunctionContext for StandardFunctionContext<'a, B> {
    fn query_timestamp(&self) -> i64 {
        self.query_timestamp
    }

    fn bind_alias(&mut self, alias: &str, value: FieldValue) -> Result<(), FunctionError> {
        bind_reduce_alias(&mut self.row, alias, &value);
        Ok(())
    }

    fn unbind_alias(&mut self, alias: &str) -> Result<(), FunctionError> {
        clear_alias_bindings(&mut self.row, alias);
        Ok(())
    }

    fn evaluate_expression(
        &mut self,
        handle: ExpressionHandle,
    ) -> Result<FieldValue, FunctionError> {
        let expr = self.expressions.get(&handle).ok_or_else(|| {
            FunctionError::execution(format!("invalid expression handle {}", handle.0))
        })?;
        evaluate_expression(self.db, &self.row, expr, self.query_timestamp)
            .map_err(|err| FunctionError::execution(err.to_string()))
    }
}

struct StandardProcedureContext<'a, B: StorageBackend> {
    _db: &'a Database<B>,
}

impl<'a, B: StorageBackend> ProcedureContext for StandardProcedureContext<'a, B> {}
fn evaluate_scalar_function<B: StorageBackend>(
    db: &Database<B>,
    row: &QueryRow,
    func: &ScalarFunction,
    query_timestamp: i64,
) -> Result<FieldValue, DaemonError> {
    match func {
        ScalarFunction::Coalesce(expressions) => {
            for expr in expressions {
                let value = evaluate_expression(db, row, expr, query_timestamp)?;
                if !field_value_is_null(&value) {
                    return Ok(value);
                }
            }
            Ok(FieldValue::Scalar(Value::Null))
        }
        ScalarFunction::Keys(expr) => {
            let value = evaluate_expression(db, row, expr, query_timestamp)?;
            let keys = match value {
                FieldValue::Node(node) => collect_sorted_keys(node.attributes().keys().cloned()),
                FieldValue::Relationship(edge) => {
                    collect_sorted_keys(edge.attributes().keys().cloned())
                }
                FieldValue::Scalar(Value::Map(map)) => collect_sorted_keys(map.keys().cloned()),
                FieldValue::Scalar(Value::Null) => return Ok(FieldValue::Scalar(Value::Null)),
                _ => {
                    return Err(DaemonError::Query(
                        "keys() expects node, relationship, or map input".into(),
                    ));
                }
            };
            Ok(FieldValue::Scalar(keys))
        }
        ScalarFunction::Labels(expr) => {
            let value = evaluate_expression(db, row, expr, query_timestamp)?;
            match value {
                FieldValue::Node(node) => {
                    let mut labels = node.labels().to_vec();
                    labels.sort();
                    let values = labels.into_iter().map(Value::String).collect::<Vec<_>>();
                    Ok(FieldValue::Scalar(Value::List(values)))
                }
                FieldValue::Scalar(Value::Null) => Ok(FieldValue::Scalar(Value::Null)),
                _ => Err(DaemonError::Query("labels() expects a node".into())),
            }
        }
        ScalarFunction::Nodes(expr) => {
            let value = evaluate_expression(db, row, expr, query_timestamp)?;
            match value {
                FieldValue::Scalar(Value::Null) => Ok(FieldValue::Scalar(Value::Null)),
                FieldValue::Path(path) => {
                    let items = path.nodes.into_iter().map(FieldValue::Node).collect();
                    Ok(FieldValue::List(items))
                }
                other => Err(DaemonError::Query(format!(
                    "nodes() expects a path, received {}",
                    field_value_type(&other)
                ))),
            }
        }
        ScalarFunction::Relationships(expr) => {
            let value = evaluate_expression(db, row, expr, query_timestamp)?;
            match value {
                FieldValue::Scalar(Value::Null) => Ok(FieldValue::Scalar(Value::Null)),
                FieldValue::Path(path) => {
                    let items = path
                        .edges
                        .into_iter()
                        .map(FieldValue::Relationship)
                        .collect();
                    Ok(FieldValue::List(items))
                }
                other => Err(DaemonError::Query(format!(
                    "relationships() expects a path, received {}",
                    field_value_type(&other)
                ))),
            }
        }
        ScalarFunction::Range { start, end, step } => {
            let mut args: Vec<FieldValue> = Vec::with_capacity(3);
            args.push(FieldValue::Scalar(evaluate_expression_to_scalar(
                db,
                row,
                start,
                query_timestamp,
            )?));
            args.push(FieldValue::Scalar(evaluate_expression_to_scalar(
                db,
                row,
                end,
                query_timestamp,
            )?));
            if let Some(expr) = step {
                args.push(FieldValue::Scalar(evaluate_expression_to_scalar(
                    db,
                    row,
                    expr,
                    query_timestamp,
                )?));
            }
            let value = call_standard_function("range", args)?;
            Ok(value)
        }
        ScalarFunction::Reverse(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("reverse", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Tail(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("tail", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToBooleanList(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("toBooleanList", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToFloatList(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("toFloatList", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToIntegerList(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("toIntegerList", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToStringList(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("toStringList", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::StartNode(field) => {
            let relationship = resolve_relationship_from_field(row, field)?;
            match relationship {
                Some(edge) => {
                    let node = db
                        .get_node(edge.source())?
                        .ok_or_else(|| DaemonError::Query("start node not found".into()))?;
                    Ok(FieldValue::Node((*node).clone()))
                }
                None => Ok(FieldValue::Scalar(Value::Null)),
            }
        }
        ScalarFunction::EndNode(field) => {
            let relationship = resolve_relationship_from_field(row, field)?;
            match relationship {
                Some(edge) => {
                    let node = db
                        .get_node(edge.target())?
                        .ok_or_else(|| DaemonError::Query("end node not found".into()))?;
                    Ok(FieldValue::Node((*node).clone()))
                }
                None => Ok(FieldValue::Scalar(Value::Null)),
            }
        }
        ScalarFunction::Head(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("head", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Last(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("last", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Id(field) => match resolve_field(row, field)? {
            FieldValue::Node(node) => Ok(FieldValue::Scalar(Value::String(node.id().to_string()))),
            FieldValue::Relationship(edge) => {
                Ok(FieldValue::Scalar(Value::String(edge.id().to_string())))
            }
            FieldValue::Scalar(Value::Null) => Ok(FieldValue::Scalar(Value::Null)),
            _ => Err(DaemonError::Query(
                "id() expects node or relationship alias".into(),
            )),
        },
        ScalarFunction::Properties(expr) => {
            let value = evaluate_expression(db, row, expr, query_timestamp)?;
            let map = match value {
                FieldValue::Node(node) => map_from_attributes(node.attributes()),
                FieldValue::Relationship(edge) => map_from_attributes(edge.attributes()),
                FieldValue::Scalar(Value::Map(map)) => Value::Map(map.clone()),
                FieldValue::Scalar(Value::Null) => Value::Null,
                FieldValue::Scalar(_) => {
                    return Err(DaemonError::Query(
                        "properties() expects node, relationship, or map".into(),
                    ));
                }
                FieldValue::Path(_) | FieldValue::List(_) => {
                    return Err(DaemonError::Query(
                        "properties() expects node, relationship, or map".into(),
                    ));
                }
            };
            Ok(FieldValue::Scalar(map))
        }
        ScalarFunction::RandomUuid => {
            let value = call_standard_function_scalar("randomUUID", Vec::new())?;
            Ok(FieldValue::Scalar(value))
        }
        ScalarFunction::Size(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("size", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Length(expr) => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let result = call_standard_function_scalar_unary("length", value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Timestamp => {
            let value = call_standard_function_scalar(
                "timestamp",
                vec![FieldValue::Scalar(Value::Integer(query_timestamp))],
            )?;
            Ok(FieldValue::Scalar(value))
        }
        ScalarFunction::UserDefined(call) => {
            let mut evaluated_args = Vec::with_capacity(call.arguments.len());
            for argument in &call.arguments {
                evaluated_args.push(evaluate_expression(db, row, argument, query_timestamp)?);
            }
            let value = functions::registry()
                .call(&call.name, &evaluated_args)
                .map_err(DaemonError::from)?;
            Ok(value)
        }
        ScalarFunction::Reduce {
            accumulator,
            initial,
            variable,
            list,
            expression,
        } => {
            let mut ctx = StandardFunctionContext::new(db, row, query_timestamp);
            let initial_handle = ctx.register_expression(initial);
            let list_handle = ctx.register_expression(list);
            let expression_handle = ctx.register_expression(expression);
            let mut args = Vec::with_capacity(5);
            args.push(FieldValue::Scalar(Value::String(accumulator.clone())));
            args.push(FieldValue::Scalar(Value::String(variable.clone())));
            args.push(FieldValue::Scalar(Value::Integer(initial_handle.0 as i64)));
            args.push(FieldValue::Scalar(Value::Integer(list_handle.0 as i64)));
            args.push(FieldValue::Scalar(Value::Integer(
                expression_handle.0 as i64,
            )));
            call_standard_function_with_context("reduce", args, &mut ctx)
        }
        ScalarFunction::ToBoolean {
            expr,
            null_on_unsupported,
        } => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let name = if *null_on_unsupported {
                "toBooleanOrNull"
            } else {
                "toBoolean"
            };
            let result = call_standard_function_scalar_unary(name, value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToFloat {
            expr,
            null_on_unsupported,
        } => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let name = if *null_on_unsupported {
                "toFloatOrNull"
            } else {
                "toFloat"
            };
            let result = call_standard_function_scalar_unary(name, value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToInteger {
            expr,
            null_on_unsupported,
        } => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let name = if *null_on_unsupported {
                "toIntegerOrNull"
            } else {
                "toInteger"
            };
            let result = call_standard_function_scalar_unary(name, value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::ToString {
            expr,
            null_on_unsupported,
        } => {
            let value = evaluate_expression_to_scalar(db, row, expr, query_timestamp)?;
            let name = if *null_on_unsupported {
                "toStringOrNull"
            } else {
                "toString"
            };
            let result = call_standard_function_scalar_unary(name, value)?;
            Ok(FieldValue::Scalar(result))
        }
        ScalarFunction::Type(field) => {
            let relationship = resolve_relationship_from_field(row, field)?;
            match relationship {
                Some(edge) => {
                    let value = edge
                        .attribute("__label")
                        .and_then(|attr| match attr {
                            AttributeValue::String(label) => Some(Value::String(label.clone())),
                            _ => None,
                        })
                        .unwrap_or(Value::Null);
                    Ok(FieldValue::Scalar(value))
                }
                None => Ok(FieldValue::Scalar(Value::Null)),
            }
        }
    }
}

fn evaluate_list_predicate_function(
    row: &QueryRow,
    spec: &ListPredicateFunction,
) -> Result<Value, DaemonError> {
    let list_value = list_expression_value(row, &spec.list)?;
    let list = match list_value {
        Value::Null => return Ok(Value::Null),
        Value::List(_) => list_value,
        other => {
            return Err(DaemonError::Query(format!(
                "{} expects LIST input, received {other:?}",
                list_predicate_function_name(spec.kind)
            )));
        }
    };

    let predicate_value = serialize_list_predicate(&spec.predicate);
    let args = vec![
        FieldValue::Scalar(list),
        FieldValue::Scalar(predicate_value),
    ];
    let result = call_standard_function(list_predicate_function_name(spec.kind), args)?;
    field_value_to_scalar(result)
}

fn list_expression_value(row: &QueryRow, expr: &ListExpression) -> Result<Value, DaemonError> {
    match expr {
        ListExpression::Field(field) => {
            let value = resolve_field(row, field)?;
            field_value_to_scalar(value)
        }
        ListExpression::Literal(value) => Ok(value.clone()),
    }
}

fn list_predicate_function_name(kind: ListPredicateKind) -> &'static str {
    match kind {
        ListPredicateKind::All => "all",
        ListPredicateKind::Any => "any",
        ListPredicateKind::None => "none",
        ListPredicateKind::Single => "single",
    }
}

fn serialize_list_predicate(predicate: &ListPredicate) -> Value {
    match predicate {
        ListPredicate::Comparison { operator, value } => {
            let mut map = HashMap::new();
            map.insert("type".into(), Value::String("comparison".into()));
            map.insert(
                "operator".into(),
                Value::String(operator_symbol(operator).into()),
            );
            map.insert("value".into(), value.clone());
            Value::Map(map)
        }
        ListPredicate::IsNull { negated } => {
            let mut map = HashMap::new();
            map.insert("type".into(), Value::String("isNull".into()));
            map.insert("negated".into(), Value::Boolean(*negated));
            Value::Map(map)
        }
    }
}

fn value_operand_value(row: &QueryRow, operand: &ValueOperand) -> Result<Value, DaemonError> {
    match operand {
        ValueOperand::Field(field) => {
            let value = resolve_field(row, field)?;
            field_value_to_scalar(value)
        }
        ValueOperand::Literal(value) => Ok(value.clone()),
    }
}

fn clear_alias_bindings(row: &mut QueryRow, alias: &str) {
    row.nodes.remove(alias);
    row.relationships.remove(alias);
    row.paths.remove(alias);
    row.lists.remove(alias);
    row.scalars.remove(alias);
}

fn bind_reduce_alias(row: &mut QueryRow, alias: &str, value: &FieldValue) {
    clear_alias_bindings(row, alias);
    match value {
        FieldValue::Node(node) => {
            row.nodes.insert(alias.to_string(), Some(node.clone()));
        }
        FieldValue::Relationship(edge) => {
            row.relationships
                .insert(alias.to_string(), Some(edge.clone()));
        }
        FieldValue::Path(path) => {
            row.paths.insert(alias.to_string(), Some(path.clone()));
        }
        FieldValue::List(items) => {
            row.lists.insert(alias.to_string(), items.clone());
        }
        FieldValue::Scalar(val) => {
            row.scalars.insert(alias.to_string(), val.clone());
        }
    }
}

fn field_value_type(value: &FieldValue) -> &'static str {
    match value {
        FieldValue::Scalar(_) => "scalar",
        FieldValue::Node(_) => "node",
        FieldValue::Relationship(_) => "relationship",
        FieldValue::Path(_) => "path",
        FieldValue::List(_) => "list",
    }
}

fn evaluate_exists_function<B: StorageBackend>(
    db: &Database<B>,
    row: &QueryRow,
    func: &ExistsFunction,
) -> Result<Value, DaemonError> {
    let alias = func
        .pattern
        .left
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("exists() requires a start alias".into()))?;

    match row.nodes.get(alias.as_str()) {
        Some(Some(node)) => {
            let node_arc = Arc::new(node.clone());
            if !node_matches_pattern(&node_arc, &func.pattern.left) {
                return Ok(Value::Boolean(false));
            }
            let reachable = reachable_nodes_from(
                db,
                node,
                &func.pattern.relationship,
                &func.pattern.right,
                &[],
            )?;
            Ok(Value::Boolean(!reachable.is_empty()))
        }
        Some(None) => Ok(Value::Null),
        None => Err(DaemonError::Query(format!(
            "alias '{}' not bound in exists()",
            alias
        ))),
    }
}

fn attribute_to_query_value(attr: &AttributeValue) -> Value {
    match attr {
        AttributeValue::String(s) => Value::String(s.clone()),
        AttributeValue::Integer(i) => Value::Integer(*i),
        AttributeValue::Float(f) => Value::Float(*f),
        AttributeValue::Boolean(b) => Value::Boolean(*b),
        AttributeValue::Null => Value::Null,
        AttributeValue::List(values) => {
            Value::List(values.iter().map(attribute_to_query_value).collect())
        }
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => (*x - *y).abs() < f64::EPSILON,
        (Value::Integer(x), Value::Float(y)) | (Value::Float(y), Value::Integer(x)) => {
            ((*x as f64) - *y).abs() < f64::EPSILON
        }
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Null, Value::Null) => true,
        (Value::List(xs), Value::List(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(lx, ly)| values_equal(lx, ly))
        }
        (Value::Map(xs), Value::Map(ys)) => {
            xs.len() == ys.len()
                && xs.iter().all(|(key, value)| {
                    ys.get(key)
                        .map(|other| values_equal(value, other))
                        .unwrap_or(false)
                })
        }
        _ => false,
    }
}

fn compare_query_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Boolean(x), Value::Boolean(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

#[derive(Clone)]
enum AliasState {
    Bound(Node),
    Null,
    Unbound,
}

fn alias_state(row: &QueryRow, alias: &str) -> AliasState {
    match row.nodes.get(alias) {
        Some(Some(node)) => AliasState::Bound(node.clone()),
        Some(None) => AliasState::Null,
        None => AliasState::Unbound,
    }
}

#[derive(Clone)]
enum RelationshipAliasState {
    Bound(Edge),
    Null,
    Unbound,
}

fn relationship_alias_state(row: &QueryRow, alias: &str) -> RelationshipAliasState {
    match row.relationships.get(alias) {
        Some(Some(edge)) => RelationshipAliasState::Bound(edge.clone()),
        Some(None) => RelationshipAliasState::Null,
        None => RelationshipAliasState::Unbound,
    }
}

fn extend_with_node_pattern<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    node_pattern: &NodePattern,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    let alias = node_pattern
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("nodes in MATCH must have aliases".into()))?
        .to_string();

    match alias_state(&row, &alias) {
        AliasState::Bound(node) => {
            let arc = Arc::new(node.clone());
            if node_matches_pattern(&arc, node_pattern)
                && conditions_by_alias
                    .get(&alias)
                    .map(|conds| {
                        conds
                            .iter()
                            .all(|cond| node_satisfies_condition(&arc, cond))
                    })
                    .unwrap_or(true)
            {
                Ok(vec![row])
            } else {
                Ok(Vec::new())
            }
        }
        AliasState::Null => Ok(Vec::new()),
        AliasState::Unbound => {
            let alias_conditions = conditions_by_alias.get(&alias).cloned().unwrap_or_default();
            let nodes = select_nodes(db, node_pattern, &alias_conditions)?;
            let mut results = Vec::new();
            for node in nodes {
                let mut next = row.clone();
                next.nodes.insert(alias.clone(), Some(node));
                results.push(next);
            }
            Ok(results)
        }
    }
}

fn extend_with_relationship_pattern<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    rel: &RelationshipMatch,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    if rel.relationship.alias.is_some() {
        ensure_single_length(&rel.relationship.length)?;
        return extend_relationship_with_alias(db, row, rel, conditions_by_alias);
    }
    extend_relationship_without_alias(db, row, rel, conditions_by_alias)
}

fn ensure_single_length(length: &PathLength) -> Result<(), DaemonError> {
    match length {
        PathLength::Exact(1) => Ok(()),
        _ => Err(DaemonError::Query(
            "relationship aliases are only supported for single-hop patterns".into(),
        )),
    }
}

fn extend_relationship_without_alias<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    rel: &RelationshipMatch,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    let left_alias = rel
        .left
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("left node requires alias".into()))?
        .to_string();
    let right_alias = rel
        .right
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("right node requires alias".into()))?
        .to_string();

    match (
        alias_state(&row, &left_alias),
        alias_state(&row, &right_alias),
    ) {
        (AliasState::Bound(left_node), AliasState::Bound(right_node)) => {
            if relationship_pair_satisfies(db, &left_node, &right_node, rel, conditions_by_alias)? {
                Ok(vec![row])
            } else {
                Ok(Vec::new())
            }
        }
        (AliasState::Bound(left_node), AliasState::Unbound) => {
            let candidates = reachable_nodes_from(
                db,
                &left_node,
                &rel.relationship,
                &rel.right,
                conditions_by_alias
                    .get(&right_alias)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]),
            )?;
            let mut results = Vec::new();
            for node in candidates {
                let mut next = row.clone();
                next.nodes.insert(right_alias.clone(), Some(node));
                results.push(next);
            }
            Ok(results)
        }
        (AliasState::Unbound, AliasState::Bound(right_node)) => {
            let mut reversed = rel.clone();
            reversed.left = rel.right.clone();
            reversed.right = rel.left.clone();
            reversed.relationship.direction = match rel.relationship.direction {
                RelationshipDirection::Outbound => RelationshipDirection::Inbound,
                RelationshipDirection::Inbound => RelationshipDirection::Outbound,
                RelationshipDirection::Undirected => RelationshipDirection::Undirected,
            };
            let candidates = reachable_nodes_from(
                db,
                &right_node,
                &reversed.relationship,
                &reversed.right,
                conditions_by_alias
                    .get(&left_alias)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]),
            )?;
            let mut results = Vec::new();
            for node in candidates {
                let mut next = row.clone();
                next.nodes.insert(left_alias.clone(), Some(node));
                results.push(next);
            }
            Ok(results)
        }
        (AliasState::Unbound, AliasState::Unbound) => {
            let left_candidates = select_nodes(
                db,
                &rel.left,
                &conditions_by_alias
                    .get(&left_alias)
                    .cloned()
                    .unwrap_or_default(),
            )?;
            let mut results = Vec::new();
            for left in left_candidates {
                let reachable = reachable_nodes_from(
                    db,
                    &left,
                    &rel.relationship,
                    &rel.right,
                    conditions_by_alias
                        .get(&right_alias)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]),
                )?;
                for right in reachable {
                    let mut next = row.clone();
                    next.nodes.insert(left_alias.clone(), Some(left.clone()));
                    next.nodes.insert(right_alias.clone(), Some(right));
                    results.push(next);
                }
            }
            Ok(results)
        }
        _ => Ok(Vec::new()),
    }
}

fn extend_relationship_with_alias<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    rel: &RelationshipMatch,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    let alias = rel
        .relationship
        .alias
        .as_ref()
        .expect("alias checked earlier")
        .clone();

    match relationship_alias_state(&row, &alias) {
        RelationshipAliasState::Bound(edge) => {
            if !edge_matches_pattern(&Arc::new(edge.clone()), &rel.relationship) {
                return Ok(Vec::new());
            }
            reconcile_bound_relationship(db, row, rel, edge, &alias, conditions_by_alias)
        }
        RelationshipAliasState::Null => Ok(Vec::new()),
        RelationshipAliasState::Unbound => {
            let mut bare = rel.clone();
            bare.relationship.alias = None;
            let base_rows = extend_relationship_without_alias(db, row, &bare, conditions_by_alias)?;
            let mut results = Vec::new();
            for base in base_rows {
                let left_alias = rel
                    .left
                    .alias
                    .as_ref()
                    .ok_or_else(|| DaemonError::Query("left node requires alias".into()))?;
                let right_alias = rel
                    .right
                    .alias
                    .as_ref()
                    .ok_or_else(|| DaemonError::Query("right node requires alias".into()))?;
                let left_node = match base.nodes.get(left_alias).and_then(|n| n.clone()) {
                    Some(node) => node,
                    None => continue,
                };
                let right_node = match base.nodes.get(right_alias).and_then(|n| n.clone()) {
                    Some(node) => node,
                    None => continue,
                };

                let edges =
                    matching_edges_between_nodes(db, &left_node, &right_node, &rel.relationship)?;
                for edge in edges {
                    let mut next = base.clone();
                    next.relationships
                        .insert(alias.clone(), Some((*edge).clone()));
                    results.push(next);
                }
            }
            Ok(results)
        }
    }
}

fn reconcile_bound_relationship<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    rel: &RelationshipMatch,
    edge: Edge,
    alias: &str,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    let mut next = row.clone();
    next.relationships
        .insert(alias.to_string(), Some(edge.clone()));
    let left_state = rel
        .left
        .alias
        .as_ref()
        .map(|alias| alias_state(&next, alias))
        .unwrap_or(AliasState::Unbound);
    let right_state = rel
        .right
        .alias
        .as_ref()
        .map(|alias| alias_state(&next, alias))
        .unwrap_or(AliasState::Unbound);

    let (expected_left_id, expected_right_id) = match relationship_endpoints_from_states(
        &edge,
        &rel.relationship.direction,
        &left_state,
        &right_state,
    ) {
        Some(pair) => pair,
        None => return Ok(Vec::new()),
    };

    if !bind_node_for_edge(
        db,
        &mut next,
        &rel.left,
        expected_left_id,
        conditions_by_alias,
    )? {
        return Ok(Vec::new());
    }

    if !bind_node_for_edge(
        db,
        &mut next,
        &rel.right,
        expected_right_id,
        conditions_by_alias,
    )? {
        return Ok(Vec::new());
    }

    Ok(vec![next])
}

fn relationship_endpoints_from_states(
    edge: &Edge,
    direction: &RelationshipDirection,
    left_state: &AliasState,
    right_state: &AliasState,
) -> Option<(NodeId, NodeId)> {
    match direction {
        RelationshipDirection::Outbound => Some((edge.source(), edge.target())),
        RelationshipDirection::Inbound => Some((edge.target(), edge.source())),
        RelationshipDirection::Undirected => {
            let source = edge.source();
            let target = edge.target();
            match (left_state, right_state) {
                (AliasState::Bound(node), _) if node.id() == target => Some((target, source)),
                (_, AliasState::Bound(node)) if node.id() == source => Some((source, target)),
                (AliasState::Bound(node), _) if node.id() == source => Some((source, target)),
                (_, AliasState::Bound(node)) if node.id() == target => Some((source, target)),
                _ => Some((source, target)),
            }
        }
    }
}

fn bind_node_for_edge<B: StorageBackend>(
    db: &Database<B>,
    row: &mut QueryRow,
    pattern: &NodePattern,
    node_id: NodeId,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<bool, DaemonError> {
    let alias = match &pattern.alias {
        Some(alias) => alias,
        None => return Ok(true),
    };

    match alias_state(row, alias) {
        AliasState::Bound(node) => {
            Ok(node.id() == node_id && node_matches_pattern(&Arc::new(node), pattern))
        }
        AliasState::Null => Ok(false),
        AliasState::Unbound => {
            if let Some(node_arc) = db.get_node(node_id)? {
                if !node_matches_pattern(&node_arc, pattern) {
                    return Ok(false);
                }
                if let Some(conditions) = conditions_by_alias.get(alias) {
                    if !conditions
                        .iter()
                        .all(|cond| node_satisfies_condition(&node_arc, cond))
                    {
                        return Ok(false);
                    }
                }
                row.nodes.insert(alias.clone(), Some((*node_arc).clone()));
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

fn matching_edges_between_nodes<B: StorageBackend>(
    db: &Database<B>,
    left: &Node,
    right: &Node,
    relationship: &RelationshipPattern,
) -> Result<Vec<Arc<Edge>>, DaemonError> {
    let mut matches = Vec::new();
    let mut seen = HashSet::new();
    for edge in db.edges_for_node(left.id())? {
        if !edge_matches_pattern(&edge, relationship) {
            continue;
        }
        if edge_satisfies_direction(&edge, left.id(), right.id(), &relationship.direction) {
            if seen.insert(edge.id()) {
                matches.push(edge);
            }
        }
    }
    Ok(matches)
}

fn edge_satisfies_direction(
    edge: &Edge,
    left_id: NodeId,
    right_id: NodeId,
    direction: &RelationshipDirection,
) -> bool {
    match direction {
        RelationshipDirection::Outbound => edge.source() == left_id && edge.target() == right_id,
        RelationshipDirection::Inbound => edge.target() == left_id && edge.source() == right_id,
        RelationshipDirection::Undirected => {
            (edge.source() == left_id && edge.target() == right_id)
                || (edge.source() == right_id && edge.target() == left_id)
        }
    }
}

fn extend_with_path_pattern<B: StorageBackend>(
    db: &Database<B>,
    row: QueryRow,
    path: &PathPattern,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<Vec<QueryRow>, DaemonError> {
    if path.pattern.relationship.alias.is_some() {
        ensure_single_length(&path.pattern.relationship.length)?;
    }

    let left_alias = path
        .pattern
        .left
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("left node requires alias".into()))?
        .to_string();
    let right_alias = path
        .pattern
        .right
        .alias
        .as_ref()
        .ok_or_else(|| DaemonError::Query("right node requires alias".into()))?
        .to_string();

    match (
        alias_state(&row, &left_alias),
        alias_state(&row, &right_alias),
    ) {
        (AliasState::Null, _) | (_, AliasState::Null) => Ok(Vec::new()),
        (AliasState::Bound(left_node), AliasState::Bound(right_node)) => {
            if !relationship_pair_satisfies(
                db,
                &left_node,
                &right_node,
                &path.pattern,
                conditions_by_alias,
            )? {
                return Ok(Vec::new());
            }
            let mut end_ids = HashSet::new();
            end_ids.insert(right_node.id());
            let path_states = enumerate_path_states_for_nodes(
                db,
                &[left_node.clone()],
                &end_ids,
                &path.pattern.relationship,
                path.mode.clone(),
            )?;
            let mut results = Vec::new();
            for state in path_states {
                let query_path = path_state_to_query_path(db, &state)?;
                let mut next = row.clone();
                store_path_value(
                    &mut next,
                    &path.alias,
                    query_path,
                    path.pattern.relationship.alias.as_ref(),
                )?;
                results.push(next);
            }
            Ok(results)
        }
        (AliasState::Bound(left_node), AliasState::Unbound) => {
            let right_candidates = select_nodes(
                db,
                &path.pattern.right,
                alias_conditions_slice(conditions_by_alias, &right_alias),
            )?;
            if right_candidates.is_empty() {
                return Ok(Vec::new());
            }
            let mut right_index = HashMap::new();
            let mut end_ids = HashSet::new();
            for (idx, node) in right_candidates.iter().enumerate() {
                right_index.insert(node.id(), idx);
                end_ids.insert(node.id());
            }
            let path_states = enumerate_path_states_for_nodes(
                db,
                &[left_node.clone()],
                &end_ids,
                &path.pattern.relationship,
                path.mode.clone(),
            )?;
            let mut results = Vec::new();
            for state in path_states {
                if let Some(&idx) = state.nodes.last().and_then(|id| right_index.get(id)) {
                    let query_path = path_state_to_query_path(db, &state)?;
                    let mut next = row.clone();
                    next.nodes
                        .insert(right_alias.clone(), Some(right_candidates[idx].clone()));
                    store_path_value(
                        &mut next,
                        &path.alias,
                        query_path,
                        path.pattern.relationship.alias.as_ref(),
                    )?;
                    results.push(next);
                }
            }
            Ok(results)
        }
        (AliasState::Unbound, AliasState::Bound(right_node)) => {
            let left_candidates = select_nodes(
                db,
                &path.pattern.left,
                alias_conditions_slice(conditions_by_alias, &left_alias),
            )?;
            if left_candidates.is_empty() {
                return Ok(Vec::new());
            }
            let mut left_index = HashMap::new();
            for (idx, node) in left_candidates.iter().enumerate() {
                left_index.insert(node.id(), idx);
            }
            let mut end_ids = HashSet::new();
            end_ids.insert(right_node.id());
            let path_states = enumerate_path_states_for_nodes(
                db,
                &left_candidates,
                &end_ids,
                &path.pattern.relationship,
                path.mode.clone(),
            )?;
            let mut results = Vec::new();
            for state in path_states {
                if let Some(&idx) = state.nodes.first().and_then(|id| left_index.get(id)) {
                    let query_path = path_state_to_query_path(db, &state)?;
                    let mut next = row.clone();
                    next.nodes
                        .insert(left_alias.clone(), Some(left_candidates[idx].clone()));
                    store_path_value(
                        &mut next,
                        &path.alias,
                        query_path,
                        path.pattern.relationship.alias.as_ref(),
                    )?;
                    results.push(next);
                }
            }
            Ok(results)
        }
        (AliasState::Unbound, AliasState::Unbound) => {
            let left_candidates = select_nodes(
                db,
                &path.pattern.left,
                alias_conditions_slice(conditions_by_alias, &left_alias),
            )?;
            let right_candidates = select_nodes(
                db,
                &path.pattern.right,
                alias_conditions_slice(conditions_by_alias, &right_alias),
            )?;
            if left_candidates.is_empty() || right_candidates.is_empty() {
                return Ok(Vec::new());
            }
            let mut left_index = HashMap::new();
            for (idx, node) in left_candidates.iter().enumerate() {
                left_index.insert(node.id(), idx);
            }
            let mut right_index = HashMap::new();
            let mut end_ids = HashSet::new();
            for (idx, node) in right_candidates.iter().enumerate() {
                right_index.insert(node.id(), idx);
                end_ids.insert(node.id());
            }
            let path_states = enumerate_path_states_for_nodes(
                db,
                &left_candidates,
                &end_ids,
                &path.pattern.relationship,
                path.mode.clone(),
            )?;
            let mut results = Vec::new();
            for state in path_states {
                let start_idx = state
                    .nodes
                    .first()
                    .and_then(|id| left_index.get(id))
                    .copied();
                let end_idx = state
                    .nodes
                    .last()
                    .and_then(|id| right_index.get(id))
                    .copied();
                if let (Some(l_idx), Some(r_idx)) = (start_idx, end_idx) {
                    let query_path = path_state_to_query_path(db, &state)?;
                    let mut next = row.clone();
                    next.nodes
                        .insert(left_alias.clone(), Some(left_candidates[l_idx].clone()));
                    next.nodes
                        .insert(right_alias.clone(), Some(right_candidates[r_idx].clone()));
                    store_path_value(
                        &mut next,
                        &path.alias,
                        query_path,
                        path.pattern.relationship.alias.as_ref(),
                    )?;
                    results.push(next);
                }
            }
            Ok(results)
        }
    }
}

fn alias_conditions_slice<'a>(
    conditions_by_alias: &'a HashMap<String, Vec<graphdb_core::query::Condition>>,
    alias: &str,
) -> &'a [graphdb_core::query::Condition] {
    conditions_by_alias
        .get(alias)
        .map(|conds| conds.as_slice())
        .unwrap_or(&[])
}

fn enumerate_path_states_for_nodes<B: StorageBackend>(
    db: &Database<B>,
    starts: &[Node],
    end_ids: &HashSet<NodeId>,
    relationship: &RelationshipPattern,
    mode: PathQueryMode,
) -> Result<Vec<PathState>, DaemonError> {
    if starts.is_empty() || end_ids.is_empty() {
        return Ok(Vec::new());
    }
    let start_arcs: Vec<_> = starts.iter().cloned().map(Arc::new).collect();
    let start_ids: HashSet<NodeId> = start_arcs.iter().map(|node| node.id()).collect();
    let bounds = traversal::length_bounds(&relationship.length);
    match mode {
        PathQueryMode::Shortest => traversal::shortest_path(
            db,
            &start_arcs,
            &start_ids,
            end_ids,
            relationship,
            bounds,
            None,
            "",
        ),
        PathQueryMode::All => traversal::enumerate_paths(
            db,
            &start_arcs,
            &start_ids,
            end_ids,
            relationship,
            bounds,
            None,
            "",
        ),
    }
}

fn relationship_pair_satisfies<B: StorageBackend>(
    db: &Database<B>,
    left: &Node,
    right: &Node,
    rel: &RelationshipMatch,
    conditions_by_alias: &HashMap<String, Vec<graphdb_core::query::Condition>>,
) -> Result<bool, DaemonError> {
    let left_alias = rel.left.alias.as_deref().unwrap_or("");
    let right_alias = rel.right.alias.as_deref().unwrap_or("");

    let left_arc = Arc::new(left.clone());
    let right_arc = Arc::new(right.clone());
    if !node_matches_pattern(&left_arc, &rel.left) || !node_matches_pattern(&right_arc, &rel.right)
    {
        return Ok(false);
    }

    if let Some(conds) = conditions_by_alias.get(left_alias) {
        if !conds
            .iter()
            .all(|cond| node_satisfies_condition(&left_arc, cond))
        {
            return Ok(false);
        }
    }
    if let Some(conds) = conditions_by_alias.get(right_alias) {
        if !conds
            .iter()
            .all(|cond| node_satisfies_condition(&right_arc, cond))
        {
            return Ok(false);
        }
    }

    relationship_path_exists_between(db, left.id(), right.id(), &rel.relationship)
}

fn reachable_nodes_from<B: StorageBackend>(
    db: &Database<B>,
    start: &Node,
    relationship: &RelationshipPattern,
    target_pattern: &NodePattern,
    target_conditions: &[graphdb_core::query::Condition],
) -> Result<Vec<Node>, DaemonError> {
    let (min_hops, max_hops) = traversal::length_bounds(&relationship.length);
    let mut results = Vec::new();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    queue.push_back((start.id(), 0u32));
    visited.insert(start.id());

    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= max_hops {
            continue;
        }
        for next_id in neighbor_node_ids(db, node_id, relationship)? {
            if visited.insert(next_id) {
                let next_depth = depth + 1;
                if let Some(node) = db.get_node(next_id)? {
                    if next_depth >= min_hops
                        && node_matches_pattern(&node, target_pattern)
                        && target_conditions
                            .iter()
                            .all(|cond| node_satisfies_condition(&node, cond))
                    {
                        results.push((*node).clone());
                    }
                    if next_depth < max_hops {
                        queue.push_back((next_id, next_depth));
                    }
                }
            }
        }
    }
    Ok(results)
}

fn relationship_path_exists_between<B: StorageBackend>(
    db: &Database<B>,
    start: NodeId,
    target: NodeId,
    pattern: &RelationshipPattern,
) -> Result<bool, DaemonError> {
    let (min_hops, max_hops) = traversal::length_bounds(&pattern.length);
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    queue.push_back((start, 0u32));
    visited.insert(start);

    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= max_hops {
            continue;
        }
        for next_id in neighbor_node_ids(db, node_id, pattern)? {
            let next_depth = depth + 1;
            if next_depth >= min_hops && next_id == target {
                return Ok(true);
            }
            if visited.insert(next_id) {
                if next_depth < max_hops {
                    queue.push_back((next_id, next_depth));
                }
            }
        }
    }

    Ok(false)
}

fn neighbor_node_ids<B: StorageBackend>(
    db: &Database<B>,
    node_id: NodeId,
    pattern: &RelationshipPattern,
) -> Result<Vec<NodeId>, DaemonError> {
    let mut neighbors = Vec::new();
    for edge in db.edges_for_node(node_id)? {
        match pattern.direction {
            RelationshipDirection::Outbound => {
                if edge.source() == node_id && edge_matches_pattern(&edge, pattern) {
                    neighbors.push(edge.target());
                }
            }
            RelationshipDirection::Inbound => {
                if edge.target() == node_id && edge_matches_pattern(&edge, pattern) {
                    neighbors.push(edge.source());
                }
            }
            RelationshipDirection::Undirected => {
                if edge.source() == node_id && edge_matches_pattern(&edge, pattern) {
                    neighbors.push(edge.target());
                } else if edge.target() == node_id && edge_matches_pattern(&edge, pattern) {
                    neighbors.push(edge.source());
                }
            }
        }
    }
    Ok(neighbors)
}

fn register_missing_aliases(row: &mut QueryRow, clause: &SelectMatchClause) {
    for pattern in &clause.patterns {
        match pattern {
            MatchPattern::Node(node) => {
                if let Some(alias) = node.alias.as_ref() {
                    row.nodes.entry(alias.clone()).or_insert(None);
                }
            }
            MatchPattern::Relationship(rel) => {
                for node in [&rel.left, &rel.right] {
                    if let Some(alias) = node.alias.as_ref() {
                        row.nodes.entry(alias.clone()).or_insert(None);
                    }
                }
                if let Some(alias) = rel.relationship.alias.as_ref() {
                    row.relationships.entry(alias.clone()).or_insert(None);
                }
            }
            MatchPattern::Path(path) => {
                for node in [&path.pattern.left, &path.pattern.right] {
                    if let Some(alias) = node.alias.as_ref() {
                        row.nodes.entry(alias.clone()).or_insert(None);
                    }
                }
                if let Some(alias) = path.pattern.relationship.alias.as_ref() {
                    row.relationships.entry(alias.clone()).or_insert(None);
                }
                row.paths.entry(path.alias.clone()).or_insert(None);
            }
        }
    }
}

fn clause_aliases(clause: &SelectMatchClause) -> Vec<String> {
    let mut aliases = Vec::new();
    for pattern in &clause.patterns {
        match pattern {
            MatchPattern::Node(node) => {
                if let Some(alias) = node.alias.as_ref() {
                    if !aliases.contains(alias) {
                        aliases.push(alias.clone());
                    }
                }
            }
            MatchPattern::Relationship(rel) => {
                for node in [&rel.left, &rel.right] {
                    if let Some(alias) = node.alias.as_ref() {
                        if !aliases.contains(alias) {
                            aliases.push(alias.clone());
                        }
                    }
                }
                if let Some(alias) = rel.relationship.alias.as_ref() {
                    if !aliases.contains(alias) {
                        aliases.push(alias.clone());
                    }
                }
            }
            MatchPattern::Path(path) => {
                for node in [&path.pattern.left, &path.pattern.right] {
                    if let Some(alias) = node.alias.as_ref() {
                        if !aliases.contains(alias) {
                            aliases.push(alias.clone());
                        }
                    }
                }
                if let Some(alias) = path.pattern.relationship.alias.as_ref() {
                    if !aliases.contains(alias) {
                        aliases.push(alias.clone());
                    }
                }
                if !aliases.contains(&path.alias) {
                    aliases.push(path.alias.clone());
                }
            }
        }
    }
    aliases
}
fn execute_create<B: StorageBackend>(
    db: &Database<B>,
    pattern: CreatePattern,
    ctx: &mut ExecutionReport,
) -> Result<(), DaemonError> {
    let left_node = materialize_node(pattern.left)?;
    let left_id = left_node.id();
    db.insert_node(left_node)?;
    ctx.messages.push(format!("created node {}", left_id));

    if let Some(CreateRelationship { edge, right }) = pattern.relationship {
        let right_node = materialize_node(right)?;
        let right_id = right_node.id();
        db.insert_node(right_node)?;
        ctx.messages.push(format!("created node {}", right_id));

        let relationship = materialize_edge(edge, left_id, right_id)?;
        let edge_id = relationship.id();
        db.insert_edge(relationship)?;
        ctx.messages.push(format!("created edge {}", edge_id));
    }

    Ok(())
}

fn execute_procedure<B: StorageBackend>(
    db: &Database<B>,
    procedure: Procedure,
) -> Result<ProcedureResult, DaemonError> {
    match procedure {
        Procedure::GraphDb(graph_proc) => {
            let name = graph_proc.canonical_name().to_string();
            let rows = match graph_proc {
                GraphDbProcedure::NodeClasses => {
                    let entries = db
                        .catalog()
                        .list_node_classes()
                        .map_err(graphdb_core::DatabaseError::from)?;
                    node_class_rows(entries)
                }
                GraphDbProcedure::EdgeClasses => {
                    let entries = db
                        .catalog()
                        .list_edge_classes()
                        .map_err(graphdb_core::DatabaseError::from)?;
                    edge_class_rows(entries)
                }
                GraphDbProcedure::Users => {
                    let entries = db
                        .catalog()
                        .list_users()
                        .map_err(graphdb_core::DatabaseError::from)?;
                    user_rows(entries)
                }
                GraphDbProcedure::Roles => {
                    let entries = db
                        .catalog()
                        .list_roles()
                        .map_err(graphdb_core::DatabaseError::from)?;
                    role_rows(entries)
                }
            };
            Ok(ProcedureResult { name, rows })
        }
        Procedure::User(call) => {
            if call.name.eq_ignore_ascii_case("path.dijkstra") {
                let args = evaluate_procedure_arguments(db, &call)?;
                execute_path_dijkstra(db, args, &call)
            } else if call.name.eq_ignore_ascii_case("path.astar") {
                let args = evaluate_procedure_arguments(db, &call)?;
                execute_path_astar(db, args, &call)
            } else {
                execute_user_procedure(db, call)
            }
        }
    }
}

fn evaluate_procedure_arguments<B: StorageBackend>(
    db: &Database<B>,
    call: &UserProcedureCall,
) -> Result<Vec<FieldValue>, DaemonError> {
    let row = QueryRow::default();
    let timestamp = current_timestamp_millis();
    let mut values = Vec::with_capacity(call.arguments.len());
    for argument in &call.arguments {
        values.push(evaluate_expression(db, &row, argument, timestamp)?);
    }
    Ok(values)
}

fn execute_user_procedure<B: StorageBackend>(
    db: &Database<B>,
    call: UserProcedureCall,
) -> Result<ProcedureResult, DaemonError> {
    let evaluated_args = evaluate_procedure_arguments(db, &call)?;

    let mut ctx = StandardProcedureContext { _db: db };
    let (available_columns, stream) = functions::procedures()
        .call(&call.name, &evaluated_args, &mut ctx)
        .map_err(DaemonError::from)?;
    let rows: Vec<_> = stream.collect();

    procedure_rows_to_result(call.name, available_columns, rows, call.yield_items)
}

fn procedure_rows_to_result(
    name: String,
    available_columns: Vec<String>,
    rows: Vec<HashMap<String, FieldValue>>,
    yield_items: Option<Vec<String>>,
) -> Result<ProcedureResult, DaemonError> {
    let columns = if let Some(items) = yield_items {
        for column in &items {
            if !available_columns
                .iter()
                .any(|candidate| candidate == column)
            {
                return Err(DaemonError::Query(format!(
                    "procedure '{}' does not yield column '{}'",
                    name, column
                )));
            }
        }
        items
    } else {
        available_columns.clone()
    };

    let mut json_rows = Vec::with_capacity(rows.len());
    for mut row in rows {
        let mut json_map = serde_json::Map::new();
        for column in &columns {
            let value = row
                .remove(column)
                .unwrap_or_else(|| FieldValue::Scalar(Value::Null));
            json_map.insert(column.clone(), field_value_to_json(&value)?);
        }
        json_rows.push(JsonValue::Object(json_map));
    }

    Ok(ProcedureResult {
        name,
        rows: json_rows,
    })
}

fn compute_aggregate(rows: &[QueryRow], expr: &AggregateExpression) -> Result<Value, DaemonError> {
    match expr.function {
        AggregateFunction::CountAll => Ok(Value::Integer(rows.len() as i64)),
        AggregateFunction::Count => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("count requires an argument".into()))?;
            let mut total = 0;
            for row in rows {
                match resolve_field(row, field)? {
                    FieldValue::Scalar(Value::Null) => {}
                    FieldValue::Scalar(_)
                    | FieldValue::Node(_)
                    | FieldValue::Relationship(_)
                    | FieldValue::Path(_)
                    | FieldValue::List(_) => total += 1,
                }
            }
            Ok(Value::Integer(total))
        }
        AggregateFunction::Collect => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("collect requires an argument".into()))?;
            let mut items = Vec::new();
            for row in rows {
                match resolve_field(row, field)? {
                    FieldValue::Scalar(Value::Null) => {}
                    FieldValue::Scalar(value) => items.push(value),
                    other => {
                        let json_value = field_value_to_json(&other)?;
                        let serialized = serde_json::to_string(&json_value).map_err(|err| {
                            DaemonError::Query(format!(
                                "failed to serialize value for collect: {err}"
                            ))
                        })?;
                        items.push(Value::String(serialized));
                    }
                }
            }
            Ok(Value::List(items))
        }
        AggregateFunction::Sum => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("sum requires an argument".into()))?;
            let (values, all_int) = numeric_values(rows, field)?;
            if values.is_empty() {
                return Ok(Value::Integer(0));
            }
            let sum: f64 = values.iter().copied().sum();
            if all_int {
                Ok(Value::Integer(sum.round() as i64))
            } else {
                Ok(Value::Float(sum))
            }
        }
        AggregateFunction::Avg => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("avg requires an argument".into()))?;
            let (values, _all_int) = numeric_values(rows, field)?;
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let sum: f64 = values.iter().copied().sum();
            Ok(Value::Float(sum / values.len() as f64))
        }
        AggregateFunction::Max => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("max requires an argument".into()))?;
            let values = scalar_values(rows, field)?;
            let mut best: Option<Value> = None;
            for value in values {
                if value == Value::Null {
                    continue;
                }
                if let Some(current) = &best {
                    if compare_values(&value, current) == Ordering::Greater {
                        best = Some(value);
                    }
                } else {
                    best = Some(value);
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        AggregateFunction::Min => {
            let field = expr
                .target
                .as_ref()
                .ok_or_else(|| DaemonError::Query("min requires an argument".into()))?;
            let values = scalar_values(rows, field)?;
            let mut best: Option<Value> = None;
            for value in values {
                if value == Value::Null {
                    continue;
                }
                if let Some(current) = &best {
                    if compare_values(&value, current) == Ordering::Less {
                        best = Some(value);
                    }
                } else {
                    best = Some(value);
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        AggregateFunction::PercentileCont => {
            let percentile = expr.percentile.ok_or_else(|| {
                DaemonError::Query("percentileCont requires percentile parameter".into())
            })?;
            let (values, _) = percentile_values(rows, expr)?;
            percentile_cont(values, percentile)
        }
        AggregateFunction::PercentileDisc => {
            let percentile = expr.percentile.ok_or_else(|| {
                DaemonError::Query("percentileDisc requires percentile parameter".into())
            })?;
            let (values, _) = percentile_values(rows, expr)?;
            percentile_disc(values, percentile)
        }
        AggregateFunction::StDev => {
            let (values, _all_int) = numeric_values(
                rows,
                expr.target
                    .as_ref()
                    .ok_or_else(|| DaemonError::Query("stDev requires an argument".into()))?,
            )?;
            Ok(Value::Float(sample_std_dev(&values)))
        }
        AggregateFunction::StDevP => {
            let (values, _all_int) = numeric_values(
                rows,
                expr.target
                    .as_ref()
                    .ok_or_else(|| DaemonError::Query("stDevP requires an argument".into()))?,
            )?;
            Ok(Value::Float(population_std_dev(&values)))
        }
    }
}

fn execute_path_dijkstra<B: StorageBackend>(
    db: &Database<B>,
    args: Vec<FieldValue>,
    call: &UserProcedureCall,
) -> Result<ProcedureResult, DaemonError> {
    let config = dijkstra::parse_dijkstra_config(args)?;
    let rows = dijkstra::run_dijkstra(db, &config)?;
    let available_columns = vec![
        "index".to_string(),
        "sourceNode".to_string(),
        "targetNode".to_string(),
        "totalCost".to_string(),
        "nodeIds".to_string(),
        "costs".to_string(),
    ];
    procedure_rows_to_result(
        call.name.clone(),
        available_columns,
        rows,
        call.yield_items.clone(),
    )
}

fn execute_path_astar<B: StorageBackend>(
    db: &Database<B>,
    args: Vec<FieldValue>,
    call: &UserProcedureCall,
) -> Result<ProcedureResult, DaemonError> {
    let config = astar::parse_astar_config(args)?;
    let rows = astar::run_astar(db, &config)?;
    let available_columns = vec![
        "index".to_string(),
        "sourceNode".to_string(),
        "targetNode".to_string(),
        "totalCost".to_string(),
        "nodeIds".to_string(),
        "costs".to_string(),
    ];
    procedure_rows_to_result(
        call.name.clone(),
        available_columns,
        rows,
        call.yield_items.clone(),
    )
}

fn percentile_values(
    rows: &[QueryRow],
    expr: &AggregateExpression,
) -> Result<(Vec<f64>, bool), DaemonError> {
    let field = expr
        .target
        .as_ref()
        .ok_or_else(|| DaemonError::Query("percentile requires an argument".into()))?;
    numeric_values(rows, field)
}

fn percentile_cont(values: Vec<f64>, percentile: f64) -> Result<Value, DaemonError> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    if !(0.0..=1.0).contains(&percentile) {
        return Err(DaemonError::Query(
            "percentile must be between 0.0 and 1.0".into(),
        ));
    }
    let mut data = values;
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    if data.len() == 1 {
        return Ok(Value::Float(data[0]));
    }
    let rank = percentile * (data.len() as f64 - 1.0);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let fraction = rank - lower as f64;
    let lower_value = data[lower];
    let upper_value = data[upper];
    Ok(Value::Float(
        lower_value + (upper_value - lower_value) * fraction,
    ))
}

fn percentile_disc(values: Vec<f64>, percentile: f64) -> Result<Value, DaemonError> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    if !(0.0..=1.0).contains(&percentile) {
        return Err(DaemonError::Query(
            "percentile must be between 0.0 and 1.0".into(),
        ));
    }
    let mut data = values;
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let index = (percentile * (data.len() as f64 - 1.0)).round() as usize;
    Ok(Value::Float(data[index]))
}

fn numeric_values(
    rows: &[QueryRow],
    field: &FieldReference,
) -> Result<(Vec<f64>, bool), DaemonError> {
    let mut values = Vec::new();
    let mut all_int = true;
    for row in rows {
        match resolve_field(row, field)? {
            FieldValue::Scalar(Value::Integer(i)) => values.push(i as f64),
            FieldValue::Scalar(Value::Float(f)) => {
                values.push(f);
                all_int = false;
            }
            FieldValue::Scalar(Value::Null) => {}
            FieldValue::Scalar(_) => {
                return Err(DaemonError::Query(
                    "numeric aggregate received non-numeric value".into(),
                ));
            }
            FieldValue::Node(_) => {
                return Err(DaemonError::Query(
                    "numeric aggregate cannot operate on nodes".into(),
                ));
            }
            FieldValue::Relationship(_) => {
                return Err(DaemonError::Query(
                    "numeric aggregate cannot operate on relationships".into(),
                ));
            }
            FieldValue::Path(_) => {
                return Err(DaemonError::Query(
                    "numeric aggregate cannot operate on paths".into(),
                ));
            }
            FieldValue::List(_) => {
                return Err(DaemonError::Query(
                    "numeric aggregate cannot operate on list aliases".into(),
                ));
            }
        }
    }
    Ok((values, all_int))
}

fn scalar_values(rows: &[QueryRow], field: &FieldReference) -> Result<Vec<Value>, DaemonError> {
    let mut values = Vec::new();
    for row in rows {
        match resolve_field(row, field)? {
            FieldValue::Scalar(value) => values.push(value),
            FieldValue::Node(_) => {
                return Err(DaemonError::Query(
                    "aggregation cannot operate on node aliases".into(),
                ));
            }
            FieldValue::Relationship(_) => {
                return Err(DaemonError::Query(
                    "aggregation cannot operate on relationship aliases".into(),
                ));
            }
            FieldValue::Path(_) | FieldValue::List(_) => {
                return Err(DaemonError::Query(
                    "aggregation cannot operate on path or list aliases".into(),
                ));
            }
        }
    }
    Ok(values)
}

fn sample_std_dev(values: &[f64]) -> f64 {
    let n = values.len();
    if n <= 1 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = values
        .iter()
        .map(|v| {
            let diff = v - mean;
            diff * diff
        })
        .sum::<f64>()
        / (n as f64 - 1.0);
    variance.sqrt()
}

fn population_std_dev(values: &[f64]) -> f64 {
    let n = values.len();
    if n == 0 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = values
        .iter()
        .map(|v| {
            let diff = v - mean;
            diff * diff
        })
        .sum::<f64>()
        / n as f64;
    variance.sqrt()
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    let rank_a = value_rank(a);
    let rank_b = value_rank(b);
    match rank_a.cmp(&rank_b) {
        Ordering::Equal => match (a, b) {
            (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
            (Value::Integer(x), Value::Float(y)) => {
                (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
            }
            (Value::Float(x), Value::Integer(y)) => {
                x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::String(x), Value::String(y)) => x.cmp(y),
            (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
            (Value::List(xs), Value::List(ys)) => xs.len().cmp(&ys.len()),
            _ => Ordering::Equal,
        },
        ordering => ordering,
    }
}

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Integer(_) | Value::Float(_) => 3,
        Value::String(_) => 2,
        Value::Boolean(_) => 2,
        Value::List(_) | Value::Map(_) => 1,
        Value::Null => 0,
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::String(s) => json!(s),
        Value::Integer(i) => json!(i),
        Value::Float(f) => {
            if f.is_finite() {
                json!(f)
            } else if f.is_nan() {
                JsonValue::String("NaN".into())
            } else if f.is_sign_positive() {
                JsonValue::String("Infinity".into())
            } else {
                JsonValue::String("-Infinity".into())
            }
        }
        Value::Boolean(b) => json!(b),
        Value::Null => JsonValue::Null,
        Value::List(values) => JsonValue::Array(values.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries.iter() {
                map.insert(key.clone(), value_to_json(value));
            }
            JsonValue::Object(map)
        }
    }
}

fn current_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn collect_sorted_keys<I>(iter: I) -> Value
where
    I: IntoIterator<Item = String>,
{
    let mut keys: Vec<String> = iter.into_iter().collect();
    keys.sort();
    Value::List(keys.into_iter().map(Value::String).collect())
}

fn node_class_rows(entries: Vec<NodeClassEntry>) -> Vec<JsonValue> {
    entries
        .into_iter()
        .map(|entry| {
            let mut properties: Vec<_> = entry.properties.into_iter().collect();
            properties.sort();
            json!({
                "id": entry.id,
                "schema_id": entry.schema_id,
                "name": entry.name,
                "owner_role": entry.owner_role,
                "properties": properties,
                "version": entry.version,
                "created_at_seconds": seconds_since_epoch(entry.created_at),
            })
        })
        .collect()
}

fn edge_class_rows(entries: Vec<EdgeClassEntry>) -> Vec<JsonValue> {
    entries
        .into_iter()
        .map(|entry| {
            let mut properties: Vec<_> = entry.properties.into_iter().collect();
            properties.sort();
            json!({
                "id": entry.id,
                "schema_id": entry.schema_id,
                "name": entry.name,
                "owner_role": entry.owner_role,
                "properties": properties,
                "source_class": entry.source_class,
                "target_class": entry.target_class,
                "version": entry.version,
                "created_at_seconds": seconds_since_epoch(entry.created_at),
            })
        })
        .collect()
}

fn role_rows(entries: Vec<RoleEntry>) -> Vec<JsonValue> {
    entries
        .into_iter()
        .map(|entry| {
            let mut inherits: Vec<_> = entry
                .inherits
                .into_iter()
                .map(|id| id.to_string())
                .collect();
            inherits.sort();
            json!({
                "id": entry.id,
                "name": entry.name,
                "inherits": inherits,
                "created_at_seconds": seconds_since_epoch(entry.created_at),
            })
        })
        .collect()
}

fn user_rows(entries: Vec<UserEntry>) -> Vec<JsonValue> {
    entries
        .into_iter()
        .map(|entry| {
            json!({
                "id": entry.id,
                "username": entry.username,
                "login_role": entry.login_role,
                "default_role": entry.default_role,
                "auth_method": auth_method_label(&entry.auth_method),
                "created_at_seconds": seconds_since_epoch(entry.created_at),
            })
        })
        .collect()
}

fn auth_method_label(method: &AuthMethod) -> &'static str {
    match method {
        AuthMethod::InternalPassword => "internal_password",
        AuthMethod::External => "external",
    }
}

fn seconds_since_epoch(ts: std::time::SystemTime) -> u64 {
    ts.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn materialize_node(pattern: graphdb_core::query::NodePattern) -> Result<Node, DaemonError> {
    let graphdb_core::query::NodePattern {
        alias: _,
        label,
        properties,
    } = pattern;

    let properties = properties.0;
    let node_id = if let Some(value) = properties.get("id") {
        parse_node_id(value)?
    } else {
        NodeId::new_v4()
    };

    let labels = label.into_iter().collect::<Vec<_>>();

    let mut attributes = HashMap::new();
    for (key, value) in properties.into_iter() {
        attributes.insert(key, value_to_attribute(&value));
    }
    if !attributes.contains_key("id") {
        attributes.insert(
            "id".to_string(),
            AttributeValue::String(node_id.to_string()),
        );
    }

    Ok(Node::new(node_id, labels, attributes))
}

fn materialize_edge(
    pattern: graphdb_core::query::EdgePattern,
    source: NodeId,
    target: NodeId,
) -> Result<Edge, DaemonError> {
    let graphdb_core::query::EdgePattern {
        alias: _,
        label,
        properties,
    } = pattern;

    let properties = properties.0;
    let edge_id = if let Some(value) = properties.get("id") {
        parse_edge_id(value)?
    } else {
        EdgeId::new_v4()
    };

    let mut attributes = HashMap::new();
    for (key, value) in properties.into_iter() {
        attributes.insert(key, value_to_attribute(&value));
    }
    if !attributes.contains_key("id") {
        attributes.insert(
            "id".to_string(),
            AttributeValue::String(edge_id.to_string()),
        );
    }
    if let Some(label_value) = label {
        attributes
            .entry("__label".to_string())
            .or_insert_with(|| AttributeValue::String(label_value.clone()));
    }

    Ok(Edge::new(edge_id, source, target, attributes))
}

fn extract_node_id(
    pattern: graphdb_core::query::NodePattern,
    missing_msg: &str,
) -> Result<NodeId, DaemonError> {
    let graphdb_core::query::NodePattern { properties, .. } = pattern;
    let props = properties.0;
    let value = props
        .get("id")
        .ok_or_else(|| DaemonError::Query(missing_msg.into()))?;
    parse_node_id(value)
}

pub(crate) fn parse_node_id(value: &Value) -> Result<NodeId, DaemonError> {
    match value {
        Value::Integer(i) => Ok(NodeId::from_u128(*i as u128)),
        Value::String(s) => {
            let trimmed = s.trim();
            let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
            if let Ok(uuid) = NodeId::parse_str(&compact) {
                Ok(uuid)
            } else {
                let parsed = compact
                    .parse::<u128>()
                    .map_err(|_| DaemonError::Query("invalid node id".into()))?;
                Ok(NodeId::from_u128(parsed))
            }
        }
        _ => Err(DaemonError::Query("unsupported node id value".into())),
    }
}

fn parse_edge_id(value: &Value) -> Result<EdgeId, DaemonError> {
    match value {
        Value::Integer(i) => Ok(EdgeId::from_u128(*i as u128)),
        Value::String(s) => {
            let trimmed = s.trim();
            let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
            if let Ok(uuid) = EdgeId::parse_str(&compact) {
                Ok(uuid)
            } else {
                let parsed = compact
                    .parse::<u128>()
                    .map_err(|_| DaemonError::Query("invalid edge id".into()))?;
                Ok(EdgeId::from_u128(parsed))
            }
        }
        _ => Err(DaemonError::Query("unsupported edge id value".into())),
    }
}

fn value_to_attribute(value: &Value) -> AttributeValue {
    match value {
        Value::String(s) => AttributeValue::String(s.clone()),
        Value::Integer(i) => AttributeValue::Integer(*i),
        Value::Float(f) => AttributeValue::Float(*f),
        Value::Boolean(b) => AttributeValue::Boolean(*b),
        Value::Null => AttributeValue::Null,
        Value::List(values) => {
            AttributeValue::List(values.iter().map(value_to_attribute).collect())
        }
        Value::Map(_) => AttributeValue::Null,
    }
}

fn execute_path_query<B: StorageBackend>(
    db: &Database<B>,
    query: PathMatchQuery,
    ctx: &mut ExecutionReport,
) -> Result<(), DaemonError> {
    let start_nodes = find_matching_nodes(db, &query.start)?;
    let end_nodes = find_matching_nodes(db, &query.end)?;

    if start_nodes.is_empty() || end_nodes.is_empty() {
        ctx.messages.push("path match returned no nodes".into());
        return Ok(());
    }

    let end_set: HashSet<NodeId> = end_nodes.iter().map(|node| node.id()).collect();
    let start_ids: HashSet<NodeId> = start_nodes.iter().map(|node| node.id()).collect();
    let length_bounds = traversal::length_bounds(&query.relationship.length);
    let filter = query.filter.clone();

    let paths = match query.mode {
        PathQueryMode::Shortest => traversal::shortest_path(
            db,
            &start_nodes,
            &start_ids,
            &end_set,
            &query.relationship,
            length_bounds,
            filter.as_ref(),
            &query.start_alias,
        )?,
        PathQueryMode::All => traversal::enumerate_paths(
            db,
            &start_nodes,
            &start_ids,
            &end_set,
            &query.relationship,
            length_bounds,
            filter.as_ref(),
            &query.start_alias,
        )?,
    };

    if paths.is_empty() {
        ctx.messages
            .push("path match completed with no results".into());
        return Ok(());
    }

    match query.returns {
        PathReturn::Path { include_length } => {
            for path in paths {
                let nodes = load_nodes_by_ids(db, &path.nodes)?;
                ctx.paths.push(PathResult {
                    alias: query.path_alias.clone(),
                    nodes,
                    edge_ids: path.edges.clone(),
                    length: path.edges.len(),
                });
                if include_length {
                    ctx.messages.push(format!(
                        "path {} length {}",
                        query.path_alias,
                        path.edges.len()
                    ));
                }
            }
        }
        PathReturn::Nodes {
            start_alias,
            end_alias,
            include_length,
        } => {
            for path in paths {
                let start_node = db
                    .get_node(*path.nodes.first().expect("non-empty path"))?
                    .map(|arc| (*arc).clone())
                    .ok_or_else(|| DaemonError::Query("start node missing".into()))?;
                let end_node = db
                    .get_node(*path.nodes.last().expect("non-empty path"))?
                    .map(|arc| (*arc).clone())
                    .ok_or_else(|| DaemonError::Query("end node missing".into()))?;
                ctx.path_pairs.push(PathPairResult {
                    start_alias: start_alias.clone(),
                    end_alias: end_alias.clone(),
                    start: start_node,
                    end: end_node,
                    length: path.edges.len(),
                });
                if include_length {
                    ctx.messages.push(format!(
                        "path {} between {} and {} length {}",
                        query.path_alias,
                        start_alias,
                        end_alias,
                        path.edges.len()
                    ));
                }
            }
        }
    }

    Ok(())
}

fn find_matching_nodes<B: StorageBackend>(
    db: &Database<B>,
    pattern: &graphdb_core::query::NodePattern,
) -> Result<Vec<std::sync::Arc<Node>>, DaemonError> {
    let mut matches = Vec::new();
    for node_id in db.node_ids()? {
        if let Some(node) = db.get_node(node_id)? {
            if node_matches_pattern(&node, pattern) {
                matches.push(node);
            }
        }
    }
    Ok(matches)
}

fn load_nodes_by_ids<B: StorageBackend>(
    db: &Database<B>,
    ids: &[NodeId],
) -> Result<Vec<Node>, DaemonError> {
    let mut nodes = Vec::new();
    for id in ids {
        if let Some(node) = db.get_node(*id)? {
            nodes.push((*node).clone());
        }
    }
    Ok(nodes)
}

fn load_edges_by_ids<B: StorageBackend>(
    db: &Database<B>,
    ids: &[EdgeId],
) -> Result<Vec<Edge>, DaemonError> {
    let mut edges = Vec::new();
    for id in ids {
        if let Some(edge) = db.get_edge(*id)? {
            edges.push((*edge).clone());
        }
    }
    Ok(edges)
}

fn path_state_to_query_path<B: StorageBackend>(
    db: &Database<B>,
    state: &PathState,
) -> Result<QueryPath, DaemonError> {
    let nodes = load_nodes_by_ids(db, &state.nodes)?;
    let edges = load_edges_by_ids(db, &state.edges)?;
    Ok(QueryPath { nodes, edges })
}

fn store_path_value(
    row: &mut QueryRow,
    alias: &str,
    path: QueryPath,
    relationship_alias: Option<&String>,
) -> Result<(), DaemonError> {
    if let Some(rel_alias) = relationship_alias {
        if path.edges.len() != 1 {
            return Err(DaemonError::Query(
                "relationship alias requires single-hop path".into(),
            ));
        }
        row.relationships
            .insert(rel_alias.clone(), Some(path.edges[0].clone()));
    }
    row.paths.insert(alias.to_string(), Some(path));
    Ok(())
}

fn path_to_json(path: &QueryPath) -> Result<JsonValue, DaemonError> {
    let mut node_json = Vec::with_capacity(path.nodes.len());
    for node in &path.nodes {
        node_json.push(
            serde_json::to_value(node)
                .map_err(|err| DaemonError::Query(format!("failed to serialize node: {err}")))?,
        );
    }
    let mut edge_json = Vec::with_capacity(path.edges.len());
    for edge in &path.edges {
        edge_json.push(serde_json::to_value(edge).map_err(|err| {
            DaemonError::Query(format!("failed to serialize relationship: {err}"))
        })?);
    }
    Ok(json!({
        "nodes": node_json,
        "relationships": edge_json,
        "length": path.edges.len(),
    }))
}

fn field_value_to_json(value: &FieldValue) -> Result<JsonValue, DaemonError> {
    match value {
        FieldValue::Scalar(val) => Ok(value_to_json(val)),
        FieldValue::Node(node) => serde_json::to_value(node)
            .map_err(|err| DaemonError::Query(format!("failed to serialize node: {err}"))),
        FieldValue::Relationship(edge) => serde_json::to_value(edge)
            .map_err(|err| DaemonError::Query(format!("failed to serialize relationship: {err}"))),
        FieldValue::Path(path) => path_to_json(path),
        FieldValue::List(items) => {
            let mut array = Vec::with_capacity(items.len());
            for item in items {
                array.push(field_value_to_json(item)?);
            }
            Ok(JsonValue::Array(array))
        }
    }
}

fn person_pattern(alias: &str, id: &str) -> graphdb_core::query::NodePattern {
    let mut props = HashMap::new();
    props.insert("id".to_string(), Value::String(id.to_string()));
    graphdb_core::query::NodePattern {
        alias: Some(alias.to_string()),
        label: Some("Person".to_string()),
        properties: Properties::new(props),
    }
}

fn node_name(node: &Node) -> Option<&str> {
    match node.attribute("name") {
        Some(AttributeValue::String(name)) => Some(name.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use function_api::register_plugin_functions;
    use graphdb_core::storage::memory::InMemoryBackend;
    use std::collections::HashMap;
    use std::sync::Once;
    use stdfunc;

    static REGISTER_STANDARD_FUNCTIONS: Once = Once::new();
    static REGISTER_TEST_PROCEDURES: Once = Once::new();

    pub(super) fn ensure_standard_functions_registered() {
        REGISTER_STANDARD_FUNCTIONS.call_once(|| {
            let registration = stdfunc::graphdb_register_functions();
            register_plugin_functions(&registration).expect("register stdfunc plugin functions");
        });
    }

    pub(super) fn ensure_test_procedures_registered() {
        REGISTER_TEST_PROCEDURES.call_once(|| {
            functions::procedures()
                .register(ProcedureSpec::new(
                    "test.range",
                    FunctionArity::Exact(2),
                    vec!["value"],
                    |args, _| {
                        let start = match &args[0] {
                            FieldValue::Scalar(Value::Integer(i)) => *i,
                            other => {
                                return Err(function_api::ProcedureError::execution(format!(
                                    "expected integer start, got {:?}",
                                    other
                                )));
                            }
                        };
                        let end = match &args[1] {
                            FieldValue::Scalar(Value::Integer(i)) => *i,
                            other => {
                                return Err(function_api::ProcedureError::execution(format!(
                                    "expected integer end, got {:?}",
                                    other
                                )));
                            }
                        };
                        let mut rows = Vec::new();
                        for value in start..=end {
                            let mut row = ProcedureRow::new();
                            row.insert("value".into(), FieldValue::Scalar(Value::Integer(value)));
                            rows.push(row);
                        }
                        Ok(Box::new(function_api::VecProcedureStream::new(rows))
                            as Box<dyn function_api::ProcedureStream>)
                    },
                ))
                .expect("register test procedure");
        });
    }

    #[test]
    fn executes_shortest_path_query() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);

        let mut ctx = ExecutionReport::default();

        let people = [
            ("a", "00000000-0000-0000-0000-000000000001"),
            ("b", "00000000-0000-0000-0000-000000000002"),
            ("c", "00000000-0000-0000-0000-000000000003"),
        ];
        for (alias, id) in people.iter() {
            let mut props = HashMap::new();
            props.insert("id".to_string(), Value::String((*id).into()));
            let pattern = graphdb_core::query::NodePattern {
                alias: Some((*alias).into()),
                label: Some("Person".into()),
                properties: Properties::new(props),
            };
            insert_node(&db, pattern).unwrap();
        }

        let edge_pattern = graphdb_core::query::EdgePattern {
            alias: None,
            label: Some("FRIEND".into()),
            properties: Properties::new(HashMap::new()),
        };
        insert_edge(
            &db,
            person_pattern("a", "00000000-0000-0000-0000-000000000001"),
            edge_pattern.clone(),
            person_pattern("b", "00000000-0000-0000-0000-000000000002"),
        )
        .unwrap();
        insert_edge(
            &db,
            person_pattern("b", "00000000-0000-0000-0000-000000000002"),
            edge_pattern.clone(),
            person_pattern("c", "00000000-0000-0000-0000-000000000003"),
        )
        .unwrap();

        let relationship = RelationshipPattern {
            alias: None,
            label: Some("FRIEND".into()),
            properties: Properties::new(HashMap::new()),
            direction: RelationshipDirection::Outbound,
            length: PathLength::Range {
                min: 1,
                max: Some(5),
            },
        };
        let query = PathMatchQuery {
            path_alias: "p".into(),
            start_alias: "a".into(),
            end_alias: "c".into(),
            start: person_pattern("a", "00000000-0000-0000-0000-000000000001"),
            end: person_pattern("c", "00000000-0000-0000-0000-000000000003"),
            relationship,
            mode: PathQueryMode::Shortest,
            filter: None,
            returns: PathReturn::Path {
                include_length: true,
            },
        };

        execute_path_query(&db, query, &mut ctx).unwrap();
        assert_eq!(ctx.paths.len(), 1);
        assert_eq!(ctx.paths[0].length, 2);
    }
}

fn execute_script_for_test(db: &Database<InMemoryBackend>, script: &str) -> ExecutionReport {
    #[cfg(test)]
    {
        tests::ensure_standard_functions_registered();
    }
    execute_script(db, script).expect("execute script")
}

fn seed_basic_graph(db: &Database<InMemoryBackend>) {
    let people = [
        ("Meg Ryan", "00000000-0000-0000-0000-000000000001"),
        ("Tom Hanks", "00000000-0000-0000-0000-000000000002"),
        ("Kevin Bacon", "00000000-0000-0000-0000-000000000003"),
        ("Carrie Fisher", "00000000-0000-0000-0000-000000000004"),
        ("Nora Ephron", "00000000-0000-0000-0000-000000000005"),
    ];
    let heuristic_steps = HashMap::from([
        ("00000000-0000-0000-0000-000000000001", 2),
        ("00000000-0000-0000-0000-000000000002", 1),
        ("00000000-0000-0000-0000-000000000003", 0),
        ("00000000-0000-0000-0000-000000000004", 1),
        ("00000000-0000-0000-0000-000000000005", 3),
    ]);
    for (name, id) in people.iter() {
        let mut props = HashMap::from([
            ("id".into(), Value::String(id.to_string())),
            ("name".into(), Value::String(name.to_string())),
        ]);
        if let Some(estimate) = heuristic_steps.get(id) {
            props.insert("heuristic_to_kevin".into(), Value::Integer(*estimate));
        }
        insert_node(
            db,
            graphdb_core::query::NodePattern {
                alias: Some(name.split_whitespace().next().unwrap().to_lowercase()),
                label: Some("Person".into()),
                properties: Properties::new(props),
            },
        )
        .unwrap();
    }

    let edges = [
        (
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ),
        (
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ),
        (
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0000-000000000004",
        ),
        (
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000005",
        ),
    ];
    for (idx, (src, dst)) in edges.iter().enumerate() {
        insert_edge(
            db,
            person_pattern("s", src),
            graphdb_core::query::EdgePattern {
                alias: None,
                label: Some("KNOWS".into()),
                properties: Properties::new(HashMap::from([(
                    "id".into(),
                    Value::String(format!("20000000-0000-0000-0000-{idx:012}")),
                )])),
            },
            person_pattern("t", dst),
        )
        .unwrap();
    }

    // blocked edge from Meg -> Carrie
    insert_edge(
        db,
        person_pattern("meg", "00000000-0000-0000-0000-000000000001"),
        graphdb_core::query::EdgePattern {
            alias: None,
            label: Some("BLOCKED".into()),
            properties: Properties::new(HashMap::from([(
                "id".into(),
                Value::String("30000000-0000-0000-0000-000000000001".into()),
            )])),
        },
        person_pattern("carrie", "00000000-0000-0000-0000-000000000004"),
    )
    .unwrap();
}

#[cfg(test)]
mod query_tests {
    use super::*;

    #[test]
    fn variable_length_paths_return_results() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);

        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (start:Person {name: "Meg Ryan"})-[:KNOWS*1..3]->(end:Person)
            RETURN p;"#,
        );

        assert!(report.paths.iter().any(|p| p.length >= 1));
    }

    #[test]
    fn shortest_path_prefers_min_length() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (start:Person {name: "Meg Ryan"}), (end:Person {name: "Kevin Bacon"})
                MATCH path = shortestPath((start)-[:KNOWS*..5]-(end))
                RETURN path, length(path);"#,
        );
        assert_eq!(report.paths.len(), 1);
        assert_eq!(report.paths[0].length, 2);
    }

    #[test]
    fn exact_hop_count_respected() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (start:Person {name: "Meg Ryan"})-[:KNOWS*2]->(end:Person)
                RETURN p;"#,
        );
        assert!(report.paths.iter().all(|path| path.length == 2));
    }

    #[test]
    fn filtered_paths_exclude_blocked_relationships() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (start:Person {name: "Meg Ryan"})-[:KNOWS*1..3]->(end:Person)
                WHERE NOT (start)-[:BLOCKED]->(:Person)
                RETURN start, end, length(p);"#,
        );
        assert!(!report.path_pairs.is_empty());
        assert!(
            report.path_pairs.iter().all(|pair| {
                !(pair.length == 1 && node_name(&pair.end) == Some("Carrie Fisher"))
            })
        );
    }

    #[test]
    fn undirected_paths_follow_both_directions() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (start:Person {name: "Kevin Bacon"})-[:KNOWS*..2]-(end:Person {name: "Meg Ryan"})
                RETURN p;"#,
        );
        assert!(report.paths.iter().any(|p| p.length == 2));
    }

    #[test]
    fn node_pairs_include_length_metadata() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (start:Person {name: "Meg Ryan"})-[:KNOWS*1..3]->(end:Person)
                RETURN start, end, length(p);"#,
        );
        assert!(report.path_pairs.iter().all(|pair| pair.length >= 1));
    }
}

#[cfg(test)]
mod predicate_tests {
    use super::JsonValue;
    use super::*;

    fn bool_value(row: &JsonValue, key: &str) -> Option<bool> {
        row.as_object()?.get(key)?.as_bool()
    }

    #[test]
    fn list_predicate_functions_handle_empty_lists() {
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            r#"
INSERT NODE (n:Sample { id: 1, values: [] });
MATCH (n:Sample { id: 1 })
RETURN
    all(x IN n.values WHERE x > 0) AS all_true,
    any(x IN n.values WHERE x > 0) AS any_true,
    none(x IN n.values WHERE x > 0) AS none_true,
    single(x IN n.values WHERE x > 0) AS single_true;
"#,
        );

        let row = report.result_rows.first().expect("result row");
        assert_eq!(bool_value(row, "all_true"), Some(true));
        assert_eq!(bool_value(row, "any_true"), Some(false));
        assert_eq!(bool_value(row, "none_true"), Some(true));
        assert_eq!(bool_value(row, "single_true"), Some(false));
    }

    #[test]
    fn list_predicate_functions_handle_nulls() {
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            r#"
INSERT NODE (n:Sample { id: 2, values: [null] });
MATCH (n:Sample { id: 2 })
RETURN
    any(x IN n.values WHERE x > 0) AS any_null,
    single(x IN n.values WHERE x > 0) AS single_null;
"#,
        );
        let row = report.result_rows.first().expect("result row");
        assert!(
            row.as_object()
                .and_then(|map| map.get("any_null"))
                .map(|val| val.is_null())
                .unwrap_or(false)
        );
        assert!(
            row.as_object()
                .and_then(|map| map.get("single_null"))
                .map(|val| val.is_null())
                .unwrap_or(false)
        );
    }

    #[test]
    fn is_empty_filters_results() {
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            r#"
INSERT NODE (:Person { id: 1, name: "Ada", nicknames: [] });
INSERT NODE (:Person { id: 2, name: "Bob", nicknames: ["Bobby"] });
INSERT NODE (:Person { id: 3, name: "Cara" });
MATCH (p:Person)
WHERE p.nicknames IS NOT NULL AND NOT isEmpty(p.nicknames)
RETURN p.name AS name;
"#,
        );
        let names: Vec<_> = report
            .result_rows
            .iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|val| val.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        assert_eq!(names, vec!["Bob".to_string()]);
    }

    #[test]
    fn exists_function_detects_relationships() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"
MATCH (p:Person)
RETURN p.name AS name, exists((p)-[:KNOWS]->()) AS has_knows;
"#,
        );

        let mut seen = HashMap::new();
        for row in &report.result_rows {
            if let (Some(name), Some(flag)) = (
                row.get("name").and_then(|val| val.as_str()),
                row.get("has_knows").and_then(|val| val.as_bool()),
            ) {
                seen.insert(name.to_string(), flag);
            }
        }

        assert_eq!(seen.get("Meg Ryan"), Some(&true));
        assert_eq!(seen.get("Tom Hanks"), Some(&true));
        assert_eq!(seen.get("Kevin Bacon"), Some(&true));
        assert_eq!(seen.get("Carrie Fisher"), Some(&false));
    }

    #[test]
    fn exists_function_filters_rows() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"
MATCH (p:Person)
WHERE exists((p)-[:KNOWS]->())
RETURN p.name AS name;
"#,
        );
        let mut names: Vec<_> = report
            .result_rows
            .iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|val| val.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        names.sort();
        assert_eq!(names, vec!["Kevin Bacon", "Meg Ryan", "Tom Hanks"]);
    }

    #[test]
    fn with_clause_accepts_literal_lists() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"
MATCH (p:Person { name: "Meg Ryan" })
WITH [] AS xs
RETURN all(x IN xs WHERE x > 0) AS all_true;
"#,
        );
        let value = report
            .result_rows
            .first()
            .and_then(|row| row.get("all_true"))
            .and_then(|val| val.as_bool())
            .expect("boolean result");
        assert!(value);
    }
}

#[cfg(test)]
mod scalar_function_tests {
    use super::*;

    #[test]
    fn coalesce_falls_back_to_name() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN coalesce(p.nickname, p.name, "unknown") AS display;"#,
        );
        let value = report.result_rows[0]
            .get("display")
            .and_then(|val| val.as_str())
            .unwrap();
        assert_eq!(value, "Meg Ryan");
    }

    #[test]
    fn relationship_functions_return_endpoints() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (a:Person {name: "Meg Ryan"})-[r:KNOWS]->(b:Person {name: "Tom Hanks"})
            RETURN properties(startNode(r)) AS start, properties(endNode(r)) AS end, type(r) AS relType, id(r) AS relId;"#,
        );
        let row = &report.result_rows[0];
        let start = row
            .get("start")
            .and_then(|props| props.get("name"))
            .and_then(|val| val.as_str())
            .unwrap();
        let end = row
            .get("end")
            .and_then(|props| props.get("name"))
            .and_then(|val| val.as_str())
            .unwrap();
        assert_eq!(start, "Meg Ryan");
        assert_eq!(end, "Tom Hanks");
        assert_eq!(row.get("relType").and_then(|v| v.as_str()), Some("KNOWS"));
        assert!(row.get("relId").and_then(|v| v.as_str()).is_some());
    }

    #[test]
    fn head_last_randomuuid_and_size() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN head(['a','b','c']) AS first,
                      last(['a','b','c']) AS last,
                      size(['x','y']) AS listSize,
                      size('hi') AS strSize,
                      randomUUID() AS uuid;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(row.get("first").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(row.get("last").and_then(|v| v.as_str()), Some("c"));
        assert_eq!(row.get("listSize").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(row.get("strSize").and_then(|v| v.as_i64()), Some(2));
        let uuid = row.get("uuid").and_then(|v| v.as_str()).unwrap();
        assert_eq!(uuid.len(), 36);
    }

    #[test]
    fn conversions_work_as_expected() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN toInteger('42') AS i,
                      toFloat('1.5') AS f,
                      toBoolean(0) AS b,
                      toBooleanOrNull('maybe') AS b_null;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(row.get("i").and_then(|v| v.as_i64()), Some(42));
        assert!(
            row.get("f")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 1.5).abs() < f64::EPSILON)
                .unwrap_or(false)
        );
        assert_eq!(row.get("b").and_then(|v| v.as_bool()), Some(false));
        assert!(row.get("b_null").map(|v| v.is_null()).unwrap_or(false));
    }

    #[test]
    fn numeric_math_functions_execute() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN abs(-5) AS absInt,
                   abs(-1.5) AS absFloat,
                   ceil(1.2) AS ceilVal,
                   floor(-1.2) AS floorVal,
                   isNaN(toFloat('NaN')) AS nanCheck,
                   rand() AS randomVal,
                   round(1.2) AS roundUp,
                   round(-1.2) AS roundDown,
                   round(1.25, 1, 'HALF_DOWN') AS roundHalfDown,
                   sign(-3.0) AS signNeg,
                   sign(0.0) AS signZero;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(row.get("absInt").and_then(|v| v.as_i64()), Some(5));
        assert!(
            row.get("absFloat")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 1.5).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert!(
            row.get("ceilVal")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 2.0).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert!(
            row.get("floorVal")
                .and_then(|v| v.as_f64())
                .map(|f| (f + 2.0).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert_eq!(row.get("nanCheck").and_then(|v| v.as_bool()), Some(true));
        let rand_value = row
            .get("randomVal")
            .and_then(|v| v.as_f64())
            .expect("random value");
        assert!(rand_value >= 0.0 && rand_value < 1.0);
        assert!(
            row.get("roundUp")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 2.0).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert!(
            row.get("roundDown")
                .and_then(|v| v.as_f64())
                .map(|f| (f + 2.0).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert!(
            row.get("roundHalfDown")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 1.2).abs() < 1e-9)
                .unwrap_or(false)
        );
        assert_eq!(row.get("signNeg").and_then(|v| v.as_i64()), Some(-1));
        assert_eq!(row.get("signZero").and_then(|v| v.as_i64()), Some(0));
    }

    #[test]
    fn logarithmic_and_trig_functions_execute() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN e() AS eVal,
                   exp(1.0) AS expVal,
                   log(e()) AS lnE,
                   log10(1000.0) AS log10Val,
                   sqrt(9.0) AS sqrtVal,
                   acos(0.5) AS acosVal,
                   asin(0.5) AS asinVal,
                   atan(1.0) AS atanVal,
                   atan2(0.5, 0.4) AS atan2Val,
                   cos(0.0) AS cosVal,
                   cot(0.5) AS cotVal,
                   degrees(pi()) AS degVal,
                   haversin(0.5) AS haversinVal,
                   pi() AS piVal,
                   radians(180.0) AS radVal,
                   sin(radians(30.0)) AS sinVal,
                   tan(0.25) AS tanVal;"#,
        );
        let row = &report.result_rows[0];
        let close = |key: &str, expected: f64| {
            row.get(key)
                .and_then(|v| v.as_f64())
                .map(|value| (value - expected).abs() < 1e-9)
                .unwrap_or(false)
        };
        assert!(close("eVal", E_CONST));
        assert!(close("expVal", E_CONST));
        assert!(close("lnE", 1.0));
        assert!(close("log10Val", 3.0));
        assert!(close("sqrtVal", 3.0));
        assert!(close("acosVal", (0.5f64).acos()));
        assert!(close("asinVal", (0.5f64).asin()));
        assert!(close("atanVal", (1.0f64).atan()));
        assert!(close("atan2Val", 0.5f64.atan2(0.4)));
        assert!(close("cosVal", 1.0));
        assert!(close("cotVal", 1.0 / 0.5f64.tan()));
        assert!(close("degVal", 180.0));
        assert!(close("haversinVal", {
            let half: f64 = 0.5 / 2.0;
            let s = half.sin();
            s * s
        }));
        assert!(close("piVal", PI));
        assert!(close("radVal", PI));
        assert!(close("sinVal", 0.5));
        assert!(close("tanVal", (0.25f64).tan()));
    }

    #[test]
    fn user_defined_functions_execute() {
        let registry = functions::registry();
        if let Err(err) = registry.register(FunctionSpec::new(
            "doubleValue",
            FunctionArity::Exact(1),
            |args| match args.first() {
                Some(FieldValue::Scalar(Value::Integer(i))) => {
                    Ok(FieldValue::Scalar(Value::Integer(i * 2)))
                }
                Some(FieldValue::Scalar(Value::Float(f))) => {
                    Ok(FieldValue::Scalar(Value::Float(f * 2.0)))
                }
                Some(_) => Err(FunctionError::execution(
                    "doubleValue expects INTEGER or FLOAT",
                )),
                None => Err(FunctionError::execution(
                    "doubleValue expects at least one argument",
                )),
            },
        )) {
            if !matches!(err, FunctionError::AlreadyRegistered(_)) {
                panic!("failed to register custom function: {err}");
            }
        }

        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN doubleValue(21) AS doubled,
                   doubleValue(1.5) AS doubledFloat;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(row.get("doubled").and_then(|v| v.as_i64()), Some(42));
        assert!(
            row.get("doubledFloat")
                .and_then(|v| v.as_f64())
                .map(|f| (f - 3.0).abs() < 1e-9)
                .unwrap_or(false)
        );
    }

    #[test]
    fn properties_function_returns_map() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN properties(p) AS props;"#,
        );
        let props = report.result_rows[0]
            .get("props")
            .and_then(|v| v.as_object())
            .expect("properties map");
        assert_eq!(props.get("name").and_then(|v| v.as_str()), Some("Meg Ryan"));
    }

    #[test]
    fn timestamp_constant_across_rows() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person)
            WITH timestamp() AS t, p
            RETURN t, p.name;"#,
        );
        let timestamps: Vec<_> = report
            .result_rows
            .iter()
            .filter_map(|row| row.get("t").and_then(|v| v.as_i64()))
            .collect();
        assert!(timestamps.iter().all(|ts| *ts == timestamps[0]));
    }

    #[test]
    fn keys_and_labels_functions() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (a:Person {name: "Meg Ryan"})-[r:KNOWS]->(b:Person {name: "Tom Hanks"})
            RETURN keys(a) AS nodeKeys, labels(a) AS nodeLabels, keys(r) AS relKeys;"#,
        );
        let row = &report.result_rows[0];
        let node_keys = row
            .get("nodeKeys")
            .and_then(|v| v.as_array())
            .expect("node keys");
        let node_labels = row
            .get("nodeLabels")
            .and_then(|v| v.as_array())
            .expect("node labels");
        let rel_keys = row
            .get("relKeys")
            .and_then(|v| v.as_array())
            .expect("rel keys");
        assert!(node_labels.iter().any(|v| v.as_str() == Some("Person")));
        let key_set: Vec<_> = node_keys.iter().filter_map(|v| v.as_str()).collect();
        assert!(key_set.contains(&"name"));
        assert!(rel_keys.iter().any(|v| v.as_str() == Some("id")));
    }

    #[test]
    fn range_reverse_tail_functions() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN range(0,5,2) AS evens,
                   range(3,0,-1) AS countdown,
                   reverse(['a','b','c']) AS reversed,
                   tail(['one','two','three']) AS tailValues;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(
            row.get("evens").and_then(|v| v.as_array()).unwrap().len(),
            3
        );
        assert_eq!(
            row.get("countdown")
                .and_then(|v| v.as_array())
                .unwrap()
                .iter()
                .map(|val| val.as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 2, 1, 0]
        );
        assert_eq!(
            row.get("reversed")
                .and_then(|v| v.as_array())
                .unwrap()
                .iter()
                .map(|val| val.as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["c", "b", "a"]
        );
        assert_eq!(
            row.get("tailValues")
                .and_then(|v| v.as_array())
                .unwrap()
                .iter()
                .map(|val| val.as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["two", "three"]
        );
    }

    #[test]
    fn nodes_relationships_and_reduce_functions() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH p = (a:Person {name: "Meg Ryan"})-[:KNOWS*1..2]->(b:Person)
            RETURN nodes(p) AS pathNodes,
                   relationships(p) AS pathRels,
                   reduce(total = 0, person IN nodes(p) | total + size(person.name)) AS nameScore;"#,
        );
        assert!(!report.result_rows.is_empty());
        let row = &report.result_rows[0];
        let nodes = row
            .get("pathNodes")
            .and_then(|v| v.as_array())
            .expect("nodes list");
        let rels = row
            .get("pathRels")
            .and_then(|v| v.as_array())
            .expect("relationships list");
        assert_eq!(nodes.len(), rels.len() + 1);
        let score = row
            .get("nameScore")
            .and_then(|v| v.as_i64())
            .expect("reduce result");
        assert!(score > 0);
    }

    #[test]
    fn reduce_with_literal_list_only_using_with_clause() {
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            r#"WITH [28, 41, 37] AS ages
            RETURN reduce(total = 0, amount IN ages | total + amount) AS totalAge;"#,
        );
        let total = report.result_rows[0]
            .get("totalAge")
            .and_then(|v| v.as_i64())
            .expect("reduce total");
        assert_eq!(total, 106);
    }

    #[test]
    fn reduce_concatenates_strings_with_to_string() {
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            r#"WITH [1,2,3] AS ids
            RETURN reduce(text = '', id IN ids | text + toString(id)) AS combined;"#,
        );
        let combined = report.result_rows[0]
            .get("combined")
            .and_then(|v| v.as_str())
            .expect("combined string");
        assert_eq!(combined, "123");
    }

    #[test]
    fn call_user_defined_procedure() {
        crate::executor::tests::ensure_standard_functions_registered();
        crate::executor::tests::ensure_test_procedures_registered();
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(&db, "CALL test.range(1,3) YIELD value;");
        let values: Vec<_> = report
            .procedures
            .last()
            .expect("procedure result")
            .rows
            .iter()
            .filter_map(|row| row.get("value").and_then(|v| v.as_i64()))
            .collect();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn call_stdfunc_lines_procedure() {
        crate::executor::tests::ensure_standard_functions_registered();
        let db = Database::new(InMemoryBackend::new());
        let report = execute_script_for_test(
            &db,
            "CALL std.lines('hello world from graphdb') YIELD word;",
        );
        let words: Vec<_> = report
            .procedures
            .last()
            .expect("procedure result")
            .rows
            .iter()
            .filter_map(|row| row.get("word").and_then(|v| v.as_str()))
            .collect();
        assert_eq!(words, vec!["hello", "world", "from", "graphdb"]);
    }

    #[test]
    fn call_path_astar_procedure() {
        crate::executor::tests::ensure_standard_functions_registered();
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"CALL path.astar({
                    sourceNode: "00000000-0000-0000-0000-000000000001",
                    targetNode: "00000000-0000-0000-0000-000000000003",
                    heuristicProperty: "heuristic_to_kevin"
                }) YIELD nodeIds, totalCost;"#,
        );
        let procedure = report
            .procedures
            .iter()
            .find(|p| p.name.eq_ignore_ascii_case("path.astar"))
            .expect("astar result");
        assert_eq!(procedure.rows.len(), 1);
        let row = &procedure.rows[0];
        let node_ids: Vec<_> = row
            .get("nodeIds")
            .and_then(|value| value.as_array())
            .expect("node ids")
            .iter()
            .filter_map(|value| value.as_str())
            .collect();
        assert_eq!(
            node_ids,
            vec![
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000003",
            ]
        );
        let total_cost = row
            .get("totalCost")
            .and_then(|value| value.as_f64())
            .expect("total cost");
        assert!((total_cost - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn conversion_list_functions() {
        let db = Database::new(InMemoryBackend::new());
        seed_basic_graph(&db);
        let report = execute_script_for_test(
            &db,
            r#"MATCH (p:Person {name: "Meg Ryan"})
            RETURN toIntegerList(['1','2','bad','4']) AS ints,
                   toFloatList(['1.5','oops']) AS floats,
                   toBooleanList([1,0,'true','nope']) AS bools,
                   toStringList([1,true,null]) AS strings;"#,
        );
        let row = &report.result_rows[0];
        assert_eq!(
            row.get("ints")
                .and_then(|v| v.as_array())
                .unwrap()
                .iter()
                .map(|val| val.as_i64())
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), None, Some(4)]
        );
        let floats = row
            .get("floats")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(|val| val.as_f64())
            .collect::<Vec<_>>();
        assert_eq!(floats[0].unwrap() as i32, 1);
        assert!(floats[1].is_none());
        let bools = row.get("bools").and_then(|v| v.as_array()).unwrap();
        assert_eq!(bools[0].as_bool(), Some(true));
        assert_eq!(bools[1].as_bool(), Some(false));
        assert_eq!(bools[2].as_bool(), Some(true));
        assert!(bools[3].is_null());
        let strings = row
            .get("strings")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(|val| val.as_str())
            .collect::<Vec<_>>();
        assert_eq!(strings, vec![Some("1"), Some("true"), None]);
    }
}
