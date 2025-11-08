use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use common::attr::{AttributeContainer, AttributeValue};
use graphdb_core::query::{
    AggregateExpression, AggregateFunction, ComparisonOperator, CreatePattern, CreateRelationship,
    Expression, FieldReference, GraphDbProcedure, PathFilter, PathLength, PathMatchQuery,
    PathQueryMode, PathReturn, Procedure, Projection, Properties, Query, RelationshipDirection,
    RelationshipPattern, SelectQuery, Value, WithClause, parse_queries,
};
use graphdb_core::{
    AuthMethod, Database, Edge, EdgeClassEntry, EdgeId, InMemoryBackend, Node, NodeClassEntry,
    NodeId, RoleEntry, SimpleStorage, StorageBackend, UserEntry,
};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};

use crate::error::DaemonError;

#[derive(Debug, Default, Serialize)]
pub struct ExecutionReport {
    pub messages: Vec<String>,
    pub selected_nodes: Vec<Node>,
    pub procedures: Vec<ProcedureResult>,
    pub paths: Vec<PathResult>,
    pub path_pairs: Vec<PathPairResult>,
    pub result_rows: Vec<JsonValue>,
}

#[derive(Clone, Default)]
struct QueryRow {
    nodes: HashMap<String, Node>,
    scalars: HashMap<String, Value>,
}

impl QueryRow {
    fn from_node(alias: &str, node: Node) -> Self {
        let mut row = QueryRow::default();
        row.nodes.insert(alias.to_string(), node);
        row
    }
}

#[derive(Clone)]
enum FieldValue {
    Node(Node),
    Scalar(Value),
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
    let alias = query
        .pattern
        .alias
        .clone()
        .ok_or_else(|| DaemonError::Query("MATCH requires an alias".into()))?;

    let nodes = select_nodes(db, &query.pattern, &query.conditions)?;
    let mut rows: Vec<QueryRow> = nodes
        .into_iter()
        .map(|node| QueryRow::from_node(&alias, node))
        .collect();

    if let Some(with_clause) = query.with.as_ref() {
        rows = apply_with_clause(rows, with_clause)?;
    }

    let has_aggregate = query
        .returns
        .iter()
        .any(|projection| matches!(projection.expression, Expression::Aggregate(_)));
    if has_aggregate
        && query
            .returns
            .iter()
            .any(|projection| !matches!(projection.expression, Expression::Aggregate(_)))
    {
        return Err(DaemonError::Query(
            "cannot mix aggregate and non-aggregate RETURN items".into(),
        ));
    }

    if has_aggregate {
        let row = evaluate_aggregates(&rows, &query.returns)?;
        ctx.result_rows = vec![row];
        ctx.selected_nodes.clear();
    } else {
        let json_rows = project_rows(&rows, &query.returns)?;
        ctx.result_rows = json_rows;
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
                    .all(|cond| node_matches_condition(&node_arc, pattern, cond))
            {
                matches.push((*node_arc).clone());
            }
        }
    }
    Ok(matches)
}

fn node_matches_pattern(
    node: &std::sync::Arc<Node>,
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

fn node_matches_condition(
    node: &std::sync::Arc<Node>,
    pattern: &graphdb_core::query::NodePattern,
    condition: &graphdb_core::query::Condition,
) -> bool {
    if let Some(alias) = &pattern.alias {
        if alias != &condition.alias {
            return true;
        }
    }

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

fn value_equals_attribute(value: &Value, attribute: &AttributeValue) -> bool {
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
        _ => false,
    }
}

fn compare_query_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn collect_nodes(rows: &[QueryRow], alias: &str) -> Vec<Node> {
    rows.iter()
        .filter_map(|row| row.nodes.get(alias).cloned())
        .collect()
}

fn apply_with_clause(
    rows: Vec<QueryRow>,
    clause: &WithClause,
) -> Result<Vec<QueryRow>, DaemonError> {
    let mut projected = Vec::with_capacity(rows.len());
    for row in rows {
        let mut next = QueryRow::default();
        for projection in &clause.projections {
            let value = match &projection.expression {
                Expression::Field(field) => resolve_field(&row, field)?,
                Expression::Aggregate(_) => {
                    return Err(DaemonError::Query(
                        "aggregate expressions are not supported in WITH clauses".into(),
                    ));
                }
            };
            let name = projection_label(projection);
            match value {
                FieldValue::Node(node) => {
                    next.nodes.insert(name, node);
                }
                FieldValue::Scalar(val) => {
                    next.scalars.insert(name, val);
                }
            }
        }
        projected.push(next);
    }
    Ok(projected)
}

fn project_rows(
    rows: &[QueryRow],
    projections: &[Projection],
) -> Result<Vec<JsonValue>, DaemonError> {
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let mut map = serde_json::Map::new();
        for projection in projections {
            let value = match &projection.expression {
                Expression::Field(field) => resolve_field(row, field)?,
                Expression::Aggregate(_) => {
                    return Err(DaemonError::Query(
                        "unexpected aggregate in projection".into(),
                    ));
                }
            };
            let json_value = match value {
                FieldValue::Node(node) => serde_json::to_value(node).map_err(|err| {
                    DaemonError::Query(format!("failed to serialize node: {err}"))
                })?,
                FieldValue::Scalar(val) => value_to_json(&val),
            };
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
            Expression::Field(_) => {
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

fn projection_label(projection: &Projection) -> String {
    if let Some(alias) = &projection.alias {
        return alias.clone();
    }

    match &projection.expression {
        Expression::Field(field) => field_label(field),
        Expression::Aggregate(agg) => aggregate_label(agg),
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

    if let Some(node) = row.nodes.get(&field.alias) {
        if let Some(property) = &field.property {
            let attr = node.attribute(property);
            let value = attr.map(attribute_to_query_value).unwrap_or(Value::Null);
            return Ok(FieldValue::Scalar(value));
        }
        return Ok(FieldValue::Node(node.clone()));
    }

    Err(DaemonError::Query(format!(
        "unknown alias '{}'",
        field.alias
    )))
}

fn attribute_to_query_value(attr: &AttributeValue) -> Value {
    match attr {
        AttributeValue::String(s) => Value::String(s.clone()),
        AttributeValue::Integer(i) => Value::Integer(*i),
        AttributeValue::Float(f) => Value::Float(*f),
        AttributeValue::Boolean(b) => Value::Boolean(*b),
        AttributeValue::Null => Value::Null,
        AttributeValue::List(items) => {
            Value::List(items.iter().map(attribute_to_query_value).collect())
        }
    }
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
    let name = procedure.canonical_name().to_string();
    let rows = match procedure {
        Procedure::GraphDb(graph_proc) => match graph_proc {
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
        },
    };

    Ok(ProcedureResult { name, rows })
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
                    FieldValue::Scalar(_) | FieldValue::Node(_) => total += 1,
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
                    FieldValue::Node(node) => {
                        let serialized = serde_json::to_string(&node).map_err(|err| {
                            DaemonError::Query(format!(
                                "failed to serialize node for collect: {err}"
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
        Value::List(_) => 1,
        Value::Null => 0,
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::String(s) => json!(s),
        Value::Integer(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::Boolean(b) => json!(b),
        Value::Null => JsonValue::Null,
        Value::List(values) => JsonValue::Array(values.iter().map(value_to_json).collect()),
    }
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

fn parse_node_id(value: &Value) -> Result<NodeId, DaemonError> {
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
    }
}

const DEFAULT_MAX_HOPS: u32 = 10;
const MAX_RETURNED_PATHS: usize = 64;
const EDGE_LABEL_KEY: &str = "__label";

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
    let length_bounds = length_bounds(&query.relationship.length);
    let filter = query.filter.clone();

    let paths = match query.mode {
        PathQueryMode::Shortest => shortest_path(
            db,
            &start_nodes,
            &start_ids,
            &end_set,
            &query.relationship,
            length_bounds,
            filter.as_ref(),
            &query.start_alias,
        )?,
        PathQueryMode::All => enumerate_paths(
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

#[derive(Clone)]
struct PathState {
    nodes: Vec<NodeId>,
    edges: Vec<EdgeId>,
}

fn length_bounds(spec: &PathLength) -> (u32, u32) {
    match spec {
        PathLength::Exact(v) => (*v, *v),
        PathLength::Range { min, max } => {
            let upper = max.unwrap_or(DEFAULT_MAX_HOPS);
            (*min, upper)
        }
    }
}

fn shortest_path<B: StorageBackend>(
    db: &Database<B>,
    starts: &[std::sync::Arc<Node>],
    start_ids: &HashSet<NodeId>,
    end_set: &HashSet<NodeId>,
    relationship: &RelationshipPattern,
    (min_hops, max_hops): (u32, u32),
    filter: Option<&PathFilter>,
    start_alias: &str,
) -> Result<Vec<PathState>, DaemonError> {
    let mut queue = VecDeque::new();
    let mut visited: HashSet<NodeId> = HashSet::new();
    for node in starts {
        queue.push_back(PathState {
            nodes: vec![node.id()],
            edges: Vec::new(),
        });
        visited.insert(node.id());
    }

    while let Some(state) = queue.pop_front() {
        let depth = state.edges.len() as u32;
        let current = *state.nodes.last().unwrap();
        if depth >= min_hops && end_set.contains(&current) {
            return Ok(vec![state]);
        }
        if depth >= max_hops {
            continue;
        }
        for (edge, next) in
            relationship_neighbors(db, current, relationship, start_ids, filter, start_alias)?
        {
            if state.nodes.contains(&next) {
                continue;
            }
            if !visited.insert(next) {
                continue;
            }
            let mut next_state = PathState {
                nodes: state.nodes.clone(),
                edges: state.edges.clone(),
            };
            next_state.nodes.push(next);
            next_state.edges.push(edge.id());
            queue.push_back(next_state);
        }
    }

    Ok(Vec::new())
}

fn enumerate_paths<B: StorageBackend>(
    db: &Database<B>,
    starts: &[std::sync::Arc<Node>],
    start_ids: &HashSet<NodeId>,
    end_set: &HashSet<NodeId>,
    relationship: &RelationshipPattern,
    (min_hops, max_hops): (u32, u32),
    filter: Option<&PathFilter>,
    start_alias: &str,
) -> Result<Vec<PathState>, DaemonError> {
    let mut results = Vec::new();
    for node in starts {
        let mut state = PathState {
            nodes: vec![node.id()],
            edges: Vec::new(),
        };
        dfs_paths(
            db,
            &mut state,
            start_ids,
            end_set,
            relationship,
            min_hops,
            max_hops,
            &mut results,
            filter,
            start_alias,
        )?;
        if results.len() >= MAX_RETURNED_PATHS {
            break;
        }
    }
    Ok(results)
}

fn dfs_paths<B: StorageBackend>(
    db: &Database<B>,
    state: &mut PathState,
    start_ids: &HashSet<NodeId>,
    end_set: &HashSet<NodeId>,
    relationship: &RelationshipPattern,
    min_hops: u32,
    max_hops: u32,
    results: &mut Vec<PathState>,
    filter: Option<&PathFilter>,
    start_alias: &str,
) -> Result<(), DaemonError> {
    let depth = state.edges.len() as u32;
    let current = *state.nodes.last().unwrap();
    if depth >= min_hops && end_set.contains(&current) {
        results.push(state.clone());
    }
    if depth >= max_hops || results.len() >= MAX_RETURNED_PATHS {
        return Ok(());
    }
    for (edge, next) in
        relationship_neighbors(db, current, relationship, start_ids, filter, start_alias)?
    {
        if state.nodes.contains(&next) {
            continue;
        }
        state.nodes.push(next);
        state.edges.push(edge.id());
        dfs_paths(
            db,
            state,
            start_ids,
            end_set,
            relationship,
            min_hops,
            max_hops,
            results,
            filter,
            start_alias,
        )?;
        state.nodes.pop();
        state.edges.pop();
        if results.len() >= MAX_RETURNED_PATHS {
            break;
        }
    }
    Ok(())
}

fn relationship_neighbors<B: StorageBackend>(
    db: &Database<B>,
    node_id: NodeId,
    pattern: &RelationshipPattern,
    start_ids: &HashSet<NodeId>,
    filter: Option<&PathFilter>,
    start_alias: &str,
) -> Result<Vec<(std::sync::Arc<Edge>, NodeId)>, DaemonError> {
    let edges = db.edges_for_node(node_id)?;
    let mut matches = Vec::new();
    for edge in edges {
        match pattern.direction {
            RelationshipDirection::Outbound => {
                if edge.source() != node_id {
                    continue;
                }
                if !edge_matches_pattern(&edge, pattern) {
                    continue;
                }
                if should_skip_edge(
                    db,
                    node_id,
                    edge.target(),
                    &edge,
                    start_ids,
                    filter,
                    start_alias,
                )? {
                    continue;
                }
                matches.push((edge.clone(), edge.target()));
            }
            RelationshipDirection::Inbound => {
                if edge.target() != node_id {
                    continue;
                }
                if !edge_matches_pattern(&edge, pattern) {
                    continue;
                }
                if should_skip_edge(
                    db,
                    node_id,
                    edge.source(),
                    &edge,
                    start_ids,
                    filter,
                    start_alias,
                )? {
                    continue;
                }
                matches.push((edge.clone(), edge.source()));
            }
            RelationshipDirection::Undirected => {
                if edge_matches_pattern(&edge, pattern) {
                    let next = if edge.source() == node_id {
                        edge.target()
                    } else {
                        edge.source()
                    };
                    if should_skip_edge(db, node_id, next, &edge, start_ids, filter, start_alias)? {
                        continue;
                    }
                    matches.push((edge.clone(), next));
                }
            }
        }
    }
    Ok(matches)
}

fn edge_matches_pattern(edge: &Edge, pattern: &RelationshipPattern) -> bool {
    if let Some(label) = &pattern.label {
        match edge.attributes().get(EDGE_LABEL_KEY) {
            Some(AttributeValue::String(value)) if value == label => {}
            Some(AttributeValue::String(_)) => return false,
            _ => return false,
        }
    }
    for (key, value) in pattern.properties.0.iter() {
        match edge.attributes().get(key) {
            Some(attr) if value_equals_attribute(value, attr) => {}
            _ => return false,
        }
    }
    true
}

fn should_skip_edge<B: StorageBackend>(
    db: &Database<B>,
    current: NodeId,
    next: NodeId,
    edge: &Edge,
    start_ids: &HashSet<NodeId>,
    filter: Option<&PathFilter>,
    start_alias: &str,
) -> Result<bool, DaemonError> {
    if let Some(PathFilter::ExcludeRelationship {
        from_alias,
        relationship,
        to_pattern,
    }) = filter
    {
        if from_alias == start_alias && start_ids.contains(&current) {
            if edge_matches_pattern(edge, relationship) {
                if let Some(neighbor) = db.get_node(next)? {
                    if node_matches_pattern(&neighbor, to_pattern) {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
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
    use graphdb_core::storage::memory::InMemoryBackend;
    use std::collections::HashMap;

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
    for (name, id) in people.iter() {
        insert_node(
            db,
            graphdb_core::query::NodePattern {
                alias: Some(name.split_whitespace().next().unwrap().to_lowercase()),
                label: Some("Person".into()),
                properties: Properties::new(HashMap::from([
                    ("id".into(), Value::String(id.to_string())),
                    ("name".into(), Value::String(name.to_string())),
                ])),
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
