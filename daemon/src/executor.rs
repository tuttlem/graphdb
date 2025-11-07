use std::collections::{HashMap, HashSet, VecDeque};

use common::attr::{AttributeContainer, AttributeValue};
use graphdb_core::query::{
    ComparisonOperator, CreatePattern, CreateRelationship, GraphDbProcedure, PathFilter,
    PathLength, PathMatchQuery, PathQueryMode, PathReturn, Procedure, Properties, Query,
    RelationshipDirection, RelationshipPattern, Value, parse_queries,
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
        Query::Select {
            pattern,
            conditions,
            returns: _,
        } => {
            let nodes = select_nodes(db, &pattern, &conditions)?;
            ctx.selected_nodes = nodes;
            ctx.messages.push("select completed".into());
            Ok(())
        }
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

    if let Some(attr) = node.attribute(&condition.property) {
        matches!(condition.operator, ComparisonOperator::Equals)
            && value_equals_attribute(&condition.value, attr)
    } else {
        false
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
