use std::collections::HashMap;

use common::attr::{AttributeContainer, AttributeValue};
use graphdb_core::query::{
    ComparisonOperator, CreatePattern, CreateRelationship, Query, Value, parse_queries,
};
use graphdb_core::{
    Database, Edge, EdgeId, InMemoryBackend, Node, NodeId, SimpleStorage, StorageBackend,
};
use serde::Serialize;

use crate::error::DaemonError;

#[derive(Debug, Default, Serialize)]
pub struct ExecutionReport {
    pub messages: Vec<String>,
    pub selected_nodes: Vec<Node>,
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
        label: _,
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
            if let Ok(uuid) = NodeId::parse_str(s) {
                Ok(uuid)
            } else {
                let parsed = s
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
            if let Ok(uuid) = EdgeId::parse_str(s) {
                Ok(uuid)
            } else {
                let parsed = s
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
    }
}
