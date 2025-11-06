use std::collections::HashMap;

use common::attr::{AttributeContainer, AttributeValue};
use graphdb_core::{
    Database, Edge, EdgeId, InMemoryBackend, Node, NodeId,
    query::{ComparisonOperator, Query, Value, parse_queries},
};
use std::sync::Arc;

type ExecResult<T> = Result<T, String>;

#[derive(Default)]
struct QueryContext {
    last_nodes: Vec<Node>,
}

#[test]
fn end_to_end_insert_and_select() {
    let backend = InMemoryBackend::new();
    let db = Database::new(backend);
    let script = r#"
        INSERT NODE (a:Person { id: 1, name: "Alice", city: "Zurich" });
        INSERT NODE (b:Person { id: 2, name: "Bob", city: "Zurich" });
        INSERT EDGE (a:Person { id: 1 })-[r:KNOWS { id: 10, since: 2020 }]->(b:Person { id: 2 });
        SELECT MATCH (n:Person { city: "Zurich" }) WHERE n.id = 1 RETURN n;
    "#;

    let queries = parse_queries(script).expect("parse queries");
    let mut ctx = QueryContext::default();
    for query in queries {
        execute_query(&db, query, &mut ctx).expect("execute query");
    }

    assert_eq!(ctx.last_nodes.len(), 1, "expected single match");
    let node = &ctx.last_nodes[0];
    assert_eq!(node.labels(), &["Person".to_string()]);
    let name_attr = node.attribute("name").expect("name attribute");
    assert!(matches!(name_attr, AttributeValue::String(s) if s == "Alice"));

    let neighbors = db
        .neighbor_ids(NodeId::from_u128(1))
        .expect("neighbors query");
    assert_eq!(neighbors, vec![NodeId::from_u128(2)]);
}

fn execute_query(
    db: &Database<InMemoryBackend>,
    query: Query,
    ctx: &mut QueryContext,
) -> ExecResult<()> {
    match query {
        Query::InsertNode { pattern } => {
            insert_node(db, pattern)?;
            Ok(())
        }
        Query::InsertEdge {
            source,
            edge,
            target,
        } => {
            insert_edge(db, source, edge, target)?;
            Ok(())
        }
        Query::DeleteNode { id } => {
            let node_id = parse_node_id(&Value::String(id))?;
            db.remove_node(node_id).map_err(|e| e.to_string())?;
            Ok(())
        }
        Query::DeleteEdge { id } => {
            let edge_id = parse_edge_id(&Value::String(id))?;
            db.remove_edge(edge_id).map_err(|e| e.to_string())?;
            Ok(())
        }
        Query::Select {
            pattern,
            conditions,
            returns,
        } => {
            let nodes = select_nodes(db, &pattern, &conditions, &returns)?;
            ctx.last_nodes = nodes;
            Ok(())
        }
        Query::Create { .. } => Err("CREATE not supported in executor test".into()),
        Query::UpdateNode { .. } | Query::UpdateEdge { .. } => {
            Err("UPDATE not supported in executor test".into())
        }
        Query::CallProcedure { .. } => Ok(()),
    }
}

fn insert_node(
    db: &Database<InMemoryBackend>,
    pattern: graphdb_core::query::NodePattern,
) -> ExecResult<()> {
    let id_value = pattern
        .properties
        .0
        .get("id")
        .ok_or_else(|| "node id missing".to_string())?
        .clone();
    let node_id = parse_node_id(&id_value)?;
    let labels = pattern
        .label
        .into_iter()
        .map(|label| label.to_string())
        .collect::<Vec<_>>();

    let mut attributes = HashMap::new();
    for (key, value) in pattern.properties.0.iter() {
        attributes.insert(key.clone(), value_to_attribute(value));
    }

    let node = Node::new(node_id, labels, attributes);
    db.insert_node(node).map_err(|e| e.to_string())?;
    Ok(())
}

fn insert_edge(
    db: &Database<InMemoryBackend>,
    source_pattern: graphdb_core::query::NodePattern,
    edge_pattern: graphdb_core::query::EdgePattern,
    target_pattern: graphdb_core::query::NodePattern,
) -> ExecResult<()> {
    let source_id = parse_node_id(
        source_pattern
            .properties
            .0
            .get("id")
            .ok_or_else(|| "source id missing".to_string())?,
    )?;
    let target_id = parse_node_id(
        target_pattern
            .properties
            .0
            .get("id")
            .ok_or_else(|| "target id missing".to_string())?,
    )?;

    let edge_id = parse_edge_id(
        edge_pattern
            .properties
            .0
            .get("id")
            .ok_or_else(|| "edge id missing".to_string())?,
    )?;

    let mut attributes = HashMap::new();
    for (key, value) in edge_pattern.properties.0.iter() {
        attributes.insert(key.clone(), value_to_attribute(value));
    }

    let edge = Edge::new(edge_id, source_id, target_id, attributes);
    db.insert_edge(edge).map_err(|e| e.to_string())?;
    Ok(())
}

fn select_nodes(
    db: &Database<InMemoryBackend>,
    pattern: &graphdb_core::query::NodePattern,
    conditions: &[graphdb_core::query::Condition],
    _returns: &[String],
) -> ExecResult<Vec<Node>> {
    let mut matches = Vec::new();
    let node_ids = db.node_ids().map_err(|e| e.to_string())?;
    for node_id in node_ids {
        if let Some(node_arc) = db.get_node(node_id).map_err(|e| e.to_string())? {
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

fn node_matches_pattern(node: &Arc<Node>, pattern: &graphdb_core::query::NodePattern) -> bool {
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
    node: &Arc<Node>,
    pattern: &graphdb_core::query::NodePattern,
    condition: &graphdb_core::query::Condition,
) -> bool {
    if let Some(alias) = &pattern.alias {
        if alias != &condition.alias {
            return true; // condition for different alias, ignore in this simple executor
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

fn parse_node_id(value: &Value) -> ExecResult<NodeId> {
    match value {
        Value::Integer(i) => Ok(NodeId::from_u128(*i as u128)),
        Value::String(s) => s
            .parse::<u128>()
            .map(NodeId::from_u128)
            .map_err(|_| "invalid node id".to_string()),
        _ => Err("unsupported node id value".into()),
    }
}

fn parse_edge_id(value: &Value) -> ExecResult<EdgeId> {
    match value {
        Value::Integer(i) => Ok(EdgeId::from_u128(*i as u128)),
        Value::String(s) => s
            .parse::<u128>()
            .map(EdgeId::from_u128)
            .map_err(|_| "invalid edge id".to_string()),
        _ => Err("unsupported edge id value".into()),
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
