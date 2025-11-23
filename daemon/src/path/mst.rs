use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use common::edge::EdgeId;
use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::executor::parse_node_id;
use crate::path::dijkstra::{config_map_from_args, edge_weight, relationship_allowed};

pub(crate) struct SpanningTreeConfig {
    pub(crate) start: Option<NodeId>,
    pub(crate) allowed_nodes: Option<HashSet<NodeId>>,
    pub(crate) relationship_types: Option<HashSet<String>>,
    pub(crate) weight_property: Option<String>,
}

pub(crate) fn parse_spanning_tree_config(
    args: Vec<FieldValue>,
) -> Result<SpanningTreeConfig, DaemonError> {
    let mut map = config_map_from_args(args, "path.spanningTree")?;
    let start = map
        .remove("startNode")
        .map(|value| parse_node_id(&value))
        .transpose()?;
    let allowed_nodes = map
        .remove("nodeIds")
        .map(|value| match value {
            Value::List(values) => {
                let mut set = HashSet::new();
                for entry in values {
                    set.insert(parse_node_id(&entry)?);
                }
                Ok(set)
            }
            _ => Err(DaemonError::Query(
                "path.spanningTree nodeIds must be list".into(),
            )),
        })
        .transpose()?;

    let relationship_types = map
        .remove("relationshipTypes")
        .map(|value| match value {
            Value::List(values) => {
                let mut types = HashSet::new();
                for entry in values {
                    match entry {
                        Value::String(name) => {
                            types.insert(name);
                        }
                        _ => {
                            return Err(DaemonError::Query(
                                "relationshipTypes must be list of STRING".into(),
                            ));
                        }
                    }
                }
                Ok(types)
            }
            _ => Err(DaemonError::Query(
                "relationshipTypes must be a list".into(),
            )),
        })
        .transpose()?;

    let weight_property = map
        .remove("relationshipWeightProperty")
        .map(|value| match value {
            Value::String(prop) => Ok(Some(prop)),
            Value::Null => Ok(None),
            _ => Err(DaemonError::Query(
                "relationshipWeightProperty must be STRING".into(),
            )),
        })
        .transpose()?
        .unwrap_or(None);

    Ok(SpanningTreeConfig {
        start,
        allowed_nodes,
        relationship_types,
        weight_property,
    })
}

pub(crate) fn run_spanning_tree<B: StorageBackend>(
    db: &Database<B>,
    config: &SpanningTreeConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut candidate_nodes: HashSet<NodeId> = if let Some(nodes) = &config.allowed_nodes {
        nodes.clone()
    } else {
        db.node_ids()?.into_iter().collect()
    };

    if candidate_nodes.is_empty() {
        return Ok(Vec::new());
    }

    let start = config
        .start
        .or_else(|| candidate_nodes.iter().next().copied())
        .ok_or_else(|| DaemonError::Query("graph has no nodes".into()))?;

    if !candidate_nodes.contains(&start) {
        candidate_nodes.insert(start);
    }

    let mut in_tree = HashSet::new();
    in_tree.insert(start);

    let mut heap = BinaryHeap::new();
    push_edges(db, start, config, &in_tree, &candidate_nodes, &mut heap)?;

    let mut rows = Vec::new();
    let mut index = 0i64;

    while let Some(edge) = heap.pop() {
        if in_tree.contains(&edge.to) {
            continue;
        }
        in_tree.insert(edge.to);
        rows.push(edge.to_row(index));
        index += 1;
        push_edges(db, edge.to, config, &in_tree, &candidate_nodes, &mut heap)?;
    }

    Ok(rows)
}

fn push_edges<B: StorageBackend>(
    db: &Database<B>,
    node: NodeId,
    config: &SpanningTreeConfig,
    in_tree: &HashSet<NodeId>,
    allowed_nodes: &HashSet<NodeId>,
    heap: &mut BinaryHeap<MstEdge>,
) -> Result<(), DaemonError> {
    for edge in db.edges_for_node(node)? {
        if !relationship_allowed(&edge, &config.relationship_types) {
            continue;
        }
        let neighbor = if edge.source() == node {
            edge.target()
        } else if edge.target() == node {
            edge.source()
        } else {
            continue;
        };
        if in_tree.contains(&neighbor) {
            continue;
        }
        if !allowed_nodes.is_empty() && !allowed_nodes.contains(&neighbor) {
            continue;
        }
        let weight = edge_weight(&edge, config.weight_property.as_deref())?;
        heap.push(MstEdge {
            from: node,
            to: neighbor,
            weight,
            edge_id: edge.id(),
        });
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct MstEdge {
    from: NodeId,
    to: NodeId,
    weight: f64,
    edge_id: EdgeId,
}

impl MstEdge {
    fn to_row(&self, index: i64) -> HashMap<String, FieldValue> {
        let mut row = HashMap::new();
        row.insert("index".into(), FieldValue::Scalar(Value::Integer(index)));
        row.insert(
            "sourceNode".into(),
            FieldValue::Scalar(Value::String(self.from.to_string())),
        );
        row.insert(
            "targetNode".into(),
            FieldValue::Scalar(Value::String(self.to.to_string())),
        );
        row.insert(
            "edgeId".into(),
            FieldValue::Scalar(Value::String(self.edge_id.to_string())),
        );
        row.insert(
            "weight".into(),
            FieldValue::Scalar(Value::Float(self.weight)),
        );
        row
    }
}

impl PartialEq for MstEdge {
    fn eq(&self, other: &Self) -> bool {
        self.weight == other.weight && self.edge_id == other.edge_id
    }
}

impl Eq for MstEdge {}

impl PartialOrd for MstEdge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MstEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .weight
            .total_cmp(&self.weight)
            .then_with(|| self.edge_id.as_u128().cmp(&other.edge_id.as_u128()))
    }
}
