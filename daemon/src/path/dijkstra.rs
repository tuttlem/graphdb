use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use common::attr::{AttributeContainer, AttributeValue};
use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, Edge, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::executor::{field_value_to_scalar, parse_node_id};

#[derive(Clone, Copy)]
struct QueueEntry {
    node: NodeId,
    cost: f64,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && (self.cost - other.cost).abs() < f64::EPSILON
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.total_cmp(&self.cost)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DijkstraDirection {
    Outgoing,
    Incoming,
    Both,
}

pub(crate) struct DijkstraConfig {
    pub(crate) source: NodeId,
    pub(crate) target: Option<NodeId>,
    pub(crate) weight_property: Option<String>,
    pub(crate) relationship_types: Option<HashSet<String>>,
    pub(crate) direction: DijkstraDirection,
    pub(crate) heuristic_property: Option<String>,
}

pub(crate) fn parse_dijkstra_config(args: Vec<FieldValue>) -> Result<DijkstraConfig, DaemonError> {
    if args.is_empty() {
        return Err(DaemonError::Query(
            "path.dijkstra expects at least a configuration argument".into(),
        ));
    }

    let config_field = match args.len() {
        1 => args.into_iter().next().unwrap(),
        2 => args.into_iter().nth(1).unwrap(),
        _ => {
            return Err(DaemonError::Query(
                "path.dijkstra expects one or two arguments".into(),
            ));
        }
    };

    let config_value = field_value_to_scalar(config_field)?;
    let map = match config_value {
        Value::Map(map) => map,
        _ => {
            return Err(DaemonError::Query(
                "path.dijkstra configuration must be a map".into(),
            ));
        }
    };

    let source_value = map
        .get("sourceNode")
        .ok_or_else(|| DaemonError::Query("path.dijkstra requires sourceNode".into()))?;
    let source = parse_node_id(source_value)?;

    let target = if let Some(value) = map.get("targetNode") {
        Some(parse_node_id(value)?)
    } else {
        None
    };

    let weight_property = map
        .get("relationshipWeightProperty")
        .map(|value| match value {
            Value::String(s) => Ok(s.clone()),
            Value::Null => Ok(String::new()),
            _ => Err(DaemonError::Query(
                "relationshipWeightProperty must be STRING".into(),
            )),
        })
        .transpose()?;
    let weight_property = weight_property.filter(|s| !s.is_empty());

    let relationship_types = map
        .get("relationshipTypes")
        .map(|value| match value {
            Value::List(values) => {
                let mut types = HashSet::new();
                for entry in values {
                    match entry {
                        Value::String(name) => {
                            types.insert(name.clone());
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

    let direction = if let Some(value) = map.get("direction") {
        match value {
            Value::String(s) => match s.to_ascii_uppercase().as_str() {
                "OUTGOING" => Ok(DijkstraDirection::Outgoing),
                "INCOMING" => Ok(DijkstraDirection::Incoming),
                "BOTH" => Ok(DijkstraDirection::Both),
                _ => Err(DaemonError::Query(
                    "direction must be OUTGOING, INCOMING, or BOTH".into(),
                )),
            },
            _ => Err(DaemonError::Query("direction must be STRING".into())),
        }?
    } else {
        DijkstraDirection::Outgoing
    };

    let heuristic_property = map
        .get("heuristicProperty")
        .map(|value| match value {
            Value::String(s) => Ok(s.clone()),
            Value::Null => Ok(String::new()),
            _ => Err(DaemonError::Query(
                "heuristicProperty must be STRING".into(),
            )),
        })
        .transpose()?;
    let heuristic_property = heuristic_property.filter(|s| !s.is_empty());

    Ok(DijkstraConfig {
        source,
        target,
        weight_property,
        relationship_types,
        direction,
        heuristic_property,
    })
}

pub(crate) fn run_dijkstra<B: StorageBackend>(
    db: &Database<B>,
    config: &DijkstraConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut distances = HashMap::new();
    distances.insert(config.source, 0.0);
    let mut predecessors: HashMap<NodeId, NodeId> = HashMap::new();
    let mut heap = BinaryHeap::new();
    heap.push(QueueEntry {
        node: config.source,
        cost: 0.0,
    });
    let mut visited = HashSet::new();
    let mut order = Vec::new();

    while let Some(entry) = heap.pop() {
        if visited.contains(&entry.node) {
            continue;
        }
        visited.insert(entry.node);
        order.push(entry.node);

        if config.target == Some(entry.node) {
            break;
        }

        let neighbors = neighbors_for_node(db, entry.node, config)?;
        for (neighbor_id, weight) in neighbors {
            let next_cost = entry.cost + weight;
            if next_cost.is_nan() || next_cost.is_infinite() || next_cost < 0.0 {
                return Err(DaemonError::Query(
                    "path.dijkstra encountered invalid relationship weights".into(),
                ));
            }
            let current_best = distances
                .get(&neighbor_id)
                .copied()
                .unwrap_or(f64::INFINITY);
            if next_cost < current_best - f64::EPSILON {
                distances.insert(neighbor_id, next_cost);
                predecessors.insert(neighbor_id, entry.node);
                heap.push(QueueEntry {
                    node: neighbor_id,
                    cost: next_cost,
                });
            }
        }
    }

    let targets: Vec<NodeId> = if let Some(target) = config.target {
        if distances.contains_key(&target) {
            vec![target]
        } else {
            Vec::new()
        }
    } else {
        order
            .into_iter()
            .filter(|node| *node != config.source && distances.contains_key(node))
            .collect()
    };

    let mut rows = Vec::with_capacity(targets.len());
    for (index, node_id) in targets.into_iter().enumerate() {
        let (path_nodes, path_costs) =
            reconstruct_path(node_id, config.source, &predecessors, &distances)?;
        let mut row = HashMap::new();
        row.insert(
            "index".into(),
            FieldValue::Scalar(Value::Integer(index as i64)),
        );
        row.insert(
            "sourceNode".into(),
            FieldValue::Scalar(Value::String(config.source.to_string())),
        );
        row.insert(
            "targetNode".into(),
            FieldValue::Scalar(Value::String(node_id.to_string())),
        );
        row.insert(
            "totalCost".into(),
            FieldValue::Scalar(Value::Float(*distances.get(&node_id).unwrap_or(&0.0))),
        );
        let node_values = path_nodes
            .iter()
            .map(|id| Value::String(id.to_string()))
            .collect();
        row.insert(
            "nodeIds".into(),
            FieldValue::Scalar(Value::List(node_values)),
        );
        let cost_values = path_costs.iter().map(|cost| Value::Float(*cost)).collect();
        row.insert("costs".into(), FieldValue::Scalar(Value::List(cost_values)));
        rows.push(row);
    }

    Ok(rows)
}

pub(crate) fn neighbors_for_node<B: StorageBackend>(
    db: &Database<B>,
    node_id: NodeId,
    config: &DijkstraConfig,
) -> Result<Vec<(NodeId, f64)>, DaemonError> {
    let edges = db.edges_for_node(node_id)?;
    let mut neighbors = Vec::new();
    for edge in edges {
        match config.direction {
            DijkstraDirection::Outgoing => {
                if edge.source() == node_id {
                    if relationship_allowed(&edge, &config.relationship_types) {
                        let weight = edge_weight(&edge, config.weight_property.as_deref())?;
                        neighbors.push((edge.target(), weight));
                    }
                }
            }
            DijkstraDirection::Incoming => {
                if edge.target() == node_id {
                    if relationship_allowed(&edge, &config.relationship_types) {
                        let weight = edge_weight(&edge, config.weight_property.as_deref())?;
                        neighbors.push((edge.source(), weight));
                    }
                }
            }
            DijkstraDirection::Both => {
                if edge.source() == node_id {
                    if relationship_allowed(&edge, &config.relationship_types) {
                        let weight = edge_weight(&edge, config.weight_property.as_deref())?;
                        neighbors.push((edge.target(), weight));
                    }
                }
                if edge.target() == node_id {
                    if relationship_allowed(&edge, &config.relationship_types) {
                        let weight = edge_weight(&edge, config.weight_property.as_deref())?;
                        neighbors.push((edge.source(), weight));
                    }
                }
            }
        }
    }
    Ok(neighbors)
}

fn relationship_allowed(edge: &Edge, allowed_types: &Option<HashSet<String>>) -> bool {
    if let Some(types) = allowed_types {
        let label = edge.attribute("__label").and_then(|value| match value {
            AttributeValue::String(s) => Some(s.as_str()),
            _ => None,
        });
        if let Some(label) = label {
            types.contains(label)
        } else {
            false
        }
    } else {
        true
    }
}

fn edge_weight(edge: &Edge, property: Option<&str>) -> Result<f64, DaemonError> {
    if let Some(prop) = property {
        match edge.attribute(prop) {
            Some(&AttributeValue::Float(value)) => Ok(value),
            Some(&AttributeValue::Integer(value)) => Ok(value as f64),
            Some(AttributeValue::String(value)) => value
                .parse::<f64>()
                .map_err(|_| DaemonError::Query("relationship weight must be numeric".into())),
            Some(_) => Err(DaemonError::Query(
                "relationship weight must be numeric".into(),
            )),
            None => Err(DaemonError::Query(format!(
                "relationship is missing property '{}'",
                prop
            ))),
        }
    } else {
        Ok(1.0)
    }
}

pub(crate) fn reconstruct_path(
    target: NodeId,
    source: NodeId,
    predecessors: &HashMap<NodeId, NodeId>,
    distances: &HashMap<NodeId, f64>,
) -> Result<(Vec<NodeId>, Vec<f64>), DaemonError> {
    let mut nodes = Vec::new();
    let mut current = target;
    nodes.push(current);
    while current != source {
        current = *predecessors
            .get(&current)
            .ok_or_else(|| DaemonError::Query("failed to reconstruct shortest path".into()))?;
        nodes.push(current);
    }
    nodes.reverse();

    let mut costs = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let cost = if *node == source {
            0.0
        } else {
            *distances
                .get(node)
                .ok_or_else(|| DaemonError::Query("missing cost for path node".into()))?
        };
        costs.push(cost);
    }

    Ok((nodes, costs))
}
