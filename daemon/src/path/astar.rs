use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use common::attr::{AttributeContainer, AttributeValue};
use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::path::dijkstra::{self, DijkstraConfig};

pub(crate) struct AStarConfig {
    base: DijkstraConfig,
    target: NodeId,
}

pub(crate) fn parse_astar_config(args: Vec<FieldValue>) -> Result<AStarConfig, DaemonError> {
    let base = dijkstra::parse_dijkstra_config(args)?;
    let target = base
        .target
        .ok_or_else(|| DaemonError::Query("path.astar requires targetNode".into()))?;
    Ok(AStarConfig { base, target })
}

pub(crate) fn run_astar<B: StorageBackend>(
    db: &Database<B>,
    config: &AStarConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut open_set = BinaryHeap::new();
    let mut g_scores = HashMap::new();
    let mut predecessors: HashMap<NodeId, NodeId> = HashMap::new();
    let mut heuristic_cache = HashMap::new();

    g_scores.insert(config.base.source, 0.0);
    let start_heuristic = heuristic_cost(
        db,
        config.base.source,
        config.target,
        config.base.heuristic_property.as_deref(),
        &mut heuristic_cache,
    )?;
    open_set.push(AStarEntry {
        node: config.base.source,
        g_cost: 0.0,
        f_cost: start_heuristic,
    });

    while let Some(entry) = open_set.pop() {
        if entry.node == config.target {
            let (nodes, costs) = dijkstra::reconstruct_path(
                config.target,
                config.base.source,
                &predecessors,
                &g_scores,
            )?;
            return Ok(vec![path_row(
                entry.g_cost,
                &nodes,
                &costs,
                config.base.source,
                config.target,
            )]);
        }

        let current_best = match g_scores.get(&entry.node) {
            Some(cost) => *cost,
            None => continue,
        };
        if entry.g_cost > current_best + f64::EPSILON {
            continue;
        }

        let neighbors = dijkstra::neighbors_for_node(db, entry.node, &config.base, None, None)?;
        for (neighbor, weight) in neighbors {
            let tentative_g = current_best + weight;
            if tentative_g.is_nan() || tentative_g.is_infinite() || tentative_g < 0.0 {
                return Err(DaemonError::Query(
                    "path.astar encountered invalid relationship weights".into(),
                ));
            }
            let best_known = g_scores.get(&neighbor).copied().unwrap_or(f64::INFINITY);
            if tentative_g + f64::EPSILON < best_known {
                g_scores.insert(neighbor, tentative_g);
                predecessors.insert(neighbor, entry.node);
                let heuristic = heuristic_cost(
                    db,
                    neighbor,
                    config.target,
                    config.base.heuristic_property.as_deref(),
                    &mut heuristic_cache,
                )?;
                let f_cost = tentative_g + heuristic;
                open_set.push(AStarEntry {
                    node: neighbor,
                    g_cost: tentative_g,
                    f_cost,
                });
            }
        }
    }

    Ok(Vec::new())
}

fn path_row(
    total_cost: f64,
    nodes: &[NodeId],
    costs: &[f64],
    source: NodeId,
    target: NodeId,
) -> HashMap<String, FieldValue> {
    let mut row = HashMap::new();
    row.insert("index".into(), FieldValue::Scalar(Value::Integer(0)));
    row.insert(
        "sourceNode".into(),
        FieldValue::Scalar(Value::String(source.to_string())),
    );
    row.insert(
        "targetNode".into(),
        FieldValue::Scalar(Value::String(target.to_string())),
    );
    row.insert(
        "totalCost".into(),
        FieldValue::Scalar(Value::Float(total_cost)),
    );
    let node_values = nodes
        .iter()
        .map(|id| Value::String(id.to_string()))
        .collect();
    row.insert(
        "nodeIds".into(),
        FieldValue::Scalar(Value::List(node_values)),
    );
    let cost_values = costs.iter().map(|c| Value::Float(*c)).collect();
    row.insert("costs".into(), FieldValue::Scalar(Value::List(cost_values)));
    row
}

fn heuristic_cost<B: StorageBackend>(
    db: &Database<B>,
    node: NodeId,
    target: NodeId,
    property: Option<&str>,
    cache: &mut HashMap<NodeId, f64>,
) -> Result<f64, DaemonError> {
    if node == target {
        return Ok(0.0);
    }
    if let Some(value) = cache.get(&node) {
        return Ok(*value);
    }

    let estimate = if let Some(prop) = property {
        let stored = db
            .get_node(node)?
            .ok_or_else(|| DaemonError::Query("path.astar referenced missing node".into()))?;
        match stored.attribute(prop) {
            Some(AttributeValue::Float(value)) => *value,
            Some(AttributeValue::Integer(value)) => *value as f64,
            Some(AttributeValue::String(value)) => value
                .parse::<f64>()
                .map_err(|_| DaemonError::Query("path.astar heuristic must be numeric".into()))?,
            Some(_) => {
                return Err(DaemonError::Query(
                    "path.astar heuristic must be numeric".into(),
                ));
            }
            None => {
                return Err(DaemonError::Query(format!(
                    "node {} missing heuristic property '{}'",
                    node, prop
                )));
            }
        }
    } else {
        0.0
    };

    if !estimate.is_finite() || estimate < 0.0 {
        return Err(DaemonError::Query(
            "path.astar heuristic must be finite and non-negative".into(),
        ));
    }
    cache.insert(node, estimate);
    Ok(estimate)
}

#[derive(Clone, Copy)]
struct AStarEntry {
    node: NodeId,
    g_cost: f64,
    f_cost: f64,
}

impl PartialEq for AStarEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && (self.f_cost - other.f_cost).abs() < f64::EPSILON
    }
}

impl Eq for AStarEntry {}

impl PartialOrd for AStarEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AStarEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.f_cost.total_cmp(&self.f_cost)
    }
}
