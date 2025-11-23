use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::path::dijkstra::{
    self, config_map_from_args, parse_dijkstra_config_from_map, reconstruct_path,
};

pub(crate) struct DeltaSteppingConfig {
    pub(crate) base: dijkstra::DijkstraConfig,
    pub(crate) delta: f64,
}

pub(crate) fn parse_delta_config(
    args: Vec<FieldValue>,
) -> Result<DeltaSteppingConfig, DaemonError> {
    let mut map = config_map_from_args(args, "path.deltaStepping")?;
    let delta_value = map
        .remove("delta")
        .ok_or_else(|| DaemonError::Query("path.deltaStepping requires delta".into()))?;
    let delta = match delta_value {
        Value::Float(v) if v.is_finite() && v > 0.0 => v,
        Value::Integer(v) if v > 0 => v as f64,
        _ => {
            return Err(DaemonError::Query(
                "path.deltaStepping delta must be positive".into(),
            ));
        }
    };
    let base = parse_dijkstra_config_from_map(&map, "path.deltaStepping")?;
    Ok(DeltaSteppingConfig { base, delta })
}

pub(crate) fn run_delta<B: StorageBackend>(
    db: &Database<B>,
    config: &DeltaSteppingConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut distances = HashMap::new();
    distances.insert(config.base.source, 0.0);
    let mut predecessors: HashMap<NodeId, NodeId> = HashMap::new();
    let mut buckets: BTreeMap<usize, HashSet<NodeId>> = BTreeMap::new();
    insert_bucket(&mut buckets, 0, config.base.source);

    while let Some((&bucket_id, nodes)) = buckets.iter().next() {
        let mut bucket_nodes = nodes.clone();
        buckets.remove(&bucket_id);

        let mut settled = HashSet::new();
        let mut frontier: VecDeque<NodeId> = bucket_nodes.drain().collect();
        while let Some(node) = frontier.pop_front() {
            if !settled.insert(node) {
                continue;
            }
            let neighbors = dijkstra::neighbors_for_node(db, node, &config.base, None, None)?;
            for (neighbor, weight) in neighbors {
                if weight >= config.delta {
                    continue;
                }
                if relax(node, neighbor, weight, &mut distances, &mut predecessors) {
                    frontier.push_back(neighbor);
                }
            }
        }

        for node in &settled {
            let neighbors = dijkstra::neighbors_for_node(db, *node, &config.base, None, None)?;
            for (neighbor, weight) in neighbors {
                if weight < config.delta {
                    continue;
                }
                if relax(*node, neighbor, weight, &mut distances, &mut predecessors) {
                    let bucket = bucket_index(distances[&neighbor], config.delta);
                    insert_bucket(&mut buckets, bucket, neighbor);
                }
            }
        }
    }

    build_rows(&config.base, &distances, &predecessors)
}

fn relax(
    from: NodeId,
    to: NodeId,
    weight: f64,
    distances: &mut HashMap<NodeId, f64>,
    predecessors: &mut HashMap<NodeId, NodeId>,
) -> bool {
    let from_cost = *distances
        .get(&from)
        .expect("relax called before source distance set");
    let candidate = from_cost + weight;
    let current = distances.get(&to).copied().unwrap_or(f64::INFINITY);
    if candidate + f64::EPSILON < current {
        distances.insert(to, candidate);
        predecessors.insert(to, from);
        true
    } else {
        false
    }
}

fn insert_bucket(buckets: &mut BTreeMap<usize, HashSet<NodeId>>, index: usize, node: NodeId) {
    buckets.entry(index).or_default().insert(node);
}

fn bucket_index(cost: f64, delta: f64) -> usize {
    if cost <= 0.0 {
        0
    } else {
        (cost / delta).floor() as usize
    }
}

fn build_rows(
    config: &dijkstra::DijkstraConfig,
    distances: &HashMap<NodeId, f64>,
    predecessors: &HashMap<NodeId, NodeId>,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut targets = Vec::new();
    if let Some(target) = config.target {
        if distances.contains_key(&target) {
            targets.push(target);
        }
    } else {
        for node in distances.keys() {
            if *node != config.source {
                targets.push(*node);
            }
        }
        targets.sort_by(|a, b| {
            let ca = distances.get(a).copied().unwrap_or(f64::INFINITY);
            let cb = distances.get(b).copied().unwrap_or(f64::INFINITY);
            ca.partial_cmp(&cb).unwrap_or(Ordering::Equal)
        });
    }

    let mut rows = Vec::with_capacity(targets.len());
    for (index, node) in targets.into_iter().enumerate() {
        let (path_nodes, costs) = reconstruct_path(node, config.source, predecessors, distances)?;
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
            FieldValue::Scalar(Value::String(node.to_string())),
        );
        row.insert(
            "totalCost".into(),
            FieldValue::Scalar(Value::Float(*costs.last().unwrap_or(&0.0))),
        );
        let node_values = path_nodes
            .iter()
            .map(|id| Value::String(id.to_string()))
            .collect();
        row.insert(
            "nodeIds".into(),
            FieldValue::Scalar(Value::List(node_values)),
        );
        let cost_values = costs.iter().map(|cost| Value::Float(*cost)).collect();
        row.insert("costs".into(), FieldValue::Scalar(Value::List(cost_values)));
        rows.push(row);
    }
    Ok(rows)
}
