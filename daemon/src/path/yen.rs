use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::path::dijkstra::{
    self, PathDetail, config_map_from_args, parse_dijkstra_config_from_map,
};

pub(crate) struct YenConfig {
    pub(crate) base: dijkstra::DijkstraConfig,
    pub(crate) target: NodeId,
    pub(crate) k: usize,
}

pub(crate) fn parse_yen_config(args: Vec<FieldValue>) -> Result<YenConfig, DaemonError> {
    let map = config_map_from_args(args, "path.yen")?;
    let base = parse_dijkstra_config_from_map(&map, "path.yen")?;
    let target = base
        .target
        .ok_or_else(|| DaemonError::Query("path.yen requires targetNode".into()))?;
    let k_value = map
        .get("k")
        .ok_or_else(|| DaemonError::Query("path.yen requires k".into()))?;
    let k = match k_value {
        Value::Integer(value) if *value > 0 => *value as usize,
        _ => {
            return Err(DaemonError::Query(
                "path.yen k must be a positive integer".into(),
            ));
        }
    };
    Ok(YenConfig { base, target, k })
}

pub(crate) fn run_yen<B: StorageBackend>(
    db: &Database<B>,
    config: &YenConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let first = match dijkstra::shortest_path_detail(db, &config.base, config.target, None, None)? {
        Some(detail) => PathCandidate::from_detail(detail),
        None => return Ok(Vec::new()),
    };

    let mut results = Vec::new();
    let mut seen_paths: HashSet<Vec<NodeId>> = HashSet::new();
    seen_paths.insert(first.nodes.clone());
    results.push(first.clone());

    if config.k == 1 {
        return Ok(paths_to_rows(&results, config));
    }

    let mut candidates = BinaryHeap::new();
    let mut candidate_cache: HashSet<Vec<NodeId>> = HashSet::new();

    for kth in 1..config.k {
        let previous = match results.get(kth - 1) {
            Some(path) => path.clone(),
            None => break,
        };
        let path_len = previous.nodes.len();
        if path_len < 2 {
            break;
        }

        for spur_index in 0..path_len - 1 {
            let spur_node = previous.nodes[spur_index];
            let root_prefix = previous.nodes[..=spur_index].to_vec();
            let mut banned_edges = HashSet::new();
            for path in &results {
                if path.nodes.len() > spur_index && path.nodes[..=spur_index] == root_prefix {
                    let edge = (path.nodes[spur_index], path.nodes[spur_index + 1]);
                    banned_edges.insert(edge);
                }
            }
            let banned_nodes: HashSet<NodeId> = root_prefix[..spur_index].iter().copied().collect();
            let banned_nodes_ref = if banned_nodes.is_empty() {
                None
            } else {
                Some(&banned_nodes)
            };
            let banned_edges_ref = if banned_edges.is_empty() {
                None
            } else {
                Some(&banned_edges)
            };

            let mut spur_config = config.base.clone();
            spur_config.source = spur_node;

            let spur_detail = match dijkstra::shortest_path_detail(
                db,
                &spur_config,
                config.target,
                banned_nodes_ref,
                banned_edges_ref,
            )? {
                Some(detail) => detail,
                None => continue,
            };

            if !spur_detail.nodes.is_empty() && spur_detail.nodes[0] != spur_node {
                continue;
            }

            let candidate = combine_paths(&previous, &spur_detail, spur_index);
            if seen_paths.contains(&candidate.nodes) {
                continue;
            }
            if !candidate_cache.insert(candidate.nodes.clone()) {
                continue;
            }
            candidates.push(candidate);
        }

        if let Some(best) = candidates.pop() {
            candidate_cache.remove(&best.nodes);
            if !seen_paths.insert(best.nodes.clone()) {
                continue;
            }
            results.push(best.clone());
        } else {
            break;
        }
    }

    Ok(paths_to_rows(&results, config))
}

fn combine_paths(root: &PathCandidate, spur: &PathDetail, spur_index: usize) -> PathCandidate {
    let mut nodes = root.nodes[..=spur_index].to_vec();
    nodes.extend(spur.nodes.iter().skip(1).copied());

    let mut costs = root.costs[..=spur_index].to_vec();
    let root_cost = *costs.last().unwrap_or(&0.0);
    for additional in spur.costs.iter().skip(1) {
        costs.push(root_cost + additional);
    }

    PathCandidate { nodes, costs }
}

fn paths_to_rows(paths: &[PathCandidate], config: &YenConfig) -> Vec<HashMap<String, FieldValue>> {
    let mut rows = Vec::with_capacity(paths.len());
    for (index, path) in paths.iter().enumerate() {
        let mut row = HashMap::new();
        row.insert(
            "index".into(),
            FieldValue::Scalar(Value::Integer(index as i64)),
        );
        row.insert(
            "sourceNode".into(),
            FieldValue::Scalar(Value::String(config.base.source.to_string())),
        );
        row.insert(
            "targetNode".into(),
            FieldValue::Scalar(Value::String(config.target.to_string())),
        );
        row.insert(
            "totalCost".into(),
            FieldValue::Scalar(Value::Float(path.total_cost())),
        );
        let node_values = path
            .nodes
            .iter()
            .map(|id| Value::String(id.to_string()))
            .collect();
        row.insert(
            "nodeIds".into(),
            FieldValue::Scalar(Value::List(node_values)),
        );
        let cost_values = path.costs.iter().map(|cost| Value::Float(*cost)).collect();
        row.insert("costs".into(), FieldValue::Scalar(Value::List(cost_values)));
        rows.push(row);
    }
    rows
}

#[derive(Clone)]
struct PathCandidate {
    nodes: Vec<NodeId>,
    costs: Vec<f64>,
}

impl PathCandidate {
    fn from_detail(detail: PathDetail) -> Self {
        Self {
            nodes: detail.nodes,
            costs: detail.costs,
        }
    }

    fn total_cost(&self) -> f64 {
        *self.costs.last().unwrap_or(&0.0)
    }
}

impl PartialEq for PathCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.total_cost().eq(&other.total_cost())
    }
}

impl Eq for PathCandidate {}

impl PartialOrd for PathCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other.total_cost().total_cmp(&self.total_cost())
    }
}
