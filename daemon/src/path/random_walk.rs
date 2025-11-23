use std::collections::{HashMap, HashSet};

use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use crate::error::DaemonError;
use crate::executor::parse_node_id;
use crate::path::dijkstra::{self, DijkstraConfig, DijkstraDirection};

pub(crate) struct RandomWalkConfig {
    pub(crate) source: NodeId,
    pub(crate) steps: usize,
    pub(crate) relationship_types: Option<HashSet<String>>,
    pub(crate) direction: DijkstraDirection,
    pub(crate) seed: Option<u64>,
}

pub(crate) fn parse_random_walk_config(
    args: Vec<FieldValue>,
) -> Result<RandomWalkConfig, DaemonError> {
    let mut map = dijkstra::config_map_from_args(args, "path.randomWalk")?;
    let source_value = map
        .remove("sourceNode")
        .ok_or_else(|| DaemonError::Query("path.randomWalk requires sourceNode".into()))?;
    let source = parse_node_id(&source_value)?;

    let steps_value = map
        .remove("steps")
        .ok_or_else(|| DaemonError::Query("path.randomWalk requires steps".into()))?;
    let steps = match steps_value {
        Value::Integer(value) if value >= 0 => value as usize,
        Value::Float(value) if value >= 0.0 => value as usize,
        _ => {
            return Err(DaemonError::Query(
                "path.randomWalk steps must be a non-negative number".into(),
            ));
        }
    };

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

    let direction = map
        .remove("direction")
        .map(|value| match value {
            Value::String(raw) => match raw.to_ascii_uppercase().as_str() {
                "OUTGOING" => Ok(DijkstraDirection::Outgoing),
                "INCOMING" => Ok(DijkstraDirection::Incoming),
                "BOTH" => Ok(DijkstraDirection::Both),
                _ => Err(DaemonError::Query(
                    "direction must be OUTGOING, INCOMING, or BOTH".into(),
                )),
            },
            _ => Err(DaemonError::Query("direction must be STRING".into())),
        })
        .transpose()?
        .unwrap_or(DijkstraDirection::Outgoing);

    let seed = map
        .remove("seed")
        .map(|value| match value {
            Value::Integer(v) if v >= 0 => Ok(v as u64),
            Value::Float(v) if v >= 0.0 => Ok(v as u64),
            _ => Err(DaemonError::Query(
                "seed must be non-negative number".into(),
            )),
        })
        .transpose()?;

    Ok(RandomWalkConfig {
        source,
        steps,
        relationship_types,
        direction,
        seed,
    })
}

pub(crate) fn run_random_walk<B: StorageBackend>(
    db: &Database<B>,
    config: &RandomWalkConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let mut rng: Box<dyn RngCore> = if let Some(seed) = config.seed {
        Box::new(StdRng::seed_from_u64(seed))
    } else {
        Box::new(rand::thread_rng())
    };

    let mut visited = Vec::with_capacity(config.steps + 1);
    visited.push(config.source);
    let mut current = config.source;
    for _ in 0..config.steps {
        let neighbors = neighbor_ids(db, current, config)?;
        if neighbors.is_empty() {
            break;
        }
        let idx = rng.gen_range(0..neighbors.len());
        current = neighbors[idx];
        visited.push(current);
    }

    let node_values = visited
        .iter()
        .map(|id| Value::String(id.to_string()))
        .collect();
    let mut row = HashMap::new();
    row.insert("index".into(), FieldValue::Scalar(Value::Integer(0)));
    row.insert(
        "nodeIds".into(),
        FieldValue::Scalar(Value::List(node_values)),
    );
    row.insert(
        "length".into(),
        FieldValue::Scalar(Value::Integer((visited.len().saturating_sub(1)) as i64)),
    );
    Ok(vec![row])
}

fn neighbor_ids<B: StorageBackend>(
    db: &Database<B>,
    node: NodeId,
    config: &RandomWalkConfig,
) -> Result<Vec<NodeId>, DaemonError> {
    let temp = DijkstraConfig {
        source: node,
        target: None,
        weight_property: None,
        relationship_types: config.relationship_types.clone(),
        direction: config.direction,
        heuristic_property: None,
    };
    let neighbors = dijkstra::neighbors_for_node(db, node, &temp, None, None)?;
    Ok(neighbors
        .into_iter()
        .map(|(neighbor, _)| neighbor)
        .collect())
}
