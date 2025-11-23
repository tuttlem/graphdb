use std::collections::HashMap;

use common::value::FieldValue;
use graphdb_core::query::Value;
use graphdb_core::{Database, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::executor::parse_node_id;
use crate::path::dijkstra::{self, config_map_from_args, parse_dijkstra_config_from_map};

pub(crate) struct AllPairsConfig {
    settings: HashMap<String, Value>,
    explicit_sources: Option<Vec<NodeId>>,
}

pub(crate) fn parse_all_pairs_config(args: Vec<FieldValue>) -> Result<AllPairsConfig, DaemonError> {
    let mut map = config_map_from_args(args, "path.allPairs")?;
    let sources = map
        .remove("sourceNodes")
        .map(|value| parse_node_list(value, "sourceNodes"))
        .transpose()?;
    map.remove("sourceNode");
    map.remove("targetNode");
    Ok(AllPairsConfig {
        settings: map,
        explicit_sources: sources,
    })
}

fn parse_node_list(value: Value, field: &str) -> Result<Vec<NodeId>, DaemonError> {
    match value {
        Value::List(values) => {
            let mut nodes = Vec::with_capacity(values.len());
            for entry in values {
                let id = parse_node_id(&entry)?;
                nodes.push(id);
            }
            Ok(nodes)
        }
        _ => Err(DaemonError::Query(format!(
            "path.allPairs field '{}' must be a list",
            field
        ))),
    }
}

pub(crate) fn run_all_pairs<B: StorageBackend>(
    db: &Database<B>,
    config: &AllPairsConfig,
) -> Result<Vec<HashMap<String, FieldValue>>, DaemonError> {
    let sources = if let Some(explicit) = config.explicit_sources.clone() {
        explicit
    } else {
        db.node_ids()?
    };

    if sources.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    let mut index = 0i64;
    let mut template = config.settings.clone();

    for source in sources {
        template.insert("sourceNode".into(), Value::String(source.to_string()));
        let mut dijkstra_cfg = parse_dijkstra_config_from_map(&template, "path.allPairs")?;
        dijkstra_cfg.target = None;
        let mut per_rows = dijkstra::run_dijkstra(db, &dijkstra_cfg)?;
        for row in per_rows.iter_mut() {
            row.insert("index".into(), FieldValue::Scalar(Value::Integer(index)));
            index += 1;
        }
        rows.extend(per_rows);
        template.remove("sourceNode");
    }

    Ok(rows)
}
