use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use common::attr::{AttributeContainer, AttributeValue};
use graphdb_core::query::{PathFilter, PathLength, RelationshipDirection, RelationshipPattern};
use graphdb_core::{Database, Edge, EdgeId, Node, NodeId, StorageBackend};

use crate::error::DaemonError;
use crate::executor::{node_matches_pattern, value_equals_attribute};

const DEFAULT_MAX_HOPS: u32 = 10;
const MAX_RETURNED_PATHS: usize = 64;
const EDGE_LABEL_KEY: &str = "__label";

#[derive(Clone)]
pub(crate) struct PathState {
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) edges: Vec<EdgeId>,
}

pub(crate) fn length_bounds(spec: &PathLength) -> (u32, u32) {
    match spec {
        PathLength::Exact(v) => (*v, *v),
        PathLength::Range { min, max } => {
            let upper = max.unwrap_or(DEFAULT_MAX_HOPS);
            (*min, upper)
        }
    }
}

pub(crate) fn shortest_path<B: StorageBackend>(
    db: &Database<B>,
    starts: &[Arc<Node>],
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

pub(crate) fn enumerate_paths<B: StorageBackend>(
    db: &Database<B>,
    starts: &[Arc<Node>],
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
) -> Result<Vec<(Arc<Edge>, NodeId)>, DaemonError> {
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

pub(crate) fn edge_matches_pattern(edge: &Edge, pattern: &RelationshipPattern) -> bool {
    if let Some(label) = &pattern.label {
        match edge.attributes().get(EDGE_LABEL_KEY) {
            Some(AttributeValue::String(value)) if value == label.as_str() => {}
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
