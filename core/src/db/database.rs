use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use crate::storage::StorageBackend;

struct Cache {
    nodes: HashMap<NodeId, Arc<Node>>,
    edges: HashMap<EdgeId, Arc<Edge>>,
    adjacency: HashMap<NodeId, Vec<EdgeId>>,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            adjacency: HashMap::new(),
        }
    }
}

pub struct Database<B: StorageBackend> {
    storage: B,
    cache: RwLock<Cache>,
}

impl<B: StorageBackend> Database<B> {
    pub fn new(storage: B) -> Self {
        Self {
            storage,
            cache: RwLock::new(Cache::default()),
        }
    }

    pub fn insert_node(&self, node: Node) -> Arc<Node> {
        let id = node.id();

        if let Some(existing) = {
            let cache = self.cache.read().expect("cache lock poisoned");
            cache.nodes.get(&id).cloned()
        } {
            return existing;
        }

        let mut cache = self.cache.write().expect("cache lock poisoned");
        if let Some(existing) = cache.nodes.get(&id) {
            return existing.clone();
        }

        let node = Arc::new(node);
        cache.nodes.insert(id, node.clone());
        cache.adjacency.entry(id).or_insert_with(Vec::new);
        node
    }

    pub fn insert_edge(&self, edge: Edge) -> Arc<Edge> {
        let id = edge.id();

        if let Some(existing) = {
            let cache = self.cache.read().expect("cache lock poisoned");
            cache.edges.get(&id).cloned()
        } {
            return existing;
        }

        let source = edge.source();
        let target = edge.target();

        let mut cache = self.cache.write().expect("cache lock poisoned");
        if let Some(existing) = cache.edges.get(&id) {
            return existing.clone();
        }

        let edge = Arc::new(edge);
        cache.edges.insert(id, edge.clone());
        Self::link_edge(&mut cache.adjacency, source, id);
        Self::link_edge(&mut cache.adjacency, target, id);
        edge
    }

    pub fn get_node(&self, id: NodeId) -> Option<Arc<Node>> {
        if let Some(node) = {
            let cache = self.cache.read().expect("cache lock poisoned");
            cache.nodes.get(&id).cloned()
        } {
            return Some(node);
        }

        let node = self.storage.load_node(id)?;
        Some(self.insert_node(node))
    }

    pub fn get_edge(&self, id: EdgeId) -> Option<Arc<Edge>> {
        if let Some(edge) = {
            let cache = self.cache.read().expect("cache lock poisoned");
            cache.edges.get(&id).cloned()
        } {
            return Some(edge);
        }

        let edge = self.storage.load_edge(id)?;
        Some(self.insert_edge(edge))
    }

    pub fn remove_edge(&self, id: EdgeId) -> Option<Arc<Edge>> {
        let mut cache = self.cache.write().expect("cache lock poisoned");
        let edge = cache.edges.remove(&id)?;
        let source = edge.source();
        let target = edge.target();

        Self::unlink_edge(&mut cache.adjacency, source, id);
        Self::unlink_edge(&mut cache.adjacency, target, id);

        Some(edge)
    }

    pub fn remove_node(&self, id: NodeId) -> Option<Arc<Node>> {
        let mut cache = self.cache.write().expect("cache lock poisoned");
        let node = cache.nodes.remove(&id)?;
        if let Some(edges) = cache.adjacency.remove(&id) {
            for edge_id in edges {
                if let Some(edge) = cache.edges.remove(&edge_id) {
                    let other = if edge.source() == id {
                        edge.target()
                    } else {
                        edge.source()
                    };
                    Self::unlink_edge(&mut cache.adjacency, other, edge_id);
                }
            }
        }
        Some(node)
    }

    pub fn contains_node(&self, id: NodeId) -> bool {
        let cache = self.cache.read().expect("cache lock poisoned");
        cache.nodes.contains_key(&id)
    }

    pub fn contains_edge(&self, id: EdgeId) -> bool {
        let cache = self.cache.read().expect("cache lock poisoned");
        cache.edges.contains_key(&id)
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        let cache = self.cache.read().expect("cache lock poisoned");
        cache.nodes.keys().copied().collect()
    }

    pub fn edge_ids(&self) -> Vec<EdgeId> {
        let cache = self.cache.read().expect("cache lock poisoned");
        cache.edges.keys().copied().collect()
    }

    pub fn edges_for_node(&self, node_id: NodeId) -> Vec<Arc<Edge>> {
        let cache = self.cache.read().expect("cache lock poisoned");
        cache
            .adjacency
            .get(&node_id)
            .into_iter()
            .flat_map(|edge_ids| edge_ids.iter())
            .filter_map(|edge_id| cache.edges.get(edge_id).cloned())
            .collect()
    }

    pub fn neighbor_ids(&self, node_id: NodeId) -> Vec<NodeId> {
        let cache = self.cache.read().expect("cache lock poisoned");
        let mut neighbors = HashSet::new();
        if let Some(edge_ids) = cache.adjacency.get(&node_id) {
            for edge_id in edge_ids {
                if let Some(edge) = cache.edges.get(edge_id) {
                    if edge.source() == node_id {
                        neighbors.insert(edge.target());
                    } else {
                        neighbors.insert(edge.source());
                    }
                }
            }
        }
        let mut result: Vec<NodeId> = neighbors.into_iter().collect();
        result.sort();
        result
    }

    fn link_edge(adjacency: &mut HashMap<NodeId, Vec<EdgeId>>, node_id: NodeId, edge_id: EdgeId) {
        let entry = adjacency.entry(node_id).or_insert_with(Vec::new);
        if !entry.contains(&edge_id) {
            entry.push(edge_id);
        }
    }

    fn unlink_edge(adjacency: &mut HashMap<NodeId, Vec<EdgeId>>, node_id: NodeId, edge_id: EdgeId) {
        if let Some(entry) = adjacency.get_mut(&node_id) {
            entry.retain(|candidate| *candidate != edge_id);
            if entry.is_empty() {
                adjacency.remove(&node_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

    use crate::storage::InMemoryBackend;

    fn make_node(id: u128, labels: &[&str]) -> Node {
        let node_id = NodeId::from_u128(id);
        let label_vec = labels.iter().map(|label| label.to_string()).collect();
        Node::new(node_id, label_vec, HashMap::new())
    }

    fn make_edge(id: u128, source: u128, target: u128) -> Edge {
        let edge_id = EdgeId::from_u128(id);
        let source_id = NodeId::from_u128(source);
        let target_id = NodeId::from_u128(target);
        Edge::new(edge_id, source_id, target_id, HashMap::new())
    }

    #[test]
    fn get_node_lazy_loads_from_storage() {
        let node = make_node(1, &["Person"]);
        let node_id = node.id();
        let backend = InMemoryBackend::new();
        backend.insert_node(node);

        let db = Database::new(backend);

        let loaded = db.get_node(node_id).expect("node should load");
        assert_eq!(loaded.id(), node_id);
        assert!(db.contains_node(node_id));
    }

    #[test]
    fn get_edge_populates_adjacency() {
        let backend = InMemoryBackend::new();
        backend.insert_edge(make_edge(1, 2, 3));

        let db = Database::new(backend);
        let edge_id = EdgeId::from_u128(1);
        let source = NodeId::from_u128(2);
        let target = NodeId::from_u128(3);

        let loaded_edge = db.get_edge(edge_id).expect("edge should load");
        assert_eq!(loaded_edge.id(), edge_id);

        let source_edges = db.edges_for_node(source);
        assert_eq!(source_edges.len(), 1);
        assert_eq!(source_edges[0].id(), edge_id);

        let target_edges = db.edges_for_node(target);
        assert_eq!(target_edges.len(), 1);
        assert_eq!(target_edges[0].id(), edge_id);

        let neighbors = db.neighbor_ids(source);
        assert_eq!(neighbors, vec![target]);
    }

    #[test]
    fn insert_node_bypasses_storage() {
        let db = Database::new(InMemoryBackend::new());
        let node = make_node(4, &["City"]);
        let node_id = node.id();

        db.insert_node(node);

        assert!(db.contains_node(node_id));
        assert!(db.get_node(node_id).is_some());
    }

    #[test]
    fn edges_for_node_returns_empty_when_absent() {
        let db = Database::new(InMemoryBackend::new());
        let edges = db.edges_for_node(NodeId::from_u128(99));
        assert!(edges.is_empty());
    }

    #[test]
    fn remove_edge_updates_cache() {
        let db = Database::new(InMemoryBackend::new());
        db.insert_node(make_node(6, &["City"]));
        db.insert_node(make_node(7, &["City"]));
        let edge = make_edge(30, 6, 7);
        let edge_id = edge.id();
        db.insert_edge(edge);

        let removed = db.remove_edge(edge_id).expect("edge removed");
        assert_eq!(removed.id(), edge_id);
        assert!(!db.contains_edge(edge_id));
        assert!(db.edges_for_node(NodeId::from_u128(6)).is_empty());
        assert!(db.edges_for_node(NodeId::from_u128(7)).is_empty());
    }

    #[test]
    fn remove_node_prunes_incident_edges() {
        let db = Database::new(InMemoryBackend::new());
        db.insert_node(make_node(8, &["Person"]));
        db.insert_node(make_node(9, &["Person"]));
        db.insert_edge(make_edge(31, 8, 9));

        let removed = db.remove_node(NodeId::from_u128(8)).expect("node removed");
        assert_eq!(removed.id(), NodeId::from_u128(8));
        assert!(!db.contains_node(NodeId::from_u128(8)));
        assert!(!db.contains_edge(EdgeId::from_u128(31)));
        assert!(db.edges_for_node(NodeId::from_u128(9)).is_empty());
    }

    #[test]
    fn concurrent_reads_and_writes_do_not_panic() {
        let backend = InMemoryBackend::new();
        backend.insert_node(make_node(10, &["Person"]));
        backend.insert_edge(make_edge(20, 10, 11));

        let db = Arc::new(Database::new(backend));
        let node_id = NodeId::from_u128(10);
        let edge_id = EdgeId::from_u128(20);

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..50 {
                        let _ = db.get_node(node_id);
                        let _ = db.get_edge(edge_id);
                    }
                })
            })
            .collect();

        let writer = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                db.insert_node(make_node(11, &["Person"]));
                db.insert_edge(make_edge(21, 10, 11));
            })
        };

        for handle in readers {
            handle.join().expect("reader thread panicked");
        }
        writer.join().expect("writer thread panicked");

        assert!(db.contains_node(NodeId::from_u128(11)));
        assert!(db.contains_edge(EdgeId::from_u128(21)));
    }
}
