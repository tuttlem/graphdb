use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use crate::storage::{StorageBackend, StorageError, StorageOp, StorageResult};

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
    const POISONED_READ: &'static str = "cache read";
    const POISONED_WRITE: &'static str = "cache write";

    pub fn new(storage: B) -> Self {
        Self {
            storage,
            cache: RwLock::new(Cache::default()),
        }
    }

    pub fn storage(&self) -> &B {
        &self.storage
    }

    pub fn insert_node(&self, node: Node) -> StorageResult<Arc<Node>> {
        self.storage.store_node(&node)?;
        self.cache_node(node)
    }

    pub fn insert_edge(&self, edge: Edge) -> StorageResult<Arc<Edge>> {
        self.storage.store_edge(&edge)?;
        self.cache_edge(edge)
    }

    pub fn get_node(&self, id: NodeId) -> StorageResult<Option<Arc<Node>>> {
        if let Some(node) = self.lookup_node(id)? {
            return Ok(Some(node));
        }

        let Some(node) = self.storage.load_node(id)? else {
            return Ok(None);
        };

        self.cache_node(node).map(Some)
    }

    pub fn get_edge(&self, id: EdgeId) -> StorageResult<Option<Arc<Edge>>> {
        if let Some(edge) = self.lookup_edge(id)? {
            return Ok(Some(edge));
        }

        let Some(edge) = self.storage.load_edge(id)? else {
            return Ok(None);
        };

        self.cache_edge(edge).map(Some)
    }

    pub fn remove_edge(&self, id: EdgeId) -> StorageResult<Option<Arc<Edge>>> {
        let edge = {
            let mut cache = self.cache_write_guard("remove edge")?;
            let edge = match cache.edges.remove(&id) {
                Some(edge) => edge,
                None => return Ok(None),
            };

            let source = edge.source();
            let target = edge.target();
            Self::unlink_edge(&mut cache.adjacency, source, id);
            Self::unlink_edge(&mut cache.adjacency, target, id);
            edge
        };

        self.storage.delete_edge(id)?;
        Ok(Some(edge))
    }

    pub fn remove_node(&self, id: NodeId) -> StorageResult<Option<Arc<Node>>> {
        let (node, incident_edges) = {
            let mut cache = self.cache_write_guard("remove node")?;
            let node = match cache.nodes.remove(&id) {
                Some(node) => node,
                None => return Ok(None),
            };

            let incident_edges = cache.adjacency.remove(&id).unwrap_or_default();
            for edge_id in &incident_edges {
                if let Some(edge) = cache.edges.remove(edge_id) {
                    let other = if edge.source() == id {
                        edge.target()
                    } else {
                        edge.source()
                    };
                    Self::unlink_edge(&mut cache.adjacency, other, *edge_id);
                }
            }

            (node, incident_edges)
        };

        for edge_id in incident_edges {
            self.storage.delete_edge(edge_id)?;
        }
        self.storage.delete_node(id)?;

        Ok(Some(node))
    }

    pub fn contains_node(&self, id: NodeId) -> StorageResult<bool> {
        let cache = self.cache_read_guard("contains node")?;
        Ok(cache.nodes.contains_key(&id))
    }

    pub fn contains_edge(&self, id: EdgeId) -> StorageResult<bool> {
        let cache = self.cache_read_guard("contains edge")?;
        Ok(cache.edges.contains_key(&id))
    }

    pub fn evict_node(&self, id: NodeId) -> StorageResult<bool> {
        let mut cache = self.cache_write_guard("evict node")?;
        Ok(cache.nodes.remove(&id).is_some())
    }

    pub fn evict_edge(&self, id: EdgeId) -> StorageResult<bool> {
        let mut cache = self.cache_write_guard("evict edge")?;
        Ok(cache.edges.remove(&id).is_some())
    }

    pub fn node_ids(&self) -> StorageResult<Vec<NodeId>> {
        let cache = self.cache_read_guard("node ids")?;
        Ok(cache.nodes.keys().copied().collect())
    }

    pub fn edge_ids(&self) -> StorageResult<Vec<EdgeId>> {
        let cache = self.cache_read_guard("edge ids")?;
        Ok(cache.edges.keys().copied().collect())
    }

    pub fn edges_for_node(&self, node_id: NodeId) -> StorageResult<Vec<Arc<Edge>>> {
        let cache = self.cache_read_guard("edges for node")?;
        let edges = cache
            .adjacency
            .get(&node_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|edge_id| cache.edges.get(edge_id).cloned())
            .collect();
        Ok(edges)
    }

    pub fn neighbor_ids(&self, node_id: NodeId) -> StorageResult<Vec<NodeId>> {
        let cache = self.cache_read_guard("neighbor ids")?;
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
        Ok(result)
    }

    fn lookup_node(&self, id: NodeId) -> StorageResult<Option<Arc<Node>>> {
        let cache = self.cache_read_guard("lookup node")?;
        Ok(cache.nodes.get(&id).cloned())
    }

    fn lookup_edge(&self, id: EdgeId) -> StorageResult<Option<Arc<Edge>>> {
        let cache = self.cache_read_guard("lookup edge")?;
        Ok(cache.edges.get(&id).cloned())
    }

    fn cache_node(&self, node: Node) -> StorageResult<Arc<Node>> {
        let id = node.id();
        if let Some(existing) = self.lookup_node(id)? {
            return Ok(existing);
        }

        let mut cache = self.cache_write_guard("cache node")?;
        if let Some(existing) = cache.nodes.get(&id) {
            return Ok(existing.clone());
        }

        let node = Arc::new(node);
        cache.nodes.insert(id, node.clone());
        cache.adjacency.entry(id).or_insert_with(Vec::new);
        Ok(node)
    }

    fn cache_edge(&self, edge: Edge) -> StorageResult<Arc<Edge>> {
        let id = edge.id();
        if let Some(existing) = self.lookup_edge(id)? {
            return Ok(existing);
        }

        let mut cache = self.cache_write_guard("cache edge")?;
        if let Some(existing) = cache.edges.get(&id) {
            return Ok(existing.clone());
        }

        let source = edge.source();
        let target = edge.target();
        let edge = Arc::new(edge);
        cache.edges.insert(id, edge.clone());
        Self::link_edge(&mut cache.adjacency, source, id);
        Self::link_edge(&mut cache.adjacency, target, id);
        Ok(edge)
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

    fn cache_read_guard(&self, label: &'static str) -> StorageResult<RwLockReadGuard<'_, Cache>> {
        self.cache.read().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache(label),
            lock: Self::POISONED_READ,
        })
    }

    fn cache_write_guard(&self, label: &'static str) -> StorageResult<RwLockWriteGuard<'_, Cache>> {
        self.cache.write().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache(label),
            lock: Self::POISONED_WRITE,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use crate::storage::memory::InMemoryBackend;

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

    #[derive(Default)]
    struct CountingBackend {
        inner: InMemoryBackend,
        node_loads: AtomicUsize,
        edge_loads: AtomicUsize,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self::default()
        }

        fn node_loads(&self) -> usize {
            self.node_loads.load(Ordering::SeqCst)
        }

        fn edge_loads(&self) -> usize {
            self.edge_loads.load(Ordering::SeqCst)
        }
    }

    impl StorageBackend for CountingBackend {
        fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
            self.node_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_node(id)
        }

        fn store_node(&self, node: &Node) -> StorageResult<()> {
            self.inner.store_node(node)
        }

        fn delete_node(&self, id: NodeId) -> StorageResult<()> {
            self.inner.delete_node(id)
        }

        fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
            self.edge_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_edge(id)
        }

        fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
            self.inner.store_edge(edge)
        }

        fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
            self.inner.delete_edge(id)
        }
    }

    #[test]
    fn get_node_lazy_loads_from_storage() {
        let node = make_node(1, &["Person"]);
        let node_id = node.id();
        let backend = InMemoryBackend::new();
        backend.store_node(&node).unwrap();

        let db = Database::new(backend);

        let loaded = db.get_node(node_id).unwrap().expect("node should load");
        assert_eq!(loaded.id(), node_id);
        assert!(db.contains_node(node_id).unwrap());
    }

    #[test]
    fn get_edge_populates_adjacency() {
        let backend = InMemoryBackend::new();
        backend.store_edge(&make_edge(1, 2, 3)).unwrap();

        let db = Database::new(backend);
        let edge_id = EdgeId::from_u128(1);
        let source = NodeId::from_u128(2);
        let target = NodeId::from_u128(3);

        let loaded_edge = db.get_edge(edge_id).unwrap().expect("edge should load");
        assert_eq!(loaded_edge.id(), edge_id);

        let source_edges = db.edges_for_node(source).unwrap();
        assert_eq!(source_edges.len(), 1);
        assert_eq!(source_edges[0].id(), edge_id);

        let target_edges = db.edges_for_node(target).unwrap();
        assert_eq!(target_edges.len(), 1);
        assert_eq!(target_edges[0].id(), edge_id);

        let neighbors = db.neighbor_ids(source).unwrap();
        assert_eq!(neighbors, vec![target]);
    }

    #[test]
    fn insert_node_persists_to_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        let node = make_node(4, &["City"]);
        let node_id = node.id();

        db.insert_node(node.clone()).unwrap();

        assert!(db.contains_node(node_id).unwrap());
        let stored = db.storage().load_node(node_id).unwrap();
        assert_eq!(stored.unwrap().id(), node_id);
    }

    #[test]
    fn edges_for_node_returns_empty_when_absent() {
        let db = Database::new(InMemoryBackend::new());
        let edges = db.edges_for_node(NodeId::from_u128(99)).unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn remove_edge_updates_cache_and_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        db.insert_node(make_node(6, &["City"])).unwrap();
        db.insert_node(make_node(7, &["City"])).unwrap();
        let edge = make_edge(30, 6, 7);
        let edge_id = edge.id();
        db.insert_edge(edge).unwrap();

        let removed = db.remove_edge(edge_id).unwrap().expect("edge removed");
        assert_eq!(removed.id(), edge_id);
        assert!(!db.contains_edge(edge_id).unwrap());
        assert!(db.storage().load_edge(edge_id).unwrap().is_none());
        assert!(db.edges_for_node(NodeId::from_u128(6)).unwrap().is_empty());
        assert!(db.edges_for_node(NodeId::from_u128(7)).unwrap().is_empty());
    }

    #[test]
    fn remove_node_prunes_incident_edges_and_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        db.insert_node(make_node(8, &["Person"])).unwrap();
        db.insert_node(make_node(9, &["Person"])).unwrap();
        db.insert_edge(make_edge(31, 8, 9)).unwrap();

        let removed = db
            .remove_node(NodeId::from_u128(8))
            .unwrap()
            .expect("node removed");
        assert_eq!(removed.id(), NodeId::from_u128(8));
        assert!(!db.contains_node(NodeId::from_u128(8)).unwrap());
        assert!(!db.contains_edge(EdgeId::from_u128(31)).unwrap());
        assert!(db.edges_for_node(NodeId::from_u128(9)).unwrap().is_empty());
        assert!(
            db.storage()
                .load_node(NodeId::from_u128(8))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn evict_node_triggers_reload_from_storage() {
        let backend = CountingBackend::new();
        let db = Database::new(backend);
        let node = make_node(50, &["Person"]);
        let node_id = node.id();

        db.insert_node(node).unwrap();
        assert_eq!(db.storage().node_loads(), 0);

        db.evict_node(node_id).unwrap();
        assert!(!db.contains_node(node_id).unwrap());

        let reloaded = db.get_node(node_id).unwrap().expect("node reload");
        assert_eq!(reloaded.id(), node_id);
        assert_eq!(db.storage().node_loads(), 1);
    }

    #[test]
    fn evict_edge_triggers_reload_from_storage() {
        let backend = CountingBackend::new();
        let db = Database::new(backend);

        db.insert_node(make_node(60, &["Person"])).unwrap();
        db.insert_node(make_node(61, &["Person"])).unwrap();
        let edge = make_edge(70, 60, 61);
        let edge_id = edge.id();

        db.insert_edge(edge).unwrap();
        assert_eq!(db.storage().edge_loads(), 0);

        db.evict_edge(edge_id).unwrap();
        assert!(!db.contains_edge(edge_id).unwrap());

        let reloaded = db.get_edge(edge_id).unwrap().expect("edge reload");
        assert_eq!(reloaded.id(), edge_id);
        assert_eq!(db.storage().edge_loads(), 1);

        let edge_ids = db.edges_for_node(NodeId::from_u128(60)).unwrap();
        assert_eq!(edge_ids.len(), 1);
    }

    #[test]
    fn concurrent_reads_and_writes_do_not_panic() {
        let backend = InMemoryBackend::new();
        backend.store_node(&make_node(10, &["Person"])).unwrap();
        backend.store_edge(&make_edge(20, 10, 11)).unwrap();

        let db = Arc::new(Database::new(backend));
        let node_id = NodeId::from_u128(10);
        let edge_id = EdgeId::from_u128(20);

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..50 {
                        let _ = db.get_node(node_id).unwrap();
                        let _ = db.get_edge(edge_id).unwrap();
                    }
                })
            })
            .collect();

        let writer = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                db.insert_node(make_node(11, &["Person"])).unwrap();
                db.insert_edge(make_edge(21, 10, 11)).unwrap();
            })
        };

        for handle in readers {
            handle.join().expect("reader thread panicked");
        }
        writer.join().expect("writer thread panicked");

        assert!(db.contains_node(NodeId::from_u128(11)).unwrap());
        assert!(db.contains_edge(EdgeId::from_u128(21)).unwrap());
    }
}
