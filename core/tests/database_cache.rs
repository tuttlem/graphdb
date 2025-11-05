use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use graphdb_core::{
    Database, Edge, EdgeId, InMemoryBackend, Node, NodeId, StorageBackend, StorageResult,
};

fn make_node(id: u128) -> Node {
    Node::new(NodeId::from_u128(id), vec![], HashMap::new())
}

fn make_edge(id: u128, source: u128, target: u128) -> Edge {
    Edge::new(
        EdgeId::from_u128(id),
        NodeId::from_u128(source),
        NodeId::from_u128(target),
        HashMap::new(),
    )
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
fn evicts_least_recently_used_nodes() {
    let backend = CountingBackend::new();
    let db = Database::with_capacity(backend, Some(2), None);

    db.insert_node(make_node(1)).unwrap();
    db.insert_node(make_node(2)).unwrap();

    db.get_node(NodeId::from_u128(1)).unwrap().unwrap();

    db.insert_node(make_node(3)).unwrap();

    assert!(db.contains_node(NodeId::from_u128(1)).unwrap());
    assert!(db.contains_node(NodeId::from_u128(3)).unwrap());
    assert!(!db.contains_node(NodeId::from_u128(2)).unwrap());
}

#[test]
fn node_reload_after_eviction_hits_storage() {
    let backend = CountingBackend::new();
    let db = Database::with_capacity(backend, Some(1), None);

    db.insert_node(make_node(10)).unwrap();
    db.insert_node(make_node(11)).unwrap();

    let before = db.storage().node_loads();
    db.get_node(NodeId::from_u128(10)).unwrap().unwrap();
    assert!(db.storage().node_loads() > before);
}

#[test]
fn edge_capacity_reloads_evicted_edge() {
    let backend = CountingBackend::new();
    let db = Database::with_capacity(backend, None, Some(1));

    db.insert_node(make_node(20)).unwrap();
    db.insert_node(make_node(21)).unwrap();
    db.insert_node(make_node(22)).unwrap();

    db.insert_edge(make_edge(30, 20, 21)).unwrap();
    db.insert_edge(make_edge(31, 20, 22)).unwrap();

    let before = db.storage().edge_loads();
    let edges = db.edges_for_node(NodeId::from_u128(20)).unwrap();
    assert_eq!(edges.len(), 2);
    assert!(db.storage().edge_loads() > before);
}

#[test]
fn neighbor_ids_consistent_after_eviction() {
    let backend = CountingBackend::new();
    let db = Database::with_capacity(backend, Some(1), Some(1));

    db.insert_node(make_node(40)).unwrap();
    db.insert_node(make_node(41)).unwrap();
    db.insert_edge(make_edge(50, 40, 41)).unwrap();

    db.insert_node(make_node(42)).unwrap();
    db.insert_edge(make_edge(51, 40, 42)).unwrap();

    let neighbors = db.neighbor_ids(NodeId::from_u128(40)).unwrap();
    assert_eq!(neighbors.len(), 2);
    assert!(neighbors.contains(&NodeId::from_u128(41)));
    assert!(neighbors.contains(&NodeId::from_u128(42)));
}
