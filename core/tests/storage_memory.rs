use std::collections::HashMap;

use graphdb_core::{Edge, EdgeId, InMemoryBackend, Node, NodeId, StorageBackend};

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

#[test]
fn stores_and_loads_nodes() {
    let backend = InMemoryBackend::new();

    let node = make_node(1);
    backend.store_node(&node).unwrap();

    let loaded = backend.load_node(node.id()).unwrap().unwrap();
    assert_eq!(loaded.id(), node.id());
}

#[test]
fn deletes_edges() {
    let backend = InMemoryBackend::new();

    let node_a = make_node(10);
    let node_b = make_node(11);
    backend.store_node(&node_a).unwrap();
    backend.store_node(&node_b).unwrap();

    let edge = make_edge(100, 10, 11);
    backend.store_edge(&edge).unwrap();
    assert!(backend.load_edge(edge.id()).unwrap().is_some());

    backend.delete_edge(edge.id()).unwrap();
    assert!(backend.load_edge(edge.id()).unwrap().is_none());
}
