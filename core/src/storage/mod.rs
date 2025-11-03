use std::collections::HashMap;
use std::sync::RwLock;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

/// Abstraction over the persistence layer for nodes and edges.
pub trait StorageBackend: Send + Sync {
    fn load_node(&self, id: NodeId) -> Option<Node>;
    fn load_edge(&self, id: EdgeId) -> Option<Edge>;
}

/// Simple in-memory storage backend useful in tests and for bootstrapping.
#[derive(Default)]
pub struct InMemoryBackend {
    nodes: RwLock<HashMap<NodeId, Node>>,
    edges: RwLock<HashMap<EdgeId, Edge>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_node(&self, node: Node) {
        let id = node.id();
        let mut nodes = self.nodes.write().expect("nodes lock poisoned");
        nodes.insert(id, node);
    }

    pub fn insert_edge(&self, edge: Edge) {
        let id = edge.id();
        let mut edges = self.edges.write().expect("edges lock poisoned");
        edges.insert(id, edge);
    }
}

impl StorageBackend for InMemoryBackend {
    fn load_node(&self, id: NodeId) -> Option<Node> {
        let nodes = self.nodes.read().expect("nodes lock poisoned");
        nodes.get(&id).cloned()
    }

    fn load_edge(&self, id: EdgeId) -> Option<Edge> {
        let edges = self.edges.read().expect("edges lock poisoned");
        edges.get(&id).cloned()
    }
}
