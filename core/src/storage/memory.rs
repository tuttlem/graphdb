use std::collections::HashMap;
use std::sync::RwLock;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use super::{StorageBackend, StorageError, StorageOp, StorageResult};

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

    pub fn insert_node(&self, node: Node) -> StorageResult<()> {
        self.store_node(&node)
    }

    pub fn insert_edge(&self, edge: Edge) -> StorageResult<()> {
        self.store_edge(&edge)
    }
}

impl StorageBackend for InMemoryBackend {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        let op = StorageOp::LoadNode(id);
        let nodes = self.nodes.read().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "nodes read",
        })?;
        Ok(nodes.get(&id).cloned())
    }

    fn store_node(&self, node: &Node) -> StorageResult<()> {
        let op = StorageOp::StoreNode(node.id());
        let mut nodes = self.nodes.write().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "nodes write",
        })?;
        nodes.insert(node.id(), node.clone());
        Ok(())
    }

    fn delete_node(&self, id: NodeId) -> StorageResult<()> {
        let op = StorageOp::DeleteNode(id);
        let mut nodes = self.nodes.write().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "nodes write",
        })?;
        nodes.remove(&id);
        Ok(())
    }

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
        let op = StorageOp::LoadEdge(id);
        let edges = self.edges.read().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "edges read",
        })?;
        Ok(edges.get(&id).cloned())
    }

    fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
        let op = StorageOp::StoreEdge(edge.id());
        let mut edges = self.edges.write().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "edges write",
        })?;
        edges.insert(edge.id(), edge.clone());
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
        let op = StorageOp::DeleteEdge(id);
        let mut edges = self.edges.write().map_err(|_| StorageError::LockPoisoned {
            op,
            lock: "edges write",
        })?;
        edges.remove(&id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_backend_round_trip() {
        let backend = InMemoryBackend::new();
        let node_id = NodeId::from_u128(1);
        let node = Node::new(node_id, vec![], HashMap::new());

        backend.store_node(&node).unwrap();
        let loaded = backend.load_node(node_id).unwrap();
        assert!(loaded.is_some());

        backend.delete_node(node_id).unwrap();
        assert!(backend.load_node(node_id).unwrap().is_none());
    }
}
