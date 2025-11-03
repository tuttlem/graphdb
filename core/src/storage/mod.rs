use std::collections::HashMap;
use std::sync::RwLock;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};
use thiserror::Error;

pub mod file;

pub use file::FileBackend;

pub type StorageResult<T> = Result<T, StorageError>;

/// Errors produced by storage backends.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("lock poisoned: {0}")]
    LockPoisoned(&'static str),
}

/// Abstraction over the persistence layer for nodes and edges.
pub trait StorageBackend: Send + Sync {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>>;
    fn store_node(&self, node: &Node) -> StorageResult<()>;
    fn delete_node(&self, id: NodeId) -> StorageResult<()>;

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>>;
    fn store_edge(&self, edge: &Edge) -> StorageResult<()>;
    fn delete_edge(&self, id: EdgeId) -> StorageResult<()>;
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

    pub fn insert_node(&self, node: Node) -> StorageResult<()> {
        self.store_node(&node)
    }

    pub fn insert_edge(&self, edge: Edge) -> StorageResult<()> {
        self.store_edge(&edge)
    }

    fn lock_poisoned(tag: &'static str) -> StorageError {
        StorageError::LockPoisoned(tag)
    }
}

impl StorageBackend for InMemoryBackend {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        let nodes = self
            .nodes
            .read()
            .map_err(|_| Self::lock_poisoned("nodes read"))?;
        Ok(nodes.get(&id).cloned())
    }

    fn store_node(&self, node: &Node) -> StorageResult<()> {
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| Self::lock_poisoned("nodes write"))?;
        nodes.insert(node.id(), node.clone());
        Ok(())
    }

    fn delete_node(&self, id: NodeId) -> StorageResult<()> {
        let mut nodes = self
            .nodes
            .write()
            .map_err(|_| Self::lock_poisoned("nodes write"))?;
        nodes.remove(&id);
        Ok(())
    }

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
        let edges = self
            .edges
            .read()
            .map_err(|_| Self::lock_poisoned("edges read"))?;
        Ok(edges.get(&id).cloned())
    }

    fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
        let mut edges = self
            .edges
            .write()
            .map_err(|_| Self::lock_poisoned("edges write"))?;
        edges.insert(edge.id(), edge.clone());
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
        let mut edges = self
            .edges
            .write()
            .map_err(|_| Self::lock_poisoned("edges write"))?;
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
