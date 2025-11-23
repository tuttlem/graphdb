use std::fmt;

use thiserror::Error;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

pub mod memory;
pub mod monolith;
pub mod simple;

pub use memory::InMemoryBackend;
pub use monolith::MonolithStorage;
pub use simple::SimpleStorage;

pub type StorageResult<T> = Result<T, StorageError>;

/// High-level description of a storage operation, used for error context.
#[derive(Debug, Clone)]
pub enum StorageOp {
    LoadNode(NodeId),
    StoreNode(NodeId),
    DeleteNode(NodeId),
    LoadEdge(EdgeId),
    StoreEdge(EdgeId),
    DeleteEdge(EdgeId),
    Cache(&'static str),
}

impl fmt::Display for StorageOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageOp::LoadNode(id) => write!(f, "load node {id}"),
            StorageOp::StoreNode(id) => write!(f, "store node {id}"),
            StorageOp::DeleteNode(id) => write!(f, "delete node {id}"),
            StorageOp::LoadEdge(id) => write!(f, "load edge {id}"),
            StorageOp::StoreEdge(id) => write!(f, "store edge {id}"),
            StorageOp::DeleteEdge(id) => write!(f, "delete edge {id}"),
            StorageOp::Cache(label) => write!(f, "cache {label}"),
        }
    }
}

/// Errors produced by storage backends.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("{op} i/o error: {source}")]
    Io {
        op: StorageOp,
        #[source]
        source: std::io::Error,
    },

    #[error("{op} serialization error: {source}")]
    Serialization {
        op: StorageOp,
        #[source]
        source: serde_json::Error,
    },

    #[error("{op} binary serialization error: {source}")]
    Binary {
        op: StorageOp,
        #[source]
        source: bincode::Error,
    },

    #[error("{op} lock poisoned: {lock}")]
    LockPoisoned { op: StorageOp, lock: &'static str },
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
