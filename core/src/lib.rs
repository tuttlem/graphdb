pub mod db;
pub mod query;
pub mod storage;

pub use db::database::{Database, DatabaseError, DatabaseResult};
pub use storage::{
    InMemoryBackend, SimpleStorage, StorageBackend, StorageError, StorageOp, StorageResult,
};

pub use common::edge::{Edge, EdgeId};
pub use common::node::{Node, NodeId};
