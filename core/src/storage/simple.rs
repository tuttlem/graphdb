use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use super::{StorageBackend, StorageError, StorageOp, StorageResult};

/// File-backed storage backend that persists JSON-encoded nodes and edges.
pub struct SimpleStorage {
    nodes_dir: PathBuf,
    edges_dir: PathBuf,
}

impl SimpleStorage {
    pub fn new<P: AsRef<Path>>(root: P) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        let nodes_dir = root.join("nodes");
        let edges_dir = root.join("edges");

        fs::create_dir_all(&nodes_dir).map_err(|source| StorageError::Io {
            op: StorageOp::Cache("create nodes dir"),
            source,
        })?;
        fs::create_dir_all(&edges_dir).map_err(|source| StorageError::Io {
            op: StorageOp::Cache("create edges dir"),
            source,
        })?;

        Ok(Self {
            nodes_dir,
            edges_dir,
        })
    }

    fn node_path(&self, id: NodeId) -> PathBuf {
        self.nodes_dir.join(format!("{}.json", id))
    }

    fn edge_path(&self, id: EdgeId) -> PathBuf {
        self.edges_dir.join(format!("{}.json", id))
    }
}

impl StorageBackend for SimpleStorage {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        let op = StorageOp::LoadNode(id);
        let path = self.node_path(id);
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Node>(&bytes)
                .map(Some)
                .map_err(|source| StorageError::Serialization { op, source }),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::Io { op, source: err }),
        }
    }

    fn store_node(&self, node: &Node) -> StorageResult<()> {
        let op = StorageOp::StoreNode(node.id());
        let path = self.node_path(node.id());
        let data = serde_json::to_vec(node).map_err(|source| StorageError::Serialization {
            op: op.clone(),
            source,
        })?;
        fs::write(path, data).map_err(|source| StorageError::Io { op, source })
    }

    fn delete_node(&self, id: NodeId) -> StorageResult<()> {
        let op = StorageOp::DeleteNode(id);
        let path = self.node_path(id);
        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io { op, source: err }),
        }
    }

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
        let op = StorageOp::LoadEdge(id);
        let path = self.edge_path(id);
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Edge>(&bytes)
                .map(Some)
                .map_err(|source| StorageError::Serialization { op, source }),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::Io { op, source: err }),
        }
    }

    fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
        let op = StorageOp::StoreEdge(edge.id());
        let path = self.edge_path(edge.id());
        let data = serde_json::to_vec(edge).map_err(|source| StorageError::Serialization {
            op: op.clone(),
            source,
        })?;
        fs::write(path, data).map_err(|source| StorageError::Io { op, source })
    }

    fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
        let op = StorageOp::DeleteEdge(id);
        let path = self.edge_path(id);
        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io { op, source: err }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, fs};

    use tempfile::TempDir;

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
    fn persists_nodes_and_edges() {
        let tmp = TempDir::new().expect("temp dir");
        let backend = SimpleStorage::new(tmp.path()).expect("backend");

        let node = make_node(1);
        backend.store_node(&node).unwrap();
        let loaded_node = backend.load_node(node.id()).unwrap().unwrap();
        assert_eq!(loaded_node.id(), node.id());

        let edge = make_edge(2, 1, 1);
        backend.store_edge(&edge).unwrap();
        let loaded_edge = backend.load_edge(edge.id()).unwrap().unwrap();
        assert_eq!(loaded_edge.id(), edge.id());

        backend.delete_edge(edge.id()).unwrap();
        assert!(backend.load_edge(edge.id()).unwrap().is_none());

        backend.delete_node(node.id()).unwrap();
        assert!(backend.load_node(node.id()).unwrap().is_none());
    }

    #[test]
    fn creates_files_on_store() {
        let tmp = TempDir::new().expect("temp dir");
        let backend = SimpleStorage::new(tmp.path()).expect("backend");

        let node = make_node(5);
        backend.store_node(&node).unwrap();
        let node_path = backend.node_path(node.id());
        assert!(node_path.exists());

        let edge = make_edge(6, 5, 5);
        backend.store_edge(&edge).unwrap();
        let edge_path = backend.edge_path(edge.id());
        assert!(edge_path.exists());
    }

    #[test]
    fn load_returns_none_for_missing_files() {
        let tmp = TempDir::new().expect("temp dir");
        let backend = SimpleStorage::new(tmp.path()).expect("backend");

        let node_id = NodeId::from_u128(42);
        assert!(backend.load_node(node_id).unwrap().is_none());

        let edge_id = EdgeId::from_u128(77);
        assert!(backend.load_edge(edge_id).unwrap().is_none());
    }

    #[test]
    fn load_fails_on_invalid_json() {
        let tmp = TempDir::new().expect("temp dir");
        let backend = SimpleStorage::new(tmp.path()).expect("backend");

        let node_id = NodeId::from_u128(99);
        let path = backend.node_path(node_id);
        fs::write(&path, b"not-json").expect("write invalid file");

        let err = backend.load_node(node_id).unwrap_err();
        match err {
            StorageError::Serialization { op, .. } => {
                assert!(matches!(op, StorageOp::LoadNode(id) if id == node_id));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
