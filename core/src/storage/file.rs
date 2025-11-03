use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use super::{StorageBackend, StorageError, StorageResult};

/// File-backed storage backend that persists JSON-encoded nodes and edges.
pub struct FileBackend {
    nodes_dir: PathBuf,
    edges_dir: PathBuf,
}

impl FileBackend {
    pub fn new<P: AsRef<Path>>(root: P) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        let nodes_dir = root.join("nodes");
        let edges_dir = root.join("edges");

        fs::create_dir_all(&nodes_dir)?;
        fs::create_dir_all(&edges_dir)?;

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

impl StorageBackend for FileBackend {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        let path = self.node_path(id);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice::<Node>(&bytes)?)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::Io(err)),
        }
    }

    fn store_node(&self, node: &Node) -> StorageResult<()> {
        let path = self.node_path(node.id());
        let data = serde_json::to_vec(node)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn delete_node(&self, id: NodeId) -> StorageResult<()> {
        let path = self.node_path(id);
        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io(err)),
        }
    }

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
        let path = self.edge_path(id);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice::<Edge>(&bytes)?)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::Io(err)),
        }
    }

    fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
        let path = self.edge_path(edge.id());
        let data = serde_json::to_vec(edge)?;
        fs::write(path, data)?;
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
        let path = self.edge_path(id);
        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        let backend = FileBackend::new(tmp.path()).expect("backend");

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
}
