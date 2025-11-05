use std::collections::HashMap;
use std::fs;

use graphdb_core::{EdgeId, Node, NodeId, SimpleStorage, StorageBackend};
use serde_json::Value;
use tempfile::TempDir;

fn make_node(id: u128) -> Node {
    Node::new(
        NodeId::from_u128(id),
        vec!["Label".to_string()],
        HashMap::new(),
    )
}

#[test]
fn simple_storage_writes_and_reads_json() {
    let tmp = TempDir::new().expect("temp dir");
    let storage = SimpleStorage::new(tmp.path()).expect("backend");

    let node = make_node(1);
    storage.store_node(&node).unwrap();

    let path = tmp.path().join("nodes").join(format!("{}.json", node.id()));
    assert!(path.exists());

    let data = fs::read(&path).unwrap();
    let json: Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(json["id"].as_str().unwrap(), node.id().to_string());

    let reloaded = storage.load_node(node.id()).unwrap().unwrap();
    assert_eq!(reloaded.labels(), node.labels());
}

#[test]
fn simple_storage_deletes_files() {
    let tmp = TempDir::new().expect("temp dir");
    let storage = SimpleStorage::new(tmp.path()).expect("backend");

    let node = make_node(2);
    storage.store_node(&node).unwrap();

    let path = tmp.path().join("nodes").join(format!("{}.json", node.id()));
    assert!(path.exists());

    storage.delete_node(node.id()).unwrap();
    assert!(!path.exists());

    storage.delete_edge(EdgeId::from_u128(999)).unwrap();
}
