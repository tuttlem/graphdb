use std::collections::HashMap;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};
use graphdb_core::{Database, SimpleStorage};
use tempfile::TempDir;

fn make_node(id: u128, labels: &[&str]) -> Node {
    let node_id = NodeId::from_u128(id);
    let label_vec = labels.iter().map(|label| label.to_string()).collect();
    Node::new(node_id, label_vec, HashMap::new())
}

fn make_edge(id: u128, source: u128, target: u128) -> Edge {
    let edge_id = EdgeId::from_u128(id);
    let source_id = NodeId::from_u128(source);
    let target_id = NodeId::from_u128(target);
    Edge::new(edge_id, source_id, target_id, HashMap::new())
}

#[test]
fn database_round_trip_with_simple_storage() {
    let temp_dir = TempDir::new().expect("temporary directory");

    let backend = SimpleStorage::new(temp_dir.path()).expect("simple storage");
    let database = Database::new(backend);

    let alice = make_node(1, &["Person", "Employee"]);
    let bob = make_node(2, &["Person"]);
    let relationship = make_edge(10, 1, 2);
    let alice_id = alice.id();
    let bob_id = bob.id();
    let relationship_id = relationship.id();

    database.insert_node(alice).expect("insert alice");
    database.insert_node(bob).expect("insert bob");
    database
        .insert_edge(relationship)
        .expect("insert relationship");

    let neighbors = database.neighbor_ids(alice_id).expect("neighbors");
    assert_eq!(neighbors, vec![bob_id]);

    drop(database);

    let backend = SimpleStorage::new(temp_dir.path()).expect("simple storage reload");
    let database = Database::new(backend);

    let alice = database
        .get_node(alice_id)
        .expect("load alice")
        .expect("alice exists");
    assert_eq!(
        alice.labels(),
        &[String::from("Person"), String::from("Employee")]
    );

    let relationship = database
        .get_edge(relationship_id)
        .expect("load relationship")
        .expect("relationship exists");
    assert_eq!(relationship.source(), alice_id);
    assert_eq!(relationship.target(), bob_id);

    let neighbors = database
        .neighbor_ids(alice_id)
        .expect("neighbors after reload");
    assert_eq!(neighbors, vec![bob_id]);

    let edges = database
        .edges_for_node(alice_id)
        .expect("edges after reload");
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), relationship_id);
}
