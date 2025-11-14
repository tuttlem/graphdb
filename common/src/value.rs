use std::collections::HashMap;

use crate::edge::Edge;
use crate::node::Node;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}

#[derive(Clone)]
pub struct QueryPath {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Clone)]
pub enum FieldValue {
    Node(Node),
    Relationship(Edge),
    Path(QueryPath),
    List(Vec<FieldValue>),
    Scalar(Value),
}
