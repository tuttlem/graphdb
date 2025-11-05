use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Properties(pub HashMap<String, Value>);

impl Properties {
    pub fn new(map: HashMap<String, Value>) -> Self {
        Self(map)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub alias: Option<String>,
    pub label: Option<String>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub alias: Option<String>,
    pub label: Option<String>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    InsertNode {
        pattern: NodePattern,
    },
    InsertEdge {
        source: NodePattern,
        edge: EdgePattern,
        target: NodePattern,
    },
    DeleteNode {
        id: String,
    },
    DeleteEdge {
        id: String,
    },
    UpdateNode {
        id: String,
        assignments: HashMap<String, Value>,
    },
    UpdateEdge {
        id: String,
        assignments: HashMap<String, Value>,
    },
    Select {
        pattern: NodePattern,
        conditions: Vec<Condition>,
        returns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub alias: String,
    pub property: String,
    pub operator: ComparisonOperator,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equals,
}
