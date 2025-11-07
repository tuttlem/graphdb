use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    List(Vec<Value>),
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
pub struct CreatePattern {
    pub left: NodePattern,
    pub relationship: Option<CreateRelationship>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRelationship {
    pub edge: EdgePattern,
    pub right: NodePattern,
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
    Create {
        pattern: CreatePattern,
    },
    CallProcedure {
        procedure: Procedure,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Procedure {
    GraphDb(GraphDbProcedure),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GraphDbProcedure {
    NodeClasses,
    EdgeClasses,
    Users,
    Roles,
}

impl Procedure {
    pub fn canonical_name(&self) -> &'static str {
        match self {
            Procedure::GraphDb(proc) => proc.canonical_name(),
        }
    }
}

impl GraphDbProcedure {
    pub fn canonical_name(&self) -> &'static str {
        match self {
            GraphDbProcedure::NodeClasses => "graphdb.nodeClasses",
            GraphDbProcedure::EdgeClasses => "graphdb.edgeClasses",
            GraphDbProcedure::Users => "graphdb.users",
            GraphDbProcedure::Roles => "graphdb.roles",
        }
    }

    pub fn from_identifier(ident: &str) -> Option<Self> {
        match ident.to_ascii_lowercase().as_str() {
            "nodeclasses" | "nodeclass" => Some(GraphDbProcedure::NodeClasses),
            "edgeclasses" | "edgeclass" => Some(GraphDbProcedure::EdgeClasses),
            "users" | "user" => Some(GraphDbProcedure::Users),
            "roles" | "role" => Some(GraphDbProcedure::Roles),
            _ => None,
        }
    }
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
