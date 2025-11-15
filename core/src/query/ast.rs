use common::value::Value;
use std::collections::HashMap;

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
    Select(SelectQuery),
    Create {
        pattern: CreatePattern,
    },
    CallProcedure {
        procedure: Procedure,
    },
    PathMatch(PathMatchQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub initial_with: Option<WithClause>,
    pub match_clauses: Vec<SelectMatchClause>,
    pub conditions: Vec<Condition>,
    pub predicates: Vec<PredicateFilter>,
    pub with: Option<WithClause>,
    pub returns: Vec<Projection>,
    pub explain: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectMatchClause {
    pub optional: bool,
    pub patterns: Vec<MatchPattern>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchPattern {
    Node(NodePattern),
    Relationship(RelationshipMatch),
    Path(PathPattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    pub alias: String,
    pub pattern: RelationshipMatch,
    pub mode: PathQueryMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipMatch {
    pub left: NodePattern,
    pub relationship: RelationshipPattern,
    pub right: NodePattern,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub projections: Vec<Projection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Field(FieldReference),
    Aggregate(AggregateExpression),
    Function(Box<FunctionExpression>),
    Literal(Value),
    BinaryOp {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Add,
    Subtract,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldReference {
    pub alias: String,
    pub property: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    pub function: AggregateFunction,
    pub target: Option<FieldReference>,
    pub percentile: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionExpression {
    ListPredicate(ListPredicateFunction),
    IsEmpty(ValueOperand),
    Exists(ExistsFunction),
    Scalar(ScalarFunction),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunction {
    Keys(Expression),
    Labels(Expression),
    Nodes(Expression),
    Relationships(Expression),
    Range {
        start: Expression,
        end: Expression,
        step: Option<Expression>,
    },
    Reverse(Expression),
    Tail(Expression),
    ToBooleanList(Expression),
    ToFloatList(Expression),
    ToIntegerList(Expression),
    ToStringList(Expression),
    Coalesce(Vec<Expression>),
    StartNode(FieldReference),
    EndNode(FieldReference),
    Head(Expression),
    Last(Expression),
    Id(FieldReference),
    Properties(Expression),
    RandomUuid,
    Size(Expression),
    Length(Expression),
    Timestamp,
    UserDefined(UserFunctionCall),
    ToBoolean {
        expr: Expression,
        null_on_unsupported: bool,
    },
    ToFloat {
        expr: Expression,
        null_on_unsupported: bool,
    },
    ToInteger {
        expr: Expression,
        null_on_unsupported: bool,
    },
    ToString {
        expr: Expression,
        null_on_unsupported: bool,
    },
    Type(FieldReference),
    Reduce {
        accumulator: String,
        initial: Expression,
        variable: String,
        list: Expression,
        expression: Expression,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserFunctionCall {
    pub name: String,
    pub arguments: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredicateFilter {
    pub function: FunctionExpression,
    pub negated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListPredicateFunction {
    pub kind: ListPredicateKind,
    pub variable: String,
    pub list: ListExpression,
    pub predicate: ListPredicate,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ListPredicateKind {
    All,
    Any,
    None,
    Single,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ListExpression {
    Field(FieldReference),
    Literal(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ListPredicate {
    Comparison {
        operator: ComparisonOperator,
        value: Value,
    },
    IsNull {
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueOperand {
    Field(FieldReference),
    Literal(Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExistsFunction {
    pub pattern: RelationshipMatch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Avg,
    Collect,
    Count,
    CountAll,
    Max,
    Min,
    PercentileCont,
    PercentileDisc,
    StDev,
    StDevP,
    Sum,
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
pub struct PathMatchQuery {
    pub path_alias: String,
    pub start_alias: String,
    pub end_alias: String,
    pub start: NodePattern,
    pub end: NodePattern,
    pub relationship: RelationshipPattern,
    pub mode: PathQueryMode,
    pub filter: Option<PathFilter>,
    pub returns: PathReturn,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathQueryMode {
    All,
    Shortest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipPattern {
    pub alias: Option<String>,
    pub label: Option<String>,
    pub properties: Properties,
    pub direction: RelationshipDirection,
    pub length: PathLength,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationshipDirection {
    Outbound,
    Inbound,
    Undirected,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathLength {
    Exact(u32),
    Range { min: u32, max: Option<u32> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathFilter {
    ExcludeRelationship {
        from_alias: String,
        relationship: RelationshipPattern,
        to_pattern: NodePattern,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathReturn {
    Path {
        include_length: bool,
    },
    Nodes {
        start_alias: String,
        end_alias: String,
        include_length: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub alias: String,
    pub property: String,
    pub operator: ComparisonOperator,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    IsNull,
    IsNotNull,
}
