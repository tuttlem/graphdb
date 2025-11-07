use graphdb_core::query::{
    ComparisonOperator, GraphDbProcedure, PathQueryMode, PathReturn, Procedure, Query,
    RelationshipDirection, Value, parse_queries,
};

#[test]
fn parse_insert_node() {
    let input =
        "INSERT NODE (n:Person {name: \"Ada\", age: 36, skills: [\"rust\", \"distributed\"]});";
    let queries = parse_queries(input).unwrap();
    assert_eq!(queries.len(), 1);
    match &queries[0] {
        Query::InsertNode { pattern } => {
            assert_eq!(pattern.alias.as_deref(), Some("n"));
            assert_eq!(pattern.label.as_deref(), Some("Person"));
            assert!(matches!(
                pattern.properties.0.get("age"),
                Some(Value::Integer(36))
            ));
            assert!(matches!(
                pattern.properties.0.get("skills"),
                Some(Value::List(values)) if values.len() == 2
            ));
        }
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_insert_edge() {
    let input = "INSERT EDGE (a:Person {id: 1})-[r:KNOWS {since: 2020}]->(b:Person {id: 2});";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::InsertEdge {
            source,
            edge,
            target,
        } => {
            assert_eq!(source.alias.as_deref(), Some("a"));
            assert_eq!(edge.label.as_deref(), Some("KNOWS"));
            assert_eq!(target.alias.as_deref(), Some("b"));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_delete_update() {
    let delete = "DELETE NODE 1234;";
    let queries = parse_queries(delete).unwrap();
    assert!(matches!(queries[0], Query::DeleteNode { .. }));

    let update = "UPDATE EDGE e123 SET weight = 0.75, active = true;";
    let queries = parse_queries(update).unwrap();
    match &queries[0] {
        Query::UpdateEdge { assignments, .. } => {
            assert!(
                matches!(assignments.get("weight"), Some(Value::Float(v)) if (*v - 0.75).abs() < f64::EPSILON)
            );
            assert!(matches!(
                assignments.get("active"),
                Some(Value::Boolean(true))
            ));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_select_where() {
    let input =
        "SELECT MATCH (n:Person {city: \"Zurich\"}) WHERE n.age = 30 AND n.active = true RETURN n;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select {
            pattern,
            conditions,
            returns,
        } => {
            assert_eq!(pattern.label.as_deref(), Some("Person"));
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0].operator, ComparisonOperator::Equals));
            assert_eq!(returns, &vec!["n".to_string()]);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_create_patterns() {
    let node_input = "CREATE (:Person {name: \"Ada\"});";
    let node_queries = parse_queries(node_input).unwrap();
    match &node_queries[0] {
        Query::Create { pattern } => {
            assert!(pattern.relationship.is_none());
            assert_eq!(pattern.left.label.as_deref(), Some("Person"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let rel_input =
        "CREATE (a:Person {name: \"Ada\"})-[:KNOWS {since: 2020}]->(:Person {name: \"Bob\"});";
    let rel_queries = parse_queries(rel_input).unwrap();
    match &rel_queries[0] {
        Query::Create { pattern } => {
            let relationship = pattern.relationship.as_ref().expect("relationship pattern");
            assert_eq!(pattern.left.alias.as_deref(), Some("a"));
            assert_eq!(relationship.edge.label.as_deref(), Some("KNOWS"));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_call_procedure() {
    let input = "CALL graphdb.nodeClasses(); CALL graphdb.users();";
    let queries = parse_queries(input).expect("call queries");
    assert_eq!(queries.len(), 2);

    match &queries[0] {
        Query::CallProcedure { procedure } => {
            assert_eq!(
                procedure,
                &Procedure::GraphDb(GraphDbProcedure::NodeClasses)
            );
        }
        other => panic!("unexpected query {other:?}"),
    }

    match &queries[1] {
        Query::CallProcedure { procedure } => {
            assert_eq!(procedure.canonical_name(), "graphdb.users");
        }
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_shortest_path_query() {
    let input = r#"
MATCH (start:Person {name: "Meg"}), (end:Person {name: "Kevin"})
MATCH path = shortestPath((start)-[:ACTED_IN*..10]-(end))
RETURN path, length(path);
"#;
    let queries = parse_queries(input).expect("path query");
    match &queries[0] {
        Query::PathMatch(spec) => {
            assert_eq!(spec.path_alias, "path");
            assert_eq!(spec.start_alias, "start");
            assert_eq!(spec.end_alias, "end");
            assert!(matches!(spec.mode, PathQueryMode::Shortest));
            assert!(matches!(
                spec.returns,
                PathReturn::Path {
                    include_length: true
                }
            ));
            assert_eq!(
                spec.relationship.direction,
                RelationshipDirection::Undirected
            );
        }
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_queries_with_comments() {
    let input = r#"
    // insert a person
    INSERT NODE (:Person {name: "Ada", id: 1});
    /* multi-line
       comment */
    MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person)
    RETURN p;
    "#;
    let queries = parse_queries(input).expect("parses with comments");
    assert_eq!(queries.len(), 2);
    assert!(matches!(queries[0], Query::InsertNode { .. }));
    assert!(matches!(queries[1], Query::PathMatch { .. }));
}

#[test]
fn parse_path_with_where_clause() {
    let input = r#"
MATCH p = (a:Person {city: "Berlin"})-[:KNOWS*1..3]->(b:Person)
WHERE NOT (a)-[:BLOCKED]->(m:Person)
RETURN a, b;
"#;
    let queries = parse_queries(input).expect("path query");
    match &queries[0] {
        Query::PathMatch(spec) => {
            assert_eq!(spec.path_alias, "p");
            assert_eq!(spec.start_alias, "a");
            assert_eq!(spec.end_alias, "b");
            assert!(matches!(spec.mode, PathQueryMode::All));
            assert!(matches!(spec.returns, PathReturn::Nodes { .. }));
            assert!(spec.filter.is_some());
        }
        other => panic!("unexpected query {other:?}"),
    }
}
