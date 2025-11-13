use graphdb_core::query::{
    ComparisonOperator, Expression, FunctionExpression, GraphDbProcedure, ListPredicateKind,
    MatchPattern, PathQueryMode, PathReturn, Procedure, Query, RelationshipDirection,
    ScalarFunction, Value, parse_queries,
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
        "MATCH (n:Person {city: \"Zurich\"}) WHERE n.age = 30 AND n.active = true RETURN n;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            let clause = &select.match_clauses[0];
            match &clause.patterns[0] {
                MatchPattern::Node(pattern) => {
                    assert_eq!(pattern.label.as_deref(), Some("Person"));
                }
                other => panic!("unexpected match pattern {other:?}"),
            }
            assert_eq!(select.conditions.len(), 2);
            assert!(matches!(
                select.conditions[0].operator,
                ComparisonOperator::Equals
            ));
            assert_eq!(select.returns.len(), 1);
            match &select.returns[0].expression {
                Expression::Field(field) => assert_eq!(field.alias, "n"),
                other => panic!("unexpected projection {other:?}"),
            }
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_select_with_aggregate() {
    let input = "MATCH (n:Person) RETURN count(*) AS total;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.returns.len(), 1);
            assert_eq!(select.returns[0].alias.as_deref(), Some("total"));
            assert!(matches!(
                select.returns[0].expression,
                Expression::Aggregate(_)
            ));
            assert!(!select.explain);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_explain_select() {
    let input = "EXPLAIN MATCH (n:Person) RETURN n;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert!(select.explain);
            assert_eq!(select.match_clauses.len(), 1);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_where_rich_predicates() {
    let input = "MATCH (n:Person) WHERE n.age >= 30 AND n.salary <> 0 AND n.city IS NOT NULL AND n.manager IS NULL RETURN n;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.conditions.len(), 4);
            assert_eq!(
                select.conditions[0].operator,
                ComparisonOperator::GreaterThanOrEqual
            );
            assert!(matches!(
                select.conditions[0].value,
                Some(Value::Integer(30))
            ));
            assert_eq!(select.conditions[1].operator, ComparisonOperator::NotEquals);
            assert_eq!(select.conditions[2].operator, ComparisonOperator::IsNotNull);
            assert!(select.conditions[2].value.is_none());
            assert_eq!(select.conditions[3].operator, ComparisonOperator::IsNull);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_where_with_predicate_functions() {
    let input = "MATCH (p:Person) WHERE p.nicknames IS NOT NULL AND NOT isEmpty(p.nicknames) AND exists((p)-[:KNOWS]->()) RETURN p;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.conditions.len(), 1);
            assert_eq!(select.predicates.len(), 2);
            match &select.predicates[0].function {
                FunctionExpression::IsEmpty(_) => {}
                other => panic!("unexpected predicate {other:?}"),
            }
            match &select.predicates[1].function {
                FunctionExpression::Exists(_) => {}
                other => panic!("unexpected predicate {other:?}"),
            }
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_return_list_predicate_function() {
    let input = "MATCH (n:Item) RETURN all(x IN n.values WHERE x > 0) AS ok;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.returns.len(), 1);
            match &select.returns[0].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::ListPredicate(spec) => {
                        assert_eq!(spec.kind, ListPredicateKind::All);
                        assert_eq!(spec.variable, "x");
                    }
                    other => panic!("unexpected projection {other:?}"),
                },
                other => panic!("unexpected projection {other:?}"),
            }
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_with_literal_projection() {
    let input = "MATCH (n:Person) WITH [] AS xs RETURN xs;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            let with_clause = select.with.as_ref().expect("WITH clause");
            assert_eq!(with_clause.projections.len(), 1);
            match &with_clause.projections[0].expression {
                Expression::Literal(Value::List(values)) => {
                    assert!(values.is_empty());
                }
                other => panic!("unexpected WITH expression {other:?}"),
            }
            assert!(select.returns.len() == 1);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn parse_scalar_functions() {
    let input = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN coalesce(a.nickname, a.name) AS display, startNode(r) AS start, endNode(r) AS end, randomUUID() AS uuid;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.returns.len(), 4);
            match &select.returns[0].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Coalesce(args)) => {
                        assert_eq!(args.len(), 2);
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
            match &select.returns[1].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::StartNode(field)) => {
                        assert_eq!(field.alias, "r");
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
        }
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_user_defined_scalar_function() {
    let input = "MATCH (n:Person) RETURN customFunc(n.age, 2) AS result;";
    let queries = parse_queries(input).expect("custom function parsed");
    match &queries[0] {
        Query::Select(select) => match &select.returns[0].expression {
            Expression::Function(func) => match func.as_ref() {
                FunctionExpression::Scalar(ScalarFunction::UserDefined(call)) => {
                    assert_eq!(call.name, "customFunc");
                    assert_eq!(call.arguments.len(), 2);
                }
                other => panic!("unexpected expression {other:?}"),
            },
            other => panic!("unexpected expression {other:?}"),
        },
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_math_functions() {
    let input = "MATCH (p:Person) RETURN abs(p.id) AS absVal, round(1.2, 1, 'HALF_EVEN') AS rounded, atan2(1.0, 2.0) AS angle;";
    let queries = parse_queries(input).expect("math functions parsed");
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.returns.len(), 3);
            match &select.returns[0].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Abs(expr)) => {
                        assert!(matches!(expr, Expression::Field(_)));
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
            match &select.returns[1].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Round {
                        value: _,
                        precision,
                        mode,
                    }) => {
                        assert!(precision.is_some());
                        assert!(mode.is_some());
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
            match &select.returns[2].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Atan2 { y, x }) => {
                        assert!(matches!(y, Expression::Literal(_)));
                        assert!(matches!(x, Expression::Literal(_)));
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
        }
        other => panic!("unexpected query {other:?}"),
    }
}

#[test]
fn parse_list_scalar_functions() {
    let input = "MATCH (p:Person) RETURN range(0,10,2) AS nums, reverse([1,2,3]) AS rev, keys(p) AS propKeys;";
    let queries = parse_queries(input).unwrap();
    match &queries[0] {
        Query::Select(select) => {
            assert_eq!(select.returns.len(), 3);
            match &select.returns[0].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Range {
                        start: _,
                        end: _,
                        step,
                    }) => {
                        assert!(step.is_some());
                    }
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
            match &select.returns[1].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Reverse(_)) => {}
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
            match &select.returns[2].expression {
                Expression::Function(func) => match func.as_ref() {
                    FunctionExpression::Scalar(ScalarFunction::Keys(_)) => {}
                    other => panic!("unexpected expression {other:?}"),
                },
                other => panic!("unexpected expression {other:?}"),
            }
        }
        other => panic!("unexpected query {other:?}"),
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

#[test]
fn parse_reduce_function() {
    let input = r#"
MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person)
RETURN nodes(p) AS nodes,
       relationships(p) AS rels,
       reduce(total = 0, person IN nodes(p) | total + 1) AS score;
"#;
    parse_queries(input).expect("reduce query");
}
