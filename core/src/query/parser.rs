use std::collections::HashMap;

use nom::branch::alt;
use nom::bytes::complete::{escaped, tag, tag_no_case, take_while, take_while1};
use nom::character::complete::{alpha1, char, digit1, one_of};
use nom::combinator::{all_consuming, map, opt, recognize, value};
use nom::error::{ErrorKind, VerboseError, VerboseErrorKind, convert_error};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, separated_pair, tuple};

use super::ast::*;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("{0}")]
    Message(String),
}

type IResult<'a, O> = nom::IResult<&'a str, O, VerboseError<&'a str>>;

fn ws<'a, F, O>(mut inner: F) -> impl FnMut(&'a str) -> IResult<'a, O>
where
    F: FnMut(&'a str) -> IResult<'a, O>,
{
    move |input| {
        let (input, _) = skip_ws_and_comments(input)?;
        let (input, out) = inner(input)?;
        let (input, _) = skip_ws_and_comments(input)?;
        Ok((input, out))
    }
}

fn skip_ws_and_comments(input: &str) -> IResult<()> {
    let mut rest = input;
    loop {
        let before_ws = rest;
        rest = rest.trim_start_matches(|c: char| c.is_whitespace());

        if let Some(stripped) = rest.strip_prefix("//") {
            if let Some(idx) = stripped.find('\n') {
                rest = &stripped[idx + 1..];
            } else {
                rest = "";
            }
            continue;
        }

        if let Some(stripped) = rest.strip_prefix("/*") {
            if let Some(idx) = stripped.find("*/") {
                rest = &stripped[idx + 2..];
                continue;
            } else {
                return Err(nom::Err::Failure(VerboseError {
                    errors: vec![(
                        rest,
                        VerboseErrorKind::Context("unterminated block comment"),
                    )],
                }));
            }
        }

        if rest == before_ws {
            break;
        }
    }

    Ok((rest, ()))
}

fn identifier(input: &str) -> IResult<&str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        take_while(|c: char| c.is_alphanumeric() || c == '_' || c == '-'),
    ))(input)
}

fn bare_word(input: &str) -> IResult<&str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')(input)
}

fn number_literal(input: &str) -> IResult<Value> {
    let fractional = tuple((char('.'), digit1));
    let (rest, raw) = recognize(tuple((opt(char('-')), digit1, opt(fractional))))(input)?;
    if raw.contains('.') {
        match raw.parse::<f64>() {
            Ok(value) => Ok((rest, Value::Float(value))),
            Err(_) => Err(nom::Err::Error(VerboseError {
                errors: vec![(input, VerboseErrorKind::Nom(ErrorKind::Float))],
            })),
        }
    } else {
        match raw.parse::<i64>() {
            Ok(value) => Ok((rest, Value::Integer(value))),
            Err(_) => Err(nom::Err::Error(VerboseError {
                errors: vec![(input, VerboseErrorKind::Nom(ErrorKind::Digit))],
            })),
        }
    }
}

fn bool_literal(input: &str) -> IResult<Value> {
    alt((
        value(Value::Boolean(true), tag_no_case("true")),
        value(Value::Boolean(false), tag_no_case("false")),
    ))(input)
}

fn null_literal(input: &str) -> IResult<Value> {
    value(Value::Null, tag_no_case("null"))(input)
}

fn quoted_string(delimiter: char) -> impl FnMut(&str) -> IResult<String> {
    move |input| {
        let (input, _) = char(delimiter)(input)?;
        let escape = alt((
            tag("\\\\"),
            tag("\\\""),
            tag("\\'"),
            tag("\\n"),
            tag("\\r"),
            tag("\\t"),
        ));
        let (input, content) = opt(escaped(
            take_while1(|c| c != delimiter && c != '\\'),
            '\\',
            escape,
        ))(input)?;
        let (input, _) = char(delimiter)(input)?;
        let raw = content.unwrap_or("");
        Ok((input, interpret_escapes(raw)))
    }
}

fn string_literal(input: &str) -> IResult<Value> {
    map(
        alt((quoted_string('"'), quoted_string('\''))),
        Value::String,
    )(input)
}

fn interpret_escapes(raw: &str) -> String {
    raw.replace("\\\"", "\"").replace("\\'", "'")
}

fn value_literal(input: &str) -> IResult<Value> {
    alt((
        string_literal,
        array_literal,
        bool_literal,
        null_literal,
        number_literal,
        map(bare_word, |s: &str| Value::String(s.to_string())),
    ))(input)
}

fn array_literal(input: &str) -> IResult<Value> {
    map(
        delimited(
            ws(char('[')),
            separated_list0(ws(char(',')), ws(value_literal)),
            ws(char(']')),
        ),
        Value::List,
    )(input)
}

fn property_key(input: &str) -> IResult<&str> {
    alt((identifier, bare_word))(input)
}

fn properties_block(input: &str) -> IResult<HashMap<String, Value>> {
    let entry = separated_pair(ws(identifier), ws(char(':')), ws(value_literal));
    map(
        delimited(
            ws(char('{')),
            separated_list0(ws(char(',')), entry),
            ws(char('}')),
        ),
        |pairs| pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    )(input)
}

fn alias_and_label(input: &str) -> IResult<(Option<String>, Option<String>)> {
    let (input, maybe_alias) = opt(identifier)(input)?;
    let alias = maybe_alias.map(|s| s.to_string());
    let (input, _) = skip_ws_and_comments(input)?;
    let (input, colon) = opt(char(':'))(input)?;
    if colon.is_some() {
        let (input, _) = skip_ws_and_comments(input)?;
        let (input, maybe_label) = opt(identifier)(input)?;
        Ok((input, (alias, maybe_label.map(|s| s.to_string()))))
    } else {
        Ok((input, (alias, None)))
    }
}

fn node_pattern(input: &str) -> IResult<NodePattern> {
    let (input, _) = ws(char('('))(input)?;
    let (input, (alias, label)) = alias_and_label(input)?;
    let (input, properties) = opt(properties_block)(input)?;
    let (input, _) = ws(char(')'))(input)?;
    let props = properties.unwrap_or_default();
    Ok((
        input,
        NodePattern {
            alias,
            label,
            properties: Properties::new(props),
        },
    ))
}

fn edge_pattern(input: &str) -> IResult<EdgePattern> {
    let (input, _) = ws(char('['))(input)?;
    let (input, (alias, label)) = alias_and_label(input)?;
    let (input, properties) = opt(properties_block)(input)?;
    let (input, _) = ws(char(']'))(input)?;
    let props = properties.unwrap_or_default();
    Ok((
        input,
        EdgePattern {
            alias,
            label,
            properties: Properties::new(props),
        },
    ))
}

fn insert_node_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("INSERT"))(input)?;
    let (input, _) = ws(tag_no_case("NODE"))(input)?;
    let (input, pattern) = ws(node_pattern)(input)?;
    Ok((input, Query::InsertNode { pattern }))
}

fn insert_edge_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("INSERT"))(input)?;
    let (input, _) = ws(tag_no_case("EDGE"))(input)?;
    let (input, source) = ws(node_pattern)(input)?;
    let (input, _) = ws(char('-'))(input)?;
    let (input, edge) = ws(edge_pattern)(input)?;
    let (input, _) = ws(tag("->"))(input)?;
    let (input, target) = ws(node_pattern)(input)?;
    Ok((
        input,
        Query::InsertEdge {
            source,
            edge,
            target,
        },
    ))
}

fn create_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("CREATE"))(input)?;
    let (input, left) = ws(node_pattern)(input)?;
    let (input, relationship) = opt(|input| {
        let (input, _) = ws(char('-'))(input)?;
        let (input, edge) = ws(edge_pattern)(input)?;
        let (input, _) = ws(tag("->"))(input)?;
        let (input, right) = ws(node_pattern)(input)?;
        Ok((input, CreateRelationship { edge, right }))
    })(input)?;
    Ok((
        input,
        Query::Create {
            pattern: CreatePattern { left, relationship },
        },
    ))
}

fn delete_node_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("DELETE"))(input)?;
    let (input, _) = ws(tag_no_case("NODE"))(input)?;
    let (input, id) = ws(bare_word)(input)?;
    Ok((input, Query::DeleteNode { id: id.to_string() }))
}

fn delete_edge_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("DELETE"))(input)?;
    let (input, _) = ws(tag_no_case("EDGE"))(input)?;
    let (input, id) = ws(bare_word)(input)?;
    Ok((input, Query::DeleteEdge { id: id.to_string() }))
}

fn assignment_list(input: &str) -> IResult<HashMap<String, Value>> {
    let assignment = separated_pair(ws(property_key), ws(char('=')), ws(value_literal));
    map(separated_list1(ws(char(',')), assignment), |pairs| {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    })(input)
}

fn update_node_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("UPDATE"))(input)?;
    let (input, _) = ws(tag_no_case("NODE"))(input)?;
    let (input, id) = ws(bare_word)(input)?;
    let (input, _) = ws(tag_no_case("SET"))(input)?;
    let (input, assignments) = assignment_list(input)?;
    Ok((
        input,
        Query::UpdateNode {
            id: id.to_string(),
            assignments,
        },
    ))
}

fn update_edge_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("UPDATE"))(input)?;
    let (input, _) = ws(tag_no_case("EDGE"))(input)?;
    let (input, id) = ws(bare_word)(input)?;
    let (input, _) = ws(tag_no_case("SET"))(input)?;
    let (input, assignments) = assignment_list(input)?;
    Ok((
        input,
        Query::UpdateEdge {
            id: id.to_string(),
            assignments,
        },
    ))
}

enum ConditionTerm {
    Comparison(Condition),
    Predicate(PredicateFilter),
}

fn condition_expr(input: &str) -> IResult<ConditionTerm> {
    let (input, _) = skip_ws_and_comments(input)?;
    if starts_with_predicate(input) {
        predicate_condition(input)
            .map(|(rest, predicate)| (rest, ConditionTerm::Predicate(predicate)))
    } else {
        comparison_condition(input).map(|(rest, cond)| (rest, ConditionTerm::Comparison(cond)))
    }
}

fn predicate_condition(input: &str) -> IResult<PredicateFilter> {
    let (input, _) = skip_ws_and_comments(input)?;
    let (input, negated) = opt(ws(tag_no_case("NOT")))(input)?;
    let (input, _) = skip_ws_and_comments(input)?;
    let (input, function) = function_expression(input)?;
    Ok((
        input,
        PredicateFilter {
            function,
            negated: negated.is_some(),
        },
    ))
}

fn comparison_condition(input: &str) -> IResult<Condition> {
    let (input, alias) = ws(identifier)(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, property) = ws(property_key)(input)?;

    if let Ok((input, _)) = ws(tag_no_case("IS"))(input) {
        let (input, not) = opt(ws(tag_no_case("NOT")))(input)?;
        let (input, _) = ws(tag_no_case("NULL"))(input)?;
        let operator = if not.is_some() {
            ComparisonOperator::IsNotNull
        } else {
            ComparisonOperator::IsNull
        };
        return Ok((
            input,
            Condition {
                alias: alias.to_string(),
                property: property.to_string(),
                operator,
                value: None,
            },
        ));
    }

    let (input, operator) = comparison_operator(input)?;
    let (input, value) = ws(value_literal)(input)?;
    Ok((
        input,
        Condition {
            alias: alias.to_string(),
            property: property.to_string(),
            operator,
            value: Some(value),
        },
    ))
}

fn condition_list(input: &str) -> IResult<Vec<ConditionTerm>> {
    separated_list1(ws(alt((tag_no_case("AND"), tag(",")))), condition_expr)(input)
}

fn starts_with_predicate(input: &str) -> bool {
    let trimmed = input.trim_start();
    starts_with_function_call(trimmed)
        || strip_keyword(trimmed, "not")
            .map(|rest| starts_with_function_call(rest.trim_start()))
            .unwrap_or(false)
}

fn starts_with_function_call(input: &str) -> bool {
    const KEYWORDS: [&str; 6] = ["all", "any", "none", "single", "isempty", "exists"];
    KEYWORDS.iter().any(|kw| {
        strip_keyword(input, kw)
            .map(|rest| rest.trim_start().starts_with('('))
            .unwrap_or(false)
    })
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    if input.len() < keyword.len() {
        return None;
    }
    let (prefix, rest) = input.split_at(keyword.len());
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if let Some(next) = rest.chars().next() {
        if next.is_alphanumeric() || next == '_' {
            return None;
        }
    }
    Some(rest)
}

fn comparison_operator(input: &str) -> IResult<ComparisonOperator> {
    ws(alt((
        value(ComparisonOperator::GreaterThanOrEqual, tag(">=")),
        value(ComparisonOperator::LessThanOrEqual, tag("<=")),
        value(ComparisonOperator::NotEquals, alt((tag("<>"), tag("!=")))),
        value(ComparisonOperator::GreaterThan, tag(">")),
        value(ComparisonOperator::LessThan, tag("<")),
        value(ComparisonOperator::Equals, tag("=")),
    )))(input)
}

fn select_stmt(input: &str) -> IResult<Query> {
    let (input, explain_kw) = opt(ws(tag_no_case("EXPLAIN")))(input)?;
    let explain = explain_kw.is_some();
    let (input, _) = ws(tag_no_case("SELECT"))(input)?;
    let (input, match_clauses) = parse_select_match_clauses(input)?;
    let (input, condition_terms) = opt(|input| {
        let (input, _) = ws(tag_no_case("WHERE"))(input)?;
        condition_list(input)
    })(input)?;
    let (input, with_clause) = opt(with_clause)(input)?;
    let (input, _) = ws(tag_no_case("RETURN"))(input)?;
    let mut parser = projection_list(true);
    let (input, returns) = parser(input)?;

    let mut conditions = Vec::new();
    let mut predicates = Vec::new();
    if let Some(terms) = condition_terms {
        for term in terms {
            match term {
                ConditionTerm::Comparison(cond) => conditions.push(cond),
                ConditionTerm::Predicate(pred) => predicates.push(pred),
            }
        }
    }

    Ok((
        input,
        Query::Select(SelectQuery {
            match_clauses,
            conditions,
            predicates,
            with: with_clause,
            returns,
            explain,
        }),
    ))
}

fn parse_select_match_clauses(mut input: &str) -> IResult<Vec<SelectMatchClause>> {
    let mut clauses = Vec::new();
    loop {
        match parse_single_match_clause(input) {
            Ok((rest, clause)) => {
                clauses.push(clause);
                input = rest;
            }
            Err(nom::Err::Error(_)) => break,
            Err(err) => return Err(err),
        }
    }

    if clauses.is_empty() {
        return Err(nom::Err::Error(VerboseError {
            errors: vec![(input, VerboseErrorKind::Context("MATCH clause is required"))],
        }));
    }

    Ok((input, clauses))
}

fn parse_single_match_clause(input: &str) -> IResult<SelectMatchClause> {
    let (input, optional) = opt(ws(tag_no_case("OPTIONAL")))(input)?;
    let (input, _) = ws(tag_no_case("MATCH"))(input)?;
    let (input, patterns) = separated_list1(ws(char(',')), match_pattern)(input)?;
    Ok((
        input,
        SelectMatchClause {
            optional: optional.is_some(),
            patterns,
        },
    ))
}

fn match_pattern(input: &str) -> IResult<MatchPattern> {
    if let Ok((rest, path)) = path_match_pattern(input) {
        return Ok((rest, MatchPattern::Path(path)));
    }
    let (input, left) = ws(node_pattern)(input)?;
    if let Ok((input, relationship)) = parse_relationship_pattern(input) {
        let (input, right) = ws(node_pattern)(input)?;
        return Ok((
            input,
            MatchPattern::Relationship(RelationshipMatch {
                left,
                relationship,
                right,
            }),
        ));
    }
    Ok((input, MatchPattern::Node(left)))
}

fn path_match_pattern(input: &str) -> IResult<PathPattern> {
    let (input, alias) = ws(identifier)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, binding) = parse_path_expression(input, alias.to_string())?;
    Ok((
        input,
        PathPattern {
            alias: binding.alias,
            pattern: RelationshipMatch {
                left: binding.start,
                relationship: binding.relationship,
                right: binding.end,
            },
            mode: binding.mode,
        },
    ))
}

fn with_clause(input: &str) -> IResult<WithClause> {
    let (input, _) = ws(tag_no_case("WITH"))(input)?;
    let mut parser = projection_list(false);
    let (input, projections) = parser(input)?;
    Ok((input, WithClause { projections }))
}

fn projection_list<'a>(
    allow_aggregate: bool,
) -> impl FnMut(&'a str) -> IResult<'a, Vec<Projection>> {
    move |input| separated_list1(ws(char(',')), projection(allow_aggregate))(input)
}

fn projection<'a>(allow_aggregate: bool) -> impl FnMut(&'a str) -> IResult<'a, Projection> {
    move |input| {
        let (input, expression) = if allow_aggregate {
            parse_expression(input)?
        } else {
            non_aggregate_expression(input)?
        };
        let (input, alias) = opt(projection_alias)(input)?;
        Ok((input, Projection { expression, alias }))
    }
}

fn parse_expression(input: &str) -> IResult<Expression> {
    if let Ok((rest, agg)) = aggregate_expression(input) {
        return Ok((rest, Expression::Aggregate(agg)));
    }
    non_aggregate_expression(input)
}

fn non_aggregate_expression(input: &str) -> IResult<Expression> {
    parse_additive_expression(input)
}

fn parse_additive_expression(input: &str) -> IResult<Expression> {
    let (mut input, mut expr) = non_aggregate_term(input)?;
    loop {
        let op_result = ws(one_of("+-"))(input);
        let (next_input, op_char) = match op_result {
            Ok(result) => result,
            Err(_) => break,
        };
        let (next_input, rhs) = non_aggregate_term(next_input)?;
        let operator = match op_char {
            '+' => BinaryOperator::Add,
            '-' => BinaryOperator::Subtract,
            _ => unreachable!(),
        };
        expr = Expression::BinaryOp {
            left: Box::new(expr),
            operator,
            right: Box::new(rhs),
        };
        input = next_input;
    }
    Ok((input, expr))
}

fn non_aggregate_term(input: &str) -> IResult<Expression> {
    if let Ok((rest, func)) = function_expression(input) {
        return Ok((rest, Expression::Function(Box::new(func))));
    }
    if let Ok((rest, literal)) = literal_expression(input) {
        return Ok((rest, literal));
    }
    let (rest, field) = field_reference(input)?;
    Ok((rest, Expression::Field(field)))
}

fn projection_alias(input: &str) -> IResult<String> {
    let (input, _) = ws(tag_no_case("AS"))(input)?;
    let (input, alias) = ws(identifier)(input)?;
    Ok((input, alias.to_string()))
}

fn field_reference(input: &str) -> IResult<FieldReference> {
    let (input, alias) = ws(identifier)(input)?;
    let (input, property) = opt(|input| {
        let (input, _) = ws(char('.'))(input)?;
        let (input, key) = ws(property_key)(input)?;
        Ok((input, key.to_string()))
    })(input)?;

    Ok((
        input,
        FieldReference {
            alias: alias.to_string(),
            property,
        },
    ))
}

fn aggregate_expression(input: &str) -> IResult<AggregateExpression> {
    alt((
        avg_function,
        collect_function,
        count_function,
        max_function,
        min_function,
        percentile_cont_function,
        percentile_disc_function,
        stdev_function,
        stdevp_function,
        sum_function,
    ))(input)
}

fn function_expression(input: &str) -> IResult<FunctionExpression> {
    alt((
        scalar_function_expression,
        list_predicate_function("all", ListPredicateKind::All),
        list_predicate_function("any", ListPredicateKind::Any),
        list_predicate_function("none", ListPredicateKind::None),
        list_predicate_function("single", ListPredicateKind::Single),
        is_empty_function,
        exists_function,
    ))(input)
}

fn scalar_function_expression(input: &str) -> IResult<FunctionExpression> {
    map(
        alt((
            scalar_function_group_one,
            scalar_function_group_two,
            scalar_function_group_three,
        )),
        FunctionExpression::Scalar,
    )(input)
}

fn scalar_function_group_one(input: &str) -> IResult<ScalarFunction> {
    alt((
        nodes_function,
        relationships_function,
        keys_function,
        labels_function,
        range_function,
        reverse_function,
        tail_function,
        to_boolean_list_function,
        to_float_list_function,
        to_integer_list_function,
        to_string_list_function,
    ))(input)
}

fn scalar_function_group_two(input: &str) -> IResult<ScalarFunction> {
    alt((
        coalesce_function,
        start_node_function,
        end_node_function,
        head_function,
        last_function,
        id_function,
        properties_function,
        random_uuid_function,
        size_function,
        length_function,
        timestamp_function,
        reduce_function,
    ))(input)
}

fn scalar_function_group_three(input: &str) -> IResult<ScalarFunction> {
    alt((
        to_boolean_function,
        to_boolean_or_null_function,
        to_float_function,
        to_float_or_null_function,
        to_integer_function,
        to_integer_or_null_function,
        type_function,
    ))(input)
}

fn coalesce_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("coalesce"))(input)?;
    let (input, args) = delimited(
        ws(char('(')),
        separated_list1(ws(char(',')), non_aggregate_expression),
        ws(char(')')),
    )(input)?;
    Ok((input, ScalarFunction::Coalesce(args)))
}

fn start_node_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("startNode"))(input)?;
    let (input, field) = delimited(ws(char('(')), field_reference, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::StartNode(field)))
}

fn end_node_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("endNode"))(input)?;
    let (input, field) = delimited(ws(char('(')), field_reference, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::EndNode(field)))
}

fn head_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("head"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Head(expr)))
}

fn last_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("last"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Last(expr)))
}

fn id_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("id"))(input)?;
    let (input, field) = delimited(ws(char('(')), field_reference, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Id(field)))
}

fn properties_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("properties"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Properties(expr)))
}

fn random_uuid_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("randomUUID"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, ScalarFunction::RandomUuid))
}

fn size_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("size"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Size(expr)))
}

fn length_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("length"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Length(expr)))
}

fn timestamp_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("timestamp"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, ScalarFunction::Timestamp))
}

fn to_boolean_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toBoolean"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToBoolean {
            expr,
            null_on_unsupported: false,
        },
    ))
}

fn to_boolean_or_null_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toBooleanOrNull"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToBoolean {
            expr,
            null_on_unsupported: true,
        },
    ))
}

fn to_float_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toFloat"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToFloat {
            expr,
            null_on_unsupported: false,
        },
    ))
}

fn to_float_or_null_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toFloatOrNull"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToFloat {
            expr,
            null_on_unsupported: true,
        },
    ))
}

fn to_integer_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toInteger"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToInteger {
            expr,
            null_on_unsupported: false,
        },
    ))
}

fn to_integer_or_null_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toIntegerOrNull"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((
        input,
        ScalarFunction::ToInteger {
            expr,
            null_on_unsupported: true,
        },
    ))
}

fn type_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("type"))(input)?;
    let (input, field) = delimited(ws(char('(')), field_reference, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Type(field)))
}

fn keys_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("keys"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Keys(expr)))
}

fn labels_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("labels"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Labels(expr)))
}

fn nodes_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("nodes"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Nodes(expr)))
}

fn relationships_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("relationships"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Relationships(expr)))
}

fn range_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("range"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, start) = non_aggregate_expression(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, end) = non_aggregate_expression(input)?;
    let (input, step) = opt(|input| {
        let (input, _) = ws(char(','))(input)?;
        let (input, expr) = non_aggregate_expression(input)?;
        Ok((input, expr))
    })(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, ScalarFunction::Range { start, end, step }))
}

fn reduce_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("reduce"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, accumulator) = ws(identifier)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, initial) = non_aggregate_expression(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, variable) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("IN"))(input)?;
    let (input, list_expr) = non_aggregate_expression(input)?;
    let (input, _) = ws(char('|'))(input)?;
    let (input, expression) = non_aggregate_expression(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        ScalarFunction::Reduce {
            accumulator: accumulator.to_string(),
            initial,
            variable: variable.to_string(),
            list: list_expr,
            expression,
        },
    ))
}

fn reverse_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("reverse"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Reverse(expr)))
}

fn tail_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("tail"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::Tail(expr)))
}

fn to_boolean_list_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toBooleanList"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::ToBooleanList(expr)))
}

fn to_float_list_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toFloatList"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::ToFloatList(expr)))
}

fn to_integer_list_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toIntegerList"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::ToIntegerList(expr)))
}

fn to_string_list_function(input: &str) -> IResult<ScalarFunction> {
    let (input, _) = ws(tag_no_case("toStringList"))(input)?;
    let (input, expr) = delimited(ws(char('(')), non_aggregate_expression, ws(char(')')))(input)?;
    Ok((input, ScalarFunction::ToStringList(expr)))
}

fn avg_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("avg"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Avg,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn collect_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("collect"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Collect,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn count_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("count"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    if let Ok((input, _)) = ws(char('*'))(input) {
        let (input, _) = ws(char(')'))(input)?;
        return Ok((
            input,
            AggregateExpression {
                function: AggregateFunction::CountAll,
                target: None,
                percentile: None,
            },
        ));
    }
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Count,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn max_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("max"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Max,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn min_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("min"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Min,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn percentile_cont_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("percentilecont"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, percentile) = numeric_literal(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::PercentileCont,
            target: Some(field),
            percentile: Some(percentile),
        },
    ))
}

fn percentile_disc_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("percentiledisc"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, percentile) = numeric_literal(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::PercentileDisc,
            target: Some(field),
            percentile: Some(percentile),
        },
    ))
}

fn stdev_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("stdev"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::StDev,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn stdevp_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("stdevp"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::StDevP,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn sum_function(input: &str) -> IResult<AggregateExpression> {
    let (input, _) = ws(tag_no_case("sum"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, field) = field_reference(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        AggregateExpression {
            function: AggregateFunction::Sum,
            target: Some(field),
            percentile: None,
        },
    ))
}

fn list_predicate_function<'a>(
    keyword: &'static str,
    kind: ListPredicateKind,
) -> impl FnMut(&'a str) -> IResult<'a, FunctionExpression> {
    move |input| {
        let (input, _) = ws(tag_no_case(keyword))(input)?;
        let (input, _) = ws(char('('))(input)?;
        let (input, variable) = ws(identifier)(input)?;
        let (input, _) = ws(tag_no_case("IN"))(input)?;
        let (input, list) = list_expression(input)?;
        let (input, _) = ws(tag_no_case("WHERE"))(input)?;
        let (input, predicate) = list_predicate_expr(variable)(input)?;
        let (input, _) = ws(char(')'))(input)?;
        Ok((
            input,
            FunctionExpression::ListPredicate(ListPredicateFunction {
                kind,
                variable: variable.to_string(),
                list,
                predicate,
            }),
        ))
    }
}

fn list_expression(input: &str) -> IResult<ListExpression> {
    if let Ok((rest, field)) = field_reference(input) {
        return Ok((rest, ListExpression::Field(field)));
    }
    let (rest, literal) = ws(value_literal)(input)?;
    Ok((rest, ListExpression::Literal(literal)))
}

fn list_predicate_expr<'a>(variable: &'a str) -> impl FnMut(&'a str) -> IResult<'a, ListPredicate> {
    move |input| {
        let (input, ident) = ws(identifier)(input)?;
        if ident != variable {
            return Err(nom::Err::Failure(VerboseError {
                errors: vec![(
                    input,
                    VerboseErrorKind::Context("predicate variable mismatch"),
                )],
            }));
        }

        if let Ok((input, _)) = ws(tag_no_case("IS"))(input) {
            let (input, not_kw) = opt(ws(tag_no_case("NOT")))(input)?;
            let (input, _) = ws(tag_no_case("NULL"))(input)?;
            return Ok((
                input,
                ListPredicate::IsNull {
                    negated: not_kw.is_some(),
                },
            ));
        }

        let (input, operator) = comparison_operator(input)?;
        let (input, value) = ws(value_literal)(input)?;
        Ok((input, ListPredicate::Comparison { operator, value }))
    }
}

fn value_operand(input: &str) -> IResult<ValueOperand> {
    if let Ok((rest, field)) = field_reference(input) {
        return Ok((rest, ValueOperand::Field(field)));
    }
    let (rest, literal) = ws(literal_value)(input)?;
    Ok((rest, ValueOperand::Literal(literal)))
}

fn literal_expression(input: &str) -> IResult<Expression> {
    let (input, value) = ws(literal_value)(input)?;
    Ok((input, Expression::Literal(value)))
}

fn literal_value(input: &str) -> IResult<Value> {
    alt((
        string_literal,
        array_literal,
        bool_literal,
        null_literal,
        number_literal,
    ))(input)
}

fn is_empty_function(input: &str) -> IResult<FunctionExpression> {
    let (input, _) = ws(tag_no_case("isEmpty"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, operand) = value_operand(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, FunctionExpression::IsEmpty(operand)))
}

fn exists_function(input: &str) -> IResult<FunctionExpression> {
    let (input, _) = ws(tag_no_case("exists"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, left) = ws(node_pattern)(input)?;
    let (input, relationship) = ws(parse_relationship_pattern)(input)?;
    let (input, right) = ws(node_pattern)(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((
        input,
        FunctionExpression::Exists(ExistsFunction {
            pattern: RelationshipMatch {
                left,
                relationship,
                right,
            },
        }),
    ))
}

fn numeric_literal(input: &str) -> IResult<f64> {
    let (input, value) = ws(value_literal)(input)?;
    match value {
        Value::Float(f) => Ok((input, f)),
        Value::Integer(i) => Ok((input, i as f64)),
        _ => Err(nom::Err::Failure(VerboseError {
            errors: vec![(
                input,
                VerboseErrorKind::Context("percentile requires numeric literal"),
            )],
        })),
    }
}

fn call_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("CALL"))(input)?;
    let (input, _) = ws(tag_no_case("GRAPHDB"))(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, proc_ident) = ws(identifier)(input)?;
    let procedure = GraphDbProcedure::from_identifier(proc_ident).ok_or_else(|| {
        nom::Err::Failure(VerboseError {
            errors: vec![(proc_ident, VerboseErrorKind::Context("unknown procedure"))],
        })
    })?;
    let (input, _) = ws(char('('))(input)?;
    let (input, _) = ws(char(')'))(input)?;

    Ok((
        input,
        Query::CallProcedure {
            procedure: Procedure::GraphDb(procedure),
        },
    ))
}

fn path_match_stmt(origin: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("MATCH"))(origin)?;

    let mut rest = input;
    let mut node_bindings: HashMap<String, NodePattern> = HashMap::new();
    let mut path_binding: Option<PathBinding> = None;

    let (next, clause) = match_clause(rest)?;
    rest = next;
    apply_clause(clause, &mut node_bindings, &mut path_binding, rest)?;

    loop {
        let (next_input, _) = skip_ws_and_comments(rest)?;
        if let Ok((after_match, _)) = ws(tag_no_case("MATCH"))(next_input) {
            let (after_clause, clause) = match_clause(after_match)?;
            rest = after_clause;
            apply_clause(clause, &mut node_bindings, &mut path_binding, rest)?;
        } else {
            rest = next_input;
            break;
        }
    }

    let mut filter = None;
    if let Ok((after_where, _)) = ws(tag_no_case("WHERE"))(rest) {
        let (after_filter, parsed_filter) = where_clause(after_where)?;
        rest = after_filter;
        filter = Some(parsed_filter);
    }

    let (rest, return_items) = return_clause(rest)?;

    let binding = path_binding.ok_or_else(|| {
        nom::Err::Failure(VerboseError {
            errors: vec![(origin, VerboseErrorKind::Context("missing path binding"))],
        })
    })?;

    let start_pattern = resolve_node_pattern(binding.start.clone(), &node_bindings, origin)?;
    let end_pattern = resolve_node_pattern(binding.end.clone(), &node_bindings, origin)?;

    let start_alias = start_pattern.alias.clone().ok_or_else(|| {
        nom::Err::Failure(VerboseError {
            errors: vec![(
                input,
                VerboseErrorKind::Context("start node must have an alias"),
            )],
        })
    })?;
    let end_alias = end_pattern.alias.clone().ok_or_else(|| {
        nom::Err::Failure(VerboseError {
            errors: vec![(
                input,
                VerboseErrorKind::Context("end node must have an alias"),
            )],
        })
    })?;

    // ensure bindings reflect potentially resolved patterns
    node_bindings.insert(start_alias.clone(), start_pattern.clone());
    node_bindings.insert(end_alias.clone(), end_pattern.clone());

    let path_alias_value = binding.alias.clone();
    let start_alias_value = start_alias.clone();
    let end_alias_value = end_alias.clone();
    let returns = build_return_clause(
        return_items,
        path_alias_value,
        start_alias_value,
        end_alias_value,
        origin,
    )?;

    let query = PathMatchQuery {
        path_alias: binding.alias,
        start_alias,
        end_alias,
        start: start_pattern,
        end: end_pattern,
        relationship: binding.relationship,
        mode: binding.mode,
        filter,
        returns,
    };

    Ok((rest, Query::PathMatch(query)))
}

#[derive(Debug, Clone)]
struct PathBinding {
    alias: String,
    start: NodePattern,
    end: NodePattern,
    relationship: RelationshipPattern,
    mode: PathQueryMode,
}

#[derive(Debug, Clone)]
enum MatchClause {
    NodeList(Vec<NodePattern>),
    Path(PathBinding),
}

#[derive(Debug, Clone)]
enum ReturnItemAst {
    Identifier(String),
    Length(String),
}

fn apply_clause<'a>(
    clause: MatchClause,
    bindings: &mut HashMap<String, NodePattern>,
    path_binding: &mut Option<PathBinding>,
    ctx: &'a str,
) -> Result<(), nom::Err<VerboseError<&'a str>>> {
    match clause {
        MatchClause::NodeList(nodes) => {
            for node in nodes {
                if let Some(alias) = node.alias.clone() {
                    bindings.insert(alias, node);
                } else {
                    return Err(nom::Err::Failure(VerboseError {
                        errors: vec![(
                            ctx,
                            VerboseErrorKind::Context("node in MATCH must have alias"),
                        )],
                    }));
                }
            }
        }
        MatchClause::Path(binding) => {
            if path_binding.is_some() {
                return Err(nom::Err::Failure(VerboseError {
                    errors: vec![(
                        ctx,
                        VerboseErrorKind::Context("multiple path patterns not supported"),
                    )],
                }));
            }
            *path_binding = Some(binding);
        }
    }
    Ok(())
}

fn resolve_node_pattern<'a>(
    pattern: NodePattern,
    bindings: &HashMap<String, NodePattern>,
    _ctx: &'a str,
) -> Result<NodePattern, nom::Err<VerboseError<&'a str>>> {
    if let Some(alias) = &pattern.alias {
        if pattern.label.is_none() && pattern.properties.0.is_empty() {
            if let Some(existing) = bindings.get(alias) {
                return Ok(existing.clone());
            }
        }
    }
    Ok(pattern)
}

fn match_clause(input: &str) -> IResult<MatchClause> {
    if looks_like_path_clause(input) {
        path_binding_clause(input)
    } else {
        node_list_clause(input).map(|(rest, nodes)| (rest, MatchClause::NodeList(nodes)))
    }
}

fn looks_like_path_clause(input: &str) -> bool {
    let mut depth = 0i32;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
                        chars.next();
                    }
                    return matches!(chars.peek(), Some('-') | Some('<'));
                }
            }
            '=' if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

fn node_list_clause(input: &str) -> IResult<Vec<NodePattern>> {
    separated_list1(ws(char(',')), ws(node_pattern))(input)
}

fn path_binding_clause(input: &str) -> IResult<MatchClause> {
    alt((
        map(path_binding_with_alias, |binding| {
            MatchClause::Path(binding)
        }),
        map(path_binding_without_alias, |binding| {
            MatchClause::Path(binding)
        }),
    ))(input)
}

fn path_binding_with_alias(input: &str) -> IResult<PathBinding> {
    let (input, alias) = ws(identifier)(input)?;
    let (input, _) = ws(char('='))(input)?;
    parse_path_expression(input, alias.to_string())
}

fn path_binding_without_alias(input: &str) -> IResult<PathBinding> {
    parse_path_expression(input, "path".to_string())
}

fn parse_path_expression(input: &str, alias: String) -> IResult<PathBinding> {
    if let Ok((input, _)) = ws(tag_no_case("shortestpath"))(input) {
        let (input, _) = ws(char('('))(input)?;
        let (input, start) = ws(node_pattern)(input)?;
        let (input, relationship) = ws(parse_relationship_pattern)(input)?;
        let (input, end) = ws(node_pattern)(input)?;
        let (input, _) = ws(char(')'))(input)?;
        Ok((
            input,
            PathBinding {
                alias,
                start,
                end,
                relationship,
                mode: PathQueryMode::Shortest,
            },
        ))
    } else {
        let (input, start) = ws(node_pattern)(input)?;
        let (input, relationship) = ws(parse_relationship_pattern)(input)?;
        let (input, end) = ws(node_pattern)(input)?;
        Ok((
            input,
            PathBinding {
                alias,
                start,
                end,
                relationship,
                mode: PathQueryMode::All,
            },
        ))
    }
}

fn parse_relationship_pattern(input: &str) -> IResult<RelationshipPattern> {
    alt((
        parse_outbound_relationship,
        parse_inbound_relationship,
        parse_undirected_relationship,
    ))(input)
}

fn parse_outbound_relationship(input: &str) -> IResult<RelationshipPattern> {
    let (input, _) = ws(char('-'))(input)?;
    let (input, (edge, length)) = parse_bracketed_relationship(input)?;
    let (input, _) = ws(tag("->"))(input)?;
    Ok((
        input,
        RelationshipPattern {
            alias: edge.alias,
            label: edge.label,
            properties: edge.properties,
            direction: RelationshipDirection::Outbound,
            length,
        },
    ))
}

fn parse_inbound_relationship(input: &str) -> IResult<RelationshipPattern> {
    let (input, _) = ws(tag("<-"))(input)?;
    let (input, (edge, length)) = parse_bracketed_relationship(input)?;
    let (input, _) = ws(char('-'))(input)?;
    Ok((
        input,
        RelationshipPattern {
            alias: edge.alias,
            label: edge.label,
            properties: edge.properties,
            direction: RelationshipDirection::Inbound,
            length,
        },
    ))
}

fn parse_undirected_relationship(input: &str) -> IResult<RelationshipPattern> {
    let (input, _) = ws(char('-'))(input)?;
    let (input, (edge, length)) = parse_bracketed_relationship(input)?;
    let (input, _) = ws(char('-'))(input)?;
    Ok((
        input,
        RelationshipPattern {
            alias: edge.alias,
            label: edge.label,
            properties: edge.properties,
            direction: RelationshipDirection::Undirected,
            length,
        },
    ))
}

fn parse_bracketed_relationship(input: &str) -> IResult<(EdgePattern, PathLength)> {
    let (input, _) = ws(char('['))(input)?;
    let (input, (alias, label)) = alias_and_label(input)?;
    let (input, length) = opt(path_length_modifier)(input)?;
    let (input, properties) = opt(properties_block)(input)?;
    let (input, _) = ws(char(']'))(input)?;
    let props = properties.unwrap_or_default();
    Ok((
        input,
        (
            EdgePattern {
                alias,
                label,
                properties: Properties::new(props),
            },
            length.unwrap_or(PathLength::Exact(1)),
        ),
    ))
}

fn path_length_modifier(input: &str) -> IResult<PathLength> {
    let (input, _) = ws(char('*'))(input)?;
    let (input, min_part) = opt(ws(digit1))(input)?;
    let (input, range) = opt(ws(tag("..")))(input)?;
    if let Some(_) = range {
        let min = min_part
            .map(|digits| digits.parse::<u32>().unwrap_or(0))
            .unwrap_or(0);
        let (input, max_part) = opt(ws(digit1))(input)?;
        let max = max_part.map(|digits| digits.parse::<u32>().unwrap_or(min));
        Ok((input, PathLength::Range { min, max }))
    } else if let Some(digits) = min_part {
        let value = digits.parse::<u32>().unwrap_or(1);
        Ok((input, PathLength::Exact(value)))
    } else {
        Ok((input, PathLength::Range { min: 1, max: None }))
    }
}

fn where_clause(input: &str) -> IResult<PathFilter> {
    let (input, _) = ws(tag_no_case("NOT"))(input)?;
    let (input, from_node) = ws(node_pattern)(input)?;
    let (input, relationship) = ws(parse_relationship_pattern)(input)?;
    let (input, to_node) = ws(node_pattern)(input)?;

    let from_alias = from_node.alias.clone().ok_or_else(|| {
        nom::Err::Failure(VerboseError {
            errors: vec![(
                input,
                VerboseErrorKind::Context("WHERE clause requires alias"),
            )],
        })
    })?;
    Ok((
        input,
        PathFilter::ExcludeRelationship {
            from_alias,
            relationship,
            to_pattern: to_node,
        },
    ))
}

fn return_clause(input: &str) -> IResult<Vec<ReturnItemAst>> {
    let (input, _) = ws(tag_no_case("RETURN"))(input)?;
    separated_list1(ws(char(',')), parse_return_item)(input)
}

fn parse_return_item(input: &str) -> IResult<ReturnItemAst> {
    if let Ok((input, _)) = ws(tag_no_case("length"))(input) {
        let (input, _) = ws(char('('))(input)?;
        let (input, ident) = ws(identifier)(input)?;
        let (input, _) = ws(char(')'))(input)?;
        return Ok((input, ReturnItemAst::Length(ident.to_string())));
    }
    let (input, ident) = ws(identifier)(input)?;
    Ok((input, ReturnItemAst::Identifier(ident.to_string())))
}

fn build_return_clause(
    items: Vec<ReturnItemAst>,
    path_alias: String,
    start_alias: String,
    end_alias: String,
    ctx: &str,
) -> Result<PathReturn, nom::Err<VerboseError<&str>>> {
    match items.as_slice() {
        [ReturnItemAst::Identifier(alias)] if alias == &path_alias => Ok(PathReturn::Path {
            include_length: false,
        }),
        [
            ReturnItemAst::Identifier(alias),
            ReturnItemAst::Length(len_alias),
        ] if alias == &path_alias && len_alias == &path_alias => Ok(PathReturn::Path {
            include_length: true,
        }),
        [ReturnItemAst::Identifier(a), ReturnItemAst::Identifier(b)]
            if a == &start_alias && b == &end_alias =>
        {
            Ok(PathReturn::Nodes {
                start_alias,
                end_alias,
                include_length: false,
            })
        }
        [
            ReturnItemAst::Identifier(a),
            ReturnItemAst::Identifier(b),
            ReturnItemAst::Length(len),
        ] if a == &start_alias && b == &end_alias && len == &path_alias => Ok(PathReturn::Nodes {
            start_alias,
            end_alias,
            include_length: true,
        }),
        _ => Err(nom::Err::Failure(VerboseError {
            errors: vec![(ctx, VerboseErrorKind::Context("unsupported RETURN clause"))],
        })),
    }
}

fn statement(input: &str) -> IResult<Query> {
    alt((
        path_match_stmt,
        call_stmt,
        create_stmt,
        insert_node_stmt,
        insert_edge_stmt,
        delete_node_stmt,
        delete_edge_stmt,
        update_node_stmt,
        update_edge_stmt,
        select_stmt,
    ))(input)
}

fn statement_terminator(input: &str) -> IResult<()> {
    let (input, _) = many1(ws(char(';')))(input)?;
    Ok((input, ()))
}

fn statement_with_opt_terminator(input: &str) -> IResult<Query> {
    let (input, stmt) = statement(input)?;
    let (input, _) = opt(statement_terminator)(input)?;
    Ok((input, stmt))
}

fn query_list(input: &str) -> IResult<Vec<Query>> {
    let (input, _) = skip_ws_and_comments(input)?;
    let (input, statements) = many0(delimited(
        skip_ws_and_comments,
        statement_with_opt_terminator,
        skip_ws_and_comments,
    ))(input)?;
    let (input, _) = opt(statement_terminator)(input)?;
    let (input, _) = skip_ws_and_comments(input)?;
    Ok((input, statements))
}

pub fn parse_queries(input: &str) -> Result<Vec<Query>, ParseError> {
    match all_consuming(query_list)(input) {
        Ok((_, queries)) => Ok(queries.into_iter().collect()),
        Err(nom::Err::Error(err)) | Err(nom::Err::Failure(err)) => {
            Err(ParseError::Message(convert_error(input, err)))
        }
        Err(nom::Err::Incomplete(_)) => Err(ParseError::Message("unexpected end of input".into())),
    }
}
