use std::collections::HashMap;

use nom::branch::alt;
use nom::bytes::complete::{escaped, tag, tag_no_case, take_while, take_while1};
use nom::character::complete::{alpha1, char, digit1, multispace0};
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
        let (input, _) = multispace0(input)?;
        let (input, out) = inner(input)?;
        let (input, _) = multispace0(input)?;
        Ok((input, out))
    }
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
        bool_literal,
        null_literal,
        number_literal,
        map(bare_word, |s: &str| Value::String(s.to_string())),
    ))(input)
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
    let (input, _) = multispace0(input)?;
    let (input, colon) = opt(char(':'))(input)?;
    if colon.is_some() {
        let (input, _) = multispace0(input)?;
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

fn condition_expr(input: &str) -> IResult<Condition> {
    let (input, alias) = ws(identifier)(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, property) = ws(property_key)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, value) = ws(value_literal)(input)?;
    Ok((
        input,
        Condition {
            alias: alias.to_string(),
            property: property.to_string(),
            operator: ComparisonOperator::Equals,
            value,
        },
    ))
}

fn condition_list(input: &str) -> IResult<Vec<Condition>> {
    separated_list1(ws(alt((tag_no_case("AND"), tag(",")))), condition_expr)(input)
}

fn return_list(input: &str) -> IResult<Vec<String>> {
    map(separated_list1(ws(char(',')), ws(bare_word)), |items| {
        items.into_iter().map(|s| s.to_string()).collect()
    })(input)
}

fn select_stmt(input: &str) -> IResult<Query> {
    let (input, _) = ws(tag_no_case("SELECT"))(input)?;
    let (input, _) = ws(tag_no_case("MATCH"))(input)?;
    let (input, pattern) = ws(node_pattern)(input)?;
    let (input, conditions) = opt(|input| {
        let (input, _) = ws(tag_no_case("WHERE"))(input)?;
        condition_list(input)
    })(input)?;
    let (input, _) = ws(tag_no_case("RETURN"))(input)?;
    let (input, returns) = return_list(input)?;
    Ok((
        input,
        Query::Select {
            pattern,
            conditions: conditions.unwrap_or_default(),
            returns,
        },
    ))
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

fn statement(input: &str) -> IResult<Query> {
    alt((
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
    let (input, _) = multispace0(input)?;
    let (input, statements) = many0(delimited(
        multispace0,
        statement_with_opt_terminator,
        multispace0,
    ))(input)?;
    let (input, _) = opt(statement_terminator)(input)?;
    let (input, _) = multispace0(input)?;
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
