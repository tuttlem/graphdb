use std::collections::HashMap;

use thiserror::Error;

use super::ast::*;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("{0}")]
    Message(String),
}

pub fn parse_queries(input: &str) -> Result<Vec<Query>, ParseError> {
    let mut queries = Vec::new();
    for stmt in input.split(';') {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        queries.push(parse_statement(trimmed)?);
    }
    Ok(queries)
}

fn parse_statement(input: &str) -> Result<Query, ParseError> {
    let stmt = input.trim_start();
    if let Some(rest) = strip_prefix_ci(stmt, "INSERT NODE") {
        return parse_insert_node(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "INSERT EDGE") {
        return parse_insert_edge(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "CREATE") {
        return parse_create(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "DELETE NODE") {
        return parse_delete_node(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "DELETE EDGE") {
        return parse_delete_edge(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "UPDATE NODE") {
        return parse_update_node(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "UPDATE EDGE") {
        return parse_update_edge(rest);
    }
    if let Some(rest) = strip_prefix_ci(stmt, "SELECT") {
        return parse_select(rest);
    }
    Err(ParseError::Message("unrecognised statement".into()))
}

fn parse_insert_node(rest: &str) -> Result<Query, ParseError> {
    let (pattern_str, remainder) = extract_group(rest, '(', ')')?;
    if !remainder.trim().is_empty() {
        return Err(ParseError::Message(
            "unexpected tokens after node pattern".into(),
        ));
    }
    let pattern = parse_node_pattern_str(&pattern_str)?;
    Ok(Query::InsertNode { pattern })
}

fn parse_insert_edge(rest: &str) -> Result<Query, ParseError> {
    let (source_str, after_source) = extract_group(rest, '(', ')')?;
    let after_source = after_source.trim_start();
    if !after_source.starts_with('-') {
        return Err(ParseError::Message("expected '-' after source node".into()));
    }
    let after_dash = after_source[1..].trim_start();
    let (edge_detail, after_edge) = extract_group(after_dash, '[', ']')?;
    let after_edge = after_edge.trim_start();
    if !after_edge.starts_with("->") {
        return Err(ParseError::Message("expected '->' in edge pattern".into()));
    }
    let (target_str, trailing) = extract_group(&after_edge[2..], '(', ')')?;
    if !trailing.trim().is_empty() {
        return Err(ParseError::Message(
            "unexpected tokens after edge pattern".into(),
        ));
    }

    let source = parse_node_pattern_str(&source_str)?;
    let edge = parse_edge_detail_str(&edge_detail)?;
    let target = parse_node_pattern_str(&target_str)?;

    Ok(Query::InsertEdge {
        source,
        edge,
        target,
    })
}

fn parse_create(rest: &str) -> Result<Query, ParseError> {
    let remaining = rest.trim_start();
    if !remaining.starts_with('(') {
        return Err(ParseError::Message("expected node pattern".into()));
    }

    let (left_str, after_left) = extract_group(remaining, '(', ')')?;
    let left = parse_node_pattern_str(&left_str)?;

    let after_left = after_left.trim_start();
    if after_left.is_empty() {
        return Ok(Query::Create {
            pattern: CreatePattern {
                left,
                relationship: None,
            },
        });
    }

    if !after_left.starts_with('-') {
        return Err(ParseError::Message("expected relationship pattern".into()));
    }

    let mut remaining = after_left[1..].trim_start();
    let (edge_str, after_edge) = extract_group(remaining, '[', ']')?;
    let edge = parse_edge_detail_str(&edge_str)?;
    remaining = after_edge.trim_start();

    if !remaining.starts_with("->") {
        return Err(ParseError::Message(
            "expected '->' in relationship pattern".into(),
        ));
    }

    let (right_str, trailing) = extract_group(&remaining[2..], '(', ')')?;
    if !trailing.trim().is_empty() {
        return Err(ParseError::Message(
            "unexpected tokens after create pattern".into(),
        ));
    }

    let right = parse_node_pattern_str(&right_str)?;

    Ok(Query::Create {
        pattern: CreatePattern {
            left,
            relationship: Some(CreateRelationship { edge, right }),
        },
    })
}

fn parse_delete_node(rest: &str) -> Result<Query, ParseError> {
    let id = rest.trim();
    if id.is_empty() {
        return Err(ParseError::Message("expected node identifier".into()));
    }
    Ok(Query::DeleteNode { id: id.to_string() })
}

fn parse_delete_edge(rest: &str) -> Result<Query, ParseError> {
    let id = rest.trim();
    if id.is_empty() {
        return Err(ParseError::Message("expected edge identifier".into()));
    }
    Ok(Query::DeleteEdge { id: id.to_string() })
}

fn parse_update_node(rest: &str) -> Result<Query, ParseError> {
    let (id, after_id) = split_token(rest)?;
    let after_set = expect_keyword(after_id, "SET")?;
    let assignments = parse_assignments_str(after_set)?;
    Ok(Query::UpdateNode {
        id: id.to_string(),
        assignments,
    })
}

fn parse_update_edge(rest: &str) -> Result<Query, ParseError> {
    let (id, after_id) = split_token(rest)?;
    let after_set = expect_keyword(after_id, "SET")?;
    let assignments = parse_assignments_str(after_set)?;
    Ok(Query::UpdateEdge {
        id: id.to_string(),
        assignments,
    })
}

fn parse_select(rest: &str) -> Result<Query, ParseError> {
    let after_match = expect_keyword(rest, "MATCH")?;
    let (pattern_str, after_pattern) = extract_group(after_match, '(', ')')?;
    let pattern = parse_node_pattern_str(&pattern_str)?;

    let mut rest = after_pattern.trim_start();
    let mut conditions = Vec::new();
    if let Ok(after_where) = expect_keyword(rest, "WHERE") {
        let (cond_text, after_cond) = split_until_keyword(after_where, &["RETURN"])?;
        conditions = parse_conditions(cond_text)?;
        rest = after_cond;
    }

    let after_return = expect_keyword(rest, "RETURN")?;
    let returns = parse_return_list(after_return)?;

    Ok(Query::Select {
        pattern,
        conditions,
        returns,
    })
}

fn parse_node_pattern_str(pattern: &str) -> Result<NodePattern, ParseError> {
    let mut remaining = pattern.trim();
    let properties = if let Some(start) = remaining.find('{') {
        let props_str = remaining[start..].trim();
        if !props_str.ends_with('}') {
            return Err(ParseError::Message("unterminated properties".into()));
        }
        remaining = remaining[..start].trim();
        Properties::new(parse_properties_str(&props_str[1..props_str.len() - 1])?)
    } else {
        Properties::new(HashMap::new())
    };

    let (alias, label) = parse_alias_label(remaining)?;

    Ok(NodePattern {
        alias,
        label,
        properties,
    })
}

fn parse_edge_detail_str(detail: &str) -> Result<EdgePattern, ParseError> {
    let mut remaining = detail.trim();
    let properties = if let Some(start) = remaining.find('{') {
        let props_str = remaining[start..].trim();
        if !props_str.ends_with('}') {
            return Err(ParseError::Message("unterminated properties".into()));
        }
        remaining = remaining[..start].trim();
        Properties::new(parse_properties_str(&props_str[1..props_str.len() - 1])?)
    } else {
        Properties::new(HashMap::new())
    };

    let (alias, label) = parse_alias_label(remaining)?;

    Ok(EdgePattern {
        alias,
        label,
        properties,
    })
}

fn parse_alias_label(segment: &str) -> Result<(Option<String>, Option<String>), ParseError> {
    let mut alias = None;
    let mut label = None;
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return Ok((alias, label));
    }

    let mut parts = trimmed.split(':').map(str::trim).filter(|s| !s.is_empty());
    match (parts.next(), parts.next()) {
        (Some(first), Some(second)) => {
            alias = Some(first.to_string());
            label = Some(second.to_string());
        }
        (Some(first), None) => {
            if segment.trim_start().starts_with(':') {
                label = Some(first.to_string());
            } else {
                alias = Some(first.to_string());
            }
        }
        _ => {}
    }

    Ok((alias, label))
}

fn parse_properties_str(input: &str) -> Result<HashMap<String, Value>, ParseError> {
    let mut map = HashMap::new();
    for item in split_top_level(input, ',') {
        if item.trim().is_empty() {
            continue;
        }
        let mut parts = item.splitn(2, ':');
        let key = parts
            .next()
            .ok_or_else(|| ParseError::Message("property key missing".into()))?
            .trim();
        let value_str = parts
            .next()
            .ok_or_else(|| ParseError::Message("property value missing".into()))?
            .trim();
        map.insert(key.to_string(), parse_value_str(value_str)?);
    }
    Ok(map)
}

fn parse_assignments_str(input: &str) -> Result<HashMap<String, Value>, ParseError> {
    let mut map = HashMap::new();
    for item in split_top_level(input, ',') {
        if item.trim().is_empty() {
            continue;
        }
        let mut parts = item.splitn(2, '=');
        let key = parts
            .next()
            .ok_or_else(|| ParseError::Message("assignment key missing".into()))?
            .trim();
        let value_str = parts
            .next()
            .ok_or_else(|| ParseError::Message("assignment value missing".into()))?
            .trim();
        map.insert(key.to_string(), parse_value_str(value_str)?);
    }
    Ok(map)
}

fn parse_conditions(input: &str) -> Result<Vec<Condition>, ParseError> {
    let mut conditions = Vec::new();
    for part in split_top_level(input, ',').into_iter() {
        for segment in part.split("AND") {
            let seg = segment.trim();
            if seg.is_empty() {
                continue;
            }
            let mut lhs_rhs = seg.splitn(2, '=');
            let lhs = lhs_rhs
                .next()
                .ok_or_else(|| ParseError::Message("missing left side of condition".into()))?
                .trim();
            let rhs = lhs_rhs
                .next()
                .ok_or_else(|| ParseError::Message("missing right side of condition".into()))?
                .trim();
            let mut alias_prop = lhs.splitn(2, '.');
            let alias = alias_prop
                .next()
                .ok_or_else(|| ParseError::Message("missing alias in condition".into()))?
                .trim();
            let property = alias_prop
                .next()
                .ok_or_else(|| ParseError::Message("missing property in condition".into()))?
                .trim();
            conditions.push(Condition {
                alias: alias.to_string(),
                property: property.to_string(),
                operator: ComparisonOperator::Equals,
                value: parse_value_str(rhs)?,
            });
        }
    }
    Ok(conditions)
}

fn parse_return_list(input: &str) -> Result<Vec<String>, ParseError> {
    let mut list = Vec::new();
    for item in input.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        list.push(trimmed.to_string());
    }
    if list.is_empty() {
        return Err(ParseError::Message(
            "RETURN clause requires at least one identifier".into(),
        ));
    }
    Ok(list)
}

fn parse_value_str(text: &str) -> Result<Value, ParseError> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(Value::Boolean(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(Value::Boolean(false));
    }
    if (trimmed.starts_with("\"") && trimmed.ends_with("\""))
        || (trimmed.starts_with("'") && trimmed.ends_with("'"))
    {
        return Ok(Value::String(unescape(trimmed)));
    }
    if let Ok(int) = trimmed.parse::<i64>() {
        return Ok(Value::Integer(int));
    }
    if let Ok(float) = trimmed.parse::<f64>() {
        return Ok(Value::Float(float));
    }
    Ok(Value::String(trimmed.to_string()))
}

fn unescape(raw: &str) -> String {
    let inner = &raw[1..raw.len() - 1];
    inner.replace("\\\"", "\"").replace("\\'", "'")
}

fn expect_keyword<'a>(input: &'a str, keyword: &str) -> Result<&'a str, ParseError> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return Err(ParseError::Message(format!("expected {keyword}")));
    }
    let (prefix, rest) = trimmed.split_at(keyword.len());
    if prefix.eq_ignore_ascii_case(keyword) {
        Ok(rest.trim_start())
    } else {
        Err(ParseError::Message(format!("expected {keyword}")))
    }
}

fn extract_group<'a>(
    input: &'a str,
    open: char,
    close: char,
) -> Result<(String, &'a str), ParseError> {
    let trimmed = input.trim_start();
    if !trimmed.starts_with(open) {
        return Err(ParseError::Message(format!("expected '{open}'")));
    }
    let mut depth = 0usize;
    for (idx, ch) in trimmed.char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                let inside = trimmed[1..idx].to_string();
                let rest = &trimmed[idx + 1..];
                return Ok((inside, rest));
            }
        }
    }
    Err(ParseError::Message("unterminated group".into()))
}

fn split_token<'a>(input: &'a str) -> Result<(&'a str, &'a str), ParseError> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(ParseError::Message("expected identifier".into()));
    }
    let mut chars = trimmed.char_indices();
    let mut end = trimmed.len();
    while let Some((idx, ch)) = chars.next() {
        if ch.is_whitespace() {
            end = idx;
            break;
        }
    }
    Ok((&trimmed[..end], &trimmed[end..]))
}

fn split_until_keyword<'a>(
    input: &'a str,
    keywords: &[&str],
) -> Result<(&'a str, &'a str), ParseError> {
    let mut idx = input.len();
    for &keyword in keywords {
        if let Some(pos) = input.to_uppercase().find(&keyword.to_uppercase()) {
            if pos < idx {
                idx = pos;
            }
        }
    }
    if idx == input.len() {
        Ok((input.trim(), ""))
    } else {
        Ok((input[..idx].trim(), &input[idx..]))
    }
}

fn split_top_level(input: &str, delimiter: char) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '\0';
    for ch in input.chars() {
        match ch {
            '"' | '\'' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current.push(ch);
            }
            ch if in_quotes && ch == quote_char => {
                in_quotes = false;
                current.push(ch);
            }
            c if c == delimiter && !in_quotes => {
                items.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn strip_prefix_ci<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < prefix.len() {
        return None;
    }
    let (head, tail) = trimmed.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail.trim_start())
    } else {
        None
    }
}
