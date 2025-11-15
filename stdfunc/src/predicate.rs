use function_api::{FieldValue, PluginFunctionSpec, Value};
use std::cmp::Ordering;

use crate::cstr;
use crate::util::{
    apply_scalar_fn, args_slice, expect_value_arg_count, handle_result, plugin_spec,
};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 4] = [
    plugin_spec(cstr!("all"), all_callback, 2, 2),
    plugin_spec(cstr!("any"), any_callback, 2, 2),
    plugin_spec(cstr!("none"), none_callback, 2, 2),
    plugin_spec(cstr!("single"), single_callback, 2, 2),
];

unsafe extern "C" fn all_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "all", |values| {
            list_predicate_impl(ListPredicateKind::All, values)
        }),
        out,
    )
}

unsafe extern "C" fn any_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "any", |values| {
            list_predicate_impl(ListPredicateKind::Any, values)
        }),
        out,
    )
}

unsafe extern "C" fn none_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "none", |values| {
            list_predicate_impl(ListPredicateKind::None, values)
        }),
        out,
    )
}

unsafe extern "C" fn single_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "single", |values| {
            list_predicate_impl(ListPredicateKind::Single, values)
        }),
        out,
    )
}

fn list_predicate_impl(kind: ListPredicateKind, args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 2, kind.name())?;
    let list = &args[0];
    let spec = parse_predicate_spec(&args[1])?;

    match list {
        Value::Null => Ok(Value::Null),
        Value::List(items) => evaluate_predicate(kind, items, &spec),
        other => Err(format!(
            "{} expects LIST input, received {:?}",
            kind.name(),
            other
        )),
    }
}

fn evaluate_predicate(
    kind: ListPredicateKind,
    items: &[Value],
    spec: &ListPredicateSpec,
) -> Result<Value, String> {
    match kind {
        ListPredicateKind::All => {
            let mut saw_null = false;
            for item in items {
                match evaluate_item(item, spec)? {
                    Some(true) => {}
                    Some(false) => return Ok(Value::Boolean(false)),
                    None => saw_null = true,
                }
            }
            if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(true))
            }
        }
        ListPredicateKind::Any => {
            let mut saw_null = false;
            for item in items {
                match evaluate_item(item, spec)? {
                    Some(true) => return Ok(Value::Boolean(true)),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }
            if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(false))
            }
        }
        ListPredicateKind::None => {
            let mut saw_null = false;
            for item in items {
                match evaluate_item(item, spec)? {
                    Some(true) => return Ok(Value::Boolean(false)),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }
            if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(true))
            }
        }
        ListPredicateKind::Single => {
            let mut true_count = 0;
            let mut saw_null_without_true = false;
            for item in items {
                match evaluate_item(item, spec)? {
                    Some(true) => {
                        true_count += 1;
                        if true_count > 1 {
                            return Ok(Value::Boolean(false));
                        }
                    }
                    Some(false) => {}
                    None => {
                        if true_count == 0 {
                            saw_null_without_true = true;
                        }
                    }
                }
            }
            if true_count == 1 {
                Ok(Value::Boolean(true))
            } else if true_count == 0 {
                if saw_null_without_true {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Boolean(false))
                }
            } else {
                Ok(Value::Boolean(false))
            }
        }
    }
}

fn evaluate_item(item: &Value, spec: &ListPredicateSpec) -> Result<Option<bool>, String> {
    match spec {
        ListPredicateSpec::Comparison { operator, value } => match operator {
            ComparisonOp::Equals => {
                if matches!(item, Value::Null) || matches!(value, Value::Null) {
                    Ok(None)
                } else {
                    Ok(Some(values_equal(item, value)))
                }
            }
            ComparisonOp::NotEquals => {
                if matches!(item, Value::Null) || matches!(value, Value::Null) {
                    Ok(None)
                } else {
                    Ok(Some(!values_equal(item, value)))
                }
            }
            ComparisonOp::GreaterThan
            | ComparisonOp::GreaterThanOrEqual
            | ComparisonOp::LessThan
            | ComparisonOp::LessThanOrEqual => {
                if let Some(ordering) = compare_query_values(item, value) {
                    let result = match operator {
                        ComparisonOp::GreaterThan => ordering == Ordering::Greater,
                        ComparisonOp::GreaterThanOrEqual => {
                            ordering == Ordering::Greater || ordering == Ordering::Equal
                        }
                        ComparisonOp::LessThan => ordering == Ordering::Less,
                        ComparisonOp::LessThanOrEqual => {
                            ordering == Ordering::Less || ordering == Ordering::Equal
                        }
                        _ => false,
                    };
                    Ok(Some(result))
                } else {
                    Ok(None)
                }
            }
        },
        ListPredicateSpec::IsNull { negated } => {
            let is_null = matches!(item, Value::Null);
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
    }
}

fn parse_predicate_spec(value: &Value) -> Result<ListPredicateSpec, String> {
    let map = match value {
        Value::Map(entries) => entries,
        other => {
            return Err(format!(
                "list predicate spec must be MAP, received {:?}",
                other
            ));
        }
    };

    let predicate_type = map
        .get("type")
        .and_then(|v| v.as_string())
        .ok_or_else(|| "list predicate spec missing type".to_string())?;

    match predicate_type.as_str() {
        "comparison" => {
            let operator = map
                .get("operator")
                .and_then(|v| v.as_string())
                .ok_or_else(|| "comparison predicate missing operator".to_string())?;
            let comparison = ComparisonOp::try_from(operator.as_str())?;
            let value = map
                .get("value")
                .cloned()
                .ok_or_else(|| "comparison predicate missing value".to_string())?;
            Ok(ListPredicateSpec::Comparison {
                operator: comparison,
                value,
            })
        }
        "isNull" => {
            let negated = map
                .get("negated")
                .and_then(|v| v.as_boolean())
                .ok_or_else(|| "isNull predicate missing negated flag".to_string())?;
            Ok(ListPredicateSpec::IsNull { negated })
        }
        other => Err(format!("unknown predicate type '{other}'")),
    }
}

#[derive(Clone, Copy)]
enum ListPredicateKind {
    All,
    Any,
    None,
    Single,
}

impl ListPredicateKind {
    fn name(&self) -> &'static str {
        match self {
            ListPredicateKind::All => "all",
            ListPredicateKind::Any => "any",
            ListPredicateKind::None => "none",
            ListPredicateKind::Single => "single",
        }
    }
}

#[derive(Clone)]
enum ListPredicateSpec {
    Comparison {
        operator: ComparisonOp,
        value: Value,
    },
    IsNull {
        negated: bool,
    },
}

#[derive(Clone, Copy)]
enum ComparisonOp {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl ComparisonOp {
    fn try_from(token: &str) -> Result<Self, String> {
        match token {
            "=" => Ok(ComparisonOp::Equals),
            "<>" => Ok(ComparisonOp::NotEquals),
            ">" => Ok(ComparisonOp::GreaterThan),
            ">=" => Ok(ComparisonOp::GreaterThanOrEqual),
            "<" => Ok(ComparisonOp::LessThan),
            "<=" => Ok(ComparisonOp::LessThanOrEqual),
            other => Err(format!("unsupported comparison operator '{other}'")),
        }
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => (*x - *y).abs() < f64::EPSILON,
        (Value::Integer(x), Value::Float(y)) | (Value::Float(y), Value::Integer(x)) => {
            ((*x as f64) - *y).abs() < f64::EPSILON
        }
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Null, Value::Null) => true,
        (Value::List(xs), Value::List(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(lx, ly)| values_equal(lx, ly))
        }
        (Value::Map(xs), Value::Map(ys)) => {
            xs.len() == ys.len()
                && xs.iter().all(|(key, value)| {
                    ys.get(key)
                        .map(|other| values_equal(value, other))
                        .unwrap_or(false)
                })
        }
        (Value::String(x), Value::String(y)) => x == y,
        _ => false,
    }
}

fn compare_query_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Boolean(x), Value::Boolean(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

trait ValueExt {
    fn as_string(&self) -> Option<String>;
    fn as_boolean(&self) -> Option<bool>;
}

impl ValueExt for Value {
    fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}
