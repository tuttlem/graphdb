use function_api::{PluginFunctionSpec, Value};

use crate::cstr;
use crate::util::{args_slice, expect_arg_count, handle_result, plugin_spec};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 9] = [
    plugin_spec(cstr!("reverse"), reverse_callback, 1, 1),
    plugin_spec(cstr!("tail"), tail_callback, 1, 1),
    plugin_spec(cstr!("head"), head_callback, 1, 1),
    plugin_spec(cstr!("last"), last_callback, 1, 1),
    plugin_spec(cstr!("length"), length_callback, 1, 1),
    plugin_spec(cstr!("toBooleanList"), to_boolean_list_callback, 1, 1),
    plugin_spec(cstr!("toFloatList"), to_float_list_callback, 1, 1),
    plugin_spec(cstr!("toIntegerList"), to_integer_list_callback, 1, 1),
    plugin_spec(cstr!("toStringList"), to_string_list_callback, 1, 1),
];

unsafe extern "C" fn reverse_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(reverse_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn tail_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(tail_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn head_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(head_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn last_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(last_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn length_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(length_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_boolean_list_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(to_boolean_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_float_list_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(to_float_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_integer_list_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(to_integer_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_string_list_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(to_string_list_impl(args_slice(args, len)), out)
}

fn reverse_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "reverse")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => {
            let mut reversed = values.clone();
            reversed.reverse();
            Ok(Value::List(reversed))
        }
        other => Err(format!(
            "reverse() expects LIST input, received {:?}",
            other
        )),
    }
}

fn tail_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "tail")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => {
            if values.is_empty() {
                Ok(Value::List(Vec::new()))
            } else {
                let tail = values.iter().skip(1).cloned().collect();
                Ok(Value::List(tail))
            }
        }
        other => Err(format!("tail() expects LIST input, received {:?}", other)),
    }
}

fn head_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "head")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => Ok(values.first().cloned().unwrap_or(Value::Null)),
        _ => Err("head() expects a LIST value".into()),
    }
}

fn last_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "last")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => Ok(values.last().cloned().unwrap_or(Value::Null)),
        _ => Err("last() expects a LIST value".into()),
    }
}

fn length_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "length")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => Ok(Value::Integer(values.len() as i64)),
        _ => Err("length() expects a LIST value".into()),
    }
}

fn to_boolean_list_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "toBooleanList")?;
    list_conversion_impl(&args[0], "function", |value| {
        convert_to_boolean(value, true)
    })
}

fn to_float_list_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "toFloatList")?;
    list_conversion_impl(&args[0], "function", |value| convert_to_float(value, true))
}

fn to_integer_list_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "toIntegerList")?;
    list_conversion_impl(&args[0], "function", |value| {
        convert_to_integer(value, true)
    })
}

fn to_string_list_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "toStringList")?;
    list_conversion_impl(&args[0], "function", |value| convert_to_string(value, true))
}

fn list_conversion_impl<F>(value: &Value, context: &str, mapper: F) -> Result<Value, String>
where
    F: Fn(&Value) -> Result<Value, String>,
{
    match value {
        Value::Null => Ok(Value::Null),
        Value::List(values) => {
            let mut mapped = Vec::with_capacity(values.len());
            for item in values {
                mapped.push(mapper(item)?);
            }
            Ok(Value::List(mapped))
        }
        other => Err(format!(
            "{} expects LIST input, received {:?}",
            context, other
        )),
    }
}

fn convert_to_boolean(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::Boolean(b) => Value::Boolean(*b),
        Value::Integer(i) => Value::Boolean(*i != 0),
        Value::Float(f) => Value::Boolean(*f != 0.0),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" => Value::Boolean(true),
            "false" => Value::Boolean(false),
            _ => Value::Null,
        },
        other => {
            if null_on_unsupported {
                Value::Null
            } else {
                return Err(format!("toBoolean() does not support value {:?}", other));
            }
        }
    })
}

fn convert_to_float(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::Float(f) => Value::Float(*f),
        Value::Integer(i) => Value::Float(*i as f64),
        Value::String(s) => match s.parse::<f64>() {
            Ok(parsed) => Value::Float(parsed),
            Err(_) => Value::Null,
        },
        other => {
            if null_on_unsupported {
                Value::Null
            } else {
                return Err(format!("toFloat() does not support value {:?}", other));
            }
        }
    })
}

fn convert_to_integer(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::Integer(i) => Value::Integer(*i),
        Value::Float(f) => Value::Integer(*f as i64),
        Value::Boolean(true) => Value::Integer(1),
        Value::Boolean(false) => Value::Integer(0),
        Value::String(s) => match s.parse::<i64>() {
            Ok(parsed) => Value::Integer(parsed),
            Err(_) => Value::Null,
        },
        other => {
            if null_on_unsupported {
                Value::Null
            } else {
                return Err(format!("toInteger() does not support value {:?}", other));
            }
        }
    })
}

fn convert_to_string(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::String(s) => Value::String(s.clone()),
        Value::Integer(i) => Value::String(i.to_string()),
        Value::Float(f) => Value::String(f.to_string()),
        Value::Boolean(b) => Value::String(b.to_string()),
        other => {
            if null_on_unsupported {
                Value::Null
            } else {
                return Err(format!("toStringList() does not support value {:?}", other));
            }
        }
    })
}
