use function_api::{PluginFunctionSpec, Value};
use uuid::Uuid;

use crate::cstr;
use crate::util::{
    args_slice, convert_to_boolean, convert_to_float, convert_to_integer, expect_arg_count,
    handle_result, plugin_spec, write_value,
};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 9] = [
    plugin_spec(cstr!("toBoolean"), to_boolean_callback, 1, 1),
    plugin_spec(cstr!("toBooleanOrNull"), to_boolean_or_null_callback, 1, 1),
    plugin_spec(cstr!("toFloat"), to_float_callback, 1, 1),
    plugin_spec(cstr!("toFloatOrNull"), to_float_or_null_callback, 1, 1),
    plugin_spec(cstr!("toInteger"), to_integer_callback, 1, 1),
    plugin_spec(cstr!("toIntegerOrNull"), to_integer_or_null_callback, 1, 1),
    plugin_spec(cstr!("range"), range_callback, 2, 3),
    plugin_spec(cstr!("randomUUID"), random_uuid_callback, 0, 0),
    plugin_spec(cstr!("timestamp"), timestamp_callback, 1, 1),
];

unsafe extern "C" fn to_boolean_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(
        to_boolean_impl(args_slice(args, len), false, "toBoolean"),
        out,
    )
}

unsafe extern "C" fn to_boolean_or_null_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(
        to_boolean_impl(args_slice(args, len), true, "toBooleanOrNull"),
        out,
    )
}

unsafe extern "C" fn to_float_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(to_float_impl(args_slice(args, len), false, "toFloat"), out)
}

unsafe extern "C" fn to_float_or_null_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(
        to_float_impl(args_slice(args, len), true, "toFloatOrNull"),
        out,
    )
}

unsafe extern "C" fn to_integer_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(
        to_integer_impl(args_slice(args, len), false, "toInteger"),
        out,
    )
}

unsafe extern "C" fn to_integer_or_null_callback(
    args: *const Value,
    len: usize,
    out: *mut Value,
) -> bool {
    handle_result(
        to_integer_impl(args_slice(args, len), true, "toIntegerOrNull"),
        out,
    )
}

unsafe extern "C" fn range_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(range_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn random_uuid_callback(
    _args: *const Value,
    _len: usize,
    out: *mut Value,
) -> bool {
    write_value(out, Value::String(Uuid::new_v4().to_string()));
    true
}

unsafe extern "C" fn timestamp_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(timestamp_impl(args_slice(args, len)), out)
}

fn to_boolean_impl(args: &[Value], null_on_unsupported: bool, name: &str) -> Result<Value, String> {
    expect_arg_count(args, 1, name)?;
    convert_to_boolean(&args[0], null_on_unsupported)
}

fn to_float_impl(args: &[Value], null_on_unsupported: bool, name: &str) -> Result<Value, String> {
    expect_arg_count(args, 1, name)?;
    convert_to_float(&args[0], null_on_unsupported)
}

fn to_integer_impl(args: &[Value], null_on_unsupported: bool, name: &str) -> Result<Value, String> {
    expect_arg_count(args, 1, name)?;
    convert_to_integer(&args[0], null_on_unsupported)
}

fn range_impl(args: &[Value]) -> Result<Value, String> {
    if args.len() < 2 || args.len() > 3 {
        return Err("range() expects 2 or 3 arguments".into());
    }

    let start = match coerce_range_bound(&args[0], "range start")? {
        Some(value) => value,
        None => return Ok(Value::Null),
    };
    let end = match coerce_range_bound(&args[1], "range end")? {
        Some(value) => value,
        None => return Ok(Value::Null),
    };
    let step = if args.len() == 3 {
        match coerce_range_bound(&args[2], "range step")? {
            Some(value) => value,
            None => return Ok(Value::Null),
        }
    } else {
        1
    };

    if step == 0 {
        return Err("range() step cannot be 0".into());
    }

    Ok(Value::List(build_range(start, end, step)))
}

fn coerce_range_bound(value: &Value, context: &str) -> Result<Option<i64>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Integer(i) => Ok(Some(*i)),
        Value::Float(f) => Ok(Some(*f as i64)),
        Value::Boolean(true) => Ok(Some(1)),
        Value::Boolean(false) => Ok(Some(0)),
        other => Err(format!(
            "{context} expects integer input, received {:?}",
            other
        )),
    }
}

fn build_range(start: i64, end: i64, step: i64) -> Vec<Value> {
    let ascending = start <= end;
    if (ascending && step < 0) || (!ascending && step > 0) {
        return Vec::new();
    }

    let mut values = Vec::new();
    let mut current = start;
    if step > 0 {
        while current <= end {
            values.push(Value::Integer(current));
            match current.checked_add(step) {
                Some(next) => current = next,
                None => break,
            }
        }
    } else {
        while current >= end {
            values.push(Value::Integer(current));
            match current.checked_add(step) {
                Some(next) => current = next,
                None => break,
            }
        }
    }
    values
}

fn timestamp_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "timestamp")?;
    match &args[0] {
        Value::Integer(ts) => Ok(Value::Integer(*ts)),
        other => Err(format!(
            "timestamp() expects INTEGER context argument, received {:?}",
            other
        )),
    }
}
