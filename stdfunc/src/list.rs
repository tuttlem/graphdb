use function_api::{FieldValue, HostContext, PluginFunctionSpec, Value};

use crate::cstr;
use crate::util::{
    args_slice, context_bind_alias, context_evaluate_expression, context_plugin_spec,
    convert_to_boolean, convert_to_float, convert_to_integer, convert_to_string, expect_arg_count,
    expect_scalar, expression_handle_from_field, field_value_to_scalar_owned, handle_result,
    normalize_list_items, plugin_spec, scalar_field, scalar_result, string_from_field,
};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 10] = [
    plugin_spec(cstr!("reverse"), reverse_callback, 1, 1),
    plugin_spec(cstr!("tail"), tail_callback, 1, 1),
    plugin_spec(cstr!("head"), head_callback, 1, 1),
    plugin_spec(cstr!("last"), last_callback, 1, 1),
    plugin_spec(cstr!("length"), length_callback, 1, 1),
    plugin_spec(cstr!("toBooleanList"), to_boolean_list_callback, 1, 1),
    plugin_spec(cstr!("toFloatList"), to_float_list_callback, 1, 1),
    plugin_spec(cstr!("toIntegerList"), to_integer_list_callback, 1, 1),
    plugin_spec(cstr!("toStringList"), to_string_list_callback, 1, 1),
    context_plugin_spec(cstr!("reduce"), reduce_callback, 5, 5),
];

unsafe extern "C" fn reverse_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(reverse_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn tail_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(tail_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn head_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(head_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn last_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(last_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn length_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(length_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_boolean_list_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(to_boolean_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_float_list_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(to_float_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_integer_list_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(to_integer_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn to_string_list_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(to_string_list_impl(args_slice(args, len)), out)
}

unsafe extern "C" fn reduce_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
    ctx: *mut HostContext,
) -> bool {
    handle_result(reduce_impl(args_slice(args, len), ctx), out)
}

fn reverse_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "reverse")?;
    match expect_scalar(&args[0], "reverse")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => {
            let mut reversed = values.clone();
            reversed.reverse();
            Ok(scalar_field(Value::List(reversed)))
        }
        other => Err(format!(
            "reverse() expects LIST input, received {:?}",
            other
        )),
    }
}

fn tail_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "tail")?;
    match expect_scalar(&args[0], "tail")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => {
            if values.is_empty() {
                Ok(scalar_field(Value::List(Vec::new())))
            } else {
                let tail = values.iter().skip(1).cloned().collect();
                Ok(scalar_field(Value::List(tail)))
            }
        }
        other => Err(format!("tail() expects LIST input, received {:?}", other)),
    }
}

fn head_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "head")?;
    match expect_scalar(&args[0], "head")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => Ok(scalar_field(values.first().cloned().unwrap_or(Value::Null))),
        _ => Err("head() expects a LIST value".into()),
    }
}

fn last_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "last")?;
    match expect_scalar(&args[0], "last")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => Ok(scalar_field(values.last().cloned().unwrap_or(Value::Null))),
        _ => Err("last() expects a LIST value".into()),
    }
}

fn length_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "length")?;
    match expect_scalar(&args[0], "length")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => Ok(scalar_field(Value::Integer(values.len() as i64))),
        _ => Err("length() expects a LIST value".into()),
    }
}

fn to_boolean_list_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "toBooleanList")?;
    scalar_result(list_conversion_impl(
        expect_scalar(&args[0], "toBooleanList")?,
        "function",
        |value| convert_to_boolean(value, true),
    ))
}

fn to_float_list_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "toFloatList")?;
    scalar_result(list_conversion_impl(
        expect_scalar(&args[0], "toFloatList")?,
        "function",
        |value| convert_to_float(value, true),
    ))
}

fn to_integer_list_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "toIntegerList")?;
    scalar_result(list_conversion_impl(
        expect_scalar(&args[0], "toIntegerList")?,
        "function",
        |value| convert_to_integer(value, true),
    ))
}

fn to_string_list_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "toStringList")?;
    scalar_result(list_conversion_impl(
        expect_scalar(&args[0], "toStringList")?,
        "function",
        |value| convert_to_string(value, true),
    ))
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

fn reduce_impl(args: &[FieldValue], ctx: *mut HostContext) -> Result<FieldValue, String> {
    let function = "reduce";
    expect_arg_count(args, 5, function)?;
    let accumulator_alias = string_from_field(&args[0], "reduce accumulator alias")?;
    let variable_alias = string_from_field(&args[1], "reduce variable alias")?;
    let initial_handle = expression_handle_from_field(&args[2], "reduce initial expression")?;
    let list_handle = expression_handle_from_field(&args[3], "reduce list expression")?;
    let expression_handle = expression_handle_from_field(&args[4], "reduce expression")?;

    let initial_value = context_evaluate_expression(ctx, initial_handle, function)?;
    let mut accumulator = field_value_to_scalar_owned(initial_value, "reduce initial value")?;
    let list_value = context_evaluate_expression(ctx, list_handle, function)?;
    let items = match normalize_list_items(list_value, function)? {
        Some(items) => items,
        None => return Ok(scalar_field(Value::Null)),
    };

    for item in items {
        context_bind_alias(ctx, &variable_alias, item, function)?;
        context_bind_alias(
            ctx,
            &accumulator_alias,
            FieldValue::Scalar(accumulator.clone()),
            function,
        )?;
        let next_value = context_evaluate_expression(ctx, expression_handle, function)?;
        accumulator = field_value_to_scalar_owned(next_value, "reduce expression result")?;
    }

    Ok(scalar_field(accumulator))
}
