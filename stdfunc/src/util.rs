use function_api::{
    ExpressionHandle, FieldValue, HostContext, HostContextVTable, PLUGIN_FLAG_REQUIRES_CONTEXT,
    PluginContextCallback, PluginFunctionSpec, Value, set_last_error, take_last_error,
};
use std::convert::TryFrom;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::slice;

pub(crate) const ROUND_EPSILON: f64 = 1e-12;

pub(crate) fn args_slice<'a>(args: *const FieldValue, len: usize) -> &'a [FieldValue] {
    unsafe { slice::from_raw_parts(args, len) }
}

pub(crate) fn write_value(out: *mut FieldValue, value: FieldValue) {
    unsafe {
        if let Some(slot) = out.as_mut() {
            *slot = value;
        }
    }
}

pub(crate) fn handle_result(result: Result<FieldValue, String>, out: *mut FieldValue) -> bool {
    match result {
        Ok(value) => {
            write_value(out, value);
            true
        }
        Err(message) => {
            set_last_error(message);
            false
        }
    }
}

pub(crate) fn expect_arg_count(
    args: &[FieldValue],
    expected: usize,
    name: &str,
) -> Result<(), String> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(format!("{}() expects {} argument(s)", name, expected))
    }
}

pub(crate) fn expect_value_arg_count(
    args: &[Value],
    expected: usize,
    name: &str,
) -> Result<(), String> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(format!("{}() expects {} argument(s)", name, expected))
    }
}

pub(crate) const fn plugin_spec(
    name: &'static [u8],
    callback: function_api::PluginCallback,
    min: usize,
    max: isize,
) -> PluginFunctionSpec {
    PluginFunctionSpec {
        name: name.as_ptr() as *const c_char,
        callback,
        context_callback: None,
        min_arity: min,
        max_arity: max,
        flags: 0,
    }
}

unsafe extern "C" fn unused_plugin_callback(
    _args: *const FieldValue,
    _len: usize,
    _out: *mut FieldValue,
) -> bool {
    set_last_error("function requires execution context");
    false
}

pub(crate) const fn context_plugin_spec(
    name: &'static [u8],
    callback: PluginContextCallback,
    min: usize,
    max: isize,
) -> PluginFunctionSpec {
    PluginFunctionSpec {
        name: name.as_ptr() as *const c_char,
        callback: unused_plugin_callback,
        context_callback: Some(callback),
        min_arity: min,
        max_arity: max,
        flags: PLUGIN_FLAG_REQUIRES_CONTEXT,
    }
}

pub(crate) fn scalar_field(value: Value) -> FieldValue {
    FieldValue::Scalar(value)
}

pub(crate) fn scalar_result(result: Result<Value, String>) -> Result<FieldValue, String> {
    result.map(FieldValue::Scalar)
}

pub(crate) fn expect_scalar<'a>(field: &'a FieldValue, name: &str) -> Result<&'a Value, String> {
    match field {
        FieldValue::Scalar(value) => Ok(value),
        _ => Err(format!("{}() expects scalar input", name)),
    }
}

pub(crate) fn field_value_type(value: &FieldValue) -> &'static str {
    match value {
        FieldValue::Scalar(_) => "scalar",
        FieldValue::Node(_) => "node",
        FieldValue::Relationship(_) => "relationship",
        FieldValue::Path(_) => "path",
        FieldValue::List(_) => "list",
    }
}

pub(crate) fn field_value_to_scalar_owned(value: FieldValue, name: &str) -> Result<Value, String> {
    match value {
        FieldValue::Scalar(inner) => Ok(inner),
        other => Err(format!(
            "{} expects scalar input, received {}",
            name,
            field_value_type(&other)
        )),
    }
}

pub(crate) fn string_from_field(field: &FieldValue, name: &str) -> Result<String, String> {
    match expect_scalar(field, name)? {
        Value::String(value) => Ok(value.clone()),
        other => Err(format!(
            "{} expects STRING input, received {:?}",
            name, other
        )),
    }
}

pub(crate) fn expression_handle_from_field(
    field: &FieldValue,
    name: &str,
) -> Result<ExpressionHandle, String> {
    let value = expect_scalar(field, name)?;
    match value {
        Value::Integer(raw) => {
            if *raw < 0 {
                Err(format!("{} expects non-negative INTEGER handle", name))
            } else {
                u32::try_from(*raw)
                    .map(ExpressionHandle)
                    .map_err(|_| format!("{} handle value is too large", name))
            }
        }
        other => Err(format!(
            "{} expects INTEGER handle, received {:?}",
            name, other
        )),
    }
}

pub(crate) fn normalize_list_items(
    value: FieldValue,
    function: &str,
) -> Result<Option<Vec<FieldValue>>, String> {
    match value {
        FieldValue::Scalar(Value::Null) => Ok(None),
        FieldValue::Scalar(Value::List(values)) => {
            Ok(Some(values.into_iter().map(FieldValue::Scalar).collect()))
        }
        FieldValue::List(items) => Ok(Some(items)),
        other => Err(format!(
            "{}() expects LIST input, received {}",
            function,
            field_value_type(&other)
        )),
    }
}

pub(crate) fn apply_scalar_fn<F>(
    args: &[FieldValue],
    name: &str,
    func: F,
) -> Result<FieldValue, String>
where
    F: Fn(&[Value]) -> Result<Value, String>,
{
    let mut values = Vec::with_capacity(args.len());
    for field in args {
        match field {
            FieldValue::Scalar(value) => values.push(value.clone()),
            _ => return Err(format!("{}() expects scalar input", name)),
        }
    }
    func(&values).map(FieldValue::Scalar)
}

pub(crate) fn numeric_value_from_scalar(value: &Value, context: &str) -> Result<f64, String> {
    match value {
        Value::Float(f) => Ok(*f),
        Value::Integer(i) => Ok(*i as f64),
        other => Err(format!(
            "{} expects numeric input, received {:?}",
            context, other
        )),
    }
}

pub(crate) fn integer_value_from_scalar(value: &Value, context: &str) -> Result<i64, String> {
    match value {
        Value::Integer(i) => Ok(*i),
        Value::Float(f) if f.is_finite() && (f.fract().abs() <= ROUND_EPSILON) => {
            if *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(f.trunc() as i64)
            } else {
                Err(format!("{} is outside supported integer range", context))
            }
        }
        other => Err(format!(
            "{} expects INTEGER input, received {:?}",
            context, other
        )),
    }
}

pub(crate) fn parse_round_mode(mode: &str) -> Result<RoundMode, String> {
    match mode.trim().to_ascii_uppercase().as_str() {
        "UP" => Ok(RoundMode::Up),
        "DOWN" => Ok(RoundMode::Down),
        "CEILING" => Ok(RoundMode::Ceiling),
        "FLOOR" => Ok(RoundMode::Floor),
        "HALF_UP" => Ok(RoundMode::HalfUp),
        "HALF_DOWN" => Ok(RoundMode::HalfDown),
        "HALF_EVEN" => Ok(RoundMode::HalfEven),
        other => Err(format!("round() mode '{other}' is not supported")),
    }
}

pub(crate) fn round_number(value: f64, precision: i64, mode: RoundMode) -> Result<f64, String> {
    if !value.is_finite() {
        return Ok(value);
    }

    let precision_i32 =
        i32::try_from(precision).map_err(|_| "round() precision out of range".to_string())?;
    if precision_i32 == i32::MIN {
        return Err("round() precision out of range".into());
    }

    if precision_i32 >= 0 {
        let factor = 10f64.powi(precision_i32);
        let scaled = value * factor;
        Ok(apply_round_mode(scaled, mode) / factor)
    } else {
        let power = precision_i32.abs();
        let factor = 10f64.powi(power);
        let scaled = value / factor;
        Ok(apply_round_mode(scaled, mode) * factor)
    }
}

fn apply_round_mode(value: f64, mode: RoundMode) -> f64 {
    if !value.is_finite() {
        return value;
    }

    match mode {
        RoundMode::Up => {
            if has_fractional_part(value) {
                if value.is_sign_negative() {
                    value.floor()
                } else {
                    value.ceil()
                }
            } else {
                value
            }
        }
        RoundMode::Down => value.trunc(),
        RoundMode::Ceiling => value.ceil(),
        RoundMode::Floor => value.floor(),
        RoundMode::HalfUp => round_half(value, HalfTieStrategy::AwayFromZero),
        RoundMode::HalfDown => round_half(value, HalfTieStrategy::TowardZero),
        RoundMode::HalfEven => round_half(value, HalfTieStrategy::ToEven),
    }
}

fn round_half(value: f64, strategy: HalfTieStrategy) -> f64 {
    if !value.is_finite() {
        return value;
    }

    let truncated = value.trunc();
    let fraction = value - truncated;
    let abs_fraction = fraction.abs();

    if abs_fraction < 0.5 - ROUND_EPSILON {
        return truncated;
    }
    if abs_fraction > 0.5 + ROUND_EPSILON {
        return truncated + fraction.signum();
    }

    match strategy {
        HalfTieStrategy::AwayFromZero => truncated + fraction.signum(),
        HalfTieStrategy::TowardZero => truncated,
        HalfTieStrategy::ToEven => {
            let option_one = truncated;
            let option_two = truncated + fraction.signum();
            if is_even_integer(option_one) {
                option_one
            } else if is_even_integer(option_two) {
                option_two
            } else {
                option_one
            }
        }
    }
}

fn has_fractional_part(value: f64) -> bool {
    value.fract().abs() > ROUND_EPSILON
}

fn is_even_integer(value: f64) -> bool {
    if !value.is_finite() {
        return false;
    }
    let truncated = value.trunc();
    (value - truncated).abs() <= ROUND_EPSILON && ((truncated / 2.0).fract()).abs() <= ROUND_EPSILON
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RoundMode {
    Up,
    Down,
    Ceiling,
    Floor,
    HalfUp,
    HalfDown,
    HalfEven,
}

pub(crate) fn convert_to_boolean(
    value: &Value,
    null_on_unsupported: bool,
) -> Result<Value, String> {
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

pub(crate) fn convert_to_float(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
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

pub(crate) fn convert_to_integer(
    value: &Value,
    null_on_unsupported: bool,
) -> Result<Value, String> {
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

pub(crate) fn convert_to_string(value: &Value, null_on_unsupported: bool) -> Result<Value, String> {
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

fn context_parts<'a>(
    ctx: *mut HostContext,
    function: &str,
) -> Result<(*mut c_void, &'a HostContextVTable), String> {
    let host = unsafe { ctx.as_mut() }
        .ok_or_else(|| format!("{}() requires execution context", function))?;
    let vtable = unsafe { host.vtable.as_ref() }
        .ok_or_else(|| format!("{}() is missing execution context support", function))?;
    Ok((host.data, vtable))
}

pub(crate) fn context_bind_alias(
    ctx: *mut HostContext,
    alias: &str,
    value: FieldValue,
    function: &str,
) -> Result<(), String> {
    let (data, vtable) = context_parts(ctx, function)?;
    let cname = CString::new(alias)
        .map_err(|_| format!("{} alias must not contain null bytes", function))?;
    let success = unsafe { (vtable.bind_alias)(data, cname.as_ptr(), value) };
    if success {
        Ok(())
    } else {
        Err(take_last_error().unwrap_or_else(|| format!("{}() failed to bind alias", function)))
    }
}

pub(crate) fn context_evaluate_expression(
    ctx: *mut HostContext,
    handle: ExpressionHandle,
    function: &str,
) -> Result<FieldValue, String> {
    let (data, vtable) = context_parts(ctx, function)?;
    let mut output = FieldValue::Scalar(Value::Null);
    let success =
        unsafe { (vtable.evaluate_expression)(data, handle, &mut output as *mut FieldValue) };
    if success {
        Ok(output)
    } else {
        Err(take_last_error()
            .unwrap_or_else(|| format!("{}() failed to evaluate expression", function)))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HalfTieStrategy {
    AwayFromZero,
    TowardZero,
    ToEven,
}
