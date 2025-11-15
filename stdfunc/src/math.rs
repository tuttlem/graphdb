use function_api::{FieldValue, PluginFunctionSpec, Value};
use rand::Rng;
use std::f64::consts::{E as E_CONST, PI as PI_CONST};

use crate::cstr;
use crate::util::{
    RoundMode, apply_scalar_fn, args_slice, expect_value_arg_count, handle_result,
    integer_value_from_scalar, numeric_value_from_scalar, parse_round_mode, plugin_spec,
    round_number, scalar_field,
};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 25] = [
    plugin_spec(cstr!("hello"), hello_callback, 0, 0),
    plugin_spec(cstr!("abs"), abs_callback, 1, 1),
    plugin_spec(cstr!("ceil"), ceil_callback, 1, 1),
    plugin_spec(cstr!("floor"), floor_callback, 1, 1),
    plugin_spec(cstr!("isnan"), isnan_callback, 1, 1),
    plugin_spec(cstr!("rand"), rand_callback, 0, 0),
    plugin_spec(cstr!("round"), round_callback, 1, -1),
    plugin_spec(cstr!("sign"), sign_callback, 1, 1),
    plugin_spec(cstr!("e"), e_callback, 0, 0),
    plugin_spec(cstr!("exp"), exp_callback, 1, 1),
    plugin_spec(cstr!("log"), log_callback, 1, 1),
    plugin_spec(cstr!("log10"), log10_callback, 1, 1),
    plugin_spec(cstr!("sqrt"), sqrt_callback, 1, 1),
    plugin_spec(cstr!("acos"), acos_callback, 1, 1),
    plugin_spec(cstr!("asin"), asin_callback, 1, 1),
    plugin_spec(cstr!("atan"), atan_callback, 1, 1),
    plugin_spec(cstr!("atan2"), atan2_callback, 2, 2),
    plugin_spec(cstr!("cos"), cos_callback, 1, 1),
    plugin_spec(cstr!("cot"), cot_callback, 1, 1),
    plugin_spec(cstr!("degrees"), degrees_callback, 1, 1),
    plugin_spec(cstr!("haversin"), haversin_callback, 1, 1),
    plugin_spec(cstr!("pi"), pi_callback, 0, 0),
    plugin_spec(cstr!("radians"), radians_callback, 1, 1),
    plugin_spec(cstr!("sin"), sin_callback, 1, 1),
    plugin_spec(cstr!("tan"), tan_callback, 1, 1),
];

unsafe extern "C" fn hello_callback(_: *const FieldValue, _: usize, out: *mut FieldValue) -> bool {
    handle_result(
        Ok(scalar_field(Value::String("hello from stdfunc".into()))),
        out,
    )
}

unsafe extern "C" fn abs_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "abs", abs_impl), out)
}

unsafe extern "C" fn ceil_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "ceil", ceil_impl),
        out,
    )
}

unsafe extern "C" fn floor_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "floor", floor_impl),
        out,
    )
}

unsafe extern "C" fn isnan_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "isNaN", is_nan_impl),
        out,
    )
}

unsafe extern "C" fn rand_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "rand", rand_impl),
        out,
    )
}

unsafe extern "C" fn round_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "round", round_impl),
        out,
    )
}

unsafe extern "C" fn sign_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "sign", sign_impl),
        out,
    )
}

unsafe extern "C" fn e_callback(_: *const FieldValue, _: usize, out: *mut FieldValue) -> bool {
    handle_result(Ok(scalar_field(Value::Float(E_CONST))), out)
}

unsafe extern "C" fn exp_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "exp", exp_impl), out)
}

unsafe extern "C" fn log_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "log", log_impl), out)
}

unsafe extern "C" fn log10_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "log10", log10_impl),
        out,
    )
}

unsafe extern "C" fn sqrt_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "sqrt", sqrt_impl),
        out,
    )
}

unsafe extern "C" fn acos_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "acos", acos_impl),
        out,
    )
}

unsafe extern "C" fn asin_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "asin", asin_impl),
        out,
    )
}

unsafe extern "C" fn atan_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "atan", atan_impl),
        out,
    )
}

unsafe extern "C" fn atan2_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "atan2", atan2_impl),
        out,
    )
}

unsafe extern "C" fn cos_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "cos", cos_impl), out)
}

unsafe extern "C" fn cot_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "cot", cot_impl), out)
}

unsafe extern "C" fn degrees_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "degrees", degrees_impl),
        out,
    )
}

unsafe extern "C" fn haversin_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "haversin", haversin_impl),
        out,
    )
}

unsafe extern "C" fn pi_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "pi", pi_impl), out)
}

unsafe extern "C" fn radians_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(
        apply_scalar_fn(args_slice(args, len), "radians", radians_impl),
        out,
    )
}

unsafe extern "C" fn sin_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "sin", sin_impl), out)
}

unsafe extern "C" fn tan_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(apply_scalar_fn(args_slice(args, len), "tan", tan_impl), out)
}

fn abs_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "abs")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(i) => match i.checked_abs() {
            Some(value) => Ok(Value::Integer(value)),
            None => Err("abs() cannot represent absolute value for minimum integer".into()),
        },
        Value::Float(f) => Ok(Value::Float(f.abs())),
        other => Err(format!("abs() expects numeric input, received {:?}", other)),
    }
}

fn ceil_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "ceil")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "ceil()")?.ceil(),
        )),
    }
}

fn floor_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "floor")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "floor()")?.floor(),
        )),
    }
}

fn is_nan_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "isNaN")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Float(f) => Ok(Value::Boolean(f.is_nan())),
        Value::Integer(_) => Ok(Value::Boolean(false)),
        other => Err(format!(
            "isNaN() expects numeric input, received {:?}",
            other
        )),
    }
}

fn rand_impl(args: &[Value]) -> Result<Value, String> {
    if !args.is_empty() {
        return Err("rand() does not accept arguments".into());
    }
    let mut rng = rand::thread_rng();
    Ok(Value::Float(rng.r#gen::<f64>()))
}

fn round_impl(args: &[Value]) -> Result<Value, String> {
    if args.is_empty() || args.len() > 3 {
        return Err("round() expects between 1 and 3 arguments".into());
    }
    let value = &args[0];
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let number = numeric_value_from_scalar(value, "round()")?;
    let precision = if args.len() >= 2 {
        let precision_arg = &args[1];
        if matches!(precision_arg, Value::Null) {
            return Ok(Value::Null);
        }
        integer_value_from_scalar(precision_arg, "round() precision")?
    } else {
        0
    };
    let mode = if args.len() == 3 {
        let mode_arg = &args[2];
        if matches!(mode_arg, Value::Null) {
            return Ok(Value::Null);
        }
        let mode_str = match mode_arg {
            Value::String(s) => s.clone(),
            _ => return Err("round() mode expects STRING input".into()),
        };
        parse_round_mode(&mode_str)?
    } else {
        RoundMode::Up
    };
    let rounded = round_number(number, precision, mode)?;
    Ok(Value::Float(rounded))
}

fn sign_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "sign")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(i) => Ok(Value::Integer(i.signum())),
        Value::Float(f) => {
            if f.is_nan() {
                Ok(Value::Null)
            } else if *f > 0.0 {
                Ok(Value::Integer(1))
            } else if *f < 0.0 {
                Ok(Value::Integer(-1))
            } else {
                Ok(Value::Integer(0))
            }
        }
        other => Err(format!(
            "sign() expects numeric input, received {:?}",
            other
        )),
    }
}

fn exp_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "exp")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "exp()")?.exp(),
        )),
    }
}

fn log_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "log")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "log()")?.ln(),
        )),
    }
}

fn log10_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "log10")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "log10()")?.log10(),
        )),
    }
}

fn sqrt_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "sqrt")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "sqrt()")?.sqrt(),
        )),
    }
}

fn acos_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "acos")?;
    let value = &args[0];
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let numeric = numeric_value_from_scalar(value, "acos()")?;
    Ok(Value::Float(numeric.acos()))
}

fn asin_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "asin")?;
    let value = &args[0];
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let numeric = numeric_value_from_scalar(value, "asin()")?;
    Ok(Value::Float(numeric.asin()))
}

fn atan_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "atan")?;
    let value = &args[0];
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let numeric = numeric_value_from_scalar(value, "atan()")?;
    Ok(Value::Float(numeric.atan()))
}

fn atan2_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 2, "atan2")?;
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    let y = numeric_value_from_scalar(&args[0], "atan2()")?;
    let x = numeric_value_from_scalar(&args[1], "atan2()")?;
    Ok(Value::Float(y.atan2(x)))
}

fn cos_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "cos")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "cos()")?.cos(),
        )),
    }
}

fn cot_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "cot")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            1.0 / numeric_value_from_scalar(other, "cot()")?.tan(),
        )),
    }
}

fn degrees_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "degrees")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "degrees()")?.to_degrees(),
        )),
    }
}

fn haversin_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "haversin")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => {
            let value = numeric_value_from_scalar(other, "haversin()")?;
            let half = value / 2.0;
            let s = half.sin();
            Ok(Value::Float(s * s))
        }
    }
}

fn pi_impl(args: &[Value]) -> Result<Value, String> {
    if !args.is_empty() {
        return Err("pi() does not accept arguments".into());
    }
    Ok(Value::Float(PI_CONST))
}

fn radians_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "radians")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "radians()")?.to_radians(),
        )),
    }
}

fn sin_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "sin")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "sin()")?.sin(),
        )),
    }
}

fn tan_impl(args: &[Value]) -> Result<Value, String> {
    expect_value_arg_count(args, 1, "tan")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::Float(
            numeric_value_from_scalar(other, "tan()")?.tan(),
        )),
    }
}
