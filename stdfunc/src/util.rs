use function_api::{PluginFunctionSpec, Value, set_last_error};
use std::convert::TryFrom;
use std::os::raw::c_char;
use std::slice;

pub(crate) const ROUND_EPSILON: f64 = 1e-12;

pub(crate) fn args_slice<'a>(args: *const Value, len: usize) -> &'a [Value] {
    unsafe { slice::from_raw_parts(args, len) }
}

pub(crate) fn write_value(out: *mut Value, value: Value) {
    unsafe {
        if let Some(slot) = out.as_mut() {
            *slot = value;
        }
    }
}

pub(crate) fn handle_result(result: Result<Value, String>, out: *mut Value) -> bool {
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

pub(crate) fn expect_arg_count(args: &[Value], expected: usize, name: &str) -> Result<(), String> {
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
        min_arity: min,
        max_arity: max,
    }
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum HalfTieStrategy {
    AwayFromZero,
    TowardZero,
    ToEven,
}
