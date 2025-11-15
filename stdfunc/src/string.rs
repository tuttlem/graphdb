use function_api::{FieldValue, PluginFunctionSpec, Value};

use crate::cstr;
use crate::util::{
    args_slice, expect_arg_count, expect_scalar, handle_result, plugin_spec, scalar_field,
};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 1] = [plugin_spec(cstr!("size"), size_callback, 1, 1)];

unsafe extern "C" fn size_callback(
    args: *const FieldValue,
    len: usize,
    out: *mut FieldValue,
) -> bool {
    handle_result(size_impl(args_slice(args, len)), out)
}

fn size_impl(args: &[FieldValue]) -> Result<FieldValue, String> {
    expect_arg_count(args, 1, "size")?;
    match expect_scalar(&args[0], "size")? {
        Value::Null => Ok(scalar_field(Value::Null)),
        Value::List(values) => Ok(scalar_field(Value::Integer(values.len() as i64))),
        Value::String(s) => Ok(scalar_field(Value::Integer(s.chars().count() as i64))),
        _ => Err("size() expects a LIST or STRING".into()),
    }
}
