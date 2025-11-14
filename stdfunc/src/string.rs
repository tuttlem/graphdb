use function_api::{PluginFunctionSpec, Value};

use crate::cstr;
use crate::util::{args_slice, expect_arg_count, handle_result, plugin_spec};

pub(crate) const FUNCTIONS: &[PluginFunctionSpec] = &FUNCTION_TABLE;

const FUNCTION_TABLE: [PluginFunctionSpec; 1] = [plugin_spec(cstr!("size"), size_callback, 1, 1)];

unsafe extern "C" fn size_callback(args: *const Value, len: usize, out: *mut Value) -> bool {
    handle_result(size_impl(args_slice(args, len)), out)
}

fn size_impl(args: &[Value]) -> Result<Value, String> {
    expect_arg_count(args, 1, "size")?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(values) => Ok(Value::Integer(values.len() as i64)),
        Value::String(s) => Ok(Value::Integer(s.chars().count() as i64)),
        _ => Err("size() expects a LIST or STRING".into()),
    }
}
