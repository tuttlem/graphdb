use function_api::{PluginFunctionSpec, PluginRegistration, Value};
use std::os::raw::c_char;

const HELLO_NAME: &[u8] = b"hello\0";

#[allow(improper_ctypes_definitions)]
unsafe extern "C" fn hello_callback(_: *const Value, _: usize) -> Value {
    Value::String("hello from stdfunc".into())
}

static FUNCTIONS: [PluginFunctionSpec; 1] = [PluginFunctionSpec {
    name: HELLO_NAME.as_ptr() as *const c_char,
    callback: hello_callback,
    min_arity: 0,
    max_arity: 0,
}];

#[unsafe(no_mangle)]
pub extern "C" fn graphdb_register_functions() -> PluginRegistration {
    PluginRegistration::new(FUNCTIONS.as_ptr(), FUNCTIONS.len())
}
