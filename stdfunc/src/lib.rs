mod list;
mod math;
mod predicate;
mod scalar;
mod string;
mod util;

use function_api::{
    FieldValue, HostContext, PluginFunctionSpec, PluginProcedureSpec, PluginRegistration,
    ProcedureRowCallback, Value, set_last_error,
};
use once_cell::sync::Lazy;
use std::os::raw::{c_char, c_void};

macro_rules! cstr {
    ($lit:literal) => {
        concat!($lit, "\0").as_bytes()
    };
}
pub(crate) use cstr;

struct OutputArray<const N: usize>([*const c_char; N]);

unsafe impl<const N: usize> Sync for OutputArray<N> {}

static ALL_FUNCTIONS: Lazy<Vec<PluginFunctionSpec>> = Lazy::new(|| {
    let mut specs = Vec::new();
    specs.extend_from_slice(math::FUNCTIONS);
    specs.extend_from_slice(scalar::FUNCTIONS);
    specs.extend_from_slice(predicate::FUNCTIONS);
    specs.extend_from_slice(string::FUNCTIONS);
    specs.extend_from_slice(list::FUNCTIONS);
    specs
});

const LINES_NAME: &[u8] = cstr!("std.lines");
const WORD_COLUMN: &[u8] = cstr!("word");
static LINES_OUTPUTS: OutputArray<1> = OutputArray([WORD_COLUMN.as_ptr() as *const c_char]);
static PROCEDURES: [PluginProcedureSpec; 1] = [PluginProcedureSpec {
    name: LINES_NAME.as_ptr() as *const c_char,
    callback: lines_callback,
    outputs: LINES_OUTPUTS.0.as_ptr(),
    output_len: 1,
    min_arity: 1,
    max_arity: 1,
    flags: 0,
}];

#[unsafe(no_mangle)]
pub extern "C" fn graphdb_register_functions() -> PluginRegistration {
    let functions = &*ALL_FUNCTIONS;
    PluginRegistration::new(
        functions.as_ptr(),
        functions.len(),
        PROCEDURES.as_ptr(),
        PROCEDURES.len(),
    )
}

unsafe extern "C" fn lines_callback(
    args: *const FieldValue,
    len: usize,
    _ctx: *mut HostContext,
    writer: ProcedureRowCallback,
    writer_data: *mut c_void,
) -> bool {
    let slice = unsafe { std::slice::from_raw_parts(args, len) };
    if slice.len() != 1 {
        set_last_error("lines() expects exactly one argument");
        return false;
    }

    let text_value = match &slice[0] {
        FieldValue::Scalar(value) => match util::convert_to_string(value, false) {
            Ok(Value::String(s)) => s,
            Ok(Value::Null) => String::new(),
            Ok(_) => String::new(),
            Err(err) => {
                set_last_error(err);
                return false;
            }
        },
        other => {
            set_last_error(format!(
                "lines() expects scalar input, received {}",
                util::field_value_type(other)
            ));
            return false;
        }
    };

    for word in text_value.split_whitespace() {
        let value = FieldValue::Scalar(Value::String(word.to_string()));
        let row = [value];
        if unsafe { !writer(writer_data, row.as_ptr(), row.len()) } {
            return false;
        }
    }
    true
}
