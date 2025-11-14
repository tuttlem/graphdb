mod list;
mod math;
mod string;
mod util;

use function_api::{PluginFunctionSpec, PluginRegistration};
use once_cell::sync::Lazy;

macro_rules! cstr {
    ($lit:literal) => {
        concat!($lit, "\0").as_bytes()
    };
}
pub(crate) use cstr;

static ALL_FUNCTIONS: Lazy<Vec<PluginFunctionSpec>> = Lazy::new(|| {
    let mut specs = Vec::new();
    specs.extend_from_slice(math::FUNCTIONS);
    specs.extend_from_slice(string::FUNCTIONS);
    specs.extend_from_slice(list::FUNCTIONS);
    specs
});

#[unsafe(no_mangle)]
pub extern "C" fn graphdb_register_functions() -> PluginRegistration {
    let functions = &*ALL_FUNCTIONS;
    PluginRegistration::new(functions.as_ptr(), functions.len())
}
