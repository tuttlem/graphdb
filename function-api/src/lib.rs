use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt;
use std::os::raw::c_char;
use std::sync::{Arc, RwLock};

pub use graphdb_core::query::Value;
use once_cell::sync::Lazy;

pub type FunctionResult = Result<Value, FunctionError>;
pub type FunctionHandler = Arc<dyn Fn(&[Value]) -> FunctionResult + Send + Sync + 'static>;

#[allow(improper_ctypes_definitions)]
pub type PluginCallback = unsafe extern "C" fn(*const Value, usize) -> Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionArity {
    Exact(usize),
    Variadic { min: usize },
}

impl FunctionArity {
    fn validate(&self, len: usize) -> Result<(), FunctionError> {
        match self {
            FunctionArity::Exact(expected) => {
                if len == *expected {
                    Ok(())
                } else {
                    Err(FunctionError::InvalidArity {
                        expected: self.clone(),
                        received: len,
                    })
                }
            }
            FunctionArity::Variadic { min } => {
                if len >= *min {
                    Ok(())
                } else {
                    Err(FunctionError::InvalidArity {
                        expected: self.clone(),
                        received: len,
                    })
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct FunctionSpec {
    pub name: String,
    pub arity: FunctionArity,
    pub handler: FunctionHandler,
}

impl FunctionSpec {
    pub fn new(
        name: impl Into<String>,
        arity: FunctionArity,
        handler: impl Fn(&[Value]) -> FunctionResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            arity,
            handler: Arc::new(handler),
        }
    }
}

struct RegisteredFunction {
    arity: FunctionArity,
    handler: FunctionHandler,
}

#[derive(Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, RegisteredFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, spec: FunctionSpec) -> Result<(), FunctionError> {
        let mut guard = self.functions.write().expect("function registry poisoned");
        let key = spec.name.to_ascii_lowercase();
        if guard.contains_key(&key) {
            return Err(FunctionError::AlreadyRegistered(spec.name));
        }
        guard.insert(
            key,
            RegisteredFunction {
                arity: spec.arity,
                handler: spec.handler,
            },
        );
        Ok(())
    }

    pub fn call(&self, name: &str, args: &[Value]) -> FunctionResult {
        let guard = self.functions.read().expect("function registry poisoned");
        let key = name.to_ascii_lowercase();
        let entry = guard
            .get(&key)
            .ok_or_else(|| FunctionError::NotFound(name.to_string()))?;
        entry.arity.validate(args.len())?;
        (entry.handler)(args)
    }
}

static FUNCTION_REGISTRY: Lazy<FunctionRegistry> = Lazy::new(FunctionRegistry::new);

pub fn registry() -> &'static FunctionRegistry {
    &FUNCTION_REGISTRY
}

#[repr(C)]
pub struct PluginFunctionSpec {
    pub name: *const c_char,
    pub callback: PluginCallback,
    pub min_arity: usize,
    pub max_arity: isize,
}

#[repr(C)]
pub struct PluginRegistration {
    pub functions: *const PluginFunctionSpec,
    pub len: usize,
}

unsafe impl Send for PluginFunctionSpec {}
unsafe impl Sync for PluginFunctionSpec {}
unsafe impl Send for PluginRegistration {}
unsafe impl Sync for PluginRegistration {}

impl PluginRegistration {
    pub const fn new(functions: *const PluginFunctionSpec, len: usize) -> Self {
        Self { functions, len }
    }

    pub unsafe fn as_slice(&self) -> &[PluginFunctionSpec] {
        unsafe { std::slice::from_raw_parts(self.functions, self.len) }
    }
}

impl PluginFunctionSpec {
    pub unsafe fn name(&self) -> Result<&str, FunctionError> {
        if self.name.is_null() {
            return Err(FunctionError::InvalidName);
        }
        unsafe { CStr::from_ptr(self.name) }
            .to_str()
            .map_err(|_| FunctionError::InvalidName)
    }

    pub fn arity(&self) -> Result<FunctionArity, FunctionError> {
        if self.max_arity < 0 {
            Ok(FunctionArity::Variadic {
                min: self.min_arity,
            })
        } else {
            let max = self.max_arity as usize;
            if max == self.min_arity {
                Ok(FunctionArity::Exact(max))
            } else if max >= self.min_arity {
                // emulate ranged arity by treating as variadic with upper bound handled manually
                Ok(FunctionArity::Variadic {
                    min: self.min_arity,
                })
            } else {
                Err(FunctionError::InvalidArity {
                    expected: FunctionArity::Exact(self.min_arity),
                    received: max,
                })
            }
        }
    }
}

pub fn register_plugin_functions(registration: &PluginRegistration) -> Result<(), FunctionError> {
    unsafe {
        for spec in registration.as_slice() {
            register_plugin_function(spec)?;
        }
    }
    Ok(())
}

unsafe fn register_plugin_function(spec: &PluginFunctionSpec) -> Result<(), FunctionError> {
    let name = unsafe { spec.name()? }.to_owned();
    let arity = spec.arity()?;
    let arity_for_registration = arity.clone();
    let min_arity = spec.min_arity;
    let max_arity = spec.max_arity;
    let callback = spec.callback;
    let handler = move |args: &[Value]| -> FunctionResult {
        if args.len() < min_arity {
            return Err(FunctionError::InvalidArity {
                expected: arity.clone(),
                received: args.len(),
            });
        }
        if max_arity >= 0 {
            let max = max_arity as usize;
            if args.len() > max {
                return Err(FunctionError::InvalidArity {
                    expected: arity.clone(),
                    received: args.len(),
                });
            }
        }
        Ok(unsafe { callback(args.as_ptr(), args.len()) })
    };

    registry().register(FunctionSpec::new(name, arity_for_registration, handler))
}

#[derive(Debug, Clone)]
pub enum FunctionError {
    AlreadyRegistered(String),
    NotFound(String),
    InvalidArity {
        expected: FunctionArity,
        received: usize,
    },
    Execution(String),
    InvalidName,
}

impl FunctionError {
    pub fn execution(message: impl Into<String>) -> Self {
        FunctionError::Execution(message.into())
    }
}

impl fmt::Display for FunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionError::AlreadyRegistered(name) => {
                write!(f, "function '{name}' is already registered")
            }
            FunctionError::NotFound(name) => write!(f, "function '{name}' is not registered"),
            FunctionError::InvalidArity { expected, received } => {
                write!(
                    f,
                    "invalid argument count: expected {expected}, received {received}"
                )
            }
            FunctionError::Execution(message) => write!(f, "{message}"),
            FunctionError::InvalidName => write!(f, "function name must be valid UTF-8"),
        }
    }
}

impl fmt::Display for FunctionArity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArity::Exact(n) => write!(f, "{n}"),
            FunctionArity::Variadic { min } => write!(f, "{min}+"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_and_invokes_function() {
        let registry = FunctionRegistry::new();
        registry
            .register(FunctionSpec::new(
                "add",
                FunctionArity::Exact(2),
                |args| match args {
                    [Value::Integer(a), Value::Integer(b)] => Ok(Value::Integer(a + b)),
                    _ => Err(FunctionError::execution("expected two integers")),
                },
            ))
            .expect("register function");

        let result = registry
            .call("add", &[Value::Integer(1), Value::Integer(2)])
            .expect("call add");
        assert_eq!(result, Value::Integer(3));
    }
}
