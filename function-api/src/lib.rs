#![allow(improper_ctypes_definitions)]

use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, RwLock};

pub use common::value::{FieldValue, QueryPath};
pub use graphdb_core::query::Value;
use once_cell::sync::Lazy;

pub type FunctionResult = Result<FieldValue, FunctionError>;
pub type FunctionHandler = Arc<dyn Fn(&[FieldValue]) -> FunctionResult + Send + Sync + 'static>;
pub type ContextFunctionHandler =
    Arc<dyn Fn(&[FieldValue], &mut dyn FunctionContext) -> FunctionResult + Send + Sync + 'static>;

pub type PluginCallback = unsafe extern "C" fn(*const FieldValue, usize, *mut FieldValue) -> bool;
pub type PluginContextCallback =
    unsafe extern "C" fn(*const FieldValue, usize, *mut FieldValue, *mut HostContext) -> bool;

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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExpressionHandle(pub u32);

pub trait FunctionContext {
    fn query_timestamp(&self) -> i64;
    fn bind_alias(&mut self, alias: &str, value: FieldValue) -> Result<(), FunctionError>;
    fn unbind_alias(&mut self, alias: &str) -> Result<(), FunctionError>;
    fn evaluate_expression(
        &mut self,
        handle: ExpressionHandle,
    ) -> Result<FieldValue, FunctionError>;
}

#[derive(Clone)]
pub struct FunctionSpec {
    pub name: String,
    pub arity: FunctionArity,
    pub handler: FunctionKind,
}

#[derive(Clone)]
pub enum FunctionKind {
    Simple(FunctionHandler),
    Context(ContextFunctionHandler),
}

impl FunctionSpec {
    pub fn new(
        name: impl Into<String>,
        arity: FunctionArity,
        handler: impl Fn(&[FieldValue]) -> FunctionResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            arity,
            handler: FunctionKind::Simple(Arc::new(handler)),
        }
    }

    pub fn with_context(
        name: impl Into<String>,
        arity: FunctionArity,
        handler: impl Fn(&[FieldValue], &mut dyn FunctionContext) -> FunctionResult
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            arity,
            handler: FunctionKind::Context(Arc::new(handler)),
        }
    }
}

struct RegisteredFunction {
    arity: FunctionArity,
    handler: FunctionKind,
}

struct ContextBridge<'a> {
    ctx: &'a mut dyn FunctionContext,
}

unsafe fn bridge_from_ptr<'a>(data: *mut c_void) -> &'a mut ContextBridge<'a> {
    unsafe { &mut *(data as *mut ContextBridge) }
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

    pub fn call(&self, name: &str, args: &[FieldValue]) -> FunctionResult {
        self.call_with_context(name, args, None)
    }

    pub fn call_with_context(
        &self,
        name: &str,
        args: &[FieldValue],
        ctx: Option<&mut dyn FunctionContext>,
    ) -> FunctionResult {
        let guard = self.functions.read().expect("function registry poisoned");
        let key = name.to_ascii_lowercase();
        let entry = guard
            .get(&key)
            .ok_or_else(|| FunctionError::NotFound(name.to_string()))?;
        entry.arity.validate(args.len())?;
        match (&entry.handler, ctx) {
            (FunctionKind::Simple(handler), _) => handler(args),
            (FunctionKind::Context(handler), Some(context)) => handler(args, context),
            (FunctionKind::Context(_), None) => {
                Err(FunctionError::ContextRequired(name.to_string()))
            }
        }
    }
}

static FUNCTION_REGISTRY: Lazy<FunctionRegistry> = Lazy::new(FunctionRegistry::new);

pub fn registry() -> &'static FunctionRegistry {
    &FUNCTION_REGISTRY
}

static LAST_PLUGIN_ERROR: Lazy<RwLock<Option<String>>> = Lazy::new(|| RwLock::new(None));

pub fn set_last_error(message: impl Into<String>) {
    if let Ok(mut guard) = LAST_PLUGIN_ERROR.write() {
        *guard = Some(message.into());
    }
}

pub fn take_last_error() -> Option<String> {
    LAST_PLUGIN_ERROR
        .write()
        .ok()
        .and_then(|mut guard| guard.take())
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct PluginFunctionSpec {
    pub name: *const c_char,
    pub callback: PluginCallback,
    pub context_callback: Option<PluginContextCallback>,
    pub min_arity: usize,
    pub max_arity: isize,
    pub flags: u32,
}

#[repr(C)]
pub struct HostContext {
    pub data: *mut c_void,
    pub vtable: *const HostContextVTable,
}

#[repr(C)]
pub struct HostContextVTable {
    pub query_timestamp: unsafe extern "C" fn(*mut c_void) -> i64,
    pub bind_alias: unsafe extern "C" fn(*mut c_void, *const c_char, FieldValue) -> bool,
    pub unbind_alias: unsafe extern "C" fn(*mut c_void, *const c_char) -> bool,
    pub evaluate_expression:
        unsafe extern "C" fn(*mut c_void, ExpressionHandle, *mut FieldValue) -> bool,
}

pub const PLUGIN_FLAG_REQUIRES_CONTEXT: u32 = 0x1;

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
    if (spec.flags & PLUGIN_FLAG_REQUIRES_CONTEXT) != 0 {
        let callback = spec
            .context_callback
            .ok_or_else(|| FunctionError::ContextNotSupported(name.clone()))?;
        let function_name = name.clone();
        let handler = move |args: &[FieldValue], ctx: &mut dyn FunctionContext| -> FunctionResult {
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
            let mut output = FieldValue::Scalar(Value::Null);
            let mut bridge = ContextBridge { ctx };
            let mut host_ctx = HostContext {
                data: (&mut bridge as *mut ContextBridge<'_>) as *mut c_void,
                vtable: &HOST_CONTEXT_VTABLE,
            };
            let success = unsafe {
                callback(
                    args.as_ptr(),
                    args.len(),
                    &mut output as *mut FieldValue,
                    &mut host_ctx,
                )
            };
            if success {
                Ok(output)
            } else {
                Err(FunctionError::Execution(take_last_error().unwrap_or_else(
                    || format!("plugin function '{function_name}' failed"),
                )))
            }
        };

        return registry().register(FunctionSpec {
            name,
            arity: arity_for_registration,
            handler: FunctionKind::Context(Arc::new(handler)),
        });
    }

    let callback = spec.callback;
    let function_name = name.clone();
    let handler = move |args: &[FieldValue]| -> FunctionResult {
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
        let mut output = FieldValue::Scalar(Value::Null);
        let success =
            unsafe { callback(args.as_ptr(), args.len(), &mut output as *mut FieldValue) };
        if success {
            Ok(output)
        } else {
            Err(FunctionError::Execution(take_last_error().unwrap_or_else(
                || format!("plugin function '{function_name}' failed"),
            )))
        }
    };

    registry().register(FunctionSpec {
        name,
        arity: arity_for_registration,
        handler: FunctionKind::Simple(Arc::new(handler)),
    })
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
    ContextRequired(String),
    ContextNotSupported(String),
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
            FunctionError::ContextRequired(name) => {
                write!(f, "function '{name}' requires execution context")
            }
            FunctionError::ContextNotSupported(name) => {
                write!(
                    f,
                    "function '{name}' requires context, which is not supported"
                )
            }
        }
    }
}

static HOST_CONTEXT_VTABLE: HostContextVTable = HostContextVTable {
    query_timestamp: host_query_timestamp,
    bind_alias: host_bind_alias,
    unbind_alias: host_unbind_alias,
    evaluate_expression: host_evaluate_expression,
};

unsafe extern "C" fn host_query_timestamp(data: *mut c_void) -> i64 {
    let bridge = unsafe { bridge_from_ptr(data) };
    bridge.ctx.query_timestamp()
}

unsafe extern "C" fn host_bind_alias(
    data: *mut c_void,
    alias: *const c_char,
    value: FieldValue,
) -> bool {
    let bridge = unsafe { bridge_from_ptr(data) };
    match unsafe { CStr::from_ptr(alias) }.to_str() {
        Ok(name) => match bridge.ctx.bind_alias(name, value) {
            Ok(()) => true,
            Err(err) => {
                set_last_error(err.to_string());
                false
            }
        },
        Err(_) => {
            set_last_error("alias must be valid UTF-8");
            false
        }
    }
}

unsafe extern "C" fn host_unbind_alias(data: *mut c_void, alias: *const c_char) -> bool {
    let bridge = unsafe { bridge_from_ptr(data) };
    match unsafe { CStr::from_ptr(alias) }.to_str() {
        Ok(name) => match bridge.ctx.unbind_alias(name) {
            Ok(()) => true,
            Err(err) => {
                set_last_error(err.to_string());
                false
            }
        },
        Err(_) => {
            set_last_error("alias must be valid UTF-8");
            false
        }
    }
}

unsafe extern "C" fn host_evaluate_expression(
    data: *mut c_void,
    handle: ExpressionHandle,
    out: *mut FieldValue,
) -> bool {
    let bridge = unsafe { bridge_from_ptr(data) };
    match bridge.ctx.evaluate_expression(handle) {
        Ok(value) => {
            if let Some(slot) = unsafe { out.as_mut() } {
                *slot = value;
            }
            true
        }
        Err(err) => {
            set_last_error(err.to_string());
            false
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
                    [
                        FieldValue::Scalar(Value::Integer(a)),
                        FieldValue::Scalar(Value::Integer(b)),
                    ] => Ok(FieldValue::Scalar(Value::Integer(a + b))),
                    _ => Err(FunctionError::execution("expected two integers")),
                },
            ))
            .expect("register function");

        let result = registry
            .call(
                "add",
                &[
                    FieldValue::Scalar(Value::Integer(1)),
                    FieldValue::Scalar(Value::Integer(2)),
                ],
            )
            .expect("call add");
        match result {
            FieldValue::Scalar(Value::Integer(sum)) => assert_eq!(sum, 3),
            other => panic!("unexpected result: {:?}", other),
        }
    }
}
