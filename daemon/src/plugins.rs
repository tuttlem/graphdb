use std::fs;
use std::path::Path;

use libloading::Library;
use tracing::info;

use crate::error::DaemonError;

use function_api::register_plugin_functions;

const REGISTER_SYMBOL_NAME: &str = "graphdb_register_functions";
const REGISTER_SYMBOL: &[u8] = REGISTER_SYMBOL_NAME.as_bytes();

pub struct LoadedPlugins {
    libraries: Vec<Library>,
}

impl LoadedPlugins {
    pub fn new(libraries: Vec<Library>) -> Self {
        Self { libraries }
    }

    pub fn count(&self) -> usize {
        self.libraries.len()
    }
}

impl Default for LoadedPlugins {
    fn default() -> Self {
        Self {
            libraries: Vec::new(),
        }
    }
}

pub fn load_plugins(plugin_dir: Option<&Path>) -> Result<LoadedPlugins, DaemonError> {
    let Some(dir) = plugin_dir else {
        info!(
            event = "plugins.skip",
            reason = "disabled",
            "plugin loading disabled"
        );
        return Ok(LoadedPlugins::default());
    };

    if dir.as_os_str().is_empty() {
        info!(
            event = "plugins.skip",
            reason = "empty_path",
            "plugin directory empty; skipping"
        );
        return Ok(LoadedPlugins::default());
    }

    if !dir.exists() {
        info!(
            event = "plugins.skip",
            path = %dir.display(),
            reason = "missing",
            "plugin directory not found; skipping"
        );
        return Ok(LoadedPlugins::default());
    }

    if !dir.is_dir() {
        return Err(DaemonError::Plugin(format!(
            "plugin path '{}' is not a directory",
            dir.display()
        )));
    }

    let mut libraries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if should_skip(&path) {
            continue;
        }
        unsafe {
            let library = Library::new(&path).map_err(|err| {
                DaemonError::Plugin(format!(
                    "failed to load plugin '{}': {}",
                    path.display(),
                    err
                ))
            })?;

            let register: libloading::Symbol<
                unsafe extern "C" fn() -> function_api::PluginRegistration,
            > = library.get(REGISTER_SYMBOL).map_err(|err| {
                DaemonError::Plugin(format!(
                    "plugin '{}' missing export '{}': {}",
                    path.display(),
                    REGISTER_SYMBOL_NAME,
                    err
                ))
            })?;

            let registration = register();
            if let Err(err) = register_plugin_functions(&registration) {
                return Err(DaemonError::Plugin(err.to_string()));
            }
            info!(event = "plugins.loaded", path = %path.display(), "registered plugin");
            libraries.push(library);
        }
    }

    Ok(LoadedPlugins::new(libraries))
}

fn should_skip(path: &Path) -> bool {
    if path.is_dir() {
        return true;
    }
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if matches!(ext, "so" | "dylib" | "dll") => false,
        _ => true,
    }
}
