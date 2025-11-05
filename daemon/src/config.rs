use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{DaemonError, Result};

const DEFAULT_CONFIG_ENV: &str = "GRAPHDB_DAEMON_CONFIG";

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub working_directory: PathBuf,
    pub pid_file: Option<PathBuf>,
    pub stdin: Option<PathBuf>,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
    pub umask: Option<u32>,
    pub log_level: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            working_directory: PathBuf::from("/"),
            pid_file: Some(PathBuf::from("graphdb-daemon.pid")),
            stdin: None,
            stdout: Some(PathBuf::from("graphdb-daemon.log")),
            stderr: None,
            umask: Some(0o027),
            log_level: Some(String::from("info")),
        }
    }
}

impl DaemonConfig {
    pub fn from_sources(cli_path: Option<&str>) -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let env_path = std::env::var(DEFAULT_CONFIG_ENV).ok();

        if let Some(path) = cli_path {
            if path.is_empty() {
                return Err(DaemonError::Config(
                    "configuration path must not be empty".into(),
                ));
            }
        }

        let config = if let Some(path) = cli_path {
            Self::load_from_path(path)?
        } else if let Some(path) = env_path.as_deref().filter(|p| !p.is_empty()) {
            Self::load_from_path(path)?
        } else {
            let mut cfg = Self::default();
            cfg.normalize_paths(&cwd);
            cfg
        };

        if config.working_directory.as_os_str().is_empty() {
            return Err(DaemonError::Config(
                "working_directory must not be empty".into(),
            ));
        }

        Ok(config)
    }

    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };

        let raw = fs::read_to_string(&absolute_path)?;
        let mut config: DaemonConfig = toml::from_str(&raw)?;
        let base = absolute_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        config.normalize_paths(&base);
        Ok(config)
    }

    pub fn pid_file(&self) -> Option<&Path> {
        self.pid_file.as_deref()
    }

    pub fn stdout(&self) -> Option<&Path> {
        self.stdout.as_deref()
    }

    pub fn stderr(&self) -> Option<&Path> {
        self.stderr.as_deref()
    }

    pub fn stdin(&self) -> Option<&Path> {
        self.stdin.as_deref()
    }

    fn normalize_paths(&mut self, base: &Path) {
        if self.working_directory.is_relative() {
            self.working_directory = base.join(&self.working_directory);
        }

        normalize_optional_path(&mut self.pid_file, base);
        normalize_optional_path(&mut self.stdin, base);
        normalize_optional_path(&mut self.stdout, base);
        normalize_optional_path(&mut self.stderr, base);
    }
}

fn normalize_optional_path(target: &mut Option<PathBuf>, base: &Path) {
    if let Some(path) = target {
        if path.is_relative() {
            *path = base.join(&*path);
        }
    }
}
