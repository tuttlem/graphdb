use std::fs;
use std::net::{IpAddr, SocketAddr};
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
    pub storage: StorageSettings,
    pub server: ServerSettings,
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
            storage: StorageSettings::default(),
            server: ServerSettings::default(),
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

        if let StorageBackendKind::Simple = config.storage.backend {
            if config.storage.directory.is_none() {
                return Err(DaemonError::Config(
                    "storage.directory must be set when using simple backend".into(),
                ));
            }
        }

        if let Some(limit) = config.server.concurrency_limit {
            if limit == 0 {
                return Err(DaemonError::Config(
                    "server.concurrency_limit must be greater than zero".into(),
                ));
            }
        }

        if let Some(limit) = config.server.body_limit {
            if limit == 0 {
                return Err(DaemonError::Config(
                    "server.body_limit must be greater than zero".into(),
                ));
            }
        }

        if let Some(worker_threads) = config.server.worker_threads {
            if worker_threads == 0 {
                return Err(DaemonError::Config(
                    "server.worker_threads must be greater than zero".into(),
                ));
            }
        }

        if let Some(tls) = config.server.tls.as_ref() {
            if tls.cert_path.as_os_str().is_empty() {
                return Err(DaemonError::Config(
                    "server.tls.cert_path must not be empty".into(),
                ));
            }
            if tls.key_path.as_os_str().is_empty() {
                return Err(DaemonError::Config(
                    "server.tls.key_path must not be empty".into(),
                ));
            }
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

    pub fn storage(&self) -> &StorageSettings {
        &self.storage
    }

    pub fn socket_addr(&self) -> Result<SocketAddr> {
        let addr: IpAddr = self
            .server
            .bind_address
            .parse()
            .map_err(|err| DaemonError::Config(format!("invalid bind_address: {err}")))?;
        Ok(SocketAddr::new(addr, self.server.port))
    }

    pub fn server(&self) -> &ServerSettings {
        &self.server
    }

    fn normalize_paths(&mut self, base: &Path) {
        if self.working_directory.is_relative() {
            self.working_directory = base.join(&self.working_directory);
        }

        normalize_optional_path(&mut self.pid_file, base);
        normalize_optional_path(&mut self.stdin, base);
        normalize_optional_path(&mut self.stdout, base);
        normalize_optional_path(&mut self.stderr, base);
        self.storage.normalize(base);
        self.server.normalize(base);
    }
}

fn normalize_optional_path(target: &mut Option<PathBuf>, base: &Path) {
    if let Some(path) = target {
        if path.is_relative() {
            *path = base.join(&*path);
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    Memory,
    Simple,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageSettings {
    pub backend: StorageBackendKind,
    pub directory: Option<PathBuf>,
}

impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Memory,
            directory: None,
        }
    }
}

impl StorageSettings {
    fn normalize(&mut self, base: &Path) {
        normalize_optional_path(&mut self.directory, base);
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerSettings {
    pub bind_address: String,
    pub port: u16,
    pub http2_only: bool,
    pub tcp_nodelay: bool,
    pub worker_threads: Option<usize>,
    pub concurrency_limit: Option<usize>,
    pub body_limit: Option<usize>,
    pub tls: Option<TlsSettings>,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".into(),
            port: 8080,
            http2_only: false,
            tcp_nodelay: true,
            worker_threads: None,
            concurrency_limit: None,
            body_limit: None,
            tls: None,
        }
    }
}

impl ServerSettings {
    pub fn worker_threads(&self) -> Option<usize> {
        self.worker_threads
    }

    pub fn tls(&self) -> Option<&TlsSettings> {
        self.tls.as_ref()
    }

    fn normalize(&mut self, base: &Path) {
        if let Some(tls) = &mut self.tls {
            tls.normalize(base);
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsSettings {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl TlsSettings {
    fn normalize(&mut self, base: &Path) {
        if self.cert_path.is_relative() {
            self.cert_path = base.join(&self.cert_path);
        }
        if self.key_path.is_relative() {
            self.key_path = base.join(&self.key_path);
        }
    }
}
