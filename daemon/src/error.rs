use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),
    #[error("logger initialization failed: {0}")]
    Logger(#[from] log::SetLoggerError),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("failed to parse configuration: {0}")]
    Toml(#[from] toml::de::Error),
}

pub type Result<T> = std::result::Result<T, DaemonError>;
