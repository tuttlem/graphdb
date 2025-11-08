use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),
    #[error("logger initialization failed: {0}")]
    Logger(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("failed to parse configuration: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("request serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("query parse error: {0}")]
    QueryParse(#[from] graphdb_core::query::ParseError),
    #[error("database error: {0}")]
    Database(#[from] graphdb_core::DatabaseError),
    #[error("storage error: {0}")]
    Storage(#[from] graphdb_core::StorageError),
    #[error("query execution error: {0}")]
    Query(String),
    #[error("http server error: {0}")]
    Http(#[from] hyper::Error),
}

pub type Result<T> = std::result::Result<T, DaemonError>;
