use std::sync::Arc;

use graphdb_core::{Database, InMemoryBackend, SimpleStorage};

use crate::config::{DaemonConfig, StorageBackendKind};
use crate::error::{DaemonError, Result};
use crate::executor::{ExecutionReport, execute_script_with_memory, execute_script_with_simple};

pub type DatabaseHandle = Arc<GraphDatabase>;

pub struct GraphDatabase {
    inner: GraphDatabaseInner,
}

enum GraphDatabaseInner {
    InMemory(Database<InMemoryBackend>),
    Simple(Database<SimpleStorage>),
}

impl GraphDatabase {
    pub fn from_config(config: &DaemonConfig) -> Result<Self> {
        let inner = match config.storage().backend {
            StorageBackendKind::Memory => {
                log::info!("initialising in-memory backend");
                GraphDatabaseInner::InMemory(Database::new(InMemoryBackend::new()))
            }
            StorageBackendKind::Simple => {
                let directory = config
                    .storage()
                    .directory
                    .as_ref()
                    .ok_or_else(|| {
                        DaemonError::Config(
                            "storage.directory must be set for simple backend".into(),
                        )
                    })?
                    .clone();
                log::info!(
                    "initialising simple storage backend at {}",
                    directory.display()
                );
                let backend = SimpleStorage::new(directory)?;
                GraphDatabaseInner::Simple(Database::new(backend))
            }
        };

        Ok(Self { inner })
    }

    pub fn execute_script(&self, script: &str) -> Result<ExecutionReport> {
        match &self.inner {
            GraphDatabaseInner::InMemory(db) => execute_script_with_memory(db, script),
            GraphDatabaseInner::Simple(db) => execute_script_with_simple(db, script),
        }
    }
}

pub fn shared_database(config: &DaemonConfig) -> Result<DatabaseHandle> {
    let db = GraphDatabase::from_config(config)?;
    Ok(Arc::new(db))
}
