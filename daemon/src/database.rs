use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use graphdb_core::{
    CatalogError, CatalogResult, CatalogSnapshot, CatalogSnapshotSink, Database, EdgeId,
    InMemoryBackend, NodeId, SimpleStorage, SystemCatalog,
};
use serde_json;

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
                let backend = SimpleStorage::new(directory.clone())?;
                let catalog_path = catalog_file_path(&directory);
                let catalog = load_or_bootstrap_catalog(&catalog_path)?;
                let database = Database::with_catalog(backend, catalog);
                hydrate_simple_backend(&database, &directory)?;
                GraphDatabaseInner::Simple(database)
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

fn hydrate_simple_backend(database: &Database<SimpleStorage>, root: &Path) -> Result<()> {
    preload_nodes(database, &root.join("nodes"))?;
    preload_edges(database, &root.join("edges"))?;
    Ok(())
}

fn catalog_file_path(root: &Path) -> PathBuf {
    root.join("catalog.json")
}

fn load_or_bootstrap_catalog(path: &Path) -> Result<Arc<SystemCatalog>> {
    let store = Arc::new(FileCatalogStore {
        path: path.to_path_buf(),
    });

    match read_catalog_snapshot(path)? {
        Some(snapshot) => SystemCatalog::from_snapshot(snapshot, Some(store))
            .map_err(|err| DaemonError::Config(format!("failed to load catalog: {err}"))),
        None => Ok(SystemCatalog::bootstrap_with_persistence(Some(store))),
    }
}

fn read_catalog_snapshot(path: &Path) -> Result<Option<CatalogSnapshot>> {
    match fs::read(path) {
        Ok(bytes) => {
            let snapshot = serde_json::from_slice(&bytes).map_err(DaemonError::Json)?;
            Ok(Some(snapshot))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(DaemonError::Io(err)),
    }
}

struct FileCatalogStore {
    path: PathBuf,
}

impl CatalogSnapshotSink for FileCatalogStore {
    fn save(&self, snapshot: &CatalogSnapshot) -> CatalogResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| CatalogError::Persistence(err.to_string()))?;
        }

        let tmp_path = self.path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(snapshot)
            .map_err(|err| CatalogError::Persistence(err.to_string()))?;
        fs::write(&tmp_path, data).map_err(|err| CatalogError::Persistence(err.to_string()))?;
        fs::rename(&tmp_path, &self.path)
            .map_err(|err| CatalogError::Persistence(err.to_string()))?;
        Ok(())
    }
}

fn preload_nodes(database: &Database<SimpleStorage>, dir: &Path) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(DaemonError::Io(err)),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        match NodeId::parse_str(stem) {
            Ok(id) => {
                if let Err(err) = database.get_node(id) {
                    log::warn!("failed to hydrate node {stem}: {err}");
                }
            }
            Err(err) => {
                log::warn!("unable to parse node file name {stem} as UUID: {err}");
            }
        }
    }

    Ok(())
}

fn preload_edges(database: &Database<SimpleStorage>, dir: &Path) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(DaemonError::Io(err)),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        match EdgeId::parse_str(stem) {
            Ok(id) => {
                if let Err(err) = database.get_edge(id) {
                    log::warn!("failed to hydrate edge {stem}: {err}");
                }
            }
            Err(err) => {
                log::warn!("unable to parse edge file name {stem} as UUID: {err}");
            }
        }
    }

    Ok(())
}
