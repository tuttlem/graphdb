use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Identifier assigned to schemas managed by the catalog.
pub type SchemaId = Uuid;
/// Identifier assigned to roles.
pub type RoleId = Uuid;
/// Identifier assigned to users.
pub type UserId = Uuid;
/// Identifier assigned to logical node classes.
pub type NodeClassId = Uuid;
/// Identifier assigned to logical edge classes.
pub type EdgeClassId = Uuid;
/// Identifier assigned to index descriptors.
pub type IndexId = Uuid;
/// Identifier assigned to tracked processes.
pub type ProcessId = Uuid;

pub trait CatalogSnapshotSink: Send + Sync {
    fn save(&self, snapshot: &CatalogSnapshot) -> CatalogResult<()>;
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Privilege: u16 {
        const SELECT = 0b0001;
        const INSERT = 0b0010;
        const UPDATE = 0b0100;
        const DELETE = 0b1000;
    }
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.is_empty() {
            parts.push("NONE");
        } else {
            if self.contains(Privilege::SELECT) {
                parts.push("SELECT");
            }
            if self.contains(Privilege::INSERT) {
                parts.push("INSERT");
            }
            if self.contains(Privilege::UPDATE) {
                parts.push("UPDATE");
            }
            if self.contains(Privilege::DELETE) {
                parts.push("DELETE");
            }
        }
        write!(f, "{}", parts.join("|"))
    }
}

/// The object that a privilege grant targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CatalogObject {
    Schema(SchemaId),
    NodeClass(NodeClassId),
    EdgeClass(EdgeClassId),
    Index(IndexId),
}

impl fmt::Display for CatalogObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogObject::Schema(id) => write!(f, "schema {id}"),
            CatalogObject::NodeClass(id) => write!(f, "node class {id}"),
            CatalogObject::EdgeClass(id) => write!(f, "edge class {id}"),
            CatalogObject::Index(id) => write!(f, "index {id}"),
        }
    }
}

/// Stored metadata for a schema namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub id: SchemaId,
    pub name: String,
    pub owner_role: RoleId,
    pub created_at: SystemTime,
}

/// Stored metadata for a catalog role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleEntry {
    pub id: RoleId,
    pub name: String,
    pub inherits: HashSet<RoleId>,
    pub created_at: SystemTime,
}

/// Stored metadata for a catalog user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEntry {
    pub id: UserId,
    pub username: String,
    pub login_role: RoleId,
    pub default_role: RoleId,
    pub created_at: SystemTime,
    pub auth_method: AuthMethod,
}

/// Supported authentication metadata for users.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    InternalPassword,
    External,
}

/// Stored metadata for a node class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeClassEntry {
    pub id: NodeClassId,
    pub schema_id: SchemaId,
    pub name: String,
    pub owner_role: RoleId,
    pub properties: HashSet<String>,
    pub created_at: SystemTime,
    pub version: u64,
}

/// Stored metadata for an edge class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeClassEntry {
    pub id: EdgeClassId,
    pub schema_id: SchemaId,
    pub name: String,
    pub owner_role: RoleId,
    pub properties: HashSet<String>,
    pub source_class: Option<NodeClassId>,
    pub target_class: Option<NodeClassId>,
    pub created_at: SystemTime,
    pub version: u64,
}

/// Stored metadata for an index descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub id: IndexId,
    pub schema_id: SchemaId,
    pub name: String,
    pub owner_role: RoleId,
    pub target: IndexTarget,
    pub fields: Vec<String>,
    pub created_at: SystemTime,
    pub definition: IndexDefinition,
}

/// The logical object an index is defined upon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexTarget {
    NodeClass(NodeClassId),
    EdgeClass(EdgeClassId),
}

/// Placeholder for physical index definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexDefinition {
    BTree,
    Hash,
    Custom(String),
}

/// Stored privilege grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntry {
    pub grantee: RoleId,
    pub object: CatalogObject,
    pub privileges: Privilege,
    pub granted_at: SystemTime,
    pub grantor: RoleId,
}

/// Metadata about an in-flight process for activity accounting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessEntry {
    pub id: ProcessId,
    pub user: UserId,
    pub active_role: RoleId,
    pub description: String,
    pub started_at: SystemTime,
    pub last_heartbeat: SystemTime,
}

/// Errors surfaced by the system catalog module.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("lock poisoned during {0}")]
    LockPoisoned(&'static str),
    #[error("schema {0} not found")]
    SchemaNotFound(String),
    #[error("role {0} not found")]
    RoleNotFound(String),
    #[error("user {0} not found")]
    UserNotFound(String),
    #[error("object not found in catalog")]
    ObjectMissing,
    #[error("catalog persistence failed: {0}")]
    Persistence(String),
}

pub type CatalogResult<T> = Result<T, CatalogError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationStatus {
    Created,
    Updated,
    Unchanged,
}

#[derive(Default)]
struct CatalogState {
    schemas_by_id: HashMap<SchemaId, Arc<SchemaEntry>>,
    schemas_by_name: HashMap<String, SchemaId>,
    roles_by_id: HashMap<RoleId, Arc<RoleEntry>>,
    roles_by_name: HashMap<String, RoleId>,
    users_by_id: HashMap<UserId, Arc<UserEntry>>,
    users_by_name: HashMap<String, UserId>,
    node_classes_by_id: HashMap<NodeClassId, Arc<NodeClassEntry>>,
    node_classes_by_name: HashMap<(SchemaId, String), NodeClassId>,
    edge_classes_by_id: HashMap<EdgeClassId, Arc<EdgeClassEntry>>,
    edge_classes_by_name: HashMap<(SchemaId, String), EdgeClassId>,
    indexes_by_id: HashMap<IndexId, Arc<IndexEntry>>,
    indexes_by_name: HashMap<(SchemaId, String), IndexId>,
    grants: Vec<GrantEntry>,
    processes_by_id: HashMap<ProcessId, Arc<ProcessEntry>>,
}

struct TableEpochs {
    global: AtomicU64,
    schemas: AtomicU64,
    roles: AtomicU64,
    users: AtomicU64,
    node_classes: AtomicU64,
    edge_classes: AtomicU64,
    indexes: AtomicU64,
    grants: AtomicU64,
    processes: AtomicU64,
}

impl Default for TableEpochs {
    fn default() -> Self {
        Self {
            global: AtomicU64::new(1),
            schemas: AtomicU64::new(1),
            roles: AtomicU64::new(1),
            users: AtomicU64::new(1),
            node_classes: AtomicU64::new(1),
            edge_classes: AtomicU64::new(1),
            indexes: AtomicU64::new(1),
            grants: AtomicU64::new(1),
            processes: AtomicU64::new(1),
        }
    }
}

impl TableEpochs {
    fn snapshot(&self) -> EpochSnapshot {
        EpochSnapshot {
            global: self.global.load(Ordering::SeqCst),
            schemas: self.schemas.load(Ordering::SeqCst),
            roles: self.roles.load(Ordering::SeqCst),
            users: self.users.load(Ordering::SeqCst),
            node_classes: self.node_classes.load(Ordering::SeqCst),
            edge_classes: self.edge_classes.load(Ordering::SeqCst),
            indexes: self.indexes.load(Ordering::SeqCst),
            grants: self.grants.load(Ordering::SeqCst),
            processes: self.processes.load(Ordering::SeqCst),
        }
    }

    fn from_snapshot(snapshot: &EpochSnapshot) -> Self {
        Self {
            global: AtomicU64::new(snapshot.global.max(1)),
            schemas: AtomicU64::new(snapshot.schemas.max(1)),
            roles: AtomicU64::new(snapshot.roles.max(1)),
            users: AtomicU64::new(snapshot.users.max(1)),
            node_classes: AtomicU64::new(snapshot.node_classes.max(1)),
            edge_classes: AtomicU64::new(snapshot.edge_classes.max(1)),
            indexes: AtomicU64::new(snapshot.indexes.max(1)),
            grants: AtomicU64::new(snapshot.grants.max(1)),
            processes: AtomicU64::new(snapshot.processes.max(1)),
        }
    }
}

fn bump_epoch(counter: &AtomicU64, label: &'static str) {
    let mut current = counter.load(Ordering::SeqCst);
    loop {
        let next = current.wrapping_add(1).max(1);
        match counter.compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
    log::trace!(
        "catalog epoch bump for {label}: now {}",
        counter.load(Ordering::SeqCst)
    );
}

/// Primary interface to the system catalog.
pub struct SystemCatalog {
    state: RwLock<CatalogState>,
    epochs: TableEpochs,
    system_role: RoleId,
    public_role: RoleId,
    system_schema: SchemaId,
    default_schema: SchemaId,
    persistence: Option<Arc<dyn CatalogSnapshotSink>>,
}

impl SystemCatalog {
    const SYSTEM_ROLE: &'static str = "system";
    const PUBLIC_ROLE: &'static str = "public";
    const SYSTEM_SCHEMA: &'static str = "system";
    const DEFAULT_SCHEMA: &'static str = "public";

    /// Creates a catalog initialised with system defaults.
    pub fn bootstrap() -> Arc<Self> {
        Self::bootstrap_with_persistence(None)
    }

    pub fn bootstrap_with_persistence(
        persistence: Option<Arc<dyn CatalogSnapshotSink>>,
    ) -> Arc<Self> {
        let state = CatalogState::default();
        let catalog = Arc::new(Self {
            state: RwLock::new(state),
            epochs: TableEpochs::default(),
            system_role: RoleId::new_v4(),
            public_role: RoleId::new_v4(),
            system_schema: SchemaId::new_v4(),
            default_schema: SchemaId::new_v4(),
            persistence,
        });

        catalog
            .initialise_defaults()
            .expect("bootstrap catalog defaults");
        catalog
    }

    pub fn from_snapshot(
        snapshot: CatalogSnapshot,
        persistence: Option<Arc<dyn CatalogSnapshotSink>>,
    ) -> CatalogResult<Arc<Self>> {
        let CatalogSnapshot {
            system_role,
            public_role,
            system_schema,
            default_schema,
            epochs,
            schemas,
            roles,
            users,
            node_classes,
            edge_classes,
            indexes,
            grants,
            processes,
        } = snapshot;

        let mut state = CatalogState::default();

        for schema in schemas.into_iter() {
            let id = schema.id;
            let entry = Arc::new(schema);
            state.schemas_by_id.insert(id, entry.clone());
            state.schemas_by_name.insert(entry.name.clone(), id);
        }

        for role in roles.into_iter() {
            let id = role.id;
            let entry = Arc::new(role);
            state.roles_by_id.insert(id, entry.clone());
            state.roles_by_name.insert(entry.name.clone(), id);
        }

        for user in users.into_iter() {
            let id = user.id;
            let entry = Arc::new(user);
            state.users_by_id.insert(id, entry.clone());
            state.users_by_name.insert(entry.username.clone(), id);
        }

        for class in node_classes.into_iter() {
            let id = class.id;
            let entry = Arc::new(class);
            state.node_classes_by_id.insert(id, entry.clone());
            state
                .node_classes_by_name
                .insert((entry.schema_id, entry.name.clone()), id);
        }

        for class in edge_classes.into_iter() {
            let id = class.id;
            let entry = Arc::new(class);
            state.edge_classes_by_id.insert(id, entry.clone());
            state
                .edge_classes_by_name
                .insert((entry.schema_id, entry.name.clone()), id);
        }

        for index in indexes.into_iter() {
            let id = index.id;
            let entry = Arc::new(index);
            state.indexes_by_id.insert(id, entry.clone());
            state
                .indexes_by_name
                .insert((entry.schema_id, entry.name.clone()), id);
        }

        state.grants = grants;

        for process in processes.into_iter() {
            state.processes_by_id.insert(process.id, Arc::new(process));
        }

        let catalog = Arc::new(Self {
            state: RwLock::new(state),
            epochs: TableEpochs::from_snapshot(&epochs),
            system_role,
            public_role,
            system_schema,
            default_schema,
            persistence,
        });

        Ok(catalog)
    }

    fn initialise_defaults(self: &Arc<Self>) -> CatalogResult<()> {
        let system_role = self.system_role;
        let public_role = self.public_role;
        {
            let mut state = self
                .state
                .write()
                .map_err(|_| CatalogError::LockPoisoned("init defaults"))?;

            let now = SystemTime::now();
            let system_role_entry = Arc::new(RoleEntry {
                id: system_role,
                name: Self::SYSTEM_ROLE.to_string(),
                inherits: HashSet::new(),
                created_at: now,
            });
            state
                .roles_by_id
                .insert(system_role, system_role_entry.clone());
            state
                .roles_by_name
                .insert(Self::SYSTEM_ROLE.to_string(), system_role);

            let mut public_inherits = HashSet::new();
            public_inherits.insert(system_role);
            let public_role_entry = Arc::new(RoleEntry {
                id: public_role,
                name: Self::PUBLIC_ROLE.to_string(),
                inherits: public_inherits,
                created_at: now,
            });
            state
                .roles_by_id
                .insert(public_role, public_role_entry.clone());
            state
                .roles_by_name
                .insert(Self::PUBLIC_ROLE.to_string(), public_role);

            let system_schema_entry = Arc::new(SchemaEntry {
                id: self.system_schema,
                name: Self::SYSTEM_SCHEMA.to_string(),
                owner_role: system_role,
                created_at: now,
            });
            state
                .schemas_by_id
                .insert(self.system_schema, system_schema_entry.clone());
            state
                .schemas_by_name
                .insert(Self::SYSTEM_SCHEMA.to_string(), self.system_schema);

            let default_schema_entry = Arc::new(SchemaEntry {
                id: self.default_schema,
                name: Self::DEFAULT_SCHEMA.to_string(),
                owner_role: system_role,
                created_at: now,
            });
            state
                .schemas_by_id
                .insert(self.default_schema, default_schema_entry.clone());
            state
                .schemas_by_name
                .insert(Self::DEFAULT_SCHEMA.to_string(), self.default_schema);
        }

        bump_epoch(&self.epochs.global, "bootstrap");
        bump_epoch(&self.epochs.schemas, "bootstrap.schemas");
        bump_epoch(&self.epochs.roles, "bootstrap.roles");
        self.persist_snapshot()?;
        Ok(())
    }

    fn with_state<F, T>(&self, op: &'static str, func: F) -> CatalogResult<T>
    where
        F: FnOnce(&mut CatalogState) -> CatalogResult<T>,
    {
        let mut guard = self
            .state
            .write()
            .map_err(|_| CatalogError::LockPoisoned(op))?;
        func(&mut guard)
    }

    fn with_state_read<F, T>(&self, op: &'static str, func: F) -> CatalogResult<T>
    where
        F: FnOnce(&CatalogState) -> CatalogResult<T>,
    {
        let guard = self
            .state
            .read()
            .map_err(|_| CatalogError::LockPoisoned(op))?;
        func(&guard)
    }

    /// Returns the identifier for the default schema.
    pub fn default_schema_id(&self) -> SchemaId {
        self.default_schema
    }

    /// Returns the identifier for the system schema.
    pub fn system_schema_id(&self) -> SchemaId {
        self.system_schema
    }

    /// Returns the identifier for the built-in system role.
    pub fn system_role_id(&self) -> RoleId {
        self.system_role
    }

    /// Returns the identifier for the built-in public role.
    pub fn public_role_id(&self) -> RoleId {
        self.public_role
    }

    /// Ensures a schema exists with the provided name.
    pub fn ensure_schema(&self, name: &str, owner_role: RoleId) -> CatalogResult<SchemaId> {
        let name_key = name.to_owned();
        let mut created = false;
        let id = self.with_state("ensure_schema", |state| {
            if let Some(id) = state.schemas_by_name.get(&name_key) {
                return Ok(*id);
            }

            let id = SchemaId::new_v4();
            let entry = Arc::new(SchemaEntry {
                id,
                name: name_key.clone(),
                owner_role,
                created_at: SystemTime::now(),
            });
            state.schemas_by_id.insert(id, entry);
            state.schemas_by_name.insert(name_key, id);
            created = true;
            Ok(id)
        })?;

        if created {
            bump_epoch(&self.epochs.schemas, "ensure_schema");
            bump_epoch(&self.epochs.global, "ensure_schema.global");
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Ensures a role exists with the provided name.
    pub fn ensure_role(&self, name: &str, inherits: HashSet<RoleId>) -> CatalogResult<RoleId> {
        let name_key = name.to_owned();
        let mut created = false;
        let id = self.with_state("ensure_role", |state| {
            if let Some(id) = state.roles_by_name.get(&name_key) {
                return Ok(*id);
            }

            let id = RoleId::new_v4();
            let entry = Arc::new(RoleEntry {
                id,
                name: name_key.clone(),
                inherits,
                created_at: SystemTime::now(),
            });
            state.roles_by_id.insert(id, entry);
            state.roles_by_name.insert(name_key, id);
            created = true;
            Ok(id)
        })?;

        if created {
            bump_epoch(&self.epochs.roles, "ensure_role");
            bump_epoch(&self.epochs.global, "ensure_role.global");
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Registers a new user entry in the catalog.
    pub fn register_user(
        &self,
        username: &str,
        login_role: RoleId,
        default_role: RoleId,
        auth_method: AuthMethod,
    ) -> CatalogResult<UserId> {
        let username_key = username.to_owned();
        let mut created = false;
        let id = self.with_state("register_user", |state| {
            if let Some(id) = state.users_by_name.get(&username_key) {
                return Ok(*id);
            }

            let id = UserId::new_v4();
            let entry = Arc::new(UserEntry {
                id,
                username: username_key.clone(),
                login_role,
                default_role,
                created_at: SystemTime::now(),
                auth_method,
            });
            state.users_by_id.insert(id, entry);
            state.users_by_name.insert(username_key, id);
            created = true;
            Ok(id)
        })?;

        if created {
            bump_epoch(&self.epochs.users, "register_user");
            bump_epoch(&self.epochs.global, "register_user.global");
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Ensures a node class record exists for the given schema and name,
    /// updating the known property set if necessary.
    pub fn ensure_node_class(
        &self,
        schema_id: SchemaId,
        name: &str,
        owner_role: RoleId,
        properties: &[String],
    ) -> CatalogResult<NodeClassId> {
        let key = (schema_id, name.to_owned());
        let mut status = MutationStatus::Unchanged;
        let id = self.with_state("ensure_node_class", |state| {
            if let Some(id) = state.node_classes_by_name.get(&key) {
                if let Some(entry) = state.node_classes_by_id.get(id) {
                    let mut updated = (**entry).clone();
                    let mut changed = false;
                    for prop in properties {
                        if updated.properties.insert(prop.clone()) {
                            changed = true;
                        }
                    }
                    if changed {
                        updated.version = updated.version.wrapping_add(1);
                        let arc = Arc::new(updated);
                        state.node_classes_by_id.insert(*id, arc);
                        status = MutationStatus::Updated;
                    }
                }
                return Ok(*id);
            }

            let entry = NodeClassEntry {
                id: NodeClassId::new_v4(),
                schema_id,
                name: key.1.clone(),
                owner_role,
                properties: properties.iter().cloned().collect(),
                created_at: SystemTime::now(),
                version: 1,
            };
            let id = entry.id;
            state.node_classes_by_id.insert(id, Arc::new(entry));
            state.node_classes_by_name.insert(key, id);
            status = MutationStatus::Created;
            Ok(id)
        })?;

        match status {
            MutationStatus::Created => {
                bump_epoch(&self.epochs.node_classes, "ensure_node_class.new");
                bump_epoch(&self.epochs.global, "ensure_node_class.global");
            }
            MutationStatus::Updated => {
                bump_epoch(&self.epochs.node_classes, "ensure_node_class.update");
                bump_epoch(&self.epochs.global, "ensure_node_class.global");
            }
            MutationStatus::Unchanged => {}
        }

        if !matches!(status, MutationStatus::Unchanged) {
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Ensures an edge class record exists for the given schema and name,
    /// updating the known property set if necessary.
    pub fn ensure_edge_class(
        &self,
        schema_id: SchemaId,
        name: &str,
        owner_role: RoleId,
        properties: &[String],
        source: Option<NodeClassId>,
        target: Option<NodeClassId>,
    ) -> CatalogResult<EdgeClassId> {
        let key = (schema_id, name.to_owned());
        let mut status = MutationStatus::Unchanged;
        let id = self.with_state("ensure_edge_class", |state| {
            if let Some(id) = state.edge_classes_by_name.get(&key) {
                if let Some(entry) = state.edge_classes_by_id.get(id) {
                    let mut updated = (**entry).clone();
                    let mut changed = false;
                    for prop in properties {
                        if updated.properties.insert(prop.clone()) {
                            changed = true;
                        }
                    }
                    if source != updated.source_class {
                        updated.source_class = source;
                        changed = true;
                    }
                    if target != updated.target_class {
                        updated.target_class = target;
                        changed = true;
                    }
                    if changed {
                        updated.version = updated.version.wrapping_add(1);
                        let arc = Arc::new(updated);
                        state.edge_classes_by_id.insert(*id, arc);
                        status = MutationStatus::Updated;
                    }
                }
                return Ok(*id);
            }

            let entry = EdgeClassEntry {
                id: EdgeClassId::new_v4(),
                schema_id,
                name: key.1.clone(),
                owner_role,
                properties: properties.iter().cloned().collect(),
                source_class: source,
                target_class: target,
                created_at: SystemTime::now(),
                version: 1,
            };
            let id = entry.id;
            state.edge_classes_by_id.insert(id, Arc::new(entry));
            state.edge_classes_by_name.insert(key, id);
            status = MutationStatus::Created;
            Ok(id)
        })?;

        match status {
            MutationStatus::Created => {
                bump_epoch(&self.epochs.edge_classes, "ensure_edge_class.new");
                bump_epoch(&self.epochs.global, "ensure_edge_class.global");
            }
            MutationStatus::Updated => {
                bump_epoch(&self.epochs.edge_classes, "ensure_edge_class.update");
                bump_epoch(&self.epochs.global, "ensure_edge_class.global");
            }
            MutationStatus::Unchanged => {}
        }

        if !matches!(status, MutationStatus::Unchanged) {
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Registers a catalogued index.
    pub fn register_index(
        &self,
        schema_id: SchemaId,
        name: &str,
        owner_role: RoleId,
        target: IndexTarget,
        fields: Vec<String>,
        definition: IndexDefinition,
    ) -> CatalogResult<IndexId> {
        let key = (schema_id, name.to_owned());
        let mut created = false;
        let id = self.with_state("register_index", |state| {
            if let Some(id) = state.indexes_by_name.get(&key) {
                return Ok(*id);
            }

            let entry = IndexEntry {
                id: IndexId::new_v4(),
                schema_id,
                name: key.1.clone(),
                owner_role,
                target,
                fields,
                created_at: SystemTime::now(),
                definition,
            };
            let id = entry.id;
            state.indexes_by_id.insert(id, Arc::new(entry));
            state.indexes_by_name.insert(key, id);
            created = true;
            Ok(id)
        })?;

        if created {
            bump_epoch(&self.epochs.indexes, "register_index");
            bump_epoch(&self.epochs.global, "register_index.global");
            self.persist_snapshot()?;
        }

        Ok(id)
    }

    /// Records a privilege grant.
    pub fn grant_privilege(
        &self,
        grantee: RoleId,
        object: CatalogObject,
        privileges: Privilege,
        grantor: RoleId,
    ) -> CatalogResult<()> {
        self.with_state("grant_privilege", |state| {
            state.grants.push(GrantEntry {
                grantee,
                object,
                privileges,
                grantor,
                granted_at: SystemTime::now(),
            });
            Ok(())
        })?;
        bump_epoch(&self.epochs.grants, "grant_privilege");
        bump_epoch(&self.epochs.global, "grant_privilege.global");
        self.persist_snapshot()?;
        Ok(())
    }

    /// Registers a process in the accounting chart.
    pub fn register_process(
        &self,
        user: UserId,
        active_role: RoleId,
        description: &str,
    ) -> CatalogResult<ProcessId> {
        let id = self.with_state("register_process", |state| {
            let id = ProcessId::new_v4();
            let now = SystemTime::now();
            let entry = Arc::new(ProcessEntry {
                id,
                user,
                active_role,
                description: description.to_owned(),
                started_at: now,
                last_heartbeat: now,
            });
            state.processes_by_id.insert(id, entry);
            Ok(id)
        })?;
        bump_epoch(&self.epochs.processes, "register_process");
        bump_epoch(&self.epochs.global, "register_process.global");
        self.persist_snapshot()?;
        Ok(id)
    }

    /// Heartbeat for a tracked process; returns false if the process is missing.
    pub fn heartbeat_process(&self, process_id: ProcessId) -> CatalogResult<bool> {
        let updated = self.with_state("heartbeat_process", |state| {
            if let Some(entry) = state.processes_by_id.get(&process_id) {
                let mut data = (**entry).clone();
                data.last_heartbeat = SystemTime::now();
                state.processes_by_id.insert(process_id, Arc::new(data));
                return Ok(true);
            }
            Ok(false)
        })?;
        if updated {
            bump_epoch(&self.epochs.processes, "heartbeat_process");
            self.persist_snapshot()?;
        }
        Ok(updated)
    }

    /// Deregisters a process entry if it exists.
    pub fn complete_process(&self, process_id: ProcessId) -> CatalogResult<bool> {
        let removed = self.with_state("complete_process", |state| {
            Ok(state.processes_by_id.remove(&process_id).is_some())
        })?;
        if removed {
            bump_epoch(&self.epochs.processes, "complete_process");
            bump_epoch(&self.epochs.global, "complete_process.global");
            self.persist_snapshot()?;
        }
        Ok(removed)
    }

    /// Returns the cumulative epoch for all catalog changes.
    pub fn global_epoch(&self) -> u64 {
        self.epochs.global.load(Ordering::SeqCst)
    }

    /// Returns the epoch for node class changes.
    pub fn node_class_epoch(&self) -> u64 {
        self.epochs.node_classes.load(Ordering::SeqCst)
    }

    /// Returns the epoch for edge class changes.
    pub fn edge_class_epoch(&self) -> u64 {
        self.epochs.edge_classes.load(Ordering::SeqCst)
    }

    /// Returns the epoch for grant changes.
    pub fn grants_epoch(&self) -> u64 {
        self.epochs.grants.load(Ordering::SeqCst)
    }

    /// Obtain a snapshot handle that provides cached lookups.
    pub fn cache_handle(self: &Arc<Self>) -> CatalogCache {
        CatalogCache::new(self.clone())
    }

    /// Fetches a node class entry by identifier.
    pub fn node_class_by_id(&self, id: NodeClassId) -> CatalogResult<Option<Arc<NodeClassEntry>>> {
        self.with_state_read("node_class_by_id", |state| {
            Ok(state.node_classes_by_id.get(&id).cloned())
        })
    }

    /// Fetches a node class entry by schema/name.
    pub fn node_class_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> CatalogResult<Option<Arc<NodeClassEntry>>> {
        let key = (schema_id, name.to_owned());
        self.with_state_read("node_class_by_name", |state| {
            Ok(state
                .node_classes_by_name
                .get(&key)
                .and_then(|id| state.node_classes_by_id.get(id).cloned()))
        })
    }

    /// Fetches an edge class entry by schema/name.
    pub fn edge_class_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> CatalogResult<Option<Arc<EdgeClassEntry>>> {
        let key = (schema_id, name.to_owned());
        self.with_state_read("edge_class_by_name", |state| {
            Ok(state
                .edge_classes_by_name
                .get(&key)
                .and_then(|id| state.edge_classes_by_id.get(id).cloned()))
        })
    }

    /// Looks up all grants for the given role identifier.
    pub fn grants_for_role(&self, role: RoleId) -> CatalogResult<Vec<GrantEntry>> {
        self.with_state_read("grants_for_role", |state| {
            Ok(state
                .grants
                .iter()
                .filter(|grant| grant.grantee == role)
                .cloned()
                .collect())
        })
    }

    /// Returns all ancestor role identifiers for a given role, including itself.
    pub fn role_hierarchy(&self, role: RoleId) -> CatalogResult<HashSet<RoleId>> {
        self.with_state_read("role_hierarchy", |state| {
            let mut visited = HashSet::new();
            let mut stack = vec![role];
            while let Some(current) = stack.pop() {
                if !visited.insert(current) {
                    continue;
                }
                if let Some(entry) = state.roles_by_id.get(&current) {
                    for parent in &entry.inherits {
                        stack.push(*parent);
                    }
                }
            }
            Ok(visited)
        })
    }

    pub fn list_roles(&self) -> CatalogResult<Vec<RoleEntry>> {
        self.with_state_read("list_roles", |state| {
            Ok(state
                .roles_by_id
                .values()
                .map(|entry| (**entry).clone())
                .collect())
        })
    }

    pub fn list_users(&self) -> CatalogResult<Vec<UserEntry>> {
        self.with_state_read("list_users", |state| {
            Ok(state
                .users_by_id
                .values()
                .map(|entry| (**entry).clone())
                .collect())
        })
    }

    pub fn list_node_classes(&self) -> CatalogResult<Vec<NodeClassEntry>> {
        self.with_state_read("list_node_classes", |state| {
            Ok(state
                .node_classes_by_id
                .values()
                .map(|entry| (**entry).clone())
                .collect())
        })
    }

    pub fn list_edge_classes(&self) -> CatalogResult<Vec<EdgeClassEntry>> {
        self.with_state_read("list_edge_classes", |state| {
            Ok(state
                .edge_classes_by_id
                .values()
                .map(|entry| (**entry).clone())
                .collect())
        })
    }

    pub fn snapshot(&self) -> CatalogResult<CatalogSnapshot> {
        let state = self
            .state
            .read()
            .map_err(|_| CatalogError::LockPoisoned("snapshot"))?;

        let mut schemas: Vec<_> = state
            .schemas_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        schemas.sort_by_key(|entry| entry.id);

        let mut roles: Vec<_> = state
            .roles_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        roles.sort_by_key(|entry| entry.id);

        let mut users: Vec<_> = state
            .users_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        users.sort_by_key(|entry| entry.id);

        let mut node_classes: Vec<_> = state
            .node_classes_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        node_classes.sort_by_key(|entry| entry.id);

        let mut edge_classes: Vec<_> = state
            .edge_classes_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        edge_classes.sort_by_key(|entry| entry.id);

        let mut indexes: Vec<_> = state
            .indexes_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        indexes.sort_by_key(|entry| entry.id);

        let mut processes: Vec<_> = state
            .processes_by_id
            .values()
            .map(|entry| (**entry).clone())
            .collect();
        processes.sort_by_key(|entry| entry.id);

        Ok(CatalogSnapshot {
            system_role: self.system_role,
            public_role: self.public_role,
            system_schema: self.system_schema,
            default_schema: self.default_schema,
            epochs: self.epochs.snapshot(),
            schemas,
            roles,
            users,
            node_classes,
            edge_classes,
            indexes,
            grants: state.grants.clone(),
            processes,
        })
    }

    fn persist_snapshot(&self) -> CatalogResult<()> {
        if let Some(hook) = &self.persistence {
            let snapshot = self.snapshot()?;
            hook.save(&snapshot)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub system_role: RoleId,
    pub public_role: RoleId,
    pub system_schema: SchemaId,
    pub default_schema: SchemaId,
    pub epochs: EpochSnapshot,
    pub schemas: Vec<SchemaEntry>,
    pub roles: Vec<RoleEntry>,
    pub users: Vec<UserEntry>,
    pub node_classes: Vec<NodeClassEntry>,
    pub edge_classes: Vec<EdgeClassEntry>,
    pub indexes: Vec<IndexEntry>,
    pub grants: Vec<GrantEntry>,
    pub processes: Vec<ProcessEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSnapshot {
    pub global: u64,
    pub schemas: u64,
    pub roles: u64,
    pub users: u64,
    pub node_classes: u64,
    pub edge_classes: u64,
    pub indexes: u64,
    pub grants: u64,
    pub processes: u64,
}

/// Read-through cache that amortises catalog reads while honouring epoch updates.
pub struct CatalogCache {
    catalog: Arc<SystemCatalog>,
    node_class_cache: RwLock<HashMap<(SchemaId, String), Cached<NodeClassEntry>>>,
    edge_class_cache: RwLock<HashMap<(SchemaId, String), Cached<EdgeClassEntry>>>,
    grants_cache: RwLock<HashMap<RoleId, Cached<Vec<GrantEntry>>>>,
}

#[derive(Clone)]
struct Cached<T> {
    epoch: u64,
    value: Arc<T>,
}

impl CatalogCache {
    fn new(catalog: Arc<SystemCatalog>) -> Self {
        Self {
            catalog,
            node_class_cache: RwLock::new(HashMap::new()),
            edge_class_cache: RwLock::new(HashMap::new()),
            grants_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the backing catalog instance.
    pub fn catalog(&self) -> &SystemCatalog {
        &self.catalog
    }

    /// Fetches a node class, refreshing the cache when the epoch moves forward.
    pub fn node_class(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> CatalogResult<Option<Arc<NodeClassEntry>>> {
        let epoch = self.catalog.node_class_epoch();
        let key = (schema_id, name.to_owned());

        if let Some(entry) = self
            .node_class_cache
            .read()
            .ok()
            .and_then(|guard| guard.get(&key).cloned())
        {
            if entry.epoch == epoch {
                return Ok(Some(entry.value));
            }
        }

        let mut guard = self
            .node_class_cache
            .write()
            .map_err(|_| CatalogError::LockPoisoned("node_class_cache"))?;
        guard.retain(|_, cached| cached.epoch == epoch);
        let value = self
            .catalog
            .node_class_by_name(schema_id, name)?
            .map(|entry| {
                guard.insert(
                    key.clone(),
                    Cached {
                        epoch,
                        value: entry.clone(),
                    },
                );
                entry
            });
        Ok(value)
    }

    /// Fetches an edge class with epoch-aware caching.
    pub fn edge_class(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> CatalogResult<Option<Arc<EdgeClassEntry>>> {
        let epoch = self.catalog.edge_class_epoch();
        let key = (schema_id, name.to_owned());

        if let Some(entry) = self
            .edge_class_cache
            .read()
            .ok()
            .and_then(|guard| guard.get(&key).cloned())
        {
            if entry.epoch == epoch {
                return Ok(Some(entry.value));
            }
        }

        let mut guard = self
            .edge_class_cache
            .write()
            .map_err(|_| CatalogError::LockPoisoned("edge_class_cache"))?;
        guard.retain(|_, cached| cached.epoch == epoch);
        let value = self
            .catalog
            .edge_class_by_name(schema_id, name)?
            .map(|entry| {
                guard.insert(
                    key.clone(),
                    Cached {
                        epoch,
                        value: entry.clone(),
                    },
                );
                entry
            });
        Ok(value)
    }

    /// Retrieves privilege grants for a role taking role hierarchy into account.
    pub fn privileges_for_role(&self, role: RoleId) -> CatalogResult<Privilege> {
        let epoch = self.catalog.grants_epoch();
        let mut aggregate = Privilege::empty();
        for ancestor in self.catalog.role_hierarchy(role)? {
            let grants = self.cached_grants_for_role(ancestor, epoch)?;
            aggregate |= self.merge_privileges(grants.as_ref());
        }
        Ok(aggregate)
    }

    fn cached_grants_for_role(
        &self,
        role: RoleId,
        epoch: u64,
    ) -> CatalogResult<Arc<Vec<GrantEntry>>> {
        if let Some(entry) = self
            .grants_cache
            .read()
            .ok()
            .and_then(|guard| guard.get(&role).cloned())
        {
            if entry.epoch == epoch {
                return Ok(entry.value);
            }
        }

        let mut guard = self
            .grants_cache
            .write()
            .map_err(|_| CatalogError::LockPoisoned("grants_cache"))?;
        guard.retain(|_, cached| cached.epoch == epoch);

        let grants: Vec<GrantEntry> = self.catalog.grants_for_role(role)?;
        let arc = Arc::new(grants);
        guard.insert(
            role,
            Cached {
                epoch,
                value: arc.clone(),
            },
        );
        Ok(arc)
    }

    fn merge_privileges(&self, grants: &[GrantEntry]) -> Privilege {
        grants
            .iter()
            .fold(Privilege::empty(), |acc, grant| acc | grant.privileges)
    }
}

/// Helper representing a cached view into the process chart allowing stale
/// detection via elapsed time.
pub struct ProcessWatch<'a> {
    catalog: &'a SystemCatalog,
    timeout: Duration,
}

impl<'a> ProcessWatch<'a> {
    pub fn new(catalog: &'a SystemCatalog, timeout: Duration) -> Self {
        Self { catalog, timeout }
    }

    /// Returns active process identifiers whose heartbeat is older than the
    /// configured timeout.
    pub fn stale_processes(&self) -> CatalogResult<Vec<ProcessId>> {
        self.catalog.with_state_read("stale_processes", |state| {
            let deadline = SystemTime::now()
                .checked_sub(self.timeout)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            Ok(state
                .processes_by_id
                .iter()
                .filter_map(|(id, entry)| {
                    if entry.last_heartbeat < deadline {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect())
        })
    }
}
