use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use common::attr::AttributeContainer;
use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use crate::db::catalog::{
    CatalogCache, CatalogError, CatalogObject, EdgeClassId, NodeClassId, Privilege, ProcessId,
    ProcessWatch, RoleId, SchemaId, SystemCatalog, UserId,
};
use crate::storage::{StorageBackend, StorageError, StorageOp, StorageResult};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("role {role} lacks {privilege} on {object}")]
    Unauthorized {
        role: RoleId,
        privilege: Privilege,
        object: CatalogObject,
    },
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;

struct Cache {
    nodes: HashMap<NodeId, Arc<Node>>,
    edges: HashMap<EdgeId, Arc<Edge>>,
    adjacency: HashMap<NodeId, Vec<EdgeId>>,
    node_order: VecDeque<NodeId>,
    edge_order: VecDeque<EdgeId>,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            adjacency: HashMap::new(),
            node_order: VecDeque::new(),
            edge_order: VecDeque::new(),
        }
    }
}

struct EndpointClassInfo {
    class_name: String,
    class_id: Option<NodeClassId>,
}

pub struct Database<B: StorageBackend> {
    storage: B,
    catalog: Arc<SystemCatalog>,
    catalog_cache: CatalogCache,
    default_schema: SchemaId,
    default_owner_role: RoleId,
    cache: RwLock<Cache>,
    node_capacity: Option<usize>,
    edge_capacity: Option<usize>,
}

impl<B: StorageBackend> Database<B> {
    const POISONED_READ: &'static str = "cache read";
    const POISONED_WRITE: &'static str = "cache write";
    const DEFAULT_NODE_CLASS: &'static str = "__default__";
    const DEFAULT_EDGE_CLASS: &'static str = "__link__";

    pub fn new(storage: B) -> Self {
        Self::with_capacity(storage, None, None)
    }

    pub fn with_capacity(
        storage: B,
        node_capacity: Option<usize>,
        edge_capacity: Option<usize>,
    ) -> Self {
        Self::with_catalog_and_capacity(
            storage,
            SystemCatalog::bootstrap(),
            node_capacity,
            edge_capacity,
        )
    }

    pub fn with_catalog(storage: B, catalog: Arc<SystemCatalog>) -> Self {
        Self::with_catalog_and_capacity(storage, catalog, None, None)
    }

    pub fn with_catalog_and_capacity(
        storage: B,
        catalog: Arc<SystemCatalog>,
        node_capacity: Option<usize>,
        edge_capacity: Option<usize>,
    ) -> Self {
        let catalog_cache = catalog.cache_handle();
        let default_schema = catalog.default_schema_id();
        let default_owner_role = catalog.system_role_id();

        Self {
            storage,
            catalog,
            catalog_cache,
            default_schema,
            default_owner_role,
            cache: RwLock::new(Cache::default()),
            node_capacity,
            edge_capacity,
        }
    }

    pub fn storage(&self) -> &B {
        &self.storage
    }

    pub fn catalog(&self) -> Arc<SystemCatalog> {
        self.catalog.clone()
    }

    pub fn catalog_cache(&self) -> &CatalogCache {
        &self.catalog_cache
    }

    pub fn process_watch(&self, timeout: Duration) -> ProcessWatch<'_> {
        ProcessWatch::new(self.catalog.as_ref(), timeout)
    }

    pub fn register_process(
        &self,
        user: UserId,
        active_role: RoleId,
        description: &str,
    ) -> DatabaseResult<ProcessId> {
        Ok(self
            .catalog
            .register_process(user, active_role, description)?)
    }

    pub fn heartbeat_process(&self, process_id: ProcessId) -> DatabaseResult<bool> {
        Ok(self.catalog.heartbeat_process(process_id)?)
    }

    pub fn complete_process(&self, process_id: ProcessId) -> DatabaseResult<bool> {
        Ok(self.catalog.complete_process(process_id)?)
    }

    pub fn insert_node(&self, node: Node) -> DatabaseResult<Arc<Node>> {
        let labels = Self::labels_or_default(node.labels());
        let properties = Self::node_property_keys(&node);
        let class_ids = self.ensure_node_classes_internal(&labels, &properties)?;
        self.ensure_privileges_for_classes(self.default_owner_role, &class_ids, Privilege::INSERT)?;

        self.storage.store_node(&node)?;
        Ok(self.cache_node(node)?)
    }

    pub fn insert_edge(&self, edge: Edge) -> DatabaseResult<Arc<Edge>> {
        let edge_class_id = self.ensure_edge_catalog(&edge)?;
        self.ensure_privilege(
            self.default_owner_role,
            CatalogObject::EdgeClass(edge_class_id),
            Privilege::INSERT,
        )?;

        self.storage.store_edge(&edge)?;
        Ok(self.cache_edge(edge)?)
    }

    fn ensure_node_classes_internal(
        &self,
        labels: &[String],
        properties: &[String],
    ) -> DatabaseResult<Vec<NodeClassId>> {
        let mut ids = Vec::with_capacity(labels.len().max(1));
        for label in labels {
            let id = self.catalog.ensure_node_class(
                self.default_schema,
                label,
                self.default_owner_role,
                properties,
            )?;
            ids.push(id);
        }
        Ok(ids)
    }

    fn ensure_privileges_for_classes(
        &self,
        role: RoleId,
        class_ids: &[NodeClassId],
        privilege: Privilege,
    ) -> DatabaseResult<()> {
        for class_id in class_ids {
            self.ensure_privilege(role, CatalogObject::NodeClass(*class_id), privilege)?;
        }
        Ok(())
    }

    fn ensure_privilege(
        &self,
        role: RoleId,
        object: CatalogObject,
        privilege: Privilege,
    ) -> DatabaseResult<()> {
        if role == self.default_owner_role {
            return Ok(());
        }

        let privileges = self.catalog_cache.privileges_for_role(role)?;
        if privileges.contains(privilege) {
            return Ok(());
        }

        Err(DatabaseError::Unauthorized {
            role,
            privilege,
            object,
        })
    }

    fn ensure_edge_catalog(&self, edge: &Edge) -> DatabaseResult<EdgeClassId> {
        let properties = Self::edge_property_keys(edge);
        let source = self.get_node(edge.source())?;
        let target = self.get_node(edge.target())?;
        let source_info = self.endpoint_class_info(source)?;
        let target_info = self.endpoint_class_info(target)?;

        let class_name = Self::edge_class_name(&source_info.class_name, &target_info.class_name);

        Ok(self.catalog.ensure_edge_class(
            self.default_schema,
            &class_name,
            self.default_owner_role,
            &properties,
            source_info.class_id,
            target_info.class_id,
        )?)
    }

    fn endpoint_class_info(&self, node: Option<Arc<Node>>) -> DatabaseResult<EndpointClassInfo> {
        let labels = if let Some(node_ref) = node.as_ref() {
            Self::labels_or_default(node_ref.labels())
        } else {
            vec![Self::DEFAULT_NODE_CLASS.to_string()]
        };

        let properties = if let Some(node_ref) = node.as_ref() {
            Self::node_property_keys(node_ref.as_ref())
        } else {
            Vec::new()
        };

        let primary_name = labels
            .first()
            .cloned()
            .unwrap_or_else(|| Self::DEFAULT_NODE_CLASS.to_string());

        let class_ids = self.ensure_node_classes_internal(&labels, &properties)?;
        let primary_id = class_ids.first().copied();

        Ok(EndpointClassInfo {
            class_name: primary_name,
            class_id: primary_id,
        })
    }

    fn labels_or_default(labels: &[String]) -> Vec<String> {
        if labels.is_empty() {
            vec![Self::DEFAULT_NODE_CLASS.to_string()]
        } else {
            labels.iter().cloned().collect()
        }
    }

    fn edge_class_name(source: &str, target: &str) -> String {
        if source == Self::DEFAULT_NODE_CLASS && target == Self::DEFAULT_NODE_CLASS {
            Self::DEFAULT_EDGE_CLASS.to_string()
        } else {
            format!("{}->{}", source, target)
        }
    }

    fn node_property_keys(node: &Node) -> Vec<String> {
        node.attributes().keys().cloned().collect()
    }

    fn edge_property_keys(edge: &Edge) -> Vec<String> {
        edge.attributes().keys().cloned().collect()
    }

    pub fn get_node(&self, id: NodeId) -> DatabaseResult<Option<Arc<Node>>> {
        if let Some(node) = self.lookup_node(id)? {
            self.touch_node(id)?;
            return Ok(Some(node));
        }

        let Some(node) = self.storage.load_node(id)? else {
            return Ok(None);
        };

        Ok(Some(self.cache_node(node)?))
    }

    pub fn get_edge(&self, id: EdgeId) -> DatabaseResult<Option<Arc<Edge>>> {
        if let Some(edge) = self.lookup_edge(id)? {
            self.touch_edge(id)?;
            return Ok(Some(edge));
        }

        let Some(edge) = self.storage.load_edge(id)? else {
            return Ok(None);
        };

        Ok(Some(self.cache_edge(edge)?))
    }

    pub fn remove_edge(&self, id: EdgeId) -> DatabaseResult<Option<Arc<Edge>>> {
        let edge = {
            let mut cache = self.cache_write_guard("remove edge")?;
            let edge = match cache.edges.remove(&id) {
                Some(edge) => edge,
                None => return Ok(None),
            };

            let source = edge.source();
            let target = edge.target();
            Self::unlink_edge(&mut cache.adjacency, source, id);
            Self::unlink_edge(&mut cache.adjacency, target, id);
            Self::remove_edge_from_order(&mut cache.edge_order, id);
            edge
        };

        self.storage.delete_edge(id)?;
        Ok(Some(edge))
    }

    pub fn remove_node(&self, id: NodeId) -> DatabaseResult<Option<Arc<Node>>> {
        let (node, incident_edges) = {
            let mut cache = self.cache_write_guard("remove node")?;
            let node = match cache.nodes.remove(&id) {
                Some(node) => node,
                None => return Ok(None),
            };

            Self::remove_node_from_order(&mut cache.node_order, id);

            let incident_edges = cache.adjacency.remove(&id).unwrap_or_default();
            for edge_id in &incident_edges {
                if let Some(edge) = cache.edges.remove(edge_id) {
                    let other = if edge.source() == id {
                        edge.target()
                    } else {
                        edge.source()
                    };
                    Self::unlink_edge(&mut cache.adjacency, other, *edge_id);
                    Self::remove_edge_from_order(&mut cache.edge_order, *edge_id);
                }
            }

            (node, incident_edges)
        };

        for edge_id in incident_edges {
            self.storage.delete_edge(edge_id)?;
        }
        self.storage.delete_node(id)?;

        Ok(Some(node))
    }

    pub fn contains_node(&self, id: NodeId) -> DatabaseResult<bool> {
        let cache = self.cache_read_guard("contains node")?;
        Ok(cache.nodes.contains_key(&id))
    }

    pub fn contains_edge(&self, id: EdgeId) -> DatabaseResult<bool> {
        let cache = self.cache_read_guard("contains edge")?;
        Ok(cache.edges.contains_key(&id))
    }

    pub fn evict_node(&self, id: NodeId) -> DatabaseResult<bool> {
        let mut cache = self.cache_write_guard("evict node")?;
        let existed = cache.nodes.remove(&id).is_some();
        if existed {
            Self::remove_node_from_order(&mut cache.node_order, id);
        }
        Ok(existed)
    }

    pub fn evict_edge(&self, id: EdgeId) -> DatabaseResult<bool> {
        let mut cache = self.cache_write_guard("evict edge")?;
        let existed = cache.edges.remove(&id).is_some();
        if existed {
            Self::remove_edge_from_order(&mut cache.edge_order, id);
        }
        Ok(existed)
    }

    pub fn node_ids(&self) -> DatabaseResult<Vec<NodeId>> {
        let cache = self.cache_read_guard("node ids")?;
        Ok(cache.nodes.keys().copied().collect())
    }

    pub fn edge_ids(&self) -> DatabaseResult<Vec<EdgeId>> {
        let cache = self.cache_read_guard("edge ids")?;
        Ok(cache.edges.keys().copied().collect())
    }

    pub fn edges_for_node(&self, node_id: NodeId) -> DatabaseResult<Vec<Arc<Edge>>> {
        let edge_ids = {
            let cache = self.cache_read_guard("edges for node")?;
            cache.adjacency.get(&node_id).cloned().unwrap_or_default()
        };

        let mut edges = Vec::with_capacity(edge_ids.len());
        for edge_id in edge_ids {
            if let Some(edge) = self.get_edge(edge_id)? {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    pub fn neighbor_ids(&self, node_id: NodeId) -> DatabaseResult<Vec<NodeId>> {
        let mut neighbors = HashSet::new();
        for edge in self.edges_for_node(node_id)? {
            if edge.source() == node_id {
                neighbors.insert(edge.target());
            } else {
                neighbors.insert(edge.source());
            }
        }
        let mut result: Vec<NodeId> = neighbors.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn lookup_node(&self, id: NodeId) -> StorageResult<Option<Arc<Node>>> {
        let cache = self.cache_read_guard("lookup node")?;
        Ok(cache.nodes.get(&id).cloned())
    }

    fn lookup_edge(&self, id: EdgeId) -> StorageResult<Option<Arc<Edge>>> {
        let cache = self.cache_read_guard("lookup edge")?;
        Ok(cache.edges.get(&id).cloned())
    }

    fn cache_node(&self, node: Node) -> StorageResult<Arc<Node>> {
        let id = node.id();
        if let Some(existing) = self.lookup_node(id)? {
            self.touch_node(id)?;
            return Ok(existing);
        }

        let mut cache = self.cache_write_guard("cache node")?;
        if let Some(existing) = cache.nodes.get(&id) {
            let existing_arc = existing.clone();
            Self::move_node_to_back(&mut cache.node_order, id);
            return Ok(existing_arc);
        }

        let node = Arc::new(node);
        cache.nodes.insert(id, node.clone());
        cache.adjacency.entry(id).or_insert_with(Vec::new);
        Self::move_node_to_back(&mut cache.node_order, id);
        self.enforce_node_capacity(&mut cache);
        Ok(node)
    }

    fn cache_edge(&self, edge: Edge) -> StorageResult<Arc<Edge>> {
        let id = edge.id();
        if let Some(existing) = self.lookup_edge(id)? {
            self.touch_edge(id)?;
            return Ok(existing);
        }

        let mut cache = self.cache_write_guard("cache edge")?;
        if let Some(existing) = cache.edges.get(&id) {
            let existing_arc = existing.clone();
            Self::move_edge_to_back(&mut cache.edge_order, id);
            return Ok(existing_arc);
        }

        let source = edge.source();
        let target = edge.target();
        let edge = Arc::new(edge);
        cache.edges.insert(id, edge.clone());
        Self::link_edge(&mut cache.adjacency, source, id);
        Self::link_edge(&mut cache.adjacency, target, id);
        Self::move_edge_to_back(&mut cache.edge_order, id);
        self.enforce_edge_capacity(&mut cache);
        Ok(edge)
    }

    fn link_edge(adjacency: &mut HashMap<NodeId, Vec<EdgeId>>, node_id: NodeId, edge_id: EdgeId) {
        let entry = adjacency.entry(node_id).or_insert_with(Vec::new);
        if !entry.contains(&edge_id) {
            entry.push(edge_id);
        }
    }

    fn unlink_edge(adjacency: &mut HashMap<NodeId, Vec<EdgeId>>, node_id: NodeId, edge_id: EdgeId) {
        if let Some(entry) = adjacency.get_mut(&node_id) {
            entry.retain(|candidate| *candidate != edge_id);
            if entry.is_empty() {
                adjacency.remove(&node_id);
            }
        }
    }

    fn touch_node(&self, id: NodeId) -> StorageResult<()> {
        let mut cache = self.cache_write_guard("touch node")?;
        Self::move_node_to_back(&mut cache.node_order, id);
        Ok(())
    }

    fn touch_edge(&self, id: EdgeId) -> StorageResult<()> {
        let mut cache = self.cache_write_guard("touch edge")?;
        Self::move_edge_to_back(&mut cache.edge_order, id);
        Ok(())
    }

    fn move_node_to_back(order: &mut VecDeque<NodeId>, id: NodeId) {
        if let Some(pos) = order.iter().position(|&item| item == id) {
            order.remove(pos);
        }
        order.push_back(id);
    }

    fn move_edge_to_back(order: &mut VecDeque<EdgeId>, id: EdgeId) {
        if let Some(pos) = order.iter().position(|&item| item == id) {
            order.remove(pos);
        }
        order.push_back(id);
    }

    fn remove_node_from_order(order: &mut VecDeque<NodeId>, id: NodeId) {
        order.retain(|&candidate| candidate != id);
    }

    fn remove_edge_from_order(order: &mut VecDeque<EdgeId>, id: EdgeId) {
        order.retain(|&candidate| candidate != id);
    }

    fn enforce_node_capacity(&self, cache: &mut Cache) {
        if let Some(capacity) = self.node_capacity {
            while cache.nodes.len() > capacity {
                if let Some(evicted_id) = cache.node_order.pop_front() {
                    cache.nodes.remove(&evicted_id);
                } else {
                    break;
                }
            }
        }
    }

    fn enforce_edge_capacity(&self, cache: &mut Cache) {
        if let Some(capacity) = self.edge_capacity {
            while cache.edges.len() > capacity {
                if let Some(evicted_id) = cache.edge_order.pop_front() {
                    cache.edges.remove(&evicted_id);
                } else {
                    break;
                }
            }
        }
    }

    fn cache_read_guard(&self, label: &'static str) -> StorageResult<RwLockReadGuard<'_, Cache>> {
        self.cache.read().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache(label),
            lock: Self::POISONED_READ,
        })
    }

    fn cache_write_guard(&self, label: &'static str) -> StorageResult<RwLockWriteGuard<'_, Cache>> {
        self.cache.write().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache(label),
            lock: Self::POISONED_WRITE,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    use crate::storage::{InMemoryBackend, StorageBackend};

    fn make_node(id: u128, labels: &[&str]) -> Node {
        let node_id = NodeId::from_u128(id);
        let label_vec = labels.iter().map(|label| label.to_string()).collect();
        Node::new(node_id, label_vec, HashMap::new())
    }

    fn make_edge(id: u128, source: u128, target: u128) -> Edge {
        let edge_id = EdgeId::from_u128(id);
        let source_id = NodeId::from_u128(source);
        let target_id = NodeId::from_u128(target);
        Edge::new(edge_id, source_id, target_id, HashMap::new())
    }

    #[derive(Default)]
    struct CountingBackend {
        inner: InMemoryBackend,
        node_loads: AtomicUsize,
        edge_loads: AtomicUsize,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self::default()
        }

        fn node_loads(&self) -> usize {
            self.node_loads.load(Ordering::SeqCst)
        }

        fn edge_loads(&self) -> usize {
            self.edge_loads.load(Ordering::SeqCst)
        }
    }

    impl StorageBackend for CountingBackend {
        fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
            self.node_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_node(id)
        }

        fn store_node(&self, node: &Node) -> StorageResult<()> {
            self.inner.store_node(node)
        }

        fn delete_node(&self, id: NodeId) -> StorageResult<()> {
            self.inner.delete_node(id)
        }

        fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
            self.edge_loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_edge(id)
        }

        fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
            self.inner.store_edge(edge)
        }

        fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
            self.inner.delete_edge(id)
        }
    }

    #[test]
    fn get_node_lazy_loads_from_storage() {
        let node = make_node(1, &["Person"]);
        let node_id = node.id();
        let backend = InMemoryBackend::new();
        backend.store_node(&node).unwrap();

        let db = Database::new(backend);

        let loaded = db.get_node(node_id).unwrap().expect("node should load");
        assert_eq!(loaded.id(), node_id);
        assert!(db.contains_node(node_id).unwrap());
    }

    #[test]
    fn get_edge_populates_adjacency() {
        let backend = InMemoryBackend::new();
        backend.store_edge(&make_edge(1, 2, 3)).unwrap();

        let db = Database::new(backend);
        let edge_id = EdgeId::from_u128(1);
        let source = NodeId::from_u128(2);
        let target = NodeId::from_u128(3);

        let loaded_edge = db.get_edge(edge_id).unwrap().expect("edge should load");
        assert_eq!(loaded_edge.id(), edge_id);

        let source_edges = db.edges_for_node(source).unwrap();
        assert_eq!(source_edges.len(), 1);
        assert_eq!(source_edges[0].id(), edge_id);

        let target_edges = db.edges_for_node(target).unwrap();
        assert_eq!(target_edges.len(), 1);
        assert_eq!(target_edges[0].id(), edge_id);

        let neighbors = db.neighbor_ids(source).unwrap();
        assert_eq!(neighbors, vec![target]);
    }

    #[test]
    fn insert_node_persists_to_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        let node = make_node(4, &["City"]);
        let node_id = node.id();

        db.insert_node(node.clone()).unwrap();

        assert!(db.contains_node(node_id).unwrap());
        let stored = db.storage().load_node(node_id).unwrap();
        assert_eq!(stored.unwrap().id(), node_id);
    }

    #[test]
    fn edges_for_node_returns_empty_when_absent() {
        let db = Database::new(InMemoryBackend::new());
        let edges = db.edges_for_node(NodeId::from_u128(99)).unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn remove_edge_updates_cache_and_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        db.insert_node(make_node(6, &["City"])).unwrap();
        db.insert_node(make_node(7, &["City"])).unwrap();
        let edge = make_edge(30, 6, 7);
        let edge_id = edge.id();
        db.insert_edge(edge).unwrap();

        let removed = db.remove_edge(edge_id).unwrap().expect("edge removed");
        assert_eq!(removed.id(), edge_id);
        assert!(!db.contains_edge(edge_id).unwrap());
        assert!(db.storage().load_edge(edge_id).unwrap().is_none());
        assert!(db.edges_for_node(NodeId::from_u128(6)).unwrap().is_empty());
        assert!(db.edges_for_node(NodeId::from_u128(7)).unwrap().is_empty());
    }

    #[test]
    fn remove_node_prunes_incident_edges_and_storage() {
        let backend = InMemoryBackend::new();
        let db = Database::new(backend);
        db.insert_node(make_node(8, &["Person"])).unwrap();
        db.insert_node(make_node(9, &["Person"])).unwrap();
        db.insert_edge(make_edge(31, 8, 9)).unwrap();

        let removed = db
            .remove_node(NodeId::from_u128(8))
            .unwrap()
            .expect("node removed");
        assert_eq!(removed.id(), NodeId::from_u128(8));
        assert!(!db.contains_node(NodeId::from_u128(8)).unwrap());
        assert!(!db.contains_edge(EdgeId::from_u128(31)).unwrap());
        assert!(db.edges_for_node(NodeId::from_u128(9)).unwrap().is_empty());
        assert!(
            db.storage()
                .load_node(NodeId::from_u128(8))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn evict_node_triggers_reload_from_storage() {
        let backend = CountingBackend::new();
        let db = Database::with_capacity(backend, None, None);
        let node = make_node(50, &["Person"]);
        let node_id = node.id();

        db.insert_node(node).unwrap();
        assert_eq!(db.storage().node_loads(), 0);

        db.evict_node(node_id).unwrap();
        assert!(!db.contains_node(node_id).unwrap());

        let reloaded = db.get_node(node_id).unwrap().expect("node reload");
        assert_eq!(reloaded.id(), node_id);
        assert_eq!(db.storage().node_loads(), 1);
    }

    #[test]
    fn evict_edge_triggers_reload_from_storage() {
        let backend = CountingBackend::new();
        let db = Database::with_capacity(backend, None, None);

        db.insert_node(make_node(60, &["Person"])).unwrap();
        db.insert_node(make_node(61, &["Person"])).unwrap();
        let edge = make_edge(70, 60, 61);
        let edge_id = edge.id();

        db.insert_edge(edge).unwrap();
        assert_eq!(db.storage().edge_loads(), 0);

        db.evict_edge(edge_id).unwrap();
        assert!(!db.contains_edge(edge_id).unwrap());

        let reloaded = db.get_edge(edge_id).unwrap().expect("edge reload");
        assert_eq!(reloaded.id(), edge_id);
        assert_eq!(db.storage().edge_loads(), 1);

        let edge_ids = db.edges_for_node(NodeId::from_u128(60)).unwrap();
        assert_eq!(edge_ids.len(), 1);
    }

    #[test]
    fn node_capacity_evicts_least_recently_used() {
        let backend = CountingBackend::new();
        let db = Database::with_capacity(backend, Some(1), None);

        db.insert_node(make_node(100, &["Person"])).unwrap();
        db.insert_node(make_node(101, &["Person"])).unwrap();

        assert!(!db.contains_node(NodeId::from_u128(100)).unwrap());

        let before = db.storage().node_loads();
        let reloaded = db
            .get_node(NodeId::from_u128(100))
            .unwrap()
            .expect("node reloaded");
        assert_eq!(reloaded.id(), NodeId::from_u128(100));
        assert!(db.storage().node_loads() > before);
    }

    #[test]
    fn edge_capacity_evicts_least_recently_used() {
        let backend = CountingBackend::new();
        let db = Database::with_capacity(backend, None, Some(1));

        db.insert_node(make_node(200, &["Person"])).unwrap();
        db.insert_node(make_node(201, &["Person"])).unwrap();
        db.insert_node(make_node(202, &["Person"])).unwrap();

        db.insert_edge(make_edge(210, 200, 201)).unwrap();
        db.insert_edge(make_edge(211, 200, 202)).unwrap();

        assert!(!db.contains_edge(EdgeId::from_u128(210)).unwrap());

        let before = db.storage().edge_loads();
        let edge = db
            .get_edge(EdgeId::from_u128(210))
            .unwrap()
            .expect("edge reload");
        assert_eq!(edge.id(), EdgeId::from_u128(210));
        assert!(db.storage().edge_loads() > before);
    }

    #[test]
    fn concurrent_reads_and_writes_do_not_panic() {
        let backend = InMemoryBackend::new();
        backend.store_node(&make_node(10, &["Person"])).unwrap();
        backend.store_edge(&make_edge(20, 10, 11)).unwrap();

        let db = Arc::new(Database::new(backend));
        let node_id = NodeId::from_u128(10);
        let edge_id = EdgeId::from_u128(20);

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for _ in 0..50 {
                        let _ = db.get_node(node_id).unwrap();
                        let _ = db.get_edge(edge_id).unwrap();
                    }
                })
            })
            .collect();

        let writer = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                db.insert_node(make_node(11, &["Person"])).unwrap();
                db.insert_edge(make_edge(21, 10, 11)).unwrap();
            })
        };

        for handle in readers {
            handle.join().expect("reader thread panicked");
        }
        writer.join().expect("writer thread panicked");

        assert!(db.contains_node(NodeId::from_u128(11)).unwrap());
        assert!(db.contains_edge(EdgeId::from_u128(21)).unwrap());
    }
}
