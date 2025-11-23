use std::convert::TryInto;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use bincode::{deserialize, serialize};
use memmap2::Mmap;

use common::edge::{Edge, EdgeId};
use common::node::{Node, NodeId};

use super::{StorageBackend, StorageError, StorageOp, StorageResult};

const HEADER_MAGIC: &[u8; 8] = b"GDBMNLTH";
const HEADER_VERSION: u32 = 1;
const FILE_HEADER_SIZE: u64 = 64;
const RECORD_HEADER_SIZE: usize = 8;

#[derive(Clone, Copy)]
#[repr(u8)]
enum RecordKind {
    NodeUpsert = 1,
    NodeDelete = 2,
    EdgeUpsert = 3,
    EdgeDelete = 4,
}

impl RecordKind {
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::NodeUpsert),
            2 => Some(Self::NodeDelete),
            3 => Some(Self::EdgeUpsert),
            4 => Some(Self::EdgeDelete),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct RecordPointer {
    offset: u64,
    length: u32,
}

pub struct MonolithStorage {
    file: Mutex<File>,
    mmap: Mutex<Mmap>,
    node_index: Mutex<HashMap<NodeId, RecordPointer>>,
    edge_index: Mutex<HashMap<EdgeId, RecordPointer>>,
}

use std::collections::HashMap;

impl MonolithStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|source| StorageError::Io {
                op: StorageOp::Cache("create monolith dir"),
                source,
            })?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())
            .map_err(|source| StorageError::Io {
                op: StorageOp::Cache("open monolith file"),
                source,
            })?;

        let metadata = file.metadata().map_err(|source| StorageError::Io {
            op: StorageOp::Cache("monolith metadata"),
            source,
        })?;
        if metadata.len() < FILE_HEADER_SIZE {
            file.set_len(FILE_HEADER_SIZE)
                .map_err(|source| StorageError::Io {
                    op: StorageOp::Cache("init monolith size"),
                    source,
                })?;
            file.seek(SeekFrom::Start(0))
                .map_err(|source| StorageError::Io {
                    op: StorageOp::Cache("seek header"),
                    source,
                })?;
            let header = FileHeader::new();
            file.write_all(&header.to_bytes())
                .map_err(|source| StorageError::Io {
                    op: StorageOp::Cache("write header"),
                    source,
                })?;
            file.flush().map_err(|source| StorageError::Io {
                op: StorageOp::Cache("flush header"),
                source,
            })?;
        } else {
            validate_header(&mut file)?;
        }

        let mmap = unsafe {
            Mmap::map(&file).map_err(|source| StorageError::Io {
                op: StorageOp::Cache("map monolith"),
                source,
            })?
        };

        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        parse_records(&mmap, &mut node_index, &mut edge_index)?;

        Ok(Self {
            file: Mutex::new(file),
            mmap: Mutex::new(mmap),
            node_index: Mutex::new(node_index),
            edge_index: Mutex::new(edge_index),
        })
    }

    fn append_record(&self, kind: RecordKind, payload: &[u8]) -> StorageResult<u64> {
        let mut file_guard = self.lock_file()?;
        let end = file_guard
            .seek(SeekFrom::End(0))
            .map_err(|source| StorageError::Io {
                op: StorageOp::Cache("seek end"),
                source,
            })?;
        let header = RecordHeader::new(kind, payload.len() as u32);
        file_guard
            .write_all(&header.to_bytes())
            .map_err(|source| StorageError::Io {
                op: StorageOp::Cache("write record header"),
                source,
            })?;
        file_guard
            .write_all(payload)
            .map_err(|source| StorageError::Io {
                op: StorageOp::Cache("write record payload"),
                source,
            })?;
        file_guard.flush().map_err(|source| StorageError::Io {
            op: StorageOp::Cache("flush append"),
            source,
        })?;
        drop(file_guard);
        self.remap()?;
        Ok(end + RECORD_HEADER_SIZE as u64)
    }

    fn remap(&self) -> StorageResult<()> {
        let file_guard = self.lock_file()?;
        let mmap = unsafe {
            Mmap::map(&*file_guard).map_err(|source| StorageError::Io {
                op: StorageOp::Cache("remap monolith"),
                source,
            })?
        };
        drop(file_guard);
        *self.mmap.lock().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache("remap"),
            lock: "mmap",
        })? = mmap;
        Ok(())
    }

    fn lock_file(&self) -> StorageResult<MutexGuard<'_, File>> {
        self.file.lock().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache("monolith file"),
            lock: "file",
        })
    }

    fn mmap_bytes(&self, pointer: &RecordPointer) -> StorageResult<Vec<u8>> {
        let map = self.mmap.lock().map_err(|_| StorageError::LockPoisoned {
            op: StorageOp::Cache("monolith read"),
            lock: "mmap",
        })?;
        let start = pointer.offset as usize;
        let end = start + pointer.length as usize;
        if end > map.len() {
            return Err(StorageError::Io {
                op: StorageOp::Cache("monolith bounds"),
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "record out of bounds",
                ),
            });
        }
        Ok(map[start..end].to_vec())
    }

    pub fn node_ids(&self) -> StorageResult<Vec<NodeId>> {
        let index = self
            .node_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::Cache("node ids"),
                lock: "node_index",
            })?;
        Ok(index.keys().copied().collect())
    }

    pub fn edge_ids(&self) -> StorageResult<Vec<EdgeId>> {
        let index = self
            .edge_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::Cache("edge ids"),
                lock: "edge_index",
            })?;
        Ok(index.keys().copied().collect())
    }
}

impl StorageBackend for MonolithStorage {
    fn load_node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        let pointer = {
            let index = self
                .node_index
                .lock()
                .map_err(|_| StorageError::LockPoisoned {
                    op: StorageOp::LoadNode(id),
                    lock: "node_index",
                })?;
            index.get(&id).copied()
        };
        let Some(pointer) = pointer else {
            return Ok(None);
        };
        let bytes = self.mmap_bytes(&pointer)?;
        let node: Node = deserialize(&bytes).map_err(|source| StorageError::Binary {
            op: StorageOp::LoadNode(id),
            source,
        })?;
        Ok(Some(node))
    }

    fn store_node(&self, node: &Node) -> StorageResult<()> {
        let payload = serialize(node).map_err(|source| StorageError::Binary {
            op: StorageOp::StoreNode(node.id()),
            source,
        })?;
        let offset = self.append_record(RecordKind::NodeUpsert, &payload)?;
        let pointer = RecordPointer {
            offset,
            length: payload.len() as u32,
        };
        let mut index = self
            .node_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::StoreNode(node.id()),
                lock: "node_index",
            })?;
        index.insert(node.id(), pointer);
        Ok(())
    }

    fn delete_node(&self, id: NodeId) -> StorageResult<()> {
        let payload = id.as_u128().to_be_bytes();
        self.append_record(RecordKind::NodeDelete, &payload)?;
        let mut index = self
            .node_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::DeleteNode(id),
                lock: "node_index",
            })?;
        index.remove(&id);
        Ok(())
    }

    fn load_edge(&self, id: EdgeId) -> StorageResult<Option<Edge>> {
        let pointer = {
            let index = self
                .edge_index
                .lock()
                .map_err(|_| StorageError::LockPoisoned {
                    op: StorageOp::LoadEdge(id),
                    lock: "edge_index",
                })?;
            index.get(&id).copied()
        };
        let Some(pointer) = pointer else {
            return Ok(None);
        };
        let bytes = self.mmap_bytes(&pointer)?;
        let edge: Edge = deserialize(&bytes).map_err(|source| StorageError::Binary {
            op: StorageOp::LoadEdge(id),
            source,
        })?;
        Ok(Some(edge))
    }

    fn store_edge(&self, edge: &Edge) -> StorageResult<()> {
        let payload = serialize(edge).map_err(|source| StorageError::Binary {
            op: StorageOp::StoreEdge(edge.id()),
            source,
        })?;
        let offset = self.append_record(RecordKind::EdgeUpsert, &payload)?;
        let pointer = RecordPointer {
            offset,
            length: payload.len() as u32,
        };
        let mut index = self
            .edge_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::StoreEdge(edge.id()),
                lock: "edge_index",
            })?;
        index.insert(edge.id(), pointer);
        Ok(())
    }

    fn delete_edge(&self, id: EdgeId) -> StorageResult<()> {
        let payload = id.as_u128().to_be_bytes();
        self.append_record(RecordKind::EdgeDelete, &payload)?;
        let mut index = self
            .edge_index
            .lock()
            .map_err(|_| StorageError::LockPoisoned {
                op: StorageOp::DeleteEdge(id),
                lock: "edge_index",
            })?;
        index.remove(&id);
        Ok(())
    }
}

fn parse_records(
    mmap: &Mmap,
    node_index: &mut HashMap<NodeId, RecordPointer>,
    edge_index: &mut HashMap<EdgeId, RecordPointer>,
) -> StorageResult<()> {
    let mut offset = FILE_HEADER_SIZE as usize;
    while offset + RECORD_HEADER_SIZE <= mmap.len() {
        let header = RecordHeader::from_bytes(&mmap[offset..offset + RECORD_HEADER_SIZE]);
        offset += RECORD_HEADER_SIZE;
        let end = offset + header.length as usize;
        if end > mmap.len() {
            break;
        }
        let payload = &mmap[offset..end];
        let pointer = RecordPointer {
            offset: offset as u64,
            length: header.length,
        };
        match header.kind {
            Some(RecordKind::NodeUpsert) => {
                if let Ok(node) = deserialize::<Node>(payload) {
                    node_index.insert(node.id(), pointer);
                }
            }
            Some(RecordKind::NodeDelete) => {
                if payload.len() == 16 {
                    if let Ok(bytes) = payload.try_into() {
                        let id = NodeId::from_u128(u128::from_be_bytes(bytes));
                        node_index.remove(&id);
                    }
                }
            }
            Some(RecordKind::EdgeUpsert) => {
                if let Ok(edge) = deserialize::<Edge>(payload) {
                    edge_index.insert(edge.id(), pointer);
                }
            }
            Some(RecordKind::EdgeDelete) => {
                if payload.len() == 16 {
                    if let Ok(bytes) = payload.try_into() {
                        let id = EdgeId::from_u128(u128::from_be_bytes(bytes));
                        edge_index.remove(&id);
                    }
                }
            }
            None => break,
        }
        offset = end;
    }
    Ok(())
}

struct RecordHeader {
    kind: Option<RecordKind>,
    length: u32,
}

impl RecordHeader {
    fn new(kind: RecordKind, length: u32) -> Self {
        Self {
            kind: Some(kind),
            length,
        }
    }

    fn to_bytes(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        buf[0] = self.kind.map(|k| k as u8).unwrap_or(0);
        buf[4..8].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let kind = RecordKind::from_u8(bytes[0]);
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&bytes[4..8]);
        let length = u32::from_be_bytes(len_bytes);
        Self { kind, length }
    }
}

struct FileHeader {
    magic: [u8; 8],
    version: u32,
}

impl FileHeader {
    fn new() -> Self {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(HEADER_MAGIC);
        Self {
            magic,
            version: HEADER_VERSION,
        }
    }

    fn to_bytes(&self) -> [u8; FILE_HEADER_SIZE as usize] {
        let mut buf = [0u8; FILE_HEADER_SIZE as usize];
        buf[..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_be_bytes());
        buf
    }
}

fn validate_header(file: &mut File) -> StorageResult<()> {
    let mut buf = [0u8; FILE_HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))
        .map_err(|source| StorageError::Io {
            op: StorageOp::Cache("seek header"),
            source,
        })?;
    file.read_exact(&mut buf)
        .map_err(|source| StorageError::Io {
            op: StorageOp::Cache("read header"),
            source,
        })?;
    if &buf[..8] != HEADER_MAGIC {
        return Err(StorageError::Io {
            op: StorageOp::Cache("validate header"),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid monolith header"),
        });
    }
    let mut version_bytes = [0u8; 4];
    version_bytes.copy_from_slice(&buf[8..12]);
    let version = u32::from_be_bytes(version_bytes);
    if version != HEADER_VERSION {
        return Err(StorageError::Io {
            op: StorageOp::Cache("validate header"),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported monolith version",
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn make_node(id: u128) -> Node {
        Node::new(NodeId::from_u128(id), vec![], HashMap::new())
    }

    fn make_edge(id: u128, source: u128, target: u128) -> Edge {
        Edge::new(
            EdgeId::from_u128(id),
            NodeId::from_u128(source),
            NodeId::from_u128(target),
            HashMap::new(),
        )
    }

    #[test]
    fn persists_nodes_and_edges() {
        let tmp = NamedTempFile::new().expect("temp file");
        let backend = MonolithStorage::new(tmp.path()).expect("backend");

        let node = make_node(1);
        backend.store_node(&node).unwrap();
        let loaded = backend.load_node(node.id()).unwrap().unwrap();
        assert_eq!(loaded.id(), node.id());

        let edge = make_edge(2, 1, 1);
        backend.store_edge(&edge).unwrap();
        let loaded_edge = backend.load_edge(edge.id()).unwrap().unwrap();
        assert_eq!(loaded_edge.id(), edge.id());

        backend.delete_edge(edge.id()).unwrap();
        assert!(backend.load_edge(edge.id()).unwrap().is_none());

        backend.delete_node(node.id()).unwrap();
        assert!(backend.load_node(node.id()).unwrap().is_none());
    }

    #[test]
    fn survives_reopen() {
        let tmp = NamedTempFile::new().expect("temp file");
        {
            let backend = MonolithStorage::new(tmp.path()).expect("backend");
            backend.store_node(&make_node(10)).unwrap();
        }
        let backend = MonolithStorage::new(tmp.path()).expect("reopen backend");
        assert!(backend.load_node(NodeId::from_u128(10)).unwrap().is_some());
    }
}
