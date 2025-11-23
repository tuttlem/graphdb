# Dense Storage Backend Design

## Goals

* Single memory-mappable file that contains catalog metadata, nodes, edges, indexes, and free space bookkeeping.
* Support very large graphs (multi-GB) with predictable access patterns.
* Durable format versioned explicitly so migrations are possible.
* Practical performance without immediate transactional guarantees (append + periodic vacuum is acceptable for v1).

## File Layout Overview

```
+----------------------+---------------------------+
| FileHeader (4 KiB)   | Region Directory (4 KiB)   |
+----------------------+---------------------------+
| Free Space Bitmap(s) | Catalog Segment            |
+----------------------+---------------------------+
| Node Table           | Edge Table                 |
+----------------------+---------------------------+
| Property Heap        | Index Segment(s)           |
+----------------------+---------------------------+
```

### FileHeader

| Field                  | Size | Notes                                  |
|------------------------|------|----------------------------------------|
| magic "GDBDENSE"        | 8B   | identify format                         |
| version                | 4B   | bump when layout changes                |
| page_size              | 4B   | default 4096                            |
| region_dir_offset      | 8B   | byte offset to region directory         |
| checksum               | 8B   | CRC64 of header                         |
| reserved               | ... | future use                              |

### Region Directory

Fixed array of descriptors identifying where each logical segment lives. Each descriptor includes: id (e.g. catalog, nodes, edges, heap), start page, length (pages), and growth hint. Allows relocating segments during vacuum while still using a single memory map.

### Node/Edge Tables

* **Node record (64 bytes)**: id (128-bit UUID), label class id (64-bit), attribute pointer (heap offset), degree counts, flags, padding.
* **Edge record (64 bytes)**: id, source node slot, target node slot, label class id, property pointer, weight, flags.
* Records stored in fixed-size slots. Slot number equals array index so lookups are `O(1)` via offset = base + slot * record_size.
* Free slots tracked via bitmap + freelist; when deleting we mark slot free and optionally push into freelist for reuse.

### Property Heap

Append-only area storing variable-length payloads (maps, lists, strings). Entries prefixed with length and checksum. A simple free-list per size class will allow reusing reclaimed space without immediate compaction. Periodic vacuum can rewrite the heap to eliminate fragmentation.

### Catalog & Index Segments

* Catalog segment stores schemas, roles, etc., in a compact TLV format. Given low churn we can rewrite the entire segment on change.
* Index segment is initially empty; later we can host adjacency indexes or label scans. For v1, node/edge tables include simple adjacency lists to avoid separate structures.

## Memory Mapping Strategy

1. Use `memmap2` to map the entire file in read-write mode.
2. File grows in page-size increments. When a segment needs more space, extend file, remap, update region directory under a short mutex.
3. Maintain `Arc<RwLock<>>` around the mapping metadata so readers see consistent offsets while writers remap.

## Append vs. Free List

* Node/edge slots: maintain both a freelist (vector of freed slot indices) and an append pointer. Allocate from freelist if available, else append.
* Property heap: maintain free lists keyed by size class (powers of two). If no slot fits, append to heap. Vacuum pass rewrites heap when wasted space exceeds threshold.

## Vacuum Process

* Triggered manually or when garbage ratio > threshold.
* Steps: copy live nodes/edges/properties into a new temporary file, rebuild region directory, atomically swap files, remap.
* Because all offsets are absolute from file start, rewriting requires updating node/edge property pointers as we copy (use relocation table during vacuum).

## API Impact

Implement a new backend `DenseStorage` that satisfies `StorageBackend`:

- `load_node/edge` -> compute slot offset, read record, deserialize heap pointer.
- `store_node/edge` -> write record in-place (copy-on-write semantics for heap values).
- `delete_*` -> mark slot free, drop heap entry.

Additional helpers:

- `DenseAllocator` for slot/heap allocations.
- `DenseCatalog` to read/write catalog segment.

## Phased Implementation Plan

1. **Scaffold backend**: file creation, header init, mmap management, region directory with static segments sized generously.
2. **Node/edge storage**: implement fixed-size record operations, freelist, simple heap for properties.
3. **Catalog integration**: move existing JSON catalog into catalog segment serialization.
4. **Vacuum utility**: offline compaction tool/command.
5. **Testing**: round-trip tests, corruption detection, benchmark vs. simple backend.

This design leaves room to embed indexes and adapt the heap allocator later, while providing a single dense file with predictable offsets that mmap can exploit.
