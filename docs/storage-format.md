# Data Storage Format Overview

This document describes the current development storage format. Cross-version
physical compatibility and automatic format migration are not guaranteed.

This document explains how mydb persists data, layer by layer: the data directory, correctness sources of truth, page and record encoding, index sidecars, and the transaction log with recovery. For the field-by-field byte layouts of ordinary tables, LOBs, and BTREE, see
[`table-and-btree-storage-format.md`](table-and-btree-storage-format.md); for the auto-generated table of current
versions, see [`storage-format-versions.md`](storage-format-versions.md).

## 1. Design Boundaries

- mydb currently uses fixed `16 KiB` data pages; except for the in-row value of `BIT(M)`, multi-byte scalars are
  little-endian.
- The Catalog, user heaps, LOBs, the WAL, and the still-required undo are correctness data; Zone Maps, in-memory
  R-trees, and query caches are not sources of truth for correctness.
- Both ordinary and vector indexes store a `RowId` pointing into the heap; they are not clustered indexes. The executor must go back to the heap/MVCC to verify visibility; approximate vector indexes must additionally re-rank with the exact vectors stored in the heap.
- This is the first product version. On format version mismatch the system explicitly refuses to open; it never silently migrates an old directory.
- SQL object names play no part in the paths of ordinary persistent objects. Stable table/index IDs and the physical generation determine paths, so `RENAME TABLE` and `RENAME INDEX` do not move the bulk data files.

## 2. Current Versions

[Storage Format Versions](storage-format-versions.md) is the generated list of current
versions. Catalog, WAL, heap/MVCC,
BTREE, undo, overflow, vector, Zone Map, replication, and backup formats each
have independent version identifiers. The byte layouts below describe the
current encoders; a format identifier is not a promise of migration support.

Check format compatibility before reusing a data directory, restoring a backup,
or connecting native replication nodes. A version mismatch is rejected explicitly;
use a compatible server version or rebuild the data directory.

## 3. Data Directory Layout

The following are the canonical paths; files are created on demand, and not every data directory contains every item.

```text
<MYDB_DATA_DIR>/
├── mydb.lock
├── objects/
│   ├── tables/<storage_id>/
│   │   ├── <generation>.heap
│   │   ├── <generation>.lob
│   │   ├── <generation>.zm
│   ├── indexes/<storage_id>/<generation>/
│   │   ├── <index_id>.idx
│   │   ├── <index_id>.hvec
│   │   └── <index_id>.hnsw
├── system/
│   ├── catalog/
│   │   ├── <dd_table_id>.heap
│   │   ├── <dd_table_id>.lob
│   │   ├── <dd_table_id>.zm
│   │   └── indexes/<dd_table_id>/<generation>/<index_id>.<ext>
│   ├── wal/wal-<16-hex-digit-segment-id>.log
│   ├── undo/
│   │   ├── undo.manifest
│   │   └── undo-<segment-id>.seg
│   ├── replication/
│   ├── tmp/
│   │   ├── temp_tables/
│   │   └── temp_indexes/
│   ├── commit-ledger.v1
│   └── history-index.v1
└── binlog/
    ├── manifest.json
    └── binlog.<sequence>
```

`storage_id` identifies one physical storage object; for a non-partitioned table it usually equals `table_id`, and each partition has its own
storage ID. A `generation` is an atomically publishable, complete heap/index file set: CTAS, reorganization, or a cold
build can first write a new, unreachable generation, and a transactional Catalog update then switches `active_generation`. The legacy
`schemas/<database>/tables|indexes` name-based paths belong only to historical formats or compatibility helper code; they are not the canonical location of ordinary
objects.

Temporary tables use `system/tmp/temp_tables/<table_id>.heap`, and temporary indexes use
`system/tmp/temp_indexes/<table_id>_<sanitized-index-name>.idx`; the internal page formats are identical to those of persistent
objects, but their lifecycles and recovery semantics differ.

## 4. Sources of Truth and Rebuildable Data

| Data | Role | Principle when missing or corrupted |
| --- | --- | --- |
| Data Dictionary heap | Catalog source of truth | Refuse to treat old JSON mirrors as the source of truth; handle per recovery rules |
| User `.heap` | Source of truth for current row versions | Depends on WAL/undo recovery; must not be treated as a droppable cache |
| `.lob` | Source of truth for externalized column bytes | descriptor, page-chain generation, and CRC must agree |
| WAL | Source of truth for crash recovery after a checkpoint | Truncate only an incomplete/corrupted tail; mid-stream corruption is an error |
| Undo tablespace | Rollback, old snapshots, and time-travel versions | Records not past the retention boundary must not be deleted early |
| `commit-ledger.v1` | Source of truth for commit SCN and time mapping | Repair the tail from still-retained WAL |
| `.idx` | Persistent index | Rebuildable from the Catalog definition and the heap; constraint enforcement must still preserve semantics |
| `.hvec` / `.hnsw` | Vector candidate structures | Rebuild/degrade when not finalized, missing, or incompatible |
| `.zm` | Conservative page-pruning sidecar | Fall back to a heap scan when missing, corrupted, stale, or short of memory |
| In-memory R-tree | Spatial candidate cache | Rebuilt from the persistent MBR BTREE |
| `history-index.v1` | Historical old-key accelerator | Once degraded, fall back to heap/undo; correctness is unaffected |
| `mysql.innodb_dynamic_metadata` | AUTO_INCREMENT checkpoint baseline | Recovered by merging with compact redo; `version` isolates resets |
| Sequence state | `mysql.sequence_values` catalog heap | Complete WAL reservations, checkpoint materialization and native replay |

"Rebuildable" only means queries can restore the structure from more fundamental sources of truth; it does not mean the runtime may silently ignore an index update that should have succeeded. When the foreground hits an unsupported format or cannot guarantee a constraint, it must still return a clear error.

## 5. Unified Page Conventions

Heap, BTREE, LOB, paged HNSW, paged IVF, and Zone Map files all consist of `16 KiB` pages; page number `n`
sits at file offset `n * 16384`. These pages share the key offsets required for recovery:

| Offset | Length | Convention |
| ---: | ---: | --- |
| 0 | 4 | page-kind magic |
| 4 | 2 | page version |
| 6 | 2 | kind/flags or reserved |
| 8 | 8 | page LSN |
| 16 | 4 | CRC32 (IEEE; this field is treated as 0 when computed) |
| 20 | 4+ | type-specific header and payload |

WAL redo uses the page LSN to decide whether a physical page update has already
been applied. Page-kind magic identifies the layout used for checksum validation
and recovery.

Zone Map pages use the shared LSN/checksum offsets above and the 64-byte
page-specific header described in Section 9. WAL and undo segments instead use
framed/append-only layouts with their own magic, version and CRC.

## 6. Heap, RowId, and Row Records

A `.heap` file has no separate file header; page 0 is an ordinary heap page. The page header is 32 B; the 5 B slot directory grows upward from low
addresses, and row payloads grow downward from the end of the page. A slot stores `offset:u16 + len:u16 + flags:u8`,
with the states active, deleted-pending, or free. In-page compaction may move payloads but never changes slot IDs.

```text
RowId := page_id u32 LE + slot_id u16 LE       # fixed 6 B
```

RowId carries no table ID, because the surrounding heap/index/undo context already binds the owning storage object. A single table
can address roughly `2^32 * 16 KiB = 64 TiB` at most.

The high-level layout of the current MVCC row v7 is:

```text
u8      row_version = 7
u8      flags
[u64]   last_txn_id
[u64]   undo_ptr
uvarint layout_id
         schema-aware value section
```

The low two bits of flags denote logical deletion and a migrated row piece respectively; the remaining bits indicate the two optional u64s and the
external bitmap. A fresh INSERT usually keeps the full 18 B MVCC header, in which `undo_ptr=0` is the permanent "no undo yet"
sentinel, so the first UPDATE can patch it in place.

The value section stores no per-value SQL type tag; `layout_id` instead selects the RowCodec from the Catalog:

```text
uvarint stored_count
u8      null_bitmap[ceil(stored_count / 8)]
[u8]    external_bitmap[ceil(varlen_count / 8)]
u8      fixed_block[fixed_len]
u16     var_end[varlen_count]
u8      var_payload[]
```

Fixed-width columns have no alignment padding; the variable-length directory stores cumulative end offsets. Rows written under an old layout can coexist with the new layout, and newly added columns that are not physically stored are filled from the Catalog default. When a row no longer fits back onto its page after an UPDATE, the original slot
can become a 7 B forwarding stub `0xff + target_page_id:u32 + target_slot_id:u16`, and the target row
sets the row-piece flag.

For the fixed widths of all SQL types plus the DECIMAL, temporal, JSONB, WKB, and VECTOR encodings, see
[`table-and-btree-storage-format.md`](table-and-btree-storage-format.md#6-column-data-type-formats).

## 7. LOB overflow

A variable-length string/bytes/JSON/GEOMETRY value, or a VECTOR without a declared dimension, that exceeds the inline threshold or makes the whole row unfit for a page can be moved into the table-level `.lob`. The heap's var payload then stores a 20 B descriptor instead:

```text
first_page_id u32 LE
last_page_id  u32 LE
total_len     u32 LE
generation    u64 LE
```

Page 0 of a `.lob` is the meta page, holding the free-list head, the authoritative page-count high-water mark, and the
next generation. All other pages use a 40 B common header with a payload cap of 16344 B and are chained by `next_page_id`
into a singly linked list. Head-page flags can mark LZ4 compression and contiguous pages; `total_len` is always the
decompressed logical length.

A LOB chain is immutable once written. An UPDATE creates a new chain, and old chains are retained with the undo/MVCC
historical versions until a purge safely releases them. The generation of each allocation is written into the descriptor and every page of the chain, preventing old undo from wrongly releasing a new chain after page-number reuse (ABA).

## 8. BTREE and Special Indexes

A `.idx` file is a 16 KiB page array: page 0 is the meta page, the initial root is page 1, and the remaining pages are leaf, internal,
or free pages. The meta records the file/page version, page size, root, allocator high-water mark, and
free-list head. Tree pages use a 24 B common header, and the `(offset:u16,len:u16)` slot array at the tail of the payload supports binary search directly over the raw bytes of a cold page.

A leaf entry logically has the form `IndexKey + RowId`, and the total order within a page is `(key, RowId)`. Uniqueness checks only the user-declared key prefix; included columns are a covering suffix and do not participate in uniqueness. Internal separators also carry the
full key/RowId; the right child immediately follows its separator, and the first child is stored separately in the fixed header.

A generic key consists of `part_count:u8` and tagged KeyParts, supporting NULL, signed/unsigned,
bytes/string, float, decimal, temporal values, and collated strings. Single-column date, datetime, integer,
and decimal keys, and two-column integer keys, can use a page-level compact layout that shrinks entries via a page-level base plus narrow deltas. ASC/DESC is Catalog/executor metadata and does not change the persisted key bytes.

- FULLTEXT reuses BTREE with the key `String(token), Bytes(posting-payload)`; the payload uses
  unsigned LEB128 to store `doc_len`, `tf`, and optional position deltas.
- The persistent source of truth for SPATIAL is the MBR BTREE; the in-memory R-tree is only a governor-managed derived cache, and candidates are finally
  re-verified by the exact geometry predicate over the heap.
- Neither ordinary BTREEs nor the primary key are clustered indexes; an index-only scan still judges visibility via the heap MVCC header,
  and the leaf's `page_max_trx_id` only provides a safe short-circuit.

For the complete meta, leaf/internal/free page offsets and the KeyPart tag table, see
[`table-and-btree-storage-format.md`](table-and-btree-storage-format.md#9-b-tree-idx-file-layout).

## 9. Zone Map

A `.zm` file is a conservative page-pruning structure for one storage object.
The current file format is v6 and uses 16 KiB shared-buffer-pool pages with
page version 3. It has no append-frame file header or separate checkpoint
manifest. Zone Map pages use the layout described below.

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `0x5a4d5031` |
| 4 | 2 | page version |
| 6 | 1 | page kind |
| 7 | 1 | reserved, zero |
| 8 | 8 | page LSN |
| 16 | 4 | whole-page CRC32, checksum field zeroed for calculation |
| 20 | 4 | reserved, zero |
| 24 | 8 | storage ID |
| 32 | 8 | generation, nonzero |
| 40 | 8 | page ID |
| 48 | 8 | next-page ID; PackedRecords instead stores base column and base zone as two u32 values |
| 56 | 2 | entry count |
| 58 | 2 | lower free-space bound, immediately after slots |
| 60 | 2 | upper free-space bound, immediately before payloads |
| 62 | 2 | reserved, zero |

Kinds are Meta=0, Directory=1, Bounds=2, Membership=3, ZoneMeta=4,
Overflow=5, Records=6, and PackedRecords=7. Slots start at offset 64;
payloads are packed from the end of the page. Full slots are 20 bytes:
`column:u32, kind:u8, reserved[3], zone:u64, payload_offset:u16,
payload_length:u16`. Packed slots are 8 bytes:
`column_delta:u8, kind:u8, zone_delta:u16, payload_offset:u16,
payload_length:u16`. Entries are strictly ordered by `(column, kind, zone)`;
payloads must be disjoint and tightly packed. Wider column/zone spans use full
slots; oversized values use bounded overflow pages.

Bounds carry typed min/max and NULL facts; membership records use exact or
Bloom summaries, including the compact block-Rice encoding. Foreground updates
widen summaries conservatively. Background rebuilds may tighten them only
within their snapshot/SCN validity bounds. Missing, untrusted, corrupt, or
inapplicable summaries and insufficient optional memory cause a heap scan;
the original predicate and heap MVCC visibility checks remain authoritative.

Pages participate in physical backups, checkpoints and native replication.
The first modification requiring a baseline records a full-page image (FPI).
Subsequent PageDelta records may use the complete baseline as an LZ4 dictionary;
replay validates baseline LSN/CRC, decoded page size and after-image LSN/CRC.
Recovery repairs torn baselines with FPIs before applying deltas. Invalid
physical redo is an explicit recovery error; query-time degradation of an
optional summary does not authorize accepting invalid redo.

## 10. Vector Indexes

Paged HNSW and native IVF share the `.hvec` container and use the unified 24 B page header, page LSN, CRC32, and WAL
physical page redo, and are managed by the buffer pool.

### 10.1 Paged HNSW

The page 0 meta stores the dimension, distance metric, `m/m0/ef_construction`, entry vid, top layer, page allocation
high-water mark, node/tombstone counts, finalized/built LSN, and SQ8 quantization parameters. Node pages use
the slotted layout `slot_count:u16 + heap_top:u16 + u16 record_offsets[]`.

A node record stores flags, top layer, RowId, the exact `dim*f32`, an SQ8 navigation row, a norm,
and, per layer, `count:u16 + fixed-capacity NodeId[]`. Adjacency space is reserved at node creation with the maximum capacity for that layer,
so edge updates can modify it in place. A persistent NodeId uses the high 24 bits for the page number and the low 8 bits for the slot; the runtime vid
is `(page_id << 32) | slot`.

### 10.2 Native IVF

Native IVF uses the shared vector page container with typed geometry and no
adjacency lists. Each record retains the RowId, deletion flag and native
payload. Lists maintain separate fill-page hints to group their entries.
Coarse centers and list membership are reconstructed from committed heap
values on cold open; no per-row scalar-quantization codes are stored.

Both ANN algorithms generate candidates. The executor checks MVCC visibility
and reranks using native heap values. Full probing (`nprobe=nlist`) of IVF
scores every visible candidate and yields exact TopK without residual filters.

## 11. Catalog and Generation Publication

The Catalog source of truth is the `mysql.*` Data Dictionary tables, which themselves use the ordinary heap/BTREE formats and live under
`system/catalog/`. Dictionary table IDs are allocated from a reserved range starting at `1 << 48` to avoid colliding with user table/storage
IDs. The base schema of the dictionary tables is built into the server so the catalog can be opened at startup.

The data dictionary heap and WAL make DDL changes durable. The active storage
generation lets bulk load, CTAS, and reorganization:

1. build a new, unreachable heap/index generation for the target storage ID;
2. finish the page writes, WAL, and the necessary syncs;
3. transactionally update the Data Dictionary to make the new generation reachable;
4. reclaim the old generation after a safe boundary.

No directory scan result may override the Data Dictionary's object identity and active generation.

## 12. WAL, Undo, and MVCC Chains

### 12.1 WAL Segment and Frame Layout

WAL is a variable-length frame stream under `system/wal/`, with a default
256 MiB segment rotation threshold. It does not use fixed 16 KiB framing.
The current WAL v34 segment header is 40 bytes, little-endian:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `MYDBWAL1` |
| 8 | 4 | WAL version |
| 12 | 8 | segment ID |
| 20 | 8 | creation time, Unix milliseconds |
| 28 | 8 | `replaces_through`, explicit checkpoint replacement horizon; zero for an ordinary segment |
| 36 | 4 | CRC32 of bytes 0..36 |

Each frame has a 48-byte header followed by its type-specific payload:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | record magic `0x4d57414c` |
| 4 | 4 | payload length |
| 8 | 8 | LSN |
| 16 | 8 | previous transaction LSN |
| 24 | 8 | transaction ID |
| 32 | 2 | record type |
| 34 | 2 | reserved, zero |
| 36 | 8 | `source_time_us`, append-ordered source time in Unix microseconds |
| 44 | 4 | frame CRC32 |
| 48 | variable | payload |

Frame CRC covers bytes 0..44 followed by the payload, excluding the CRC slot.
LSN and source time are assigned under the WAL mutex before checksumming.
Source time supplies recovery-time coverage; file modification time does not.

WAL carries logical heap/index/DDL records, physical page images/deltas,
transaction boundaries, checkpoints, undo, history/ledger tails, sequence
reservations, and file generation/system-file operations. A `TxnBatch` packs
ordered logical records and a final commit into one checksummed frame.
Recovery may truncate a torn/zero preallocated tail under its tail-validation
rules; a mid-stream checksum failure is an error. The replacement horizon
controls reclamation of superseded segments and must not be inferred merely
from file names or the current segment ID.

### 12.2 Undo tablespace

`system/undo/undo-<segment-id>.seg` is an append-only segmented file with a default target of 64 MiB. The 32 B
segment header contains `MYUNDO1\0`, the tablespace version, and the segment ID; the remainder is reserved as zero.
Each record uses a 16 B frame header:

```text
magic:u32="MUFR" | payload_len:u32 | payload_crc32:u32
tablespace_version:u32 | undo_payload[]
```

An undo payload begins with `UNDO_BINARY_VERSION:u8`, followed by the undo pointer, txn/time, the transaction's
rollback-chain predecessor, kind, table/RowId, an optional old MVCC header, a full-row or changed-column
before-image, the list of owned/borrowed LOB descriptors, and the chain depth. Kinds include Insert,
InsertBatch, Update, Delete, and UpdateDelta.

A live row's `undo_ptr` points to an older version of that row; inside undo, `before_header.undo_ptr` continues the row version chain,
while `prev_undo_ptr` links the rollback chain of the same transaction — the two must never be conflated. Rollback, snapshot reads, and time
travel reconstruct rows from these links. A purge must simultaneously satisfy transaction finality, the oldest snapshot, the retention window, replication slots, and the LOB
ownership constraints; a LOB chain owned by an old version is released only after the corresponding undo has been safely cleaned.

## 13. Small Persistent Files and Replication Boundaries

- AUTO_INCREMENT: no separate file; changes are written as compact redo, and checkpoints merge them into
  `mysql.innodb_dynamic_metadata`.
- SEQUENCE: definitions and values are stored in catalog tables, with no separate `.seq` files.
  `SequenceState` has a 33-byte payload: sequence ID, counter generation, signed resume value,
  is_called byte and logical logging time. Checkpoints materialize the latest state into
  `mysql.sequence_values`; `mysql.sequence_retirements` proves retired generations until WAL coverage
  permits reclaiming those proofs. Replay orders states by LSN, including decreasing and cycling values.
  See [Sequences](sequences.md) for gap, reset and asynchronous-failover boundaries.
- `commit-ledger.v1`: the 16 B header is `MYDBCTL1 + version:u32=1 + reserved:u32`; each subsequent
  32 B entry is `txn_id:u64 + commit_scn:u64 + commit_unix_us:u64 + crc32:u32 + reserved:u32`.
  WAL commit records can repair the not-yet-checkpointed tail.
- `history-index.v1`: the 16 B header is `MYDBHIX1 + version:u32=1 + reserved:u32`, followed by
  `txn_id:u64 + payload_len:u32 + crc32:u32 + payload` frames; it is a degradable accelerator.
- `binlog/`: a MySQL binlog event stream using the MySQL 4 B magic, the 19 B event header,
  and event checksums; it is the replication/CDC interface, not a heap crash-recovery log.
- `system/replication/`: MySQL→mydb channel JSON, JSON-lines relay logs, and the slot/progress
  metadata of mydb native replication. The native replication compatibility handshake compares catalog/WAL/heap/row/index/
  undo/LOB/vector/zone/page-size/endian together, never a single version number.

Binlog and replication formats are separate from heap and BTREE file formats.
For replication setup and compatibility, see [MySQL Compatibility](mysql80-compatibility.md#10-replication)
and [Native Replication](native-replication.md).

## 14. Writes, Crash Recovery, and Validation

The typical persistence relationships are:

```text
SQL DML
  ├─ undo before-image / LOB ownership
  ├─ WAL logical record or page image
  ├─ heap / LOB / BTREE / vector page (page_lsn + CRC32)
  └─ COMMIT (commit_scn + commit time, binlog position associated when needed)

startup
  ├─ validate format versions and the Data Dictionary
  ├─ scan WAL, keep the CRC-intact prefix
  ├─ REDO committed/required page and logical operations
  ├─ UNDO loser transactions, restore XA prepared state
  ├─ repair the ledger, history, and derived-structure tails
  └─ validate or rebuild the indexes/sidecars that allow rebuilding
```

Page writes must respect the write-ahead constraint: a dirty data page may be safely
flushed only after the corresponding WAL has reached the required durable LSN. A checkpoint may advance the redo start point and truncate old WAL only after every durable boundary required by heap/index/undo, the commit ledger, the history sidecar, the binlog manifest,
and the rest has been completed.

When opening a file, at minimum verify the magic, version, page size (where applicable), structural boundaries, and checksum. A blank, unallocated
page must be distinguished from a formatted page; corruption of a formatted page must never be silently interpreted as an empty page. A derived structure that fails validation may fall back
or be rebuilt, but a failed validation of a correctness source of truth must produce a clear error.
