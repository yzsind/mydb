# Table, Row, LOB, and BTREE Data Storage Formats

This document describes the persistence formats of mydb tables (heap), row records, LOB overflow, and BTREE
indexes. It describes the current development format, not a compatibility promise; on storage version mismatch,
the system refuses to open the old data directory or rebuilds derived indexes, according to the format compatibility rules.

This document discusses ordinary tables and the BTREE `.idx` file in detail. For the complete data directory, correctness sources of truth, Catalog, Zone
Map, vector indexes, WAL, undo, and recovery relationships, see [`storage-format.md`](storage-format.md).
Although FULLTEXT reuses the BTREE container, its key semantics are defined by a separate posting format.

## 1. Format Versions and Byte Order

Current numeric versions are listed only in the generated
[Storage Format Versions](storage-format-versions.md). The byte tables below
describe the current storage layouts.

Except for `BIT(M)` where expressly noted, multi-byte integers and floating-point numbers are little-endian. The page size is uniformly
`16 KiB`. heap, BTREE, BTREE meta, and LOB pages all place the page LSN at offset
8 and the checksum at offset 16; the checksum is a whole-page CRC32 (IEEE polynomial), computed with the checksum
field treated as 0.

## 2. File Layout

Ordinary persistent objects use ID/generation paths that are independent of SQL names:

```text
<data_dir>/
  objects/
    tables/<storage_id>/
      <generation>.heap
      <generation>.lob                    # created only when a LOB is first externalized
      <generation>.zm                     # paged Zone Map pruning structure
    indexes/<storage_id>/<generation>/
      <index_id>.idx
      <index_id>.hvec    # when vector indexes exist
  system/catalog/
    <dd_table_id>.heap
    indexes/<dd_table_id>/<generation>/<index_id>.idx
```

Ordinary object paths contain no database/table/index names; stable storage/index IDs are the identity source of truth,
and `active_generation` in the Catalog selects the currently reachable file set. Partitions use independent storage IDs,
so each partition has its own heap/LOB/index files. Temporary tables live under `system/tmp/temp_tables` and
`system/tmp/temp_indexes`; their file names are shorter, but the internal page format is the same. The legacy
`schemas/<database>/tables|indexes` name paths are not the canonical location for current ordinary objects.

Data dictionary tables such as `mysql.tables`, `mysql.columns`, and `mysql.indexes` are themselves ordinary heap
files and use exactly the same page and row encodings described below. The catalog source of truth is not a JSON file but the atomic
generations of these data dictionary heaps.

`system/commit-ledger.v1` is the persistent correctness source of truth for time-travel visibility; it stores fixed-length
`(txn_id, commit_scn, commit_unix_us)` records protected by CRC32. Since WAL v22, `CommitTxn`
also carries the commit
time, and startup uses untruncated WAL to repair the ledger tail. Table options persisted in catalog v43
store `system_time_epoch_us`. The historical old-key index is still only a degradable accelerator: when missing, only performance is affected —
the executor falls back to heap/undo, and result correctness is unaffected.

`system/history-index.v1` is the historical old-key accelerator sidecar v1: a 16-byte magic/version header followed by
variable-length `txn_id:u64 + payload_len:u32 + crc32:u32 + payload` frames. The `HistoryIndexBatch` introduced in WAL v22 is the
crash-recovery tail; before allowing WAL truncation, a checkpoint synchronizes the ledger and the
sidecar together. The sidecar is not a correctness source of truth; when corrupted or incompletely overwritten, it is marked degraded and the system falls back to heap/undo.

## 3. Heap File and Page Format

A `.heap` file is a 16 KiB page array with no separate file header: a page's file offset is
`page_id * 16384`. `RowId` contains no table ID and is fixed at:

```text
RowId := page_id u32 + slot_id u16       # 6 B both logically and on disk
```

Every heap handle is already bound to a table, so the table id need not be repeated inside the RowId. A `u32 page_id`
puts the theoretical addressing ceiling of a single table at roughly 64 TiB.

### 3.1 Heap Page Header

| Absolute offset | Length | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `0x4d485032` (`MHP2`) |
| 4 | 2 | version | `HEAP_PAGE_VERSION` (14 for this layout) |
| 6 | 2 | flags | bit 0: `HEAP_PAGE_REDUCED_REDO`; other bits reserved |
| 8 | 8 | page_lsn | WAL LSN of the last modification to this page |
| 16 | 4 | checksum | whole-page CRC32, this field treated as 0 when computed |
| 20 | 2 | slot_count | number of slots |
| 22 | 2 | free_start | end of the slot directory / start of the contiguous free area |
| 24 | 2 | free_end | end of the contiguous free area / start of the record area |
| 26 | 6 | reserved | currently reserved |

The page header is a fixed 32 B. The slot directory grows from offset 32 toward higher addresses, row payloads grow from the page end toward lower addresses,
and between the two lies the contiguous free area `[free_start, free_end)`.

`HEAP_PAGE_REDUCED_REDO` (`0x0001`) marks a page initialized through a
crash-safe reduced-redo extent write. Before a subsequent physiological update,
the writer records a full-page image and clears the marker, so recovery never
applies a partial update to an unproven baseline. This word is part of the
persistent header, not always-zero padding. See
[redo configuration](configuration.md) for transaction and durability boundaries.

### 3.2 Slot Directory

Each slot is exactly 5 B:

| Offset within slot | Length | Field |
| ---: | ---: | --- |
| 0 | 2 | payload offset |
| 2 | 2 | payload length |
| 4 | 1 | flags |

`flags` currently defined:

- `0x00 ACTIVE`: points to a readable payload;
- `0x01 DELETED_PENDING`: physically deleted or still referenced by MVCC/secondary indexes; not reusable;
- `0x02 FREE`: payload reclaimed; the slot id can be reused.

A logical DELETE usually sets the deleted bit in the row's MVCC header first; only after the background confirms there are no old snapshots or dangling index entries
does the slot become `FREE`. In-page compaction moves payloads and updates slot offsets, but
`(page_id, slot_id)` never changes.

A single row must fit the in-page budget. When a new slot is added, the theoretical maximum payload is:

```text
16384 - 32 - 5 = 16347 bytes
```

An actual INSERT may additionally reserve space under the table's update reserve/PCTFREE policy.

## 4. Heap Row Record Format (MVCC row v7)

The overall layout of a normal row payload is:

```text
u8      row_version = 7
u8      flags
[u64]   last_txn_id              # flags bit 2
[u64]   undo_ptr                 # flags bit 3
uvarint layout_id                # unsigned LEB128
         schema-aware value section
```

flags:

| bit | Mask | Meaning |
| ---: | ---: | --- |
| 0 | `0x01` | Row logically deleted (public MVCC flag) |
| 1 | `0x02` | Row piece (migrated row fragment) |
| 2 | `0x04` | Followed by `last_txn_id u64` |
| 3 | `0x08` | Followed by `undo_ptr u64` |
| 4 | `0x10` | Value section contains an external bitmap |

The MVCC header is 2 B at minimum, and 18 B when both optional fields are present. Real undo ids start at 1; a reserved
undo pointer whose value is 0 means no undo record. `layout_id` selects the row layout persisted in the catalog,
so physical rows from before and after online/instant DDL can coexist.

Although the format allows omitting the two u64s, an ordinary INSERT writes a
non-zero `last_txn_id` and deliberately keeps an 8 B undo slot with value 0 so that the first UPDATE can patch in place;
a newly written normal row therefore usually carries the full 18 B header.

### 4.1 Row Migration and Forwarding Stubs

When a record no longer fits on its original page after an UPDATE, the original slot can be rewritten as a 7 B forwarding stub:

```text
0xff | target_page_id u32 LE | target_slot_id u16 LE
```

The target row carries the `ROW_PIECE` flag. Forwarding never crosses heap files, so the stub stores no table id. Reads follow at most
8 hops; the steady-state design target is 1 hop.

## 5. Schema-aware value section

The value section stores no per-column type tag; column types are determined by the `RowCodec` selected through `layout_id`:

```text
uvarint stored_count = N
u8      null_bitmap[ceil(N / 8)]
[u8]    external_bitmap[ceil(varlen_count / 8)]  # present when row flags bit 4 is set
u8      fixed_block[fixed_len]
u16     var_end[varlen_count]                    # LE, cumulative end offsets
u8      var_payload[]
```

- The NULL bitmap is indexed by physical layout column ordinal; bit `i` set to 1 means column `i` is NULL.
- The fixed block packs fixed-width columns tightly in their order of appearance in the layout, with no alignment padding.
- The variable-length column directory is indexed by variable-length column ordinal; each entry is a cumulative end offset measured from the start of `var_payload`.
  Entry 0 starts at 0, and each later entry starts at the previous entry's end; hence each column needs only 2 B, with no separate length stored.
- The directory is `u16`, so the end offset of all in-row variable-length payloads (external descriptors included) must not exceed 65535;
  the 16 KiB single-page limit usually bites first.
- The external bitmap is likewise indexed by variable-length column ordinal. When a bit is 1, that column is not the raw value in `var_payload`
  but a 20 B `LobRef`. Fixed-width columns cannot be externalized.
- A NULL fixed-width column still occupies its width in the fixed slot but its content is ignored; for a NULL variable-length column, the directory end does not advance.

`stored_count` together with `layout_id` supports reading old layouts and newly added columns; new columns not physically stored in a row are filled from the current
catalog default.

## 6. Column Data Type Formats

The table below describes the schema-aware encoding of ordinary heap rows. "Fixed" means the value lives in the fixed block, and "variable-length"
means it lives in the var payload. All string lengths are counted in bytes.

| SQL type | Location/width | On-disk representation |
| --- | --- | --- |
| `TINYINT` | fixed 1 B | signed/unsigned LE integer |
| `SMALLINT` | fixed 2 B | signed/unsigned LE integer |
| `MEDIUMINT` | fixed 3 B | signed/unsigned LE integer, sign-extended on decode |
| `INT` | fixed 4 B | signed/unsigned LE integer |
| `BIGINT` | fixed 8 B | signed/unsigned LE integer |
| `BOOL` | fixed 1 B | same as the 1 B integer |
| `FLOAT` | fixed 4 B | IEEE-754 `f32` LE; a declared `(M,D)` does not change the width |
| `DOUBLE` | fixed 8 B | IEEE-754 `f64` LE; a declared `(M,D)` does not change the width |
| `DECIMAL(P,S)`, `P<=9` | fixed 4 B | `i32` LE of `value * 10^S` |
| `DECIMAL(P,S)`, `P<=18` | fixed 8 B | `i64` LE of `value * 10^S` |
| `DECIMAL(P,S)`, `P<=38` | fixed 16 B | `i128` LE of `value * 10^S` |
| `DECIMAL(P,S)`, `39<=P<=65` | variable-length `3+ceil(digits/2)` B | `sign u8 + scale u8 + digit_count u8 + packed BCD`; one digit per high/low nibble of each byte, with the trailing low nibble `0xF` when the digit count is odd |
| `CHAR` / `VARCHAR` / `TEXT` | variable-length | raw UTF-8 bytes; `CHAR` does not artificially store trailing pad spaces |
| `BINARY(n)` | fixed n B | raw bytes, right-padded with `0x00` when short; overlength is handled by upper-layer coercion |
| `VARBINARY` / `BLOB` / undeclared-width `BINARY` | variable-length | raw bytes |
| `BIT(M)` | fixed `ceil(M/8)` B | big-endian, right-aligned unsigned integer; the only multi-byte big-endian scalar in the row format |
| `ENUM` | fixed 1 or 2 B | unsigned LE of the 1-based ordinal; 0 is the empty/error value; 1 B when there are at most 255 labels, otherwise 2 B |
| `SET` | fixed `ceil(N/8)` B, at most 8 B | unsigned LE bitmask with one bit per declared label |
| `DATE` | fixed 3 B | signed 24-bit LE: ordinary dates store `days_since_1970` directly; `-8388608` denotes `0000-00-00`, and the reserved negative range after it encodes component-valid/calendar-invalid dates |
| `TIME(fsp)` | fixed 3..6 B | signed LE quantum count: `microseconds / 10^(6-fsp)` |
| `DATETIME(fsp)` | fixed 5..8 B | signed LE quantum count: `micros_since_1970 / 10^(6-fsp)` after encoding the timezone-naive wall time as UTC; the most negative value of this width is reserved for zero datetime, and the adjacent reserved range encodes permitted invalid date components |
| `TIMESTAMP(fsp)` | fixed 5..8 B | signed LE quantum count: the UTC instant `micros_since_1970_UTC / 10^(6-fsp)`; read/write boundaries convert per the session `time_zone`, and the range is not bound by the MySQL 2038 upper limit |
| `YEAR` | fixed 1 B | `year - 1900`; 0 means `0000`, and 1..255 means 1901..2155 |
| `JSON` | variable-length | mydb `jsonb` binary node stream |
| `GEOMETRY` family | variable-length | `SRID u32 LE + canonical little-endian WKB` |
| `VECTOR(n)` | fixed `n*4` B | `n` contiguous IEEE-754 `f32` LE, never externalized |
| `VECTOR` without declared dimension | variable-length | likewise a bare `f32` LE array, externalizable |

The physical width of `TIME`/`DATETIME` is `base + fractional_bytes`: fsp 0 adds 0 B, fsp 1..2
adds 1 B, fsp 3..4 adds 2 B, and fsp 5..6 adds 3 B; the TIME base is 3, and the DATETIME/TIMESTAMP
base is 5. Half-away rounding before the write, or the `TIME_TRUNCATE_FRACTIONAL` behavior, is handled by the coercion
layer; the row codec stores the quantized integer.

A bare SQL `DECIMAL` is normalized to `DECIMAL(10,0)` and uses the fixed 8 B
coefficient above. The internal codec also has an unspecified-precision decimal
form (`sign:u8 + nbytes:u8 + shortest unsigned magnitude LE`), but this is not
the layout selected by that SQL declaration.

### 6.1 JSON Binary Format

A JSON column does not store text. Every node is `tag u8 + payload`:

| tag | Type | payload |
| ---: | --- | --- |
| 0 | null | none |
| 1 | true | none |
| 2 | false | none |
| 3 | signed integer | `i64 LE` |
| 4 | unsigned integer | `u64 LE` |
| 5 | double | `f64 LE` |
| 6 | string | `uvarint byte_len + UTF-8 bytes` |
| 7 | array | `uvarint payload_len + uvarint count + count nodes` |
| 8 | object | `uvarint payload_len + uvarint count + count * (uvarint key_len + key + node)` |
| 9 | exact decimal | `uvarint byte_len + canonical decimal text` |
| 10 | date | `uvarint byte_len + YYYY-MM-DD text` |
| 11 | time | `uvarint byte_len + [-]HH:MM:SS[.ffffff] text` |
| 12 | datetime | `uvarint byte_len + YYYY-MM-DD HH:MM:SS[.ffffff] text` |

Object keys are normalized in ascending `(UTF-8 byte length, raw bytes)` order, and duplicate keys converge with last-wins during DOM parsing. The payload length of arrays/objects lets nodes be skipped in O(1).
Container payload lengths include the count varint and child/key bytes. Tags
9–12 preserve typed SQL scalar inputs; parsing ordinary JSON text does not
infer decimal or temporal tags from strings. This is mydb's internal JSONB,
separate from the MySQL binary JSON used by binlog and JSON_STORAGE_SIZE.

### 6.2 GEOMETRY and VECTOR

After GEOMETRY's 4 B SRID prefix comes standard 2-D WKB. Every WKB geometry has a byte order byte, a
`u32 type code`, and a type-specific payload; the system accepts NDR/XDR on read and canonicalizes all writes to
little-endian NDR. Z/M and type codes carrying EWKB SRID flags are currently rejected.

VECTOR has no dimension header; the dimension is derived as `byte_len / 4`. Every element must be finite (NaN,
`+Inf`, and `-Inf` are not allowed). `VECTOR(n)` with a declared dimension is a fixed block member, so table creation/alteration
checks whether it fits within the 16347 B row budget; it is never automatically moved to `.lob` the way a BLOB is.

## 7. LOB overflow Files

When a variable-length string/bytes/JSON/GEOMETRY value or an undeclared-dimension VECTOR exceeds
`mydb_lob_inline_max` (4096 B by default at startup), or when the whole row otherwise cannot fit in a heap page, the write path may
externalize it. Each table's `.lob` is a 16 KiB page array: page 0 is the meta page, and the data pages form a singly linked list.

### 7.1 In-heap LobRef

When the corresponding external bitmap bit is 1, the var payload stores:

```text
first_page_id u32 LE
last_page_id  u32 LE
total_len     u32 LE      # logical/decompressed length
generation    u64 LE      # monotonically increasing chain allocation generation
```

That is 20 B in total. `last_page_id` lets freeing an entire chain require only splicing the chain tail into the free list and updating the meta free head.
The ranges of the three u32 fields match the LONGTEXT/LONGBLOB 4 GiB cap and the roughly 64 TiB-per-table addressing.
`generation` distinguishes new chains from old ones after page-number reuse, so old undo cannot free a new generation occupying the same page numbers.

### 7.2 LOB Pages

| Offset | Length | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `0x4d4f5631` (`MOV1`) |
| 4 | 2 | version | 3 |
| 6 | 2 | reserved | 0 |
| 8 | 8 | page_lsn | WAL LSN |
| 16 | 4 | checksum | CRC32 |
| 20 | 2 | payload_len | valid payload on this page, at most 16344 |
| 22 | 2 | flags | bit 0=head, bit 1=whole chain LZ4 compressed, bit 2=page numbers contiguous |
| 24 | 8 | next_page_id | 0 means end of chain |
| 32 | 8 | generation | chain allocation generation; identical on all pages of a chain |
| 40 | <=16344 | payload | raw or compressed byte-stream fragment |

Only the head page sets the head/compressed/contiguous bits. For a compressed chain, `LobRef.total_len` is still the decompressed length, and the sum of the per-page
`payload_len` values is the compressed stream length. A LOB chain is immutable once written; an UPDATE creates a new chain, and old chains are held by undo/
MVCC until the corresponding historical version is purged, after which they enter the free list.

Page 0 uses the same 40 B common header; its first 24 payload bytes are:

```text
free_head  u64 LE       # 0 = no free page
page_count u64 LE       # authoritative high-water mark, at least 1
next_generation u64 LE  # allocation generation for the next chain, at least 1
```

The free list likewise reuses the page header's `next_page_id`. `free_count` exists only in the in-memory cache; opening the file walks the chain to
recompute it, and it is not persisted in the meta.

## 8. AUTO_INCREMENT dynamic metadata

AUTO_INCREMENT no longer has a per-table sidecar. The runtime high-water is published via compact
`AutoIncrementAdvance` redo; checkpoints merge it into
`mysql.innodb_dynamic_metadata(table_id, version, auto_increment_max, covered_lsn)`.
Recovery takes the maximum of the DD baseline and the redo beyond the covered boundary; TRUNCATE isolates old records by incrementing `version`.
(The old `.vm` visibility-map sidecar was removed on 2026-07-16; index-only visibility uniformly goes through the buffered heap MVCC header + the `page_max_trx_id` short-circuit.)

## 9. B-tree IDX File Layout

A BTREE file is also a 16 KiB page array:

- page 0: the meta page;
- page 1: the empty root at initialization;
- remaining pages: leaf, internal, or free pages;
- leaf pages are chained in ascending order via `next_leaf`; reverse scans are realized via tree seek/cursor.

An index entry is logically:

```text
IndexEntry := IndexKey + RowId(page_id u32, slot_id u16)
```

Neither ordinary nor primary-key indexes are clustered indexes: leaf entries store RowIds, and table records always live in `.heap`.
Uniqueness constraints act on the user-declared key parts; included/covering columns are stored in the entry as a key suffix.
Index pages store no table ID; the index file handle is already bound to the owning table.

### 9.1 Meta page (page 0)

| Offset | Length | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `0x4d494d34` (`MIM4`) |
| 4 | 2 | page version = 15 |
| 6 | 2 | reserved = 0 |
| 8 | 8 | page_lsn, currently written as 0 by the encoder |
| 16 | 4 | checksum CRC32 |
| 20 | 4 | index file version = 24 |
| 24 | 4 | page size = 16384 |
| 28 | 4 | reserved/zero padding |
| 32 | 8 | root_page |
| 40 | 8 | next_page_id |
| 48 | 8 | free_page_head, 0=None |
| 56.. | | zero padding |

Opening the file validates the page version, file version, page size, and checksum together.

### 9.2 Tree Page Common Header

leaf/internal/free pages share:

| Offset | Length | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `0x4d495038` (`MIP8`) |
| 4 | 2 | page version = 15 |
| 6 | 2 | reserved = 0 |
| 8 | 8 | page_lsn |
| 16 | 4 | checksum CRC32 |
| 20 | 4 | payload_len |
| 24 | `payload_len` | page-specific payload |
| 24+payload_len.. | | zero padding |

Record offsets in slots are `u16 LE` values relative to the payload start (absolute offset 24).

## 10. BTREE Leaf Page Format

Leaf payload:

```text
u8   page_type = 1
u8   key_layout
i64  base_time_or_base_or_scale LE
u64  next_leaf LE                   # 0=None
u16  entry_count LE
u64  page_max_trx_id LE
[20 B compact INT2 prefix]          # layout 7 only
entry bodies...
(u16 offset, u16 len) slots[entry_count] LE  # payload tail, sorted by key
```

The fixed leaf payload header is 28 B, 52 B together with the common header. The slot array allows binary search directly over the raw page bytes of a cold page,
decoding only O(log N) candidate entries. `payload_len` includes the slot array and is not required to fill the page tail.

`next_leaf` establishes the ascending leaf chain. Entries within a single page are sorted by `(IndexKey, RowId)`, so duplicated
non-unique keys still keep a stable RowId order.

## 11. BTREE Internal and Free Pages

Internal page payload:

```text
u8   page_type = 2
u8   key_layout
i64  base_time_or_base_or_scale LE
u64  first_child LE
u32  key_count LE
[20 B compact INT2 prefix]          # layout 7 only
separator records[key_count]        # IndexEntry followed by right_child u64
(u16 offset, u16 len) slots[key_count] LE
```

The fixed header is `24 + 1 + 1 + 8 + 8 + 4 = 46 B`. The normal B+tree logical invariant remains
`child_count = key_count + 1`; the first child lives in the fixed header, and each subsequent child follows its own separator,
so inserting a separator no longer moves the whole children array. Separators still carry the full 6 B RowId.

The free page payload is simple:

```text
u8  page_type = 3
u64 next_free LE       # 0=None
```

The meta's `free_page_head` points to this singly linked list.

## 12. Generic Index Key Encoding

Each layout 0 entry:

```text
u8      part_count
KeyPart parts[part_count]
u32     heap_page_id LE
u16     heap_slot_id LE
```

Note: the RowId inside `.idx` pages is currently fixed at 6 B. A few historical comments in the source still mention varint or
same-page RID compression, but the current encoder/decoder and length estimation all explicitly use `u32 + u16`; this document
follows the actual function implementations.

KeyPart tags:

| tag | Logical type | payload after tag | Total length per part |
| ---: | --- | --- | ---: |
| 0 | NULL | none | 1 |
| 1 | signed integer | `i64 LE` | 9 |
| 2 | unsigned integer | `u64 LE` | 9 |
| 4 | bytes | `u32 len LE + bytes` | 5+len |
| 5 | string | `u32 UTF-8 len LE + bytes` | 5+len |
| 6 | float32 | order-preserving `u32 LE` | 5 |
| 7 | float64 | order-preserving `u64 LE` | 9 |
| 8 | decimal | signed scaled magnitude `i128 LE + scale u8` | 18 |
| 9 | date | days since epoch `i32 LE` | 5 |
| 10 | time | microseconds `i64 LE` | 9 |
| 11 | datetime/timestamp | epoch microseconds `i64 LE` | 9 |
| 12 | year | `year-1900 u8` | 2 |
| 13 | collated string | `collation_id u16 LE + u32 len LE + UTF-8` | 7+len |
| 14 | wide decimal | `coefficient_len u8 + signed BigInt coefficient LE + scale u8` | 3+len |

Tag 3 is the deprecated ASCII float format and must not appear in pages of the current version.

Floating-point keys do not store raw IEEE bits. Positive numbers flip the sign bit, and negative numbers flip all bits, yielding an encoding that compares as an
unsigned integer; this makes the byte-value order of negatives, zero, and positives agree with the numeric order.

A collated string stores the collation id and the raw canonical string; comparisons generate/compare sort keys under the specified collation —
the collation sort key itself is not persisted. `CHAR` drops synthesized trailing spaces when generating keys. Prefix indexes truncate before writing the key: character types by Unicode scalar/character count, binary by byte count.

Index direction (ASC/DESC) is catalog/planner metadata and does not change the key bytes above; the physical BTREE
stays uniformly ascending, and DESC is realized through a reverse range cursor.

### 12.1 SQL Types to KeyParts

- signed integer/BOOL map to tag 1; unsigned integer and decoded `BIT` values map to tag 2.
- The fixed heap slot of `ENUM`/`SET` stores the ordinal/bitmask, but ordinary index keys are generated from the decoded
  label strings; with a collation they map to tag 13, otherwise to tag 5.
- FLOAT/DOUBLE map to the order-preserving bits of tag 6/7.
- `DECIMAL(P,S)` with `P<=38` maps to tag 8 (`i128 scaled value + scale`);
  `P=39..65` maps to tag 14 (shortest signed BigInt coefficient + scale). The two variants share the same
  numeric comparison rank but differ in on-page encoding.
- CHAR/VARCHAR/TEXT usually map to string or collated string; BINARY/VARBINARY/BLOB map to bytes.
- DATE/TIME/DATETIME/TIMESTAMP/YEAR map to tags 9..12.
- Ordinary MySQL JSON columns must extract scalars through functional/multi-valued indexes; only direct paths store
  canonical jsonb bytes as `Bytes`.
- SPATIAL indexes go through MBR/R-tree separately; the BTREE of a vector index uses only a NULL key as the persistent RowId
  registry, while the discriminating data lives in HNSW.
- FULLTEXT currently encodes the token and posting payload as two parts: `String(token), Bytes(payload)`.

## 13. Compact Page-level Key Layouts

On each leaf and internal page, the system picks the densest layout that can hold all of the page's current keys. Compact layouts hoist
part_count, tag, and some repeated high-order bits into the page header; any NULL, type mismatch, or out-of-range span causes the page to reselect a wider layout or fall back to generic.

Every compact entry still appends the fixed 6 B RowId and occupies a further 4 B `(offset,len)` in the slot array.

| layout | Applicable keys | Page-level field | Key body per entry |
| ---: | --- | --- | --- |
| 0 | any | base=0 | generic tagged key |
| 1 | single-column DateTime | `base_time=min(micros)` | `u32(value-base) LE` |
| 2 | single-column Date | `base_time=min(days)` | `u16(value-base) LE` |
| 3 | single-column Decimal, value fits i32 | low byte of `base_time` stores the scale | `i32 scaled_value LE` |
| 4 | single-column Decimal, value fits i64 | low byte of `base_time` stores the scale | `i64 scaled_value LE` |
| 5 | single-column signed Int, in-page span <= u16 | `base_time=min(value)` | `u16 delta LE` |
| 6 | single-column signed Int, in-page span <= u32 | `base_time=min(value)` | `u32 delta LE` |
| 7 | two-column signed Int | `base_time=col1_min` + INT2 prefix | `delta1(width1) + delta2(width2)` |
| 8 | single-column unsigned Int, in-page span <= u16 | `base_time=ordered(col_min)` | `u16 delta LE` |
| 9 | single-column unsigned Int, in-page span <= u32 | `base_time=ordered(col_min)` | `u32 delta LE` |

The actual key shape the current layout 1 encoder accepts is single-column `DateTime`; TIME uses the generic key.

The unsigned base is reinterpreted as i64 after `u64 ^ 0x8000_0000_0000_0000`, so the full u64 range keeps its order in
i64 min/max/span arithmetic; decoding xors again to restore.

The INT2 20 B prefix is:

```text
col2_base i64 LE
width1    u8            # 1, 2, or 4
width2    u8            # 1, 2, or 4
padding   u16 = 0
reserved  i64 = 0
```

`col1_base` already sits in the standard `base_time` field. Each column independently selects a u8/u16/u32 delta according to its in-page span.

## 14. BTREE Comparison, Uniqueness, and Covering Columns

An `IndexKey` compares part by part from left to right; under an equal prefix, the shorter key is smaller. Every position of a normal index has a fixed
type, so variant rank only guards against mixed shapes. NULL is the smallest variant.

The total order of a leaf entry is `key` followed by `RowId`. Therefore:

- a non-unique index can store any number of identical keys;
- a unique index checks the user-declared key prefix and does not treat RowId as part of uniqueness;
- INCLUDE columns are appended as a key suffix for covering-scan decoding but are not part of the unique constraint;
- index-only scans check visibility through the buffered heap MVCC header. A leaf's `page_max_trx_id` can skip those reads only when its writers are covered by the safely retained transaction watermark (or it has no transactional writer) and no pending historical key repair requires checking the heap. Comparing a snapshot SCN directly to this transaction ID is not sufficient; see [visibility rules](mvcc-and-locking.md#4-index-access-still-defers-to-heap-mvcc).

## 15. Non-table Formats: Do Not Confuse with Persistent Heap/BTREE

Two other encodings are distinct from persistent heap rows and BTREE pages:

1. The self-describing row value section used by undo: `count varint + NULL bitmap + tag/payload per
   non-NULL value`. It covers Int/UInt/Float/Double/Decimal/Bytes/String/
   DecimalCompact/Date/Time/DateTime/Year/JSON/Geometry/Vector; ordinary heap rows store none of these tags.
2. The ADD INDEX external-sort run stream uses `u32 part_count + tagged parts + fixed 6 B RowId`.
   Generic keys in `.idx` pages use the tighter `u8 part_count`.
