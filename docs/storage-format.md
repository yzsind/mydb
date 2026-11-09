# Data Directory, Persistence, and Recovery

This guide explains the files in a mydb data directory, their operational roles,
and the boundaries of crash recovery. For supported sizes and configuration,
see [Server Limits](limits.md) and [Startup Configuration](configuration.md).

## Format Compatibility

The [generated format registry](storage-format-versions.md) lists the current
catalog, WAL, heap, index, undo, replication, and backup versions. Physical
compatibility and automatic migration between development formats are not
guaranteed. Unsupported versions or required features are rejected before
writable recovery; use a compatible server or rebuild the data directory.

Check physical compatibility before reusing a data directory, restoring a
backup, or connecting native replication nodes. The product version alone does
not establish format compatibility.

## Data Directory Layout

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
│   ├── storage.control
│   ├── object-owners/<path-digest>.owner
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

Objects use stable IDs and physical generations rather than SQL names.
Renaming a table or index does not rename its bulk data files. Each partition
has a separate storage ID. Files are created on demand, and temporary tables
have a different lifecycle from persistent objects.

Treat the data directory as a unit. Do not rename, edit, or delete individual
files to repair a table, reclaim space, or copy a running database. Use
[physical backup and recovery](backup-and-recovery.md) or
[native replication](native-replication.md) for a consistent copy.

## Persistent Files and Rebuildable Data

| Data | Role and operational boundary |
| --- | --- |
| Data Dictionary and user `.heap` files | Authoritative schema and current row data; required for recovery |
| `.lob` files | Externalized large values, including values needed by older row versions |
| B-tree, full-text, spatial, and vector indexes | Persistent query structures; repair and rebuild behavior depends on the index and recovery state |
| `.zm` files | Automatic Zone Map pruning summaries; untrusted summaries fall back to ordinary scans |
| WAL | Recovery records after a checkpoint and the stream used by native replication and WAL archiving |
| Undo | Transaction rollback, consistent reads, and retained time-travel history |
| Commit ledger | Persistent commit-time information required for historical visibility |
| History index | Accelerator for historical key lookups; degraded access falls back to heap/undo when the required history remains available |
| Binlog and replication metadata | Logical replication events, retained positions, and channel state |
| `system/tmp` | Temporary tables and execution work; temporary state is not a substitute for persistent data |

Persistent indexes and summaries are managed by the server. A structure being
rebuildable does not make deleting its file a supported maintenance operation.

## WAL, Undo, and Durability

Standard heap, LOB, index, vector, and Zone Map pages are 16 KiB. WAL is a
segmented stream of variable-length records; the default segment rotation
threshold is 256 MiB. WAL is not a sequence of data pages.

The server writes recovery records before flushing the associated dirty data
pages. Checkpoints establish a recoverable boundary before old WAL can be
reclaimed. Active transactions, backups, replicas, and retained history can
keep WAL or undo alive beyond a nominal rotation or retention target.

The default `strict` durability mode synchronizes WAL before acknowledging a
commit. Its power-loss guarantee depends on storage honoring synchronization
requests. Other durability modes trade guarantees for throughput; see
[durability configuration](configuration.md).

Undo remains necessary with `mydb_undo_retention_ms=0`: active transactions and
snapshots still need rollback and older row versions. Large values may remain
allocated while old versions refer to them. Purge and compaction reclaim space
only after the applicable safety horizons have passed. See
[MVCC and Locking](mvcc-and-locking.md#undo-retention-purge-and-space-reclamation).

## Crash Recovery and Corruption

Startup validates physical format admission and the catalog, replays required
WAL, rolls back incomplete transactions, restores prepared XA state, and checks
the persistent structures needed before accepting connections.

An incomplete WAL tail can be truncated under the recovery rules. Mid-stream
corruption, missing required data, or an invalid authoritative structure is an
error. Recovery is not a promise to salvage arbitrary corruption, and a
formatted page is never silently treated as empty because it fails validation.

Optional accelerators can fall back or rebuild when supported. For example,
untrusted Zone Maps fall back to scans, and historical indexes can fall back
to heap/undo while the required history is retained. These paths preserve SQL
visibility; they do not replace lost authoritative data.

After a startup or I/O failure, preserve the complete data directory and logs
before attempting recovery. Record the exact build and error, check storage
health and free space, and follow [Backup and Recovery](backup-and-recovery.md).
Use [Reporting Issues](reporting-issues.md) when recovery fails explicitly.

## Related Documentation

- [Storage Format Versions](storage-format-versions.md)
- [Backup and Recovery](backup-and-recovery.md)
- [Native Replication](native-replication.md)
- [MVCC and Locking](mvcc-and-locking.md)
- [SQL ROWID](rowid.md)
- [Hardware and OS Requirements](hardware-and-os-requirements.md)
