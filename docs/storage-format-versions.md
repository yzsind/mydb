<!-- Automatically generated; do not edit. -->
# Storage Format Versions

The current development format is incompatible with earlier versions. If versions do not match, use a new `MYDB_DATA_DIR` or rebuild the data directory; mydb does not migrate it silently.

| Format | Version |
| --- | --- |
| storage control / owner-transfer proof | 1 / 1 |
| row codec specification | 1 |
| catalog / WAL | 67 / 40 |
| heap page / MVCC row | 15 / 9 |
| B-tree index file / page | 25 / 16 |
| undo binary / tablespace | 15 / 3 |
| overflow | 4 |
| paged vector container (HNSW / IVF) index / page | 7 / 4 |
| Zone Map file / page | 6 / 3 |
| legacy in-memory HNSW snapshot | 3 |
| spatial R-tree page | 2 |
| vector state envelope | 1 |
| JSONB / geometry value / native vector value / binlog VECTOR | 1 / 1 / 1 / 2 |
| commit ledger / history index | 2 / 2 |
| binlog manifest | 2 |
| replica progress / replication slot | 1 / 1 |
| MySQL channel / relay record | 1 / 1 |
| WAL retention / persisted variables / AI keyring | 1 / 1 / 1 |
| native replication metadata | 3 |
| commit clock watermark | 2 |
| native backup image / mode pointer | 5 / 2 |
| backup repository | MYDB_BACKUP_REPOSITORY/2 |
| backup bundle | 3 |
| backup catalog checkpoint / packed lineage | 1 / 1 |

All required formats participate in the data-directory admission fingerprint. Unsupported versions or required features are rejected before writable recovery.
