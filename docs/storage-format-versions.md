<!-- Automatically generated; do not edit. -->
# Storage Format Versions

The current development format is incompatible with earlier versions. If versions do not match, use a new `MYDB_DATA_DIR` or rebuild the data directory; mydb does not migrate it silently.

| Format | Version |
| --- | --- |
| catalog / WAL | 65 / 35 |
| heap page / MVCC row | 14 / 8 |
| B-tree index file / page | 24 / 15 |
| undo binary / tablespace | 14 / 1 |
| overflow | 3 |
| paged vector container (HNSW / IVF) index / page | 7 / 4 |
| Zone Map file / page | 6 / 3 |
| legacy in-memory HNSW snapshot | 3 |
| native replication metadata | 2 |
| commit clock watermark | 2 |
| native backup image / mode pointer | 5 / 2 |
| backup repository | MYDB_BACKUP_REPOSITORY/2 |
| backup bundle | 3 |
| backup catalog checkpoint / packed lineage | 1 / 1 |

The R-tree is an in-memory cache derived from the persistent MBR B-tree and has no separate persistent format version.
