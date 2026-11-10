# Spatial indexes

A SPATIAL index is a persistent, page-backed R-tree over geometry minimum bounding
rectangles (MBRs). It is stored in an independent `.rtree` object file. There is no
secondary MBR B-tree and no in-memory tree to rebuild after restart.

```sql
CREATE TABLE places (
    id BIGINT PRIMARY KEY,
    location POINT NOT NULL SRID 0,
    SPATIAL KEY location_idx (location)
);
INSERT INTO places VALUES (1, ST_GeomFromText('POINT(5 5)', 0));
EXPLAIN SELECT id FROM places
WHERE MBRIntersects(location,
    ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))', 0));
```

A spatial index requires one non-null geometry column. Unique, expression, prefix,
and descending spatial key parts are rejected. Ordinary nonunique geometry keys
also create SPATIAL indexes; primary and unique geometry keys retain byte-equality
B-tree semantics. An explicit column SRID is required
for spatial range access. `CREATE SPATIAL INDEX` and `ALTER TABLE ... ADD SPATIAL INDEX`
both use the existing index DDL lifecycle; unsupported online lock modes return errors.

## Queries and transactions

The optimizer recognizes supported MBR and exact spatial predicates over an indexed
column and a constant or execution-time geometry. The R-tree returns candidates;
MVCC heap reads and the complete WHERE predicate determine the result. Prepared probes
are rebound on each execution. Partition pruning selects the physical indexes to probe,
and each candidate retains its partition identity during heap access. Spatial
scans produce rows incrementally, allowing an eligible LIMIT to stop navigation
early. A memory-governed row locator accelerates repeated-MBR maintenance; it is
derived from persistent pages and is optional under memory pressure.

Inserts, updates, deletes, and rollback maintain the tree as part of ordinary row
mutation. Old-key visibility repair and historical index entries preserve snapshot
reads when geometry positions change. Spatial indexing does not change supported
GIS function semantics or add geographic projection, distance/KNN, or spatial-join
capabilities.

## Storage, recovery, and backup

Tree pages are 16 KiB and share the buffer pool, checksums, and WAL-before-data rules
with other persistent pages. Internal entries contain an MBR and child page ID;
leaf entries contain an MBR and RowId. Root, tree height, stable index ID, and free-page state are persistent.

A transactional modification first links a `SpatialIndexUndo` logical inverse into
its transaction's WAL chain. A redo-only `SpatialPageBatch` then stores compressed
complete after-images, including structural changes. Live rollback uses row undo;
startup recovery applies the WAL inverse and writes a `SpatialIndexClr` backward
cursor. This allows recovery to finish interrupted compensation without loading the
catalog or scanning the heap. Rollback never restores a shared tree page to another
transaction's old image. Native replicas apply the same page batches. Backup and immutable snapshot images
include `.rtree` files, rather than rebuilding them from heap data.

Startup opens metadata and necessary pages; the first query reads relevant tree pages
on demand. With startup index validation enabled, writable startup rebuilds
missing, incompatible, or fenced spatial
indexes from the authoritative heap. A corruption detected during execution fences
that index and returns an explicit read error; heap writes and unrelated queries
remain available. Restart completes the derived-index repair. Immutable images
require repair in a writable preparation stage before sealing. Rebuild intent
remains durable until the complete replacement is flushed.
Explicit index creation or recreation builds a new tree; large builds use bounded
sort tiles and spill intermediate levels to temporary storage.

Geographic POINT predicates are supported where the GIS function supports them.
Geographic MBR predicates involving non-point geometries return an explicit
unsupported-SRS error until geodesic envelopes are implemented; they do not use
Cartesian bounds as an approximation.
