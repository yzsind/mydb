# Vector indexes

mydb supports single-column, non-unique HNSW indexes on `VECTOR` columns declared `NOT NULL`.

```sql
CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    embedding VECTOR(128) NOT NULL
);
CREATE INDEX embedding_hnsw ON embeddings(embedding)
    USING HNSW COMMENT 'METRIC=COSINE M=16 EF_CONSTRUCTION=200';

SELECT id, COSINE_DISTANCE(embedding, '[...]') AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

Replace `[...]` with a vector of the declared dimension. L2, COSINE and DOT metrics are available; the query metric must match the index. COSINE accepts different vector magnitudes and rejects zero vectors. Vector components must be finite f32 values. Distance results use double precision, with wider intermediate arithmetic for magnitudes that would overflow or underflow ordinary f32 reductions.

## Query behavior

HNSW generates approximate nearest-neighbor candidates. mydb checks their heap visibility and computes distances using the visible row version. Increasing search effort generally improves recall; exact re-ranking does not recover neighbors that traversal never reached.

KNN scan-level truncation applies to eligible single-table distance ordering. Queries with joins, aggregation, DISTINCT or window functions retain their complete relational input and final ordering. Older snapshots and unavailable or invalid indexes can use an exact scan. Selective filters can also require exact completion when bounded ANN widening cannot supply enough visible rows.

`HNSW_EF(n)` and the HNSW session variables control exploration and candidate retention. Effective candidate capacity is bounded by the graph size and configured scan budget. `mydb_hnsw_max_scan_tuples` is not a hard upper bound on every distance computation or physical page read.

## Storage capacity

`MYDB_VECTOR_STORAGE=memory` keeps the graph and exact vectors resident. `paged` stores nodes in `.hvec` files and uses the shared data buffer, with optional mapping or compact navigation acceleration.

The current paged layout requires each complete node record to fit in one 16 KiB page. It stores the exact vector, SQ8 codes and all supported adjacency layers. The limit depends on dimension and M. DDL rejects a declared dimension/M combination that cannot fit at the maximum supported layer, including on an empty table. A vector fitting in a heap row does not necessarily fit in a paged HNSW node. Use the memory backend or a smaller dimension/M when the paged layout rejects the configuration.

HNSW files are derived data. Interrupted or stale builds are rebuilt from the committed heap; failed maintenance invalidates existing graph handles. A completed graph records its covered commit frontier. Physical replicas use a resident, revocable HNSW accelerator over their committed heap; if it cannot be admitted, queries can fall back to exact search. This can have a different memory and latency profile from the primary's paged index.

See the [vector function reference](function/vector/README.md) for scalar functions.
