# Vector datatypes and indexes

`VECTOR(n, datatype)` supports `FLOAT32`, `FLOAT16`, `BFLOAT16`, `INT8` and
`BINARY`. Omitting datatype selects FLOAT32. Bare `VECTOR` accepts variable
FLOAT32 dimensions. Type names are case-insensitive.

| Datatype | Payload bytes | Maximum dimensions | Metrics |
| --- | ---: | ---: | --- |
| FLOAT32 | `4*n` | 16383 | L2, COSINE, DOT |
| FLOAT16 | `2*n` | 32766 | L2, COSINE, DOT |
| BFLOAT16 | `2*n` | 32766 | L2, COSINE, DOT |
| INT8 | `n` | 65532 | L2, COSINE, DOT |
| BINARY | `ceil(n/8)` | 524256 bits | HAMMING, JACCARD |

The payload budget is 65532 bytes. Heap layout and paged index record limits
also apply. Large native payloads use the heap's ordinary overflow mechanism.
Floating values must remain finite after conversion. FLOAT16 and BFLOAT16
round to nearest, ties to even; subnormal values and gradual underflow are
supported. INT8 accepts integers in [-128,127], without automatic scaling,
clamping or truncation.

BINARY text uses `[0,1,...]`. Dimension counts bits, including for dimensions
not divisible by eight. Bit `i` occupies bit `i%8` of byte `i/8`, least
significant bit first. Unused high bits of the final byte must be zero.

```sql
CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    embedding VECTOR(768, FLOAT16) NOT NULL,
    signature VECTOR(769, BINARY) NOT NULL
);
CREATE INDEX embedding_hnsw ON embeddings(embedding)
    USING HNSW COMMENT 'METRIC=COSINE M=16 EF_CONSTRUCTION=200';
CREATE INDEX signature_ivf ON embeddings(signature)
    USING IVF COMMENT 'METRIC=JACCARD NLIST=128';
```

HNSW and IVF require one fixed-dimension, non-null, non-unique vector column.
Numeric indexes default to L2; BINARY indexes default to HAMMING. Unsupported
metric/type combinations raise an error. `IVF` stores native payloads and scores entries with their native datatype. Hamming IVF uses binary majority centers; Jaccard IVF uses sampled
medoids scored with Jaccard distance.

## Distances and queries

Numeric operands may have different datatypes. Distance uses the actual
stored values and preserves the query vector's precision. It does not first
round a FLOAT32 query to the column datatype. Numeric and BINARY operands
cannot be mixed implicitly.

```sql
SELECT id, COSINE_DISTANCE(embedding, TO_VECTOR('[...]')) AS distance
FROM embeddings ORDER BY distance LIMIT 10;

SELECT id FROM embeddings
ORDER BY JACCARD_DISTANCE(signature, TO_VECTOR('[...]', 'BINARY')) LIMIT 10;
```

Replace `[...]` with an array of the declared dimension. L2 returns Euclidean
distance; DOT distance is negative inner product. COSINE scalar functions
return NULL for zero vectors; COSINE indexes reject them, including vectors
that become zero after narrowing. Hamming counts differing bits. Jaccard is
`1 - popcount(a AND b)/popcount(a OR b)`; two all-zero bit vectors have
Jaccard distance zero.

Native HNSW shares navigation for identical coordinates while retaining each
row's native payload and visibility. Deleting one member preserves the other
members.

ANN indexes generate candidates. The executor checks heap MVCC visibility,
applies residual filters and re-ranks using visible native values. Increasing
ef or nprobe improves coverage; re-ranking cannot recover unvisited
neighbors. Full probing of native IVF evaluates all lists with exact native
scores.

Eligible single-table distance ordering can use KNN truncation. JOIN,
aggregation, DISTINCT and window queries retain their complete relational
input. Old snapshots, unusable indexes and selective filters may require an
exact scan. Prepared executions rebind their query vector and validate its
dimension at execution.

## Storage and recovery

`MYDB_VECTOR_STORAGE=memory` keeps exact native payloads resident. `paged`
places native payloads in WAL-protected pages managed by the shared buffer
pool. FLOAT32 keeps its established optimized index implementation. Native
HNSW construction currently inserts sequentially; FLOAT32 retains its existing
parallel builder. High-dimensional native builds can therefore be substantially
slower. Native payloads are not persistently expanded into per-row FLOAT32 copies. Numeric
coarse centers and bounded training buffers may use FLOAT32.

For paged storage, each complete HNSW node or IVF entry must fit in a 16 KiB
page. HNSW checks
maximum-layer adjacency capacity as well as payload size, so the limit also
depends on M. DDL rejects configurations that cannot fit before publication,
including for empty tables. A datatype's maximum dimension does not guarantee
that its paged index can be created.

Index files are derived from committed heap data. Native indexes rebuild from
a registered heap snapshot on cold open, then replay committed build deltas
before publication. Failed maintenance invalidates their handles. Unfinished
or corrupt index data is rebuilt; physical replicas use revocable accelerators
over their own committed heap. They may fall back to exact search when an
accelerator cannot be admitted.

Datatype changes and dimension changes require COPY rewriting and index
reconstruction. Invalid conversions fail atomically, including under IGNORE;
vectors are not repaired by truncating or padding. This development format
rejects older persistent databases; there is no automatic historical migration.

The existing MySQL vector wire type and array text output are retained.
`VECTOR_TO_BINARY` and `VECTOR_FROM_BINARY` expose explicit native payload
I/O. ROW/FULL binlog values use the versioned `MYDBVEC2` BLOB envelope to
preserve datatype and dimension.

See the [vector function reference](function/vector/README.md).
