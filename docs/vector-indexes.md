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
    USING HNSW WITH (metric=cosine, m=16, ef_construction=200, quantization=sq8);
CREATE INDEX signature_ivf ON embeddings(signature)
    USING IVF WITH (metric=jaccard, nlist=128);
```

HNSW and IVF require one fixed-dimension, non-null, non-unique vector column.
Numeric indexes default to L2; BINARY indexes default to HAMMING. Unsupported
metric/type combinations raise an error. `IVF` with `quantization=none` stores and scores native payloads. Hamming IVF uses binary majority centers; Jaccard IVF uses sampled
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

Unquantized native HNSW shares navigation for identical coordinates while retaining each
row's native payload and visibility. Deleting one member preserves the other
members.

ANN indexes generate candidates. The executor checks heap MVCC visibility,
applies residual filters and re-ranks using visible native values. Increasing
ef or nprobe improves coverage; re-ranking cannot recover unvisited
neighbors. Full probing of unquantized IVF evaluates all lists with exact native
scores. Quantized IVF still uses approximate candidate scores: full probing
with a finite candidate pool does not guarantee global exact TopK.

Eligible single-table distance ordering can use KNN truncation. JOIN,
aggregation, DISTINCT and window queries retain their complete relational
input. Old snapshots, unusable indexes and selective filters may require an
exact scan. Prepared executions rebind their query vector and validate its
dimension at execution.

## Storage and recovery

`MYDB_VECTOR_STORAGE=memory` keeps native or compressed payloads resident.
`paged` places payloads in the shared WAL-protected vector container.
Unquantized FLOAT32 retains its established implementation. Quantized HNSW
uses an admitted parallel topology builder when its temporary working set fits
the configured build budget, otherwise it uses bounded incremental construction. Native payloads are not persistently expanded into per-row FLOAT32 copies. Numeric
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

## Index options

Options follow `USING HNSW` or `USING IVF` in CREATE INDEX, inline CREATE TABLE
indexes, and ALTER TABLE ADD INDEX. Names and enum values are case-insensitive.
Enum values accept bare names or strings; numeric values require unsigned
base-10 integer literals. Unknown, duplicate, misplaced and incompatible
options are errors. Empty or repeated WITH clauses are errors. COMMENT stores
only descriptive text; former parameter assignments in COMMENT are rejected.

| Option | Algorithm | Default / accepted values |
| --- | --- | --- |
| metric | Both | L2 for numeric, HAMMING for BINARY |
| quantization | Both | none; none, sq8, sq4, pq, bq |
| rerank_mult | Both | 4; 1..1024 |
| m | HNSW | 16; 2..1024, subject to page admission |
| ef_construction | HNSW | 200; m..65535 |
| ef_search | HNSW | Optional index query default; 1..1000000 |
| nlist | IVF | 0 (automatic); 0..1048576 |
| nprobe | IVF | Optional index query default; 1..1048576 |
| kmeans_iters | IVF | 15; 1..1000 |
| train_sample | IVF | 256; sample multiplier, 1..65536 |
| pq_m | PQ | ceil(dimension/8); 1..dimension |
| pq_bits | PQ | 8; 4 or 8 |
| pq_train_samples | PQ | 65536; 2^pq_bits..1048576 |
| pq_iterations | PQ | 25; 1..1000 |

IVF build and rerank environment defaults are captured at index creation;
restarting with a different environment does not reinterpret existing indexes.
PQ-only options require `quantization=pq`. PQ uses balanced contiguous
subspaces, so dimension need not be divisible by pq_m.

```sql
CREATE INDEX pq_search ON embeddings(embedding) USING IVF
    WITH (metric=cosine, nlist=128, quantization=pq,
          pq_m=96, pq_bits=8, rerank_mult=8)
    COMMENT 'Embedding retrieval';
```

## Quantization

SQ8, SQ4, PQ and BQ support FLOAT32, FLOAT16, BFLOAT16 and INT8 with L2,
COSINE and DOT. BINARY supports only quantization=none. Quantization changes
index payloads, not the column datatype, heap values or SQL distance functions.
There is no implicit SQ8 navigation for quantization=none.

- SQ8 and SQ4 use per-dimension affine calibration over the complete build
  snapshot, with 8-bit or packed 4-bit codes. Updates outside the calibrated
  range use lossless exception records rather than silently clipping values.
- PQ uses shared per-index subspace codebooks and asymmetric query lookup
  tables, without residual coding or OPQ. Empty indexes and indexes with fewer
  than 2^pq_bits rows remain PENDING_TRAINING and execute exact scans until
  sufficient committed data is available.
- BQ uses a shared center, one sign bit per dimension and per-row amplitude
  and norm corrections. L2 and DOT retain amplitude information; they do not
  substitute raw Hamming ordering. COSINE uses stable normalization. This is
  scaled centered BQ, without a distribution-independent recall guarantee.

The executor retains a wider candidate pool, checks MVCC and residual filters,
and ranks that pool by original visible heap values. Larger ef, nprobe and
rerank_mult may improve recall at additional cost. Re-ranking cannot recover
neighbors that did not enter the candidate pool. Explicit session rerank
settings override the index default. Search and memory budgets still apply.

Normal compressed nodes retain no full-precision per-row copy. Raw code
lengths are d bytes (SQ8), ceil(d/2) (SQ4), ceil(pq_m*pq_bits/8) (PQ), and
ceil(d/8) (BQ). BQ adds amplitude/norm metadata. The actual index also contains
row identities, graph edges or lists, page overhead, codebooks, and lossless
exceptions. Small datasets and INT8/SQ8 may not become smaller or faster.

Quantizer generations are immutable and replaced with their graph and coverage
frontier. Significant scalar exceptions or PQ/BQ update growth request a
rebuild. OPTIMIZE TABLE explicitly reconstructs the index. Failed maintenance
revokes the accelerator; committed heap data remains authoritative. Native
indexes rebuild on cold open. Models and exceptions share the vector container,
page checksums and recovery lifecycle.

EXPLAIN reports quantization and the effective rerank pool. The
information_schema.mydb_vector_indexes view includes QUANTIZATION,
QUANTIZATION_STATE, QUANTIZER_BYTES, COMPRESSED_BYTES,
QUANTIZATION_EXCEPTIONS, RERANK_MULT, CANDIDATES_RETURNED, HEAP_RERANKS and
EXACT_FALLBACKS. GENERATION identifies the built quantizer snapshot.
