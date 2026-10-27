# Vector data and similarity search in mydb

mydb stores vectors alongside ordinary relational data and searches them through
SQL. A row can contain an embedding, text, JSON, and business attributes, so an
application can update its data transactionally and combine similarity search
with SQL predicates. Typical uses include semantic retrieval, recommendations,
image similarity, and agent memory.

This guide introduces the data model and the retrieval pipeline, then walks
through working SQL examples. The [vector datatype and index reference](vector-indexes.md)
contains the complete index options, storage details, and recovery boundaries.
mydb is developing its first product version; measure correctness, recall, and
performance on representative application data before adopting a configuration.

## 1. From an embedding to a search result

An embedding represents an object as a fixed-length list of numbers. Applications
normally generate embeddings with a model and write them into a `VECTOR` column.
Stored vectors and query vectors should use the same model, dimension, and
preprocessing so that their distances have the intended meaning. mydb also
provides [embedding calls](ai.md) for applications that want model invocation
through SQL.

mydb supports two retrieval approaches:

| Approach | How it finds neighbors | Main tradeoff |
| --- | --- | --- |
| Exact search | Scores every eligible visible row and selects the nearest rows | An exact reference for the stored values; scoring cost grows with the input |
| Approximate nearest-neighbor search (ANN) | Uses HNSW or IVF to generate a candidate pool, then scores visible candidates using stored heap values | Reduces search work, with recall depending on data, index construction, and search settings |

The ANN pipeline is:

```text
Query vector -> index candidates -> heap visibility and SQL filters
             -> exact scoring of retained candidates -> ordered results
```

Exact scoring at the end makes the returned distances consistent with visible
stored values. Global TopK accuracy still depends on which candidates entered
the pool. A neighbor missed during index traversal cannot be recovered by
re-ranking alone.

## 2. A complete first example

Connect to mydb with a MySQL-compatible client and select a writable database,
such as `test`. The examples use distinct table names and can be run in order
in a fresh database. Three-dimensional vectors keep the examples readable;
use the dimension of your embedding model in an application.

```sql
CREATE TABLE vector_documents (
    id BIGINT PRIMARY KEY,
    category VARCHAR(32) NOT NULL,
    content VARCHAR(255),
    embedding VECTOR(3) NOT NULL
);

INSERT INTO vector_documents VALUES
    (1, 'guide', 'Database introduction', TO_VECTOR('[1,0,0]')),
    (2, 'guide', 'Index introduction', TO_VECTOR('[1,1,0]')),
    (3, 'news', 'Release announcement', TO_VECTOR('[0,0,1]'));

CREATE INDEX idx_documents_embedding ON vector_documents(embedding)
    USING HNSW WITH (metric=l2, m=16, ef_construction=200);

SELECT id, content,
       L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]')) AS distance
FROM vector_documents
ORDER BY distance
LIMIT 2;
```

The nearest rows are IDs `1` and `2`, at distances `0` and `1`. The optimizer
can choose an exact scan for this small table even though an index exists.
Check the access path with `EXPLAIN`:

```sql
EXPLAIN SELECT id
FROM vector_documents
ORDER BY L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]'))
LIMIT 2;
```

The recognizable KNN shape is an ascending distance ordering with `LIMIT`.
Use a metric matching the index and a query vector matching the column's
dimension. Prepared executions can bind a query vector; dimensions are
validated at execution. A configured index does not guarantee an ANN plan.

## 3. Choose the vector datatype

`VECTOR(n)` means `VECTOR(n, FLOAT32)`. A bare `VECTOR` accepts variable-length
FLOAT32 vectors, but a vector index requires a fixed dimension. Type names are
case-insensitive.

| Datatype | Coordinate payload | Maximum dimension | Suitable input |
| --- | --- | ---: | --- |
| `FLOAT32` | `4 * n` bytes | 16,383 | General numeric embeddings |
| `FLOAT16` | `2 * n` bytes | 32,766 | Numeric embeddings that tolerate lower precision and its range |
| `BFLOAT16` | `2 * n` bytes | 32,766 | Numeric embeddings needing a wider exponent range than FLOAT16 |
| `INT8` | `n` bytes | 65,532 | Integer coordinates already scaled by the application |
| `BINARY` | `ceil(n / 8)` bytes | 524,256 bits | Bit signatures compared by Hamming or Jaccard distance |

These limits follow a 65,532-byte vector payload budget. They describe the
datatype, not guaranteed index capacity. A paged index must also fit each
complete HNSW node or IVF entry in a 16 KiB page. HNSW admission includes graph
adjacency capacity, so its usable dimension also depends on `m`.

Floating coordinates must remain finite after conversion. FLOAT16 and BFLOAT16
round to nearest, ties to even. INT8 requires integers in `[-128,127]` and does
not automatically scale, clamp, or truncate values. Fixed dimensions must match
exactly; invalid inputs fail rather than being padded or shortened.

```sql
SELECT VECTOR_DATATYPE(TO_VECTOR('[1,2.5,-3]', 'FLOAT16')) AS datatype,
       VECTOR_DIM(TO_VECTOR('[1,2.5,-3]', 'FLOAT16')) AS dimension;
-- FLOAT16, 3

SELECT VECTOR_TO_STRING(TO_VECTOR('[-128,0,127]', 'INT8')) AS coordinates;
-- [-128,0,127]
```

The column datatype controls the original values retained in the heap.
Choosing FLOAT16 or INT8 changes those stored values. Numeric distance operands
can have different native datatypes: a FLOAT32 query is not first narrowed to
the column datatype. Numeric and BINARY vectors cannot be mixed implicitly.

## 4. Choose the distance metric

All distance functions below use ascending order to put the closest rows first.
Both operands must have the same dimension.

| Metric | SQL function | Meaning | Supported datatypes |
| --- | --- | --- | --- |
| L2 | `L2_DISTANCE(a,b)` | Euclidean distance | Numeric |
| Cosine | `COSINE_DISTANCE(a,b)` | `1 - cosine_similarity(a,b)` | Numeric |
| Dot | `MAX_INNER_PRODUCT_DISTANCE(a,b)` | Negative inner product; smaller means a larger inner product | Numeric |
| Hamming | `HAMMING_DISTANCE(a,b)` | Number of differing bits | BINARY |
| Jaccard | `JACCARD_DISTANCE(a,b)` | `1 - intersection_bits / union_bits` | BINARY |

`DISTANCE(a,b,metric)` and its alias `VEC_DISTANCE` expose the same metrics.
Without an explicit metric they use L2 for numeric vectors and Hamming for
BINARY. `DOT_PRODUCT` and `INNER_PRODUCT` return the positive inner product;
use the negative-distance form for an ascending KNN query.

Cosine scalar functions return `NULL` if either vector has zero norm. A cosine
index rejects zero vectors, including values that become zero after conversion
to the column datatype. Jaccard distance between two all-zero BINARY vectors
is zero. Vector functions propagate `NULL` for valid calls with NULL arguments.

Choose the metric expected by the embedding model. If you normalize vectors,
apply the same policy to stored data and queries. `L2_NORMALIZE` returns FLOAT32
and leaves a zero vector zero; it does not make that vector valid for a cosine
index. `VECTOR_NORM` and `L2_NORMALIZE` reject BINARY.

## 5. HNSW: search through a proximity graph

HNSW connects nearby vectors in a graph with multiple layers. A query navigates
the graph to discover promising candidates instead of comparing every row.
Its main controls separate construction from querying:

| Option | Stage | Role |
| --- | --- | --- |
| `m` | Build | Controls graph connectivity; default 16, with memory and page-capacity implications |
| `ef_construction` | Build | Controls construction search breadth; default 200, and must be at least `m` |
| `ef_search` | Query default stored on the index | Sets base query search breadth unless overridden |
| `rerank_mult` | Query default stored on the index | Sets the candidate multiplier for heap re-ranking; default 4 |

Larger construction and query budgets can improve candidate coverage at higher
cost. Their effect depends on the dataset. Repeated coordinates retain separate
row identities and visibility, even when an unquantized native graph shares
navigation for identical coordinates.

HNSW does not require IVF coarse-cluster training. A quantized HNSW index can
still require training for its compression model, especially PQ.

## 6. IVF: search selected vector lists

IVF groups vectors into inverted lists and probes a subset of those lists for
each query. Numeric coarse clustering uses k-means; BINARY Hamming uses majority
centers, while BINARY Jaccard uses sampled medoids.

```sql
CREATE TABLE vector_documents_ivf (
    id BIGINT PRIMARY KEY,
    embedding VECTOR(3, FLOAT16) NOT NULL
);

INSERT INTO vector_documents_ivf
    SELECT id, embedding FROM vector_documents;

CREATE INDEX idx_documents_ivf ON vector_documents_ivf(embedding)
    USING IVF WITH (metric=l2, nlist=2, nprobe=1, quantization=none);

SELECT /*+ IVF_NPROBE(2) */ id,
       L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]')) AS distance
FROM vector_documents_ivf
ORDER BY distance
LIMIT 2;
```

`nlist` controls the number of lists; `0` requests automatic selection.
`kmeans_iters` and `train_sample` control coarse training. `nprobe` controls
query coverage and is clamped to `1..nlist` at execution. More probes examine
more lists and usually require more scoring work.

Full probing of an unquantized IVF index scores all lists using native values.
For a quantized IVF index, a finite candidate pool can discard neighbors before
heap re-ranking even if every list is probed. Use an exact scan to establish
the reference result when measuring recall.

Choose between HNSW and IVF by measuring the required recall, query latency,
build time, update workload, and memory or storage cost on the same data.
Neither algorithm has a universal performance advantage.

## 7. Quantization: compress index payloads

Index quantization is independent of the column datatype. A FLOAT32 column can
keep FLOAT32 heap values while its HNSW or IVF index stores compressed codes.
Supported numeric datatypes accept `none`, `sq8`, `sq4`, `pq`, and `bq`;
BINARY indexes accept only `none`.

| Mode | Representation | Main considerations |
| --- | --- | --- |
| `none` | Native index payload | Default; eligible FLOAT32 HNSW paths can still use auxiliary SQ8 navigation |
| `sq8` | 8-bit scalar codes per dimension | Shared affine calibration; out-of-range updates retain lossless exceptions |
| `sq4` | Packed 4-bit scalar codes | Fewer code bytes; the distribution and calibration affect ranking quality |
| `pq` | Subspace codebook identifiers | Requires training; codebook size and subspace layout affect ranking quality |
| `bq` | Centered sign bits plus amplitude and norm corrections | Compact numeric representation; scoring is more than raw Hamming distance |

For example, add an explicitly compressed index:

```sql
CREATE INDEX idx_documents_sq8 ON vector_documents(embedding)
    USING HNSW WITH (metric=l2, quantization=sq8, rerank_mult=8);
```

This demonstrates index creation; a three-row table is too small for a useful
space, speed, or recall comparison. Real index size includes graph edges or
lists, row identities, page overhead, shared models, and exception records.
Original heap vectors remain stored separately. INT8 with SQ8, for example,
does not necessarily save space.

PQ uses `pq_m` subspaces with 4- or 8-bit codebook identifiers. Subspaces are
balanced and contiguous; the dimension need not be divisible by `pq_m`.
An empty PQ index, or one with fewer than `2^pq_bits` training rows, remains
`PENDING_TRAINING` and serves queries through exact scans until sufficient
committed data is available.

Compressed rankings and final SQL recall measure different stages. Final
results use original visible heap values to re-rank the retained candidates.
Increasing the search breadth or re-ranking pool can mask differences in
compressed rankings while increasing heap reads. There is no guaranteed recall
ordering among the quantization modes, and `none` is not an exact-search
reference. See [quantization and recall](vector-indexes.md#interpreting-recall).

## 8. Filters, transactions, and query shape

Business filters stay in SQL:

```sql
SELECT id, content,
       L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]')) AS distance
FROM vector_documents
WHERE category = 'guide'
ORDER BY distance
LIMIT 2;
```

The executor checks heap MVCC visibility and residual filters before returning
results. Inserts, updates, and deletes participate in transactions, including
commit and rollback. A selective filter can remove many ANN candidates; the
optimizer may prefer an exact scan over a small filtered input.

By default, iterative scanning widens the search when filters or visibility
checks leave too few candidates. Its budgets still apply, and exact completion
can be necessary. `strict_order` preserves distance order among returned
candidates; it does not guarantee global exact TopK. Setting iterative scanning
to `off` can leave fewer than the requested number of rows.

JOIN, aggregation, DISTINCT, and window queries require their full relational
input. They do not inherit the simple single-table ANN truncation described
above. Old snapshots and unusable indexes can also require an exact path.
Inspect the actual plan for the complete query.

## 9. Tune and verify retrieval

Search effort resolves from a query hint, then an explicit session setting,
then the index query default, then the built-in default. A session initialized
away from the built-in default also takes precedence over the index default.
Explicitly setting a session value to the built-in default still overrides the
index. An explicit session re-ranking multiplier overrides index `rerank_mult`.

```sql
SET SESSION mydb_hnsw_ef_search = 200;
SET SESSION mydb_hnsw_rerank_mult = 8;

EXPLAIN SELECT /*+ HNSW_EF(400) */ id
FROM vector_documents
ORDER BY L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]'))
LIMIT 2;
```

HNSW's initial effective breadth is
`max(base_ef, mydb_hnsw_ef_dynamic_factor * k, k)`, where `k` includes OFFSET
and the dynamic factor defaults to 4. Thus base ef 128 with LIMIT 100 normally
starts at ef 400. Set the factor to 0 to disable dynamic scaling; iterative
scanning can still widen the search. Ef is a frontier size, not a hard limit
on distance evaluations or physical page reads.

The default IVF `nprobe` is 8, and both re-ranking multipliers default to 4.
Minimum pool sizes and memory budgets also affect candidate retention, so
`rerank_mult * k` alone does not describe every effective pool. Hints can
bypass the small-table heuristic, but cannot make an incompatible or
pending-training index usable.

Use the same query with `NO_VECTOR_INDEX` as an exact baseline:

```sql
SELECT /*+ NO_VECTOR_INDEX */ id,
       L2_DISTANCE(embedding, TO_VECTOR('[1,0,0]')) AS distance
FROM vector_documents
ORDER BY distance
LIMIT 2;
```

For each representative query, compare ANN result IDs with the exact TopK on
the same data snapshot and filters. Recall@k is the fraction of exact neighbors
found by ANN. Define how equal-distance ties are handled before comparing IDs.
Measure latency, throughput, index size, and heap/page reads at the required
recall. Start with an exact baseline, measure an unquantized index, then add
compression and tune search effort. The
[query tuning reference](vector-indexes.md#query-tuning) lists all controls.

## 10. Binary signatures and payload interchange

BINARY vectors store bits rather than numeric embedding coordinates. Dimensions
need not be multiples of eight. Text construction uses a list of zeros and ones:

```sql
CREATE TABLE vector_signatures (
    id BIGINT PRIMARY KEY,
    signature VECTOR(9, BINARY) NOT NULL
);

INSERT INTO vector_signatures VALUES
    (1, TO_VECTOR('[1,0,1,0,0,0,0,0,1]', 'BINARY')),
    (2, TO_VECTOR('[1,0,0,0,0,0,0,0,0]', 'BINARY'));

CREATE INDEX idx_signatures ON vector_signatures(signature)
    USING HNSW WITH (metric=hamming);

SELECT id, HAMMING_DISTANCE(
    signature, TO_VECTOR('[1,0,1,0,0,0,0,0,1]', 'BINARY')) AS distance
FROM vector_signatures
ORDER BY distance
LIMIT 2;
```

The distances are `0` and `2`. For native binary interchange, bit `i` occupies
bit `i % 8` of byte `i / 8`, least-significant bit first. Unused high bits of
the final byte must be zero.

```sql
SELECT HEX(VECTOR_TO_BINARY(signature)) AS payload
FROM vector_signatures WHERE id = 1;
-- 0501

SELECT VECTOR_TO_STRING(VECTOR_FROM_BINARY(X'0501', 'BINARY', 9)) AS signature;
-- [1,0,1,0,0,0,0,0,1]
```

`VECTOR_TO_BINARY` returns the native payload without a datatype or dimension
envelope. Preserve both separately and supply them to `VECTOR_FROM_BINARY`.
Numeric payloads use little-endian floating-point representations or signed
INT8 bytes as appropriate for the datatype.

## 11. SQL clients and diagnostics

By default, direct VECTOR results use MySQL's native VECTOR wire type with
little-endian FLOAT32 coordinates, including values stored in other native
datatypes. The `mysql` client can display those bytes as hex. For readable
output, call `VECTOR_TO_STRING` or change the session text result format:

```sql
SET SESSION vector_result_format = 'TEXT';
SELECT embedding FROM vector_documents WHERE id = 1;
-- [1,0,0]
SET SESSION vector_result_format = 'NATIVE';
```

This changes text-protocol result formatting; stored vectors and calculations
retain their native values. Prepared-statement binary results and cursor fetches
always use the native wire format. New or reset sessions default to `NATIVE`.
Use explicit native payload functions for datatype-preserving interchange.

Inspect the persisted configuration and current runtime state:

```sql
SHOW CREATE TABLE vector_documents;

SELECT INDEX_NAME, ALGO, METRIC, QUANTIZATION, QUANTIZATION_STATE,
       LOADED, RERANK_MULT, CANDIDATES_RETURNED, HEAP_RERANKS, EXACT_FALLBACKS
FROM information_schema.mydb_vector_indexes
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vector_documents';
```

`SHOW CREATE TABLE` emits persisted `WITH` options. `QUANTIZATION_STATE` shows
`READY` or `PENDING_TRAINING` for loaded quantized indexes; a cold index has
`LOADED=0` and no runtime training state. Runtime counters describe loaded
accelerators rather than durable lifetime totals. Candidate, heap re-ranking,
and fallback counters are collected by quantized accelerators; zero values on
an unquantized index do not imply that it has served no queries.

## 12. Storage, maintenance, and supported boundaries

`MYDB_VECTOR_STORAGE=paged` is the default. HNSW and IVF payloads use the shared
WAL-protected vector container and buffer pool, allowing index payloads to
exceed memory capacity. `memory` keeps native or compressed payloads resident.
Build buffers, graph structures, caches, and query work still require memory.
Paged HNSW and IVF are the implemented algorithms; disk residency does not
imply a complete DiskANN implementation.

Vector indexes are persistent objects. Normal recovery loads persisted
structures and models and completes committed changes from WAL. Missing or
damaged index files produce explicit errors rather than an automatic heap
rebuild or an exact-query fallback for storage failure. Native physical
replication and backup retain the required index state and recovery dependencies.

Background maintenance reclaims obsolete entries and handles requested
rebuilds or retraining. `OPTIMIZE TABLE` explicitly reconstructs vector indexes.
Datatype or dimension changes require COPY rewriting and index reconstruction;
invalid conversions fail atomically. Failed private replacements preserve the
previous valid index. See [storage and recovery](vector-indexes.md#storage-and-recovery)
for the full lifecycle.

Both index algorithms require exactly one fixed-dimension `NOT NULL` VECTOR
column in a non-unique index. Configure parameters using `WITH (...)` in
`CREATE INDEX`, inline table indexes, or `ALTER TABLE ADD INDEX`. `COMMENT`
contains descriptive text. Unknown, duplicate, misplaced, and incompatible
options are errors. Defaults are captured at index creation and are visible
through `SHOW CREATE TABLE`.

Further reading:

- [Vector datatypes and indexes](vector-indexes.md): complete options, quantization, limits, and persistence.
- [Vector functions](function/vector/README.md): construction, conversion, distance, normalization, and slicing.
- [Vector query hints](hints.md#8-vector-search-hints): per-query controls and exact-scan comparisons.
- [Startup and runtime configuration](configuration.md): storage, build budgets, caches, and maintenance.
- [Embedding calls](ai.md) and [hybrid search](hybrid-search.md): model integration and combining retrieval branches.
