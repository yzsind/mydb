# HNSW benchmark: mydb and PostgreSQL on Cohere medium 1M

**Test date:** November 12, 2026 · **Scope:** import, index construction, and Top-100 cosine search on one million 768-dimensional FLOAT32 vectors.

With HNSW candidate memoization disabled in mydb, median throughput at 16 clients was **3,005.3 queries/s** for mydb and **1,968.5 queries/s** for PostgreSQL with pgvector: a **1.53× throughput ratio** in this workload. Recall@100 was **94.283%** and **94.341%**, respectively. These recall levels are close, but not identical. The comparison uses the configurations and repeated-query workload documented below.

Both engines stored native FLOAT32 vectors. mydb used `quantization=none`; no SQ8, SQ4, PQ, BQ, half-precision, or binary-vector index variant was tested. The report separates graph-search measurements from the additional benefit of mydb’s default candidate memoization.

## Test environment

| Item | Configuration |
| --- | --- |
| Host | One KVM cloud VM; both engines and clients on the same host |
| CPU | Intel Xeon Platinum 8369B @ 2.70 GHz; 8 cores / 16 logical CPUs; one NUMA node |
| RAM | 123.7 GiB; no swap |
| Storage | Alibaba NVMe block device, shared by the test data and other host services |
| OS | Ubuntu 24.04.5 LTS; Linux 6.8.0-147-generic; x86-64 |
| mydb | 8.0.46-mydb-1.2.1-20261112 |
| PostgreSQL / pgvector | 18.6 / 0.8.7 |
| Build | `cargo build --release --bin mydb`; rustc 1.95.0 (verified from the executable's ELF metadata); existing compilation profile unchanged |
| Execution window | 2026-11-12T19:54:58+08:00 to 2026-11-12T20:17:11+08:00 (UTC+08:00) |

The engines ran sequentially through loopback TCP, using dedicated instances and newly initialized data directories. Existing database services on the host remained running. This was not an otherwise empty dedicated host, and there was no per-engine cgroup memory cap or CPU affinity.

mydb used `MYDB_MEMORY_TARGET=20G`, `MYDB_DURABILITY=strict`, and `MYDB_VECTORIZED=on`. PostgreSQL used:

```text
shared_buffers = 20GB
work_mem = 64MB
maintenance_work_mem = 1GB
max_parallel_maintenance_workers = 15
max_parallel_workers = 16
max_worker_processes = 16
max_wal_size = 4GB
fsync = on
synchronous_commit = on
full_page_writes = on
```

All listed PostgreSQL values were checked in `pg_settings`; up to **15 parallel workers** were observed during index construction. mydb’s advisory working-set target and PostgreSQL’s shared buffers control different allocations. Equal nominal settings do not establish equal process RSS, equal total memory use, or a common hard memory budget.

## Dataset and procedure

The runner was `scripts-internal/run_vector_bench.sh`, using its `cohere-medium-1m` dataset option. The source files were `train.parquet`, `test.parquet`, and `neighbors.parquet` from the public Cohere medium dataset hosted in Zilliz’s benchmark bucket. Training vectors were loaded without reducing the row count or dimensionality.

| Parameter | Value |
| --- | --- |
| Indexed rows | 1,000,000 |
| Vector dimensions and storage | 768; FLOAT32 |
| Distance / requested results | Cosine / Top-100 |
| Query set | All 1,000 queries in `test.parquet` |
| Ground truth | First 100 neighbor IDs per matching query ID in `neighbors.parquet` |
| HNSW construction | `m=16`, `ef_construction=64` on both engines |
| mydb search | Configured ef=200; resolved ef=400; `mydb_hnsw_rerank_mult=1`; rerank pool=100 |
| PostgreSQL main search | `hnsw.ef_search=200` |
| PostgreSQL sensitivity checks | ef=128 and ef=400 on the same index |
| Clients / duration | 1, 2, 4, 8, 16; 10 seconds per level per round |
| Mutations / brute force | Disabled |

Recall@100 is the mean of `|returned IDs ∩ ground-truth IDs| / 100` over all 1,000 queries. Each query-quality pass follows one separately timed first query and three warmup executions. Concurrent measurement then repeats the same query corpus, with one independent client process and database connection per client. Client `i` starts at query `i` and cycles through the corpus; requests are correlated rather than independently randomized.

Import and index construction were measured once per engine. Main PostgreSQL retrieval and default-memo mydb retrieval were measured in three rounds on the same indexes, always mydb before PostgreSQL. After those rounds and PostgreSQL sensitivity checks, mydb was gracefully restarted with `MYDB_HNSW_QUERY_MEMO=OFF` and its existing index was measured in three additional rounds. The first of those three rounds opened the existing index; subsequent rounds kept that instance running.

The main graph-search comparison below uses these three memo-disabled mydb rounds and the three main PostgreSQL rounds. A candidate-memo hit reuses graph candidates; heap visibility and exact row scoring still execute. Disabling this memo removes repeated-query candidate reuse while retaining index mappings, ordinary data caching, and exact norm caches. No OS page cache was dropped. These are warm, read-only retrieval measurements, not cold-cache or sustained mixed-write tests.

The indexed query shapes were:

```sql
-- mydb
CREATE TABLE vbench (id INT PRIMARY KEY, emb VECTOR(768) NOT NULL);
CREATE INDEX idx_emb ON vbench(emb) USING HNSW
    WITH (metric=COSINE, m=16, ef_construction=64, quantization=none);
SET SESSION mydb_hnsw_ef_search=200;
SET SESSION mydb_hnsw_rerank_mult=1;
SELECT id FROM vbench
ORDER BY DISTANCE(emb, STRING_TO_VECTOR(%s), 'COSINE') LIMIT 100;

-- PostgreSQL + pgvector
CREATE TABLE vbench (id INT PRIMARY KEY, emb vector(768) NOT NULL);
CREATE INDEX idx_emb ON vbench USING hnsw (emb vector_cosine_ops)
    WITH (m=16, ef_construction=64);
SET hnsw.ef_search=200;
SELECT id FROM vbench ORDER BY emb <=> %s::vector LIMIT 100;
```

`%s` denotes the client-bound text representation of a query vector; extension creation and database initialization precede these statements.

## Import and HNSW construction

| Engine | Import time (s) | Imported rows/s | CREATE INDEX (s) | HNSW main file size (GiB) |
| --- | ---: | ---: | ---: | ---: |
| mydb, `quantization=none` | 35.985 | 27,789.0 | 92.017 | 3.815 |
| PostgreSQL + pgvector | 323.261 | 3,093.5 | 258.007 | 3.814 |

Import time includes Parquet batch reading, client conversion and encoding, protocol transfer, server processing, and durable batch commits. Batch size was 100 rows, with a primary key present during loading. mydb used native FLOAT32 binary prepared statements through mysql-connector-python; PostgreSQL used psycopg2 and multi-row text INSERT statements. The driver formats PostgreSQL’s imported vectors, and both engines’ query vectors, with six significant decimal digits; mydb’s binary import preserves the original FLOAT32 payload. The imported coordinates are therefore not guaranteed to be bit-identical across engines, and recall is measured against the dataset’s original ground truth. PostgreSQL COPY was not tested. These are end-to-end results for the runner’s selected import paths, not a comparison using identical loading protocols or a claim about each engine’s maximum ingest throughput.

CREATE INDEX is complete synchronous DDL wall time as measured by the client. mydb’s log confirmed the `FullMemory` build path. PostgreSQL construction used the specified 1 GiB maintenance budget. Index size is the logical `.hvec` file length for mydb and `pg_relation_size(idx_emb)` for PostgreSQL; companion metadata, other relation forks, table, primary-key, WAL, and temporary files are excluded. GiB means 2³⁰ bytes.

The post-run sample of 100 rows (76,800 coordinates) found 74,202 coordinates differing between the two loaded copies; maximum absolute difference was 5.01e-06. This sample checks serialization effects; it is not a full-table equality check.

## Search quality and single-connection latency

Values are medians of the three complete 1,000-query passes for each main configuration. Latencies include SQL execution, loopback transport, and fetching all 100 IDs.

| Configuration | Resolved ef | Recall@100 | Mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mydb, candidate memo OFF | 400 | 94.283% | 2.664 | 2.652 | 3.196 | 3.366 |
| PostgreSQL + pgvector | 200 | 94.341% | 5.613 | 5.515 | 7.162 | 11.941 |

Each recall value was unchanged across its three rounds. Recall is measured in the complete query passes, not recomputed during the timed concurrent loops. Equal ef values do not imply equal search work across implementations; mydb dynamically raises the configured ef=200 to 400 for this Top-100 query. PostgreSQL uses ef=200 in the main comparison. Both use approximate candidate selection; a recall-adjusted result should be read together with its measured recall.

## Concurrent graph-search throughput

Each cell reports **median QPS (minimum–maximum)** across three 10-second rounds. The ratio divides the two medians. Higher is better.

| Clients | mydb, candidate memo OFF | PostgreSQL + pgvector | Throughput ratio |
| ---: | ---: | ---: | ---: |
| 1 | 377.3 (375.4–382.8) | 183.4 (183.2–186.5) | 2.06× |
| 2 | 777.5 (777.3–787.9) | 337.6 (336.9–342.9) | 2.30× |
| 4 | 1,406.4 (1,364.0–1,434.8) | 658.2 (650.2–658.2) | 2.14× |
| 8 | 2,438.2 (2,423.7–2,551.9) | 1,249.4 (1,236.0–1,260.9) | 1.95× |
| 16 | 3,005.3 (3,000.5–3,016.4) | 1,968.5 (1,941.2–1,985.4) | 1.53× |

Latency values below are medians of each round’s statistics, in milliseconds. Percentiles are not pooled across rounds.

| Configuration | Clients | Mean | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| mydb, candidate memo OFF | 1 | 2.647 | 2.637 | 3.162 | 3.345 |
| mydb, candidate memo OFF | 16 | 5.311 | 5.068 | 7.836 | 9.349 |
| PostgreSQL + pgvector | 1 | 5.449 | 5.396 | 6.835 | 10.106 |
| PostgreSQL + pgvector | 16 | 8.117 | 8.018 | 10.195 | 15.450 |

## Sensitivity to search effort and candidate memoization

PostgreSQL sensitivity runs reused the same graph, table, and query set after the main rounds. ef=128 and ef=400 each have one 10-second window per client level; the ef=200 row shows the three-round median. They are not equally replicated configurations.

| PostgreSQL ef | Recall@100 | QPS, 1 client | QPS, 16 clients | p99 at 16 clients (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 91.232% | 257.8 | 2,756.4 | 11.243 |
| 200 | 94.341% | 183.4 | 1,968.5 | 15.450 |
| 400 | 97.198% | 104.6 | 1,128.2 | 26.622 |

The higher-recall PostgreSQL configuration must not be treated as an equal-quality comparison with mydb’s main setting.

For the runner’s repeated-query workload, mydb’s default candidate memo produced the following separate results. Both rows use the same index, ef, rerank pool, and query corpus; only the candidate memo was disabled for the second series, which followed a process restart. Values are medians of three rounds.

| mydb candidate memo | Recall@100 | QPS, 1 client | QPS, 16 clients | p99 at 16 clients (ms) |
| --- | ---: | ---: | ---: | ---: |
| ON (default) | 94.283% | 1,454.2 | 10,562.4 | 2.625 |
| OFF (main comparison) | 94.283% | 377.3 | 3,005.3 | 9.349 |

The memo-enabled figures describe repeated requests whose candidates can be reused. They should not be extrapolated to a stream of previously unseen vectors or cited as graph-traversal throughput.

## Validation and reproducibility

Final validation checked exactly 1,000,000 rows and 1,000,000 distinct IDs in each engine, with ID range 0–999999. Samples at IDs 0, 499999, and 999999 had 768 dimensions. mydb’s index metadata confirmed `quantization=none`, 1,000,000 live nodes, zero quantizer/compressed bytes, and zero SQ8-covered nodes. mydb EXPLAIN selected HNSW with ef=400 and rerank pool=100; PostgreSQL EXPLAIN ANALYZE selected `Index Scan using idx_emb` and returned 100 rows at each tested ef. Every reported runner invocation exited successfully.

The source on the test server had no Git metadata. Against reference commit `9eff7d4c742c3c604f8082d5721eacbd1dee57fc`, hashes matched for all 1,100 captured tracked source and build files except `src/engine/executor/dispatch/setop.rs`, which contained an additional `drop(push);` before `drop(key_cache);`. The source manifest and delta were retained. The tested executable, rather than an unverified Git checkout label, identifies this build:

```text
mydb executable SHA-256: f1064570b1c40ef565383ae1f81802a170971dc6b0294eba258303fa7d2d5442
run_vector_bench.sh SHA-256: fdc482988d322837da4646ee33c3a3b649a789b72750ac0258f0552f17a77448
vector_bench.py SHA-256: 56860d14587ed4ddc5c909a09db6d568cbf14ffd91a552b3bbf000e5c67bd075
```

Dataset identity:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `train.parquet` | 3,131,995,162 | `de6b84eb08fc5203a56eef619970a14f5cae08d0a0ae0bcd23b8058d7045f381` |
| `test.parquet` | 3,133,165 | `a2ec76c907da22e5533ce31c1105e4fd4d3c4597915a7771d5e41897a95ecbed` |
| `neighbors.parquet` | 3,704,127 | `30e7467766be3e2a6b167df16388a4a35e38fa6cc9e593fb659cce81ffde0940` |

The main runner configuration is reproduced below. It assumes an isolated PostgreSQL instance already started with the settings above, and the benchmark Python dependencies installed. Paths and ports are examples; use empty dedicated locations for a full import run.

```bash
export MYDB_MEMORY_TARGET=20G MYDB_DURABILITY=strict MYDB_VECTORIZED=on
export MYDB_PORT=4838
export MYDB_BENCH_DATA_DIR=/tmp/mydb/cohere-hnsw/mydb-data
export VBENCH_MYDB=1 VBENCH_MYDB_INDEX=hnsw
export VBENCH_MYDB_QUANTIZATIONS=none VBENCH_MYDB_LOAD=binary
export VBENCH_MYDB_RERANK_MULT=1
export VBENCH_MYSQL=0 VBENCH_SEEKDB=0 VBENCH_PGVECTORSCALE=0
export VBENCH_PG=1 VBENCH_PG_AUTOSTART=0
export PG_HOST=127.0.0.1 PG_PORT=15432 PG_USER=postgres
export VBENCH_MUTATE=0 VBENCH_BRUTE=off
export VBENCH_QUERIES=1000 VBENCH_K=100 VBENCH_BATCH=100
export VBENCH_EF_SEARCH=200 VBENCH_HNSW_M=16
export VBENCH_HNSW_EF_CONSTRUCTION=64
export VBENCH_CONCURRENCY="1 2 4 8 16" VBENCH_DURATION=10
export VBENCH_KEEP_SERVER=1 VBENCH_KEEP_DATA=0
export VBENCH_OUT=/tmp/mydb/cohere-hnsw/main
bash scripts-internal/run_vector_bench.sh --dataset cohere-medium-1m
```

Repeat retrieval with `VBENCH_KEEP_DATA=1 VBENCH_USE_RUNNING_MYDB=1` and a distinct output directory per round. For the memo-disabled series, gracefully stop only the dedicated mydb test process, start it through the same runner with `MYDB_HNSW_QUERY_MEMO=OFF VBENCH_KEEP_DATA=1 VBENCH_USE_RUNNING_MYDB=0 VBENCH_PG=0`, then reuse that running instance for two further rounds. No import or index rebuild is performed in the memo-disabled series. PostgreSQL sensitivity runs use `VBENCH_MYDB=0 VBENCH_USE_RUNNING_MYDB=0 VBENCH_PG=1 VBENCH_KEEP_DATA=1` with ef=128 or 400.

## Per-round throughput

| Series | Round | QPS at 1 client | 2 clients | 4 clients | 8 clients | 16 clients |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mydb, memo OFF | 1 | 382.8 | 787.9 | 1,364.0 | 2,551.9 | 3,005.3 |
| mydb, memo OFF | 2 | 377.3 | 777.3 | 1,406.4 | 2,438.2 | 3,016.4 |
| mydb, memo OFF | 3 | 375.4 | 777.5 | 1,434.8 | 2,423.7 | 3,000.5 |
| PostgreSQL, ef=200 | 1 | 183.4 | 342.9 | 650.2 | 1,236.0 | 1,968.5 |
| PostgreSQL, ef=200 | 2 | 186.5 | 337.6 | 658.2 | 1,249.4 | 1,985.4 |
| PostgreSQL, ef=200 | 3 | 183.2 | 336.9 | 658.2 | 1,260.9 | 1,941.2 |
| mydb, memo ON | 1 | 1,632.8 | 2,895.8 | 5,107.1 | 8,551.2 | 10,562.4 |
| mydb, memo ON | 2 | 1,454.2 | 2,748.5 | 4,939.7 | 8,469.6 | 10,587.2 |
| mydb, memo ON | 3 | 1,423.5 | 2,716.1 | 4,897.8 | 8,394.1 | 10,532.5 |

## Interpretation limits

This is one dataset, one host, and one graph construction per engine. Three short retrieval rounds expose observed variation; they do not establish confidence intervals, long-run stability, or the distribution of independently rebuilt graphs. Engine order was fixed. SQL clients and databases share CPU and storage with other services, and memory residency was not equalized. Data loading used different protocols. Ordinary engine and OS caches remained active. The results do not cover filtered ANN, concurrent writes, recovery, replication, or alternative datasets and dimensions. mydb remains an early product under development; these measurements are not a production-readiness claim.

## Follow-up: memo-disabled HNSW query optimization

A follow-up test on November 12 used the same Cohere table and HNSW index to compare the original executable with a query-only optimization. No data import or graph rebuild was performed. Candidate memoization remained OFF, effective ef remained 400, and the rerank pool remained 100. PostgreSQL was not rerun for this follow-up.

The change reuses exact distances already calculated within each exact FLOAT32 mapped graph search, while still checking candidate row identity and deletion state. It also selects Top-K before sorting the retained results. Quantized or auxiliary-quantized navigation keeps its original full candidate rescoring and sorting, and the SQL executor keeps heap MVCC checks and visible-value reranking. The exact and approximate navigators are compiled as separate constant-generic paths. The SIMD kernels, prefetch strategy, compilation profile, and index format are unchanged.

Each executable ran three rounds, alternating original/optimized, with an instance restart and complete query-set evaluation before each round’s concurrent measurements. Each client level ran for 10 seconds. Values below are median QPS (minimum–maximum).

| Clients | Original | Optimized | Change |
| ---: | ---: | ---: | ---: |
| 1 | 379.0 (375.9–379.9) | 399.0 (398.5–400.6) | +5.29% |
| 2 | 778.6 (773.8–785.2) | 812.3 (808.6–819.3) | +4.33% |
| 4 | 1,413.3 (1,400.8–1,425.2) | 1,491.5 (1,411.4–1,502.8) | +5.54% |
| 8 | 2,525.9 (2,499.0–2,565.6) | 2,631.3 (2,564.4–2,647.4) | +4.17% |
| 16 | 3,013.4 (2,998.3–3,037.4) | 3,157.8 (3,152.5–3,162.8) | +4.79% |

Both executables produced **identical ordered Top-100 IDs for all 1,000 queries** and the same **94.283% Recall@100**. No individual query lost recall. The index file’s SHA-256 was unchanged. At 16 clients, median p99 latency was 9.374 ms before and 9.056 ms after the change.

Additional L2 checks reused three synthetic indexes with 10,000 rows at 128, 384, and 1536 dimensions. Each executable ran two rounds of 1- and 8-client tests, with 100 Top-10 queries and five seconds per level. All 300 synthetic queries also returned identical ordered Top-10 IDs before and after the change; recall was unchanged in every configuration. These short checks complement the regression tests; they do not establish performance for all dimensions or workloads.

Both executables identify rustc 1.95.0 in their ELF metadata. The optimized executable SHA-256 is `dfcac1a719c8e74fb419e54ec9bbc5c91b1826102f70b5339abb8c56b2bc6b12`. The original build and PostgreSQL measurements in the preceding tables are retained as dated observations; this follow-up does not establish a new simultaneous PostgreSQL comparison.
