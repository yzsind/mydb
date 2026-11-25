# Cohere 1M HNSW benchmark: MyDB and pgvector

**Measured November 24, 2026.** This benchmark compares MyDB with PostgreSQL
18.6 and pgvector 0.8.7 on one million 768-dimensional FLOAT32 vectors, using
cosine distance and Top-100 retrieval. MyDB query-candidate memoization is
disabled throughout.

On the same 16-vCPU host, MyDB delivered **2.39x the 16-client throughput**
with a 32 GiB database memory limit and **3.29x with a 2 GiB limit**.
Recall@100 is reported alongside performance; the engines were tested at the
same search parameters, rather than tuned to identical recall.

## Results: 32 GiB memory limit

| Measurement | MyDB | PostgreSQL + pgvector | MyDB advantage |
| --- | ---: | ---: | ---: |
| Search, 1 client | 631.83 queries/s | 197.73 queries/s | 3.20x throughput |
| Search, 16 clients | 5,121 queries/s | 2,142 queries/s | 2.39x throughput |
| Search p99, 16 clients | 5.63 ms | 10.21 ms | — |
| Recall@100 | 94.741% | 94.395% | — |
| HNSW index build | 55.14 s | 141.92 s | 2.57x build speedup |

## Results: 2 GiB memory limit

| Measurement | MyDB | PostgreSQL + pgvector | MyDB advantage |
| --- | ---: | ---: | ---: |
| Search, 1 client | 26.58 queries/s | 9.20 queries/s | 2.89x throughput |
| Search, 16 clients | 458 queries/s | 139 queries/s | 3.29x throughput |
| Search p99, 16 clients | 50.33 ms | 197.26 ms | — |
| Recall@100 | 95.331% | 94.341% | — |
| HNSW index build | 132.73 s | 5008.86 s | 37.74x build speedup |

Both engines completed loading, index construction, and querying within their
2 GiB cgroup configurations, with swap disabled and no OOM kills. The raw
FLOAT32 coordinates alone occupy approximately 2.86 GiB, before graph and table
overhead. MyDB built the index in **132.73 seconds**, compared with **83.48 minutes**
for pgvector. Each memory profile used its own independently constructed graph.

## Test environment

| Component | Configuration |
| --- | --- |
| CPU | Intel Xeon Platinum 8369B, 16 vCPUs, 8 cores / 16 threads |
| Host | Ubuntu 24.04, Linux 6.8, x86_64; approximately 123 GiB RAM; NVMe storage |
| MyDB | 1.0.3 development snapshot, commit [`63df0c7b`](https://github.com/yzsind/mydb/commit/63df0c7bef15da7f5984800c755750d8ac95895b) |
| PostgreSQL | 18.6 with pgvector 0.8.7 |
| Memory enforcement | Separate cgroup v2 limits of 32 GiB or 2 GiB per database; swap disabled |
| Client placement | Local loopback, outside the database cgroup; clients and database share the 16 vCPUs |
| Durability | MyDB strict durability; PostgreSQL fsync, synchronous_commit, and full_page_writes enabled |

MyDB was built with Rust/Cargo 1.95.0 using `cargo build --release --bin mydb`
and the repository's release profile, without a target-cpu override.
The [machine-readable results](vector-cohere-1m-hnsw-benchmark-2026-11-24-results.json) include executable hashes,
resolved query plans, recall, and all three query rounds.

| Memory setting | 32 GiB profile | 2 GiB profile |
| --- | ---: | ---: |
| MyDB memory target | 24 GiB | 1.5 GiB |
| PostgreSQL shared_buffers | 8 GiB | 512 MiB |
| PostgreSQL maintenance_work_mem | 8 GiB | 512 MiB |
| PostgreSQL work_mem | 16 MiB | 16 MiB |

MyDB's index-build thread limit was 16. PostgreSQL allowed up to seven parallel
maintenance workers, in addition to the leader, with a global parallel-worker
limit of 16. Memory limits include database processes, shared memory, and file
cache charged to the cgroup. The 2 GiB limit is 2,147,483,648 bytes.

## Dataset and index settings

The workload uses `cohere_medium_1m`: 1,000,000 stored vectors, 768 dimensions,
1,000 public query vectors, and the supplied `neighbors.parquet` ground truth.
The [training vectors](https://assets.zilliz.com/benchmark/cohere_medium_1m/train.parquet),
[queries](https://assets.zilliz.com/benchmark/cohere_medium_1m/test.parquet), and
[ground truth](https://assets.zilliz.com/benchmark/cohere_medium_1m/neighbors.parquet)
are identified by SHA256 in the results file.

Both engines store the original FLOAT32 vector values, in the same input order,
with row IDs 0 through 999,999. MyDB uses binary prepared INSERTs and pgvector
uses binary COPY. Row counts, ID ranges, and sample vector bytes were verified.

| Search setting | Value |
| --- | --- |
| Distance / result size | Cosine / Top-100 |
| HNSW m | 16 |
| HNSW ef_construction | 64 |
| Effective HNSW ef_search | 200 |
| Index quantization | None; stored vectors are FLOAT32 |
| MyDB rerank multiplier / candidate pool | 1 / 100 |
| MyDB query-candidate memoization | Disabled: MYDB_HNSW_QUERY_MEMO=0 |

MyDB uses auxiliary 8-bit navigation codes and exact candidate rescoring.
An unquantized index therefore does not imply that every navigation distance
uses full FLOAT32 coordinates. Final SQL results determine recall.

## Measurement method

- Index-build time is the wall-clock duration of one `CREATE INDEX` on each
  freshly loaded database. Loading time is excluded.
- Query tests use the persisted index for the corresponding memory profile.
  Database file cache is released while the instance is stopped before each
  isolated query run; queries are then warmed inside the new cgroup.
- All 1,000 queries are executed to measure recall and warm the workload before
  timed replay. Each client additionally performs three warmup requests.
  Database buffer and file caches are warm; query-candidate memoization remains
  disabled.
- Query vectors are converted to six-significant-digit decimal text before
  timing. Both engines receive the same query values. SQL execution, request
  transmission, and fetching all 100 returned IDs are timed; client vector
  serialization is excluded.
- Each concurrency level uses three 10-second rounds with 1 or 16 independent
  client processes. Clients cycle through the query corpus in fixed order,
  starting at their client index; slow rounds may cover only part of the corpus.
  Recall always covers all 1,000 queries against the supplied Top-100 truth.
- Per-round QPS is total completed requests divided by the longest client
  runtime. Each reported query metric is the median of three rounds. For p99,
  each round pools client latency samples, and the table reports the median of
  those three p99 values.
- Clients use PyMySQL 2.2.8 for MyDB and psycopg 3.3.9 for PostgreSQL. MyDB
  connections use TLS 1.3; the isolated PostgreSQL instances use unencrypted
  loopback connections. These driver and transport costs are included.

The [public query replay tool](../scripts/vector_query_bench.py) accepts JSONL
records containing `id`, `vector`, and `neighbors`. After loading the table and
creating its HNSW index, run it against the selected database, for example:

```bash
export VECTOR_BENCH_PASSWORD='your-password'
python3 scripts/vector_query_bench.py \
  --engine mydb --host 127.0.0.1 --port 4837 --user root \
  --database cohere_retest --table vbench --column emb \
  --queries /tmp/mydb/cohere-queries.jsonl \
  --k 100 --ef 200 --dynamic-factor 2 \
  --preencode --precision 6 --clients 16 --duration 10 --rounds 3 \
  --output /tmp/mydb/cohere-mydb-c16.json
```

Set `MYDB_HNSW_QUERY_MEMO=0` in the **database server's** environment before
starting MyDB. Repeat with `--clients 1`; for pgvector, select `--engine postgres`
and its connection settings. Apply the chosen memory limit to the database
service, leaving the client outside that cgroup.

## Scope

These measurements cover a read-only, unfiltered cosine Top-100 workload on
one dataset and one host. Each memory profile has a separate graph, and recall
differs between engines and profiles. The results do not establish performance
at identical recall or under concurrent writes, filtered search, other vector
dimensions, or other datasets. They describe the identified development build.
