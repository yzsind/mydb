# HNSW benchmark: MyDB and PostgreSQL on Cohere 1M, November 14

**Measurement date:** November 14, 2026. This page restores the HNSW comparison
published in README commit
[`d967fb16`](https://github.com/yzsind/mydb-archive/commit/d967fb16625d4f21c949077dfd13154f4d4c1539).
It is a historical result record reconstructed from Git, not a new benchmark run.
The measured implementation belongs to the `8.0.46-mydb-1.2.1` development
series; the product version was later unified to 1.0.0 (a version-metadata
change only).

## Reported results

The workload used one million 768-dimensional FLOAT32 vectors, cosine distance,
and Top-100 retrieval. Throughput is higher-is-better; build time and latency
are lower-is-better. Ratios below are rounded from the published values.

| Measurement | MyDB | PostgreSQL 18.6 + pgvector 0.8.7 | Ratio |
| --- | ---: | ---: | ---: |
| Search, 16 clients | 5,200 queries/s | 1,954 queries/s | 2.7x throughput |
| Search p99 latency, 16 clients | 5.6 ms | 14.7 ms | — |
| Search, 1 client | 670 queries/s | 179 queries/s | 3.7x throughput |
| Recall@100 | 94.8% | 94.4% | — |
| HNSW index build | 53 s | 118 s | 2.2x speedup |

Both engines used the same 16-vCPU host, `m=16`, `ef_construction=64`,
`ef_search=200`, unquantized FLOAT32 indexes, and durable commits. Recall
levels were close but unequal. Unquantized storage does not mean every graph
navigation distance uses full FLOAT32 coordinates: MyDB's optimization uses
auxiliary per-dimension 8-bit scan codes and exactly rescores the candidate pool.

## Source commits and optimization

The implementation commit
[`e2cbe763`](https://github.com/yzsind/mydb-archive/commit/e2cbe763e9b76f51c2ec3be791b282cf590ef10e),
"Navigate HNSW on per-dimension scan codes at build and query time," records
these before/after measurements for Cohere 1M, 768-dimensional cosine,
`m=16`, `ef_construction=64`, ef=200, and 16 vCPUs:

| Measurement | Before | After |
| --- | ---: | ---: |
| HNSW index build | 88.8 s | 53 s |
| Search, 1 client | 532 queries/s | 676 queries/s |
| Search, 16 clients | 4,030 queries/s | 5,220 queries/s |
| Recall@100 | 94.79% | 94.77% / 94.86% |

These implementation-commit observations corroborate the approximately
5,200 queries/s README summary. They are recorded separately rather than
treated as additional rounds of the PostgreSQL comparison.

The optimization navigates applicable unquantized paged FLOAT32 HNSW indexes
using per-dimension scan codes, with exact candidate rescoring. It also uses
scan codes during cold construction, while neighbor selection, pruning, and
repair retain exact distances. The commit includes batched SIMD navigation,
incremental WAL writeback, and prepared multi-row INSERT improvements.
`MYDB_HNSW_NAV_CODE=0` and `MYDB_HNSW_BUILD_NAV_CODE=0` disable the respective
query and cold-build navigation paths.

The preceding default-search change in
[`1fc3824b`](https://github.com/yzsind/mydb-archive/commit/1fc3824bf4bccde3384c10d99d0e962c6a356d23)
lowered the dynamic ef factor to 2. For Top-100 and configured ef=200, that
default resolves to ef=200. The earlier November 12 experiment used
effective ef=400 and a different graph/build; its settings must not be copied
onto this comparison.

## Reporting limits

The historical README summary does not specify candidate-memo settings,
warmup, measurement duration, round counts, aggregation method, per-round
results, process memory limits, or executable hashes. The implementation
commit identifies the optimization but does not supply a complete run manifest
for the PostgreSQL comparison. This record therefore does not label the
5,200 queries/s result as memo-OFF, a three-round median, or a measurement of
the current release. It also does not inherit the November 12 experiment's
memory settings, loading precision, or query replay details.

The [public query replay tool](../scripts/vector_query_bench.py) can measure
query throughput and recall on an already loaded table. A fresh reproduction
should record the exact build, graph, effective search settings, memo state,
warmup, rounds, and resource limits for each engine. The published values above
are historical development observations for this workload, not a guarantee
across datasets, recall targets, concurrent writes, or releases.
