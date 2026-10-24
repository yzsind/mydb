# MyDB vs. MySQL performance comparison

[Back to the project README](../README.md#performance-evaluation)

Engineering reports dated **October 22–24, 2026** show MyDB's strongest measured
advantages over **MySQL Community 8.0.46** in analytical queries, read-heavy OLTP,
and bulk loading. The results below come from those reports' completed runs.
They cover different MyDB development builds, including 0.5.0 and 0.6.0, and
are not a benchmark of one final release or a guarantee for every workload.

## Transactions and point queries

Higher throughput is better; the ratio is MyDB throughput / MySQL throughput.

| Workload | Scale / clients | MyDB | MySQL 8.0.46 | Throughput ratio |
| --- | --- | ---: | ---: | ---: |
| sysbench point select | 100M rows / 20 | 316,968.29 queries/s | 174,565.90 queries/s | **1.82x** |
| sysbench read only | 100M rows / 20 | 14,323.26 transactions/s | 7,293.66 transactions/s | **1.96x** |
| sysbench read write | 100M rows / 20 | 4,610.91 transactions/s | 4,490.61 transactions/s | **1.03x** |
| TPC-C-derived New-Order | 10 warehouses / 20 | 122,224.0 committed/min | 109,180.3 committed/min | **1.12x** |

The October 23 sysbench report uses 10 tables of 10M rows, a 60-second warmup,
and one 600-second measurement per workload and engine. Read-only P95 event
latency was **2.43 ms vs. 4.74 ms**. The read/write figure is the full-run
average: MyDB's first-minute mean was 6,009.73 TPS and its last-minute mean was
3,777.07 TPS. These OLTP runs precede the subsequent loading and concurrency
fixes and were not rerun on those newer builds.

The October 22 TPC-C-derived result uses Round 2 with Performance Schema enabled
on both engines, a 120-second warmup, and one 600-second measurement. Committed
New-Order throughput excludes expected rollbacks; P95 latency was **9 ms vs.
12 ms**. The BenchmarkSQL driver omits terminal think/keying delays.

## Analytics and bulk loading

Lower elapsed time is better; speedup is MySQL time / MyDB time.

| Workload | Measured scope | MyDB | MySQL 8.0.46 | Speedup |
| --- | --- | ---: | ---: | ---: |
| TPC-H-derived queries, SF=1 | Sum of 22 query medians | 2.863 s | 28.112 s | **9.82x** |
| TPC-H-derived queries, SF=10 | Sum of 22 query medians | 57.727 s | 882.784 s | **15.29x** |
| TPC-DS-derived queries, SF=1 | 66 matching query files | 12.760 s | 216.829 s | **16.99x** |
| TPC-DS-derived queries, SF=10 | 55 matching query files | 113.968 s | 2,576.552 s | **22.61x** |
| TPC-H-derived import, SF=10 | Full-table `LOAD DATA LOCAL INFILE` | 25.708 s | 357.495 s | **13.91x** |
| Standard sysbench prepare | 100M rows, primary and secondary indexes | 81.74 s | 145.53 s | **1.78x** |

The October 22–23 TPC-H report uses identical SQL, input data, and logical
schema/indexes across engines. Each query has one warmup and three timed runs;
the table sums their medians, with all 22 normalized results matching. The final
MyDB SF=10 run also follows 32 Q1 stress executions. SF=1 and SF=10 use different
fixed builds; MySQL measurements were retained from the original comparison.
Earlier failed MyDB loading/memory trials are excluded from these completed-run
figures. Import includes client upload and durable commit, with primary keys
enabled; secondary-index creation and ANALYZE are timed separately and excluded.

The October 24 TPC-DS report uses one pass without explicit query warmup. The
table includes only query files that completed with matching normalized results
and equivalent sorting keys on **all three tested engines**, including
PostgreSQL 18.6. These are **subsets of 99 files**, not full-suite speedups.
Failed, timed-out, and different-result files are excluded, as is Q31 due to a
PostgreSQL sorting difference. MyDB completed 99/99 files at both scales; its
answers matched MySQL on 92 files at SF=1 and 84 at SF=10. SF=10 Q30/Q81 differed
and are excluded from the ratios. Matching uses the reports' normalization,
including numeric rounding to four decimal places.

The sysbench prepare row uses the October 23 report's latest independent repeat:
20 requested threads, 10 active table loaders, and one run per engine. It includes
table creation, client data generation, multi-row INSERT, and secondary-index
creation; it excludes later ANALYZE and validation. It is a separate measurement
from both the OLTP runs and the TPC-H import.

## Test conditions and interpretation

These comparisons used the same Ubuntu 24.04 x86-64 cloud host: Intel Xeon
Platinum 8369B, **16 vCPUs**, approximately 124 GiB host RAM, and NVMe/ext4
storage. Each database had a **10 GiB cgroup memory limit with no swap**;
MyDB's memory target and MySQL's buffer pool were both **8 GiB**. Both used
**READ COMMITTED**, synchronous durable commits, and disabled MySQL-format
binlogging. Clients shared the host with the database. The memory settings
control different mechanisms, and the cgroup cap bounds charged memory rather
than every shared host-cache page a database can access.

MyDB's supported query parallelism was set to 16. MySQL's read/DDL worker
settings do not enable general parallel SELECT execution. Operator budgets and
cache states were not fully equalized. Zone maps and Performance Schema were
disabled for the sysbench OLTP runs; analytical runs retained automatic zone
maps. These are observations under the recorded configurations, not an
exhaustive tuning study. Single-run results have no confidence intervals, and
the TPC-derived workloads are **not official or TPC-audited results**.

Evaluate your own SQL, transaction semantics, and working set using the same
correctness and durability requirements on both systems. Record source/build
identity, configuration, data, warmup, throughput, latency, and failures.
[Client examples](../examples/README.md) and
[correctness regression tools](../scripts/README.md) provide starting points.

