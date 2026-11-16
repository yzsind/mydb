# Sysbench OLTP: 100 million rows, 300 clients

**Test date:** November 16, 2026 (UTC+08:00).

## Test setup

| Item | Configuration |
| --- | --- |
| Database | MyDB `8.0.46-mydb-1.3.0-20261116` |
| Client | sysbench `1.0.17`, unmodified standard OLTP scripts |
| Database server | Alibaba Cloud instance: 16 vCPUs, 64 GB RAM, PL1 cloud disk |
| Load generator | Alibaba Cloud instance: 16 vCPUs, 64 GB RAM, PL1 cloud disk |
| Connection | Separate hosts over the private network; TLS and compression disabled |
| Dataset | 10 tables × 10,000,000 rows = **100,000,000 rows** |
| Indexes | Primary key on `id`; secondary index on `k` |
| Concurrency | **300 client threads** |
| Duration | **One 300-second run per workload**, without a separate warmup |
| Access pattern | `special` distribution, seed `20261116`; prepared statements enabled |

Server settings: `READ-COMMITTED`, `strict` durability with `fdatasync`,
binary logging OFF, memory target 31,565 MiB, and query parallel degree 4.
Performance Schema, automatic zone maps, and automatic ANALYZE were enabled.

Version note: the product version was unified to `1.0.0` later on the same day,
after this run; the binary above carries the earlier `1.3.0` development label.
The numbers here are single-engine measurements and are not directly comparable
to the October sysbench comparison in
[performance comparison](performance-comparison.md), which used 20 clients on
the same host with a warmup, and zone maps and Performance Schema disabled.

## Results

| Workload | TPS | QPS | Mean latency (ms) | P95 (ms) |
| --- | ---: | ---: | ---: | ---: |
| `oltp_point_select` | — | **1,327,407.37** | 0.23 | **0.55** |
| `oltp_read_only` | 47,572.08 | 761,153.35 | 6.30 | 11.87 |
| `oltp_read_write` | 14,743.52 | 294,870.43 | 20.35 | 26.20 |
| `oltp_write_only` | 42,017.26 | 252,103.54 | 7.14 | 9.73 |

Rates cover each complete run. Latency measures one client event, including
network time: one SELECT for point select, or one complete transaction for
the other workloads. All four measured runs completed with **zero ignored
errors and zero reconnects**.

