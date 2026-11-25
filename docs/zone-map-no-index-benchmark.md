# Queries without user-created indexes: MyDB vs. MySQL

**November 25, 2026 · MyDB 1.0.3 vs. MySQL 8.0.46**

## Query results

Lower latency is better. Speedup = MySQL latency / MyDB latency, calculated
from unrounded measurements.

| Lookup | Rows returned | MyDB median (ms) | MySQL median (ms) | Speedup | MyDB data pages scanned | MySQL data pages scanned |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Order ID | 1 | **1.206** | 7,577.824 | **6,281.8x** | 64 | 136,552 |
| Customer ID | 7–21 | **32.466** | 7,678.232 | **236.5x** | 3,456–5,888 | 136,552 |

Pages are 16 KiB. MyDB's complete heap contains 136,996 pages; its scan counts
come from `EXPLAIN ANALYZE` Zone Map candidate ranges. MySQL's counts cover all
136,552 clustered leaf pages, whose records total 15,000,000. These are data
pages scanned, not physical disk reads or repeated logical page requests.

As a control, disabling Zone Maps in MyDB brought both lookup types to about
**1,000 ms**. The control ran once per value. Cross-database speedups therefore
reflect both scan reduction and execution-engine differences. Ordered order
IDs benefit especially strongly; gains depend on data layout and predicates.

## Scenario

An application needs to look up orders by order ID or customer ID without
creating an index for either field. This test compares the same equality
queries over **15,000,000 rows** from the TPC-H SF=10 `orders` dataset.
It is a focused lookup test, not a full TPC-H benchmark.

Both databases use the same schema and original dbgen input order, with no
primary key, unique index, or secondary index. Order IDs increase with physical
row order; customer IDs are scattered. MyDB uses its default automatic Zone
Maps, including range and Bloom membership summaries, to skip data segments
that cannot match. MySQL scans the entire table.

## Environment and method

- Same server: Intel Xeon Platinum 8369B @ 2.70 GHz, 16 vCPUs,
  approximately 123 GiB RAM, Ubuntu 24.04 / Linux 6.8.
- MyDB build: `8.0.46-mydb-1.0.3-20261125`; MySQL:
  `8.0.46-0ubuntu0.24.04.5`, using InnoDB.
- MyDB memory target: 20 GiB, including a 12 GiB buffer pool;
  MySQL buffer pool: 20 GiB. Both tables fit in their buffer pools.
- One client, one query at a time, single-threaded execution
  (`parallel_degree=1` in MyDB), and no query result cache.
- Run `ANALYZE TABLE` after loading. Complete warmup and wait for import-related
  dirty pages to be flushed before timing. All timed queries have zero
  buffer-pool reads from storage.
- Random seed `20261125` selects five input rows without replacement; their
  order IDs and customer IDs provide the query values below. Execute each
  lookup three times and report the median of all 15 executions per field.
- Time client execution and complete result fetching, excluding diagnostics.
  Verify every returned record against the input file; table row counts and
  aggregate checks also match between databases.

## Table schema

Use this schema in both databases, with InnoDB in MySQL:

```sql
CREATE TABLE orders (
    o_orderkey      BIGINT NOT NULL,
    o_custkey       BIGINT NOT NULL,
    o_orderstatus  CHAR(1) NOT NULL,
    o_totalprice   DECIMAL(15,2) NOT NULL,
    o_orderdate    DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk        CHAR(15) NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment      VARCHAR(79) NOT NULL
);
```

Load the unchanged SF=10 `orders.tbl` file, preserving its row order, then run:

```sql
ANALYZE TABLE orders;
SELECT COUNT(*) FROM orders;  -- 15000000
SHOW INDEX FROM orders;      -- Empty in both databases
```

InnoDB still maintains its internal hidden clustered index. Neither lookup
uses a user-created index; both MySQL plans show a full table scan.

## Test SQL

### Order ID lookup

```sql
SELECT * FROM orders WHERE o_orderkey = 638245;
SELECT * FROM orders WHERE o_orderkey = 21142821;
SELECT * FROM orders WHERE o_orderkey = 30377793;
SELECT * FROM orders WHERE o_orderkey = 37631873;
SELECT * FROM orders WHERE o_orderkey = 56679490;
```

### Customer ID lookup

```sql
SELECT * FROM orders WHERE o_custkey = 702437;
SELECT * FROM orders WHERE o_custkey = 963638;
SELECT * FROM orders WHERE o_custkey = 1378163;
SELECT * FROM orders WHERE o_custkey = 82906;
SELECT * FROM orders WHERE o_custkey = 752479;
```

## Disk space

Filesystem-allocated disk space after clean shutdown, using decimal
**GB (1 GB = 1,000,000,000 bytes)**:

| Component | Disk space (GB) |
| --- | ---: |
| MySQL table (`orders.ibd`) | **2.273** |
| MyDB table data (`.heap`) | **2.245** |
| MyDB Zone Maps (`.zm`) | **0.095** |
| MyDB table data + Zone Maps | **2.340** |

Zone Maps cover the table's fields, not just the two queried columns. Their
disk overhead is **4.24%** of MyDB's heap allocation. MyDB's combined table
and Zone Map allocation is **2.92%** larger than MySQL's table allocation.
Percentages use unrounded allocated sizes.

This table footprint includes table-file metadata and allocated free space.
It excludes WAL/redo, undo, system tables, temporary files, and the input file.
