<div align="center">

<img src="assets/logo/mydb-whale.png" alt="MyDB whale logo with layered database storage" width="256" height="256">

# MyDB

**A high-performance database, compatible with MySQL**

Let agents manage data flexibly — transactions, analytics, and vector search in a single database.

[Download](https://github.com/yzsind/mydb/releases/tag/v1.0.0) · [Performance evaluation](#performance-evaluation) · [Quick Start](#quick-start) · [Built for AI Agents](#built-for-ai-agents) · [Documentation](docs/README.md) · [MySQL Compatibility](docs/mysql80-compatibility.md)

</div>

---

MyDB is a general-purpose database built in Rust, designed around three directions: **AI agents, high-performance hybrid transactional/analytical processing (HTAP), and MySQL compatibility**. Through a familiar SQL interface, applications and agents can manage business data, evolve table schemas, run real-time analytics, and search similar vectors — without copying data across systems or maintaining multiple interfaces.

MyDB 1.0.0 is a public-preview release intended for feature validation and controlled-environment evaluation. It is not yet a production-grade MySQL replacement, and on-disk formats are not guaranteed to be backward compatible.

| Core capability | What it gives you |
| --- | --- |
| **Built for AI agents** | Online schema changes, automatic zone maps, and native vector data management reduce manual work in schema evolution and query tuning. |
| **High-performance HTAP** | Parallel hash joins, vectorized execution, and a cost-based optimizer power both transactional and analytical workloads. |
| **MySQL compatible** | Reuse MySQL clients, JDBC drivers, core SQL, and row binlog integration. |

## Performance evaluation

**MyDB vs. MySQL 8.0** :

| Workload | MyDB | MySQL | Advantage |
| --- | ---: | ---: | ---: |
| Point queries · 100M rows, 20 clients | 316,968 queries/s | 174,566 queries/s | **1.82x throughput** |
| Read-only transactions · 100M rows, 20 clients | 14,323 transactions/s | 7,294 transactions/s | **1.96x throughput** |
| TPC-H-derived analytics · SF=10, 22 query medians summed | 57.727 s | 882.784 s | **15.29x speedup** |
| TPC-DS-derived analytics · SF=10, 55 matching query files | 113.968 s | 2,576.552 s | **22.61x speedup** |
| Bulk import · TPC-H SF=10, `LOAD DATA LOCAL INFILE` | 25.708 s | 357.495 s | **13.91x speedup** |

Same 16-vCPU host, 10 GiB database memory limits, and durable commits; results
cover different development builds and TPC-derived results are unaudited. See
the [detailed comparison](docs/performance-comparison.md) for all results, test
conditions, and limitations.

## Quick Start

Download prebuilt packages for macOS ARM64, Linux x86_64, and Windows x64 from the
[1.0.0 release page](https://github.com/yzsind/mydb/releases/tag/v1.0.0), which also
includes SHA256 checksums and platform validation details.

### Run a downloaded binary

Download the package for your platform, extract it, and open a terminal in the
directory containing the executable. On macOS or Linux, start it with:

```bash
chmod +x ./mydb
./mydb --data-dir ./data
```

On Windows, run the equivalent command in PowerShell:

```powershell
.\mydb.exe --data-dir ./data
```

The server listens locally on port `8738` and stores data in `./data`. No Rust
toolchain or config file is needed. With a MySQL 8.0 client installed, connect
from another terminal:

```bash
mysql -h127.0.0.1 -P8738 -uroot -proot  test
```

A fresh data directory includes the `test` database and the local `root` account
with password `root`. Run `SELECT 1;` to check the connection. Enter `quit` to
exit the client, then press **Ctrl+C** in the server terminal to stop it. Restart
with the same data directory to reopen your data.

### Build and connect

Install Rust 1.95 or newer, a native C toolchain, Perl, and a MySQL 8.0 client;
the shell commands below target macOS and Linux.

```bash
git clone https://github.com/yzsind/mydb.git
cd mydb
cargo build --release --locked --bin mydb

MYDB_DATA_DIR=/tmp/mydb/data \
  ./target/release/mydb --host 127.0.0.1 --port 8738
```

When the server is ready, connect from another terminal:

```bash
mysql -h127.0.0.1 -P8738 -uroot -proot --ssl-mode=REQUIRED test
```

A fresh data directory creates `root@localhost` with password `root` and a `test`
database. This account accepts only loopback connections. `MYDB_ROOT_PASSWORD`
sets the initial password on first start; it does not change existing accounts.
The default protocol port is `8738`; override it with `--port`.

Try a write, an aggregate, and a window function:

```sql
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    region VARCHAR(32),
    amount DECIMAL(12, 2)
);

INSERT INTO orders VALUES
    (1, 'east', 120.00),
    (2, 'south', 80.00),
    (3, 'east', 180.00);

SELECT region,
       SUM(amount) AS revenue,
       DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS revenue_rank
FROM orders
GROUP BY region
ORDER BY revenue_rank, region;
```

Exit with `quit`, then stop the server with **Ctrl+C**. Restarting with the same
data directory reopens the data, so the CREATE and INSERT statements only need to
run once; `/tmp/mydb/data` is temporary storage and may be removed by the OS.

### Persistent data and configuration

For a separate, persistent instance, write a config file (data directory,
durability mode, `memory_target`) and start with `--config`; the server reads a
config file only when `--config` is supplied, and the Docker entrypoint supplies
its default config automatically. `strict` (the default durability mode)
synchronizes WAL before acknowledging a commit, and `memory_target` is a
working-set target — not a hard process memory limit — defaulting to 50% of
detected effective memory. See [configuration](docs/configuration.md) and
[hardware and OS requirements](docs/hardware-and-os-requirements.md) for all
settings, durability modes, and sizing.

### Application connections and TLS

TLS is advertised by default with an ephemeral self-signed certificate when none
is configured; for remote deployments, configure a trusted certificate and verify
the server identity (see [TLS configuration](docs/configuration.md#tls)).
Runnable [Java, Python, Go, C++, and Rust examples](examples/README.md) cover
connections, typed queries, DDL/DML, batches, transactions, and stored procedure
calls, including JDBC URLs for local evaluation.

### Docker

Build and run a container (stop any local instance using port `8738` first):

```bash
docker build -t mydb:local .
docker volume create mydb-data

docker run --name mydb \
  -p 127.0.0.1:8738:8738 \
  -v mydb-data:/var/lib/mydb \
  -e MYDB_ROOT_PASSWORD='change-me' \
  mydb:local

docker exec -it mydb mysql -h127.0.0.1 -P8738 -uroot -p --ssl
```

Enter `change-me` for the password on a fresh volume. Stop with `docker stop
mydb` and restart with `docker start mydb`; the named volume retains the data.
Bridge/NAT connections through the published port do not match `root@localhost` —
create a dedicated account in that SQL session for host connections. Local image
builds enable TLS, `fast-alloc`, and the MySQL replication receiver by default.
See the [Docker deployment guide](docs/docker.md) for certificates, mounted
configuration, cross-architecture builds, and connecting from the host.

### Platforms and distribution

Supported operating systems: macOS (arm64 and Intel) and Linux (x86_64 and
aarch64), plus Windows 11 for foreground operation on local NTFS. See
[hardware and OS requirements](docs/hardware-and-os-requirements.md) for
verification details, Windows limitations, and distribution scope.

## Architecture

![MyDB architecture: protocol, SQL engine, unified memory management, storage engine, transactions, backup and recovery, primary-replica HA, sessions, security, and metadata; highlighting the optimizer, executor, heap tables, Zone Map, HNSW, online DDL, unified memory management, and directly queryable backups](assets/mydb-architecture.svg)

## Built for AI Agents

An agent's data needs change with every task: add a column, build an index, write embeddings, then join retrieval results back to business data. MyDB keeps all of this inside one transactional database.

### Smart indexing: fewer full scans without manual indexes

MyDB enables automatic **zone maps** on persistent user tables by default: per-block statistics let queries skip data blocks that cannot match a predicate, often turning a full scan into a small range read on tables with no hand-built B-tree index. Pruning is conservative — whenever statistics are missing or a predicate cannot be proven safe, the executor falls back to a regular scan. See the [feature overview](docs/feature-overview.md) for how the mechanism and fallbacks work.

### Online DDL: schemas that evolve with the task

MyDB minimizes DDL locking and query waiting by coordinating schema versions, index builds, and concurrent access in the native storage layer:

- **Online index and primary key creation**: regular and unique B-tree indexes and newly added primary keys allow concurrent DML while being built.
- **Read-compatible column changes**: adding columns without storage rewrites, compatible type modifications, and some metadata-only changes keep ordinary queries running.
- **Constraints and charset settings**: online foreign keys, CHECK constraints, and default charset/collation metadata changes.

"Lock-free DDL" is a design direction, not a current guarantee: publication still coordinates with write transactions, and replacing primary keys, rewriting column data, or converting charsets still takes locks. See [ALTER TABLE](docs/statements/ddl/alter-table.md) and the [DDL compatibility boundaries](docs/mysql80-compatibility.md#table-ddl).

### Native vectors: from storage to large-scale similarity search

- **Native `VECTOR(n, datatype)` type**: FLOAT32 (default), FLOAT16, BFLOAT16, INT8, and BINARY retain their native payloads. Numeric vectors support L2, cosine, and dot-product distance; BINARY supports Hamming and Jaccard. Dimension limits depend on the datatype, from 16,383 for FLOAT32 to 524,256 bits for BINARY.
- **HNSW and IVF indexes**: both support memory and paged storage. Paged index pages participate in the buffer pool and WAL, allowing index payloads to exceed memory capacity; exact KNN scans are also available.
- **Explicit quantization**: `WITH (quantization=sq8|sq4|pq|bq)` compresses numeric index payloads, with final re-ranking against visible heap values. The default is `none`; search breadth and re-ranking budgets determine the recall, memory, and latency tradeoff.
- **Unified SQL**: the optimizer recognizes `ORDER BY distance LIMIT k` and supports parameterized queries; vector writes participate in transaction commit and rollback.

Combined with JSONB, full-text, and structured queries, this supports semantic search, knowledge retrieval, and agent memory. See [vector datatypes and indexes](docs/vector-indexes.md) for options, training, limits, and recovery, and the [vector function reference](docs/function/vector/README.md). The current implementation is HNSW / IVF; disk residency does not imply a complete DiskANN implementation.

### Try vector retrieval and RETURNING

Still in the `test` database, create a small collection on a `VECTOR` column
declared `NOT NULL`; `RETURNING` gives the inserted IDs directly to the caller:

```sql
CREATE TABLE agent_notes (
    id BIGINT PRIMARY KEY,
    content VARCHAR(128),
    embedding VECTOR(3) NOT NULL
);

INSERT INTO agent_notes VALUES
    (1, 'order summary', STRING_TO_VECTOR('[1,0,0]')),
    (2, 'shipping details', STRING_TO_VECTOR('[1,1,0]')),
    (3, 'account settings', STRING_TO_VECTOR('[0,0,2]'))
RETURNING id;

CREATE INDEX idx_agent_notes_embedding ON agent_notes (embedding)
USING HNSW WITH (metric=l2);

SELECT id, content,
       L2_DISTANCE(embedding, STRING_TO_VECTOR('[1,0,0]')) AS distance
FROM agent_notes
ORDER BY distance
LIMIT 2;
```

The INSERT returns IDs `1`, `2`, `3`; the query returns the two rows nearest to
`[1,0,0]` by L2 distance. HNSW provides approximate nearest-neighbor search —
recall and performance on larger collections depend on data and parameters. See
[CREATE INDEX](docs/statements/ddl/create-index.md),
[vector functions](docs/function/vector/README.md), and
[INSERT RETURNING](docs/statements/dml/insert.md).

## High-performance HTAP

MyDB consolidates OLTP and OLAP in one engine — from concurrent point lookups and updates to multi-table joins, aggregation, and window computation — picking an execution path per query shape.

| Design | Capability |
| --- | --- |
| **Transactional storage** | Heap tables, B-trees, MVCC, row locks, WAL, undo, and crash recovery, with SAVEPOINT and XA two-phase commit. |
| **Cost-based optimizer** | Chooses access paths, join orders, and join algorithms from statistics and a cost model. |
| **Parallel hash join** | Uses multiple cores for joins, scans, and aggregation on complex analytical SQL. |
| **Vectorized execution and SIMD** | Processes data in batches, cutting per-row dispatch and computation overhead on suitable paths. |
| **Automatic scan reduction** | Combines zone maps, partition pruning, predicate pushdown, projection pruning, and covering indexes. |
| **Unified memory management** | Coordinates the buffer pool, caches, and executor budgets; sort and hash-join operators spill to disk. |

Query capabilities include CTEs, recursive CTEs, subquery decorrelation, set operations, window functions, and `ROLLUP` / `CUBE` / `GROUPING SETS`. See [optimizer and operators](docs/optimizer-and-operators.md), [executor](docs/executor.md), and [MVCC and locking](docs/mvcc-and-locking.md).

## MySQL compatible

**Reuse the connection methods and SQL your application already knows.**

- **Clients and drivers**: MySQL classic protocol is supported, so the MySQL CLI, Connector/J, and other clients work; prepared statements, binary result sets, and read-only server-side cursors are included.
- **Core SQL**: common DDL, DML, transactions, queries, functions, and privilege management are covered. Whether a given application needs no changes should be verified against your actual SQL and transaction semantics.
- **binlog ecosystem**: MyDB can act as a MySQL binlog source or replication receiver, and downstream sync tools built on supported binlog events can ingest from it. Only **ROW/FULL** images are supported; SBR/MIXED and MINIMAL are not.

The full coverage map, transactional differences, and unsupported features are listed in the [MySQL 8.0 compatibility matrix](docs/mysql80-compatibility.md).

## Practical extensions beyond MySQL

| Feature | Use |
| --- | --- |
| **Wider TIMESTAMP range** | Supports dates beyond 2038, up to year 9999; replicating to MySQL via binlog is still bounded by the TIMESTAMP2 encoding and errors clearly on overflow. |
| **Sequences** | Standalone sequence objects with `NEXTVAL`, `CURRVAL`, and friends. |
| **DML `RETURNING`** | Return rows directly from INSERT, UPDATE, and DELETE, saving follow-up queries. |
| **`generate_series()`** | Integer, decimal, and datetime series for time axes, gap filling, and data generation. |
| **`EXPLAIN ANALYZE`** | Inspect actual per-step performance of an execution plan. |
| **`SHOW MYDB MEMORY`** | Inspect global memory usage for capacity planning and diagnostics. |
| **Native backup and replication** | Online physical backup, WAL archiving, PITR, and an independent primary/replica physical replication protocol. |

See [sequences](docs/statements/ddl/create-sequence.md), [series generation](docs/generate-series.md), [backup and recovery](docs/backup-and-recovery.md), and [native replication](docs/native-replication.md).

## Compatibility boundaries

- **MySQL compatibility has explicit scope**: the classic protocol is supported, X Protocol is not; the native storage engine is not file- or plugin-ABI-compatible with InnoDB.
- **Transaction semantics differ**: the default isolation level is `READ COMMITTED`; `REPEATABLE READ` is snapshot isolation without InnoDB gap/next-key locks; `SERIALIZABLE` range-insert prevention uses table-level shared locks.
- **Replication has prerequisites**: MySQL → MyDB replication (the `mysql-replica` feature, enabled by default) requires ROW/FULL binlog on the source; native physical replication uses controlled, manual role switching with no automatic leader election.
- **Time travel has a window**: table-level `FOR SYSTEM_TIME AS OF` depends on undo retention settings, and the default retention window is `0`; queries across schema DDL boundaries are explicitly rejected.

Unimplemented SQL or protocol capabilities return clear errors rather than failing silently. The detailed scope is defined by the [compatibility matrix](docs/mysql80-compatibility.md), the source, and the tests.

## Documentation

| Entry point | Contents |
| --- | --- |
| [Documentation index](docs/README.md) | Product, SQL, operations, and implementation docs |
| [Feature overview](docs/feature-overview.md) | Capability map and implementation boundaries |
| [SQL statements](docs/statements/README.md) / [Functions](docs/function/README.md) | Syntax, semantics, and usage examples |
| [Configuration guide](docs/configuration.md) / [Config template](mydb.cnf.example) | Startup options, environment variables, and runtime settings |
| [Storage format](docs/storage-format.md) | Data directory, pages, indexes, logs, and recovery |

Product documentation under `docs/` is written in English. When documentation
and behavior disagree, the source code and tests are authoritative; the
compatibility matrices describe the documented scope.

## Reporting issues

Submit bug reports through [GitHub Issues](https://github.com/yzsind/mydb/issues).
Follow the [issue reporting guide](docs/reporting-issues.md) and include the exact
MyDB version, operating system, relevant logs, and reproducible steps and scripts.

MyDB is licensed under the [Apache License 2.0](LICENSE).
