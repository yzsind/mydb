# MyDB

**A high-performance MySQL-compatible database for HTAP and AI agents.**

Mixed transactional and analytical workloads, with native AI retrieval.

[GitHub repository](https://github.com/yzsind/mydb) ·
[Documentation](https://github.com/yzsind/mydb/blob/main/docs/README.md) ·
[Docker deployment guide](https://github.com/yzsind/mydb/blob/main/docs/docker.md) ·
[Releases](https://github.com/yzsind/mydb/releases)

MyDB is a general-purpose database built in Rust for HTAP (hybrid
transactional/analytical processing) mixed workloads, AI agents, and MySQL
compatibility. Transactional reads and writes and analytical queries share
the same engine and business data. Applications can evolve schemas, combine
keyword and vector retrieval, and call external generation and embedding
services through a familiar SQL interface.

MyDB 1.0.2 is a public-preview release for feature validation and evaluation in
controlled environments. It is not yet a production-grade MySQL replacement,
and on-disk formats are not guaranteed to be backward compatible.

## Highlights

- **Built for AI agents:** SQL functions for text generation and embeddings,
  native vectors, BM25 full-text search, and reciprocal rank fusion (RRF) for
  hybrid retrieval, with model access controls and execution budgets.
- **High-performance HTAP:** a cost-based optimizer, parallel hash joins,
  vectorized execution, SIMD, and automatic zone maps support analytical
  queries alongside transactional reads and writes over the same data.
- **MySQL compatibility:** reuse MySQL clients and drivers, prepared statements,
  core SQL, transactions, and supported ROW/FULL binlog integrations.
- **Native vector search:** FLOAT32, FLOAT16, BFLOAT16, INT8, and BINARY vectors;
  HNSW and IVF indexes with memory or paged storage; optional quantization; and
  exact nearest-neighbor scans. Numeric indexes support SQ8, SQ4, PQ, and BQ
  quantization.
- **Online schema evolution:** build regular and unique B-tree indexes and add
  primary keys with concurrent DML. Supported column and constraint changes
  reduce query blocking; some operations still require locks.
- **Transactional storage:** heap tables, B-trees, MVCC, row locks, WAL, undo,
  crash recovery, SAVEPOINT, and XA two-phase commit.
- **SQL extensions:** DML `RETURNING`, sequences, `generate_series()`, window
  functions, recursive CTEs, and `EXPLAIN ANALYZE`.
- **Operations:** online physical backup, WAL archiving, point-in-time recovery,
  direct read-only backup queries, native primary/replica replication, a recycle
  bin, and memory diagnostics.
- **Optional agent interfaces:** REST SQL and an MCP server share an optional
  HTTP listener. The quick start below exposes only the MySQL protocol port.

AI inference runs outside MyDB. Model calls support configured Ollama, OpenAI
Responses, Chat Completions-compatible, and Embeddings-compatible services.
Calls require autocommit outside explicit transactions and cannot run in DML,
predicates, or persistent schema expressions. RRF is a technical preview.

## Performance overview

Selected MyDB vs. MySQL 8.0.46 results measured on November 17, 2026:

| Workload | MyDB | MySQL 8.0 | Advantage |
| --- | ---: | ---: | ---: |
| Point queries, 100M rows, 20 clients | 295,724 queries/s | 184,108 queries/s | 1.61x throughput |
| Read-only transactions, 100M rows, 20 clients | 13,522 transactions/s | 7,477 transactions/s | 1.81x throughput |
| TPC-H-derived analytics, SF=10, sum of 22 query medians | 50.636 s | 1,205.299 s | 23.80x speedup |
| TPC-DS-derived analytics, SF=10, 84/99 matching query files | 92.427 s | 4,468.101 s | 48.34x speedup on that subset |
| Bulk import, TPC-H SF=10, `LOAD DATA LOCAL INFILE` | 37.281 s | 374.490 s | 10.04x speedup |

These measurements used the same 16-vCPU host, 10 GiB database memory limits,
and durable commits. TPC-DS totals cover only matching MyDB/MySQL files; this
subset differs from the historical three-engine comparison. TPC-derived
results are unaudited and are not official TPC benchmark results. See the
[detailed comparison](https://github.com/yzsind/mydb/blob/main/docs/performance-comparison.md)
for build identities, methodology, result differences, and limitations; these
historical measurements do not benchmark the current Docker image.

An independent single-engine sysbench measurement on November 16, 2026 reached
**1,327,407 queries/s** point select (P95 0.55 ms) with 100M rows and 300 clients
on 16 vCPU cores. It used a separate client host, sysbench's `special` access
distribution, and one 300-second run; see the public
[sysbench record](https://github.com/yzsind/mydb/blob/main/docs/sysbench-100m-300clients-2026-11-16.md).

The [November 24 Cohere 1M HNSW benchmark](https://github.com/yzsind/mydb/blob/main/docs/vector-cohere-1m-hnsw-benchmark-2026-11-24.md)
reports **5,121 vs 2,142 queries/s (2.39x)** at 16 clients with a
32 GiB database memory limit, and **458 vs 139 queries/s (3.29x)**
with a 2 GiB limit, for MyDB and PostgreSQL 18.6 + pgvector 0.8.7.
MyDB query-candidate memoization was disabled. Recall@100 was
94.741% vs 94.395% at 32 GiB and
95.331% vs 94.341% at 2 GiB.

Both engines used the same 16-vCPU host, cosine Top-100, `m=16`,
`ef_construction=64`, effective `ef_search=200`, FLOAT32 storage, and durable
commits. The report provides build times, latency, and measurement details.
It identifies the tested development build and does not benchmark the current
Docker image.

## Quick start

Download the prebuilt `ninedata/mydb:latest` image from Docker Hub and start
the database:

```bash
docker pull ninedata/mydb:latest
docker volume create mydb-data

docker run -d --name mydb \
  -p 127.0.0.1:8738:8738 \
  -v mydb-data:/var/lib/mydb \
  -e MYDB_ROOT_PASSWORD='change-me' \
  ninedata/mydb:latest
```

Connect using the SQL client included in the container:

```bash
docker exec -it mydb mysql -h127.0.0.1 -P8738 -uroot -p --ssl test
```

Enter `change-me` when prompted, then verify the connection:

```sql
SELECT 1;
```

A fresh data directory includes the `test` database and `root@localhost`.
`MYDB_ROOT_PASSWORD` sets the initial password only; it does not change accounts
in an existing data volume. If omitted on first start, the password is `root`.
TLS is advertised by default using a temporary self-signed certificate when no
certificate is configured.

### Connect from the host

Docker bridge/NAT connections do not match `root@localhost`. In the container's
SQL session, create a dedicated application account:

```sql
CREATE USER 'app'@'%' IDENTIFIED BY 'choose-a-separate-password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP
ON test.* TO 'app'@'%';
```

With a MySQL 8.0 client installed on the host:

```bash
mysql -h127.0.0.1 -P8738 -uapp -p test
```

The example publishes the port on the host loopback interface. For remote
deployments, configure a trusted TLS certificate and verify server identity.

### Persist data and restart

```bash
docker stop mydb
docker start mydb
docker logs --tail 100 mydb
```

The named volume retains database files across container replacement. Reuse it
with a compatible image version to reopen the data. `docker stop` requests a
graceful shutdown through SIGTERM.

The `latest` tag can change. Record `SELECT VERSION()` and pin an image digest
for repeatable deployments. Before changing images, check physical-format
compatibility and preserve a verified backup; see
[changing versions](https://github.com/yzsind/mydb/blob/main/docs/changing-versions.md).

## Try vector search

Run the following in the `test` database:

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

The INSERT returns the inserted IDs. The SELECT retrieves the two nearest
notes by L2 distance. These three-dimensional vectors are illustrative; real
embedding collections must use matching model revisions and dimensions. HNSW
provides approximate nearest-neighbor search.

## Container configuration

| Setting | Behavior |
| --- | --- |
| Image architecture | `linux/amd64` only; other architectures build from source |
| MySQL protocol port | `8738` |
| Persistent data directory | `/var/lib/mydb` |
| Automatically loaded configuration | `/etc/mydb/mydb.cnf` |
| Runtime user | Non-root, UID/GID `10001:10001` |
| `MYDB_ROOT_PASSWORD` | Initial local administrator password on a fresh volume |
| `MYDB_MEMORY_TARGET` | Advisory working-set target, for example `1G`; not a hard process memory limit |
| `MYDB_DURABILITY` | Defaults to `strict`, synchronizing WAL before acknowledging commits |

The image includes the database server, `mydb-cli`, a MySQL-compatible SQL
client, `mydb-backup`, timezone data, and third-party license/source notices.

To supply a complete configuration file, make it readable by the container
user and mount it read-only:

```bash
cat > ./mydb.cnf <<'EOF'
[mydb]
host = 0.0.0.0
port = 8738
data_dir = /var/lib/mydb
durability = strict
EOF
chmod 0644 ./mydb.cnf

docker run -d --name mydb-custom \
  -p 127.0.0.1:8739:8738 \
  -v mydb-custom-data:/var/lib/mydb \
  -v "$(pwd)/mydb.cnf:/etc/mydb/mydb.cnf:ro" \
  -e MYDB_ROOT_PASSWORD='change-me' \
  -e MYDB_MEMORY_TARGET=1G \
  ninedata/mydb:latest
```

Keep `host = 0.0.0.0`, `port = 8738`, and `data_dir = /var/lib/mydb` in this
configuration to match the published port and data volume. Configuration
precedence is config file, then `MYDB_*` environment variables, then CLI
arguments. Arguments after the image name are passed to the server.

For supported persisted global variables, values saved by `SET PERSIST` are
loaded after these startup inputs and can override them on an existing volume;
see [configuration precedence](https://github.com/yzsind/mydb/blob/main/docs/configuration.md#precedence-and-when-settings-take-effect).

Bind-mounted data directories must be writable by UID/GID `10001:10001`.
On macOS Docker Desktop, use Linux named volumes for database files and backup
repositories.

## Compatibility boundaries

- MySQL classic protocol is supported; X Protocol and the MySQL plugin ABI are
  not. Native storage is not file-compatible with InnoDB.
- The default isolation level is `READ COMMITTED`. `REPEATABLE READ` uses
  snapshot isolation without InnoDB gap/next-key locks; `SERIALIZABLE` uses
  table-level shared locks for range-insert prevention.
- MySQL binlog integration requires ROW/FULL images. SBR, MIXED, and MINIMAL
  images are unsupported.
- Native physical replication uses controlled manual role switching and has no
  automatic leader election.
- Time travel depends on undo retention, whose default window is zero; queries
  across schema DDL boundaries are rejected.
- Unsupported SQL and protocol capabilities return explicit errors. Validate
  application SQL and transaction behavior against your actual workloads.

## License

MyDB is licensed under the Apache License 2.0.

This file is the Docker Hub publication copy. Update it together with the project
README and linked public reports when publishing; the
[Docker guide](https://github.com/yzsind/mydb/blob/main/docs/docker.md) is the
canonical deployment guide. See
[documentation maintenance](https://github.com/yzsind/mydb/blob/main/docs/documentation-maintenance.md).
