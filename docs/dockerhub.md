# MyDB

**A high-performance database, compatible with MySQL.**

Let agents manage data flexibly — transactions, analytics, AI model calls, and
hybrid search through SQL.

MyDB is a general-purpose database built in Rust, designed for AI agents,
high-performance hybrid transactional/analytical processing (HTAP), and MySQL
compatibility. Applications can manage business data, evolve schemas, run
real-time analytics, combine keyword and vector retrieval, and call external
text-generation and embedding services through a familiar SQL interface.

MyDB 1.2.1 is a public-preview release for feature validation and evaluation in
controlled environments. It is not yet a production-grade MySQL replacement,
and on-disk formats are not guaranteed to be backward compatible.

## Highlights

- **Built for AI agents:** SQL functions for text generation and embeddings,
  native vectors, BM25 full-text search, and reciprocal rank fusion (RRF) for
  hybrid retrieval, with model access controls and execution budgets.
- **High-performance HTAP:** a cost-based optimizer, parallel hash joins,
  vectorized execution, SIMD, and automatic zone maps serve transactional and
  analytical workloads in one engine.
- **MySQL compatibility:** reuse MySQL clients and drivers, prepared statements,
  core SQL, transactions, and supported ROW/FULL binlog integrations.
- **Native vector search:** FLOAT32, FLOAT16, BFLOAT16, INT8, and BINARY vectors;
  HNSW and IVF indexes with memory or paged storage; optional quantization; and
  exact nearest-neighbor scans.
- **Online schema evolution:** build regular and unique B-tree indexes and add
  primary keys with concurrent DML. Supported column and constraint changes
  reduce query blocking; some operations still require locks.
- **Transactional storage:** heap tables, B-trees, MVCC, row locks, WAL, undo,
  crash recovery, SAVEPOINT, and XA two-phase commit.
- **SQL extensions:** DML `RETURNING`, sequences, `generate_series()`, window
  functions, recursive CTEs, and `EXPLAIN ANALYZE`.
- **Operations:** online physical backup, WAL archiving, point-in-time recovery,
  native primary/replica replication, a recycle bin, and memory diagnostics.
- **Optional agent interfaces:** REST SQL and an MCP server share an optional
  HTTP listener. The quick start below exposes only the MySQL protocol port.

AI inference runs outside MyDB. Model calls support configured Ollama, OpenAI
Responses, Chat Completions-compatible, and Embeddings-compatible services.
Calls require autocommit outside explicit transactions and cannot run in DML,
predicates, or persistent schema expressions. RRF is a technical preview.

## Performance overview

Selected results reported in the project README:

| Workload | MyDB | MySQL 8.0 | Advantage |
| --- | ---: | ---: | ---: |
| Point queries, 100M rows, 20 clients | 316,968 queries/s | 174,566 queries/s | 1.82x throughput |
| Read-only transactions, 100M rows, 20 clients | 14,323 transactions/s | 7,294 transactions/s | 1.96x throughput |
| TPC-H-derived analytics, SF=10, sum of 22 query medians | 57.727 s | 882.784 s | 15.29x speedup |
| TPC-DS-derived analytics, SF=10, 55 matching query files | 113.968 s | 2,576.552 s | 22.61x speedup |
| Bulk import, TPC-H SF=10, `LOAD DATA LOCAL INFILE` | 25.708 s | 357.495 s | 13.91x speedup |

These measurements used the same 16-vCPU host, 10 GiB database memory limits,
and durable commits. They cover different development builds; TPC-derived
results are unaudited and are not official TPC benchmark results.

The public [Cohere HNSW report](https://github.com/yzsind/mydb/blob/main/docs/vector-cohere-1m-hnsw-benchmark-2026-11-12.md)
records 3,005.3 vs 1,968.5 queries/s at 16 clients (1.53x), candidate memo OFF,
and recall 94.283% vs 94.341%. MyDB resolved ef=400; pgvector used ef=200.
Build times were 92.017 vs 258.007 seconds. These November 12 development results
use warm repeated queries; see the report for full conditions and limitations.

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

For supported persisted global variables, values saved by `SET PERSIST` are
loaded after these startup inputs and can override them on an existing volume.
Inspect `performance_schema.persisted_variables` and `SHOW GLOBAL VARIABLES`;
use `RESET PERSIST variable_name` to remove a saved override for the next restart.
See [configuration precedence](configuration.md#precedence-and-when-settings-take-effect)
and [changing versions](changing-versions.md).

This file is the Docker Hub publication copy. Update it together with the project
README and linked public reports when publishing; [Docker](docker.md) is the
canonical deployment guide. See [documentation maintenance](documentation-maintenance.md).
