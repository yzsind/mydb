# mydb Documentation

Public product, SQL, deployment, and operations documentation. Start with the
[project README](../README.md) for downloads and a first query.

## Getting Started and Deployment

| Document | Contents |
| --- | --- |
| [Project README](../README.md) | Downloads, quick start, building, and client examples |
| [Feature Overview](feature-overview.md) | Capabilities, limitations, and release maturity |
| [MySQL 8.0 Compatibility](mysql80-compatibility.md) | Supported behavior, intentional differences, and migration boundaries |
| [MyDB CLI](mydb-cli.md) | Interactive SQL, scripts, output, TLS, and client settings |
| [Startup Configuration](configuration.md) | Configuration files, CLI, environment variables, system variables, and precedence |
| [Hardware and OS Requirements](hardware-and-os-requirements.md) | Platforms, toolchains, memory, storage, and sizing |
| [Docker](docker.md) | Image build, deployment, and container management |
| [Reporting Issues](reporting-issues.md) | Environment details, logs, and reproducible issue reports |

## SQL Reference

| Document | Contents |
| --- | --- |
| [SQL Statements](statements/README.md) | Statement categories, syntax, examples, and limitations |
| [SQL Functions](function/README.md) | Function categories and usage |
| [Data Types](data-types.md) | Ranges, conversion, comparison, and indexing semantics |
| [Operators and Expressions](operators-and-expressions.md) | Precedence, NULL handling, and evaluation |
| [SQL Syntax Capabilities](sql-parser-capabilities.md) | Supported syntax and explicit rejections |
| [Hints](hints.md) | Index, join, parallelism, and query hints |
| [DML RETURNING](dml-returning.md) | OLD/NEW row images and result-set semantics |
| [Sequences](sequences.md) | Counters, caching, durability, and replication |
| [SQL ROWID](rowid.md) | Physical addresses, direct access, lifetime, and reuse |
| [generate_series](generate-series.md) | Table function syntax, types, and boundaries |
| [Time Travel Queries](mysql80-compatibility.md#time-travel-queries) | Historical query semantics and retention boundaries |
| [XML Functions](xml-functions.md) | XPath support, ExtractValue, UpdateXML, and diagnostics |

## Search, AI, and APIs

| Document | Contents |
| --- | --- |
| [Vector Search Guide](vector-search.md) | Vector storage, similarity queries, index selection, and recall validation |
| [Vector Index Reference](vector-indexes.md) | HNSW/IVF options, quantization, training, and diagnostics |
| [Full-Text Search](fulltext-search.md) | MATCH, search modes, BM25, ngram, and limits |
| [Hybrid Search](hybrid-search.md) | Reciprocal rank fusion, branch windows, and relational semantics |
| [Spatial Indexes](spatial-indexes.md) | Spatial queries, transactions, persistence, and backup |
| [LLM and Embedding Calls](ai.md) | Models, encrypted credentials, permissions, generation, and embeddings |
| [HTTP SQL Functions](http.md) | Bounded outbound HTTP/HTTPS calls from SQL |
| [REST SQL API](rest-api.md) | Authentication, sessions, transactions, and streaming results |
| [MCP Server](mcp.md) | AI client tools, SQL sessions, authentication, and limits |

## Operations and Security

| Document | Contents |
| --- | --- |
| [Recycle Bin](recycle-bin.md) | Retained DROP/TRUNCATE tables, read-only queries, restore and reclamation |
| [Backup and Recovery](backup-and-recovery.md) | Physical backups, WAL archiving, PITR, verification, and retention |
| [Native Replication](native-replication.md) | Physical replication, role switching, and operations |
| [MVCC and Locking](mvcc-and-locking.md) | Isolation, locking reads, deadlocks, savepoints, and concurrency diagnostics |
| [Row-Level Security](row-level-security.md) | Policies, privileges, connection pools, and enforcement boundaries |
| [Online Column Type Change](online-column-type-change.md) | Supported changes, consistency, resources, and recovery |
| [Server Limits](limits.md) | Capacity limits and configurable subsystem budgets |
| [Data Directory, Persistence, and Recovery](storage-format.md) | File roles, physical compatibility, durability, and crash recovery |
| [Storage Format Versions](storage-format-versions.md) | Generated registry of current physical formats |

## Performance and Diagnostics

| Document | Contents |
| --- | --- |
| [Optimizer and Query Execution](optimizer-and-operators.md) | Access paths, join algorithms, execution modes, EXPLAIN, and tuning |
| [ANALYZE TABLE](statements/admin/analyze-table.md) | Statistics collection and concurrency boundaries |
| [Performance Schema](performance-schema.md) | Runtime tables, collection, memory, and diagnostics |
| [Metadata Naming](metadata-naming.md) | Canonical result names, native metadata views, and column units |
| [SHOW STATUS](show-status.md) | Counters, scope, reset behavior, and compatibility |
| [Error Codes](error-codes.md) | Error catalog and diagnostic details |
| [Performance Comparison](performance-comparison.md) | Published measurements, test conditions, and interpretation |

## Machine-Readable References and Licenses

- [Metadata compatibility matrix](mysql80-metadata-compat-matrix.json)
- [Protocol compatibility matrix](mysql80-protocol-compat-matrix.json)
- [REST OpenAPI 3.1](rest-openapi.json)
- [Unicode license](UNICODE-LICENSE.txt)
