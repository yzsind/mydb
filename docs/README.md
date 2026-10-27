# mydb Documentation Index

Last updated: 2026-10-25. This directory contains public product, SQL, deployment,
execution, and storage documentation.

## Product and SQL

| Document | Contents |
| --- | --- |
| [Project README](../README.md) | Product overview, quick start, and build instructions |
| [Reporting Issues](reporting-issues.md) | Required environment details, logs, reproduction steps, scripts, and an issue template |
| [Feature Overview](feature-overview.md) | Product capabilities and limitations |
| [Performance Comparison](performance-comparison.md) | MyDB vs. MySQL measurements, test conditions, and interpretation |
| [MySQL 8.0 Compatibility](mysql80-compatibility.md) | Compatibility matrix, supported boundaries, and explicit differences |
| [Data Types](data-types.md) | Declaration, ranges, conversion, comparison, and indexing semantics |
| [Vector Data and Similarity Search](vector-search.md) | Practical guide to vector storage, working SQL examples, HNSW/IVF, compression, filtering, and recall validation |
| [Vector Datatypes and Indexes](vector-indexes.md) | Native vector types, HNSW/IVF, WITH options, quantization, query tuning, training, and recovery |
| [LLM and Embedding Calls](ai.md) | Encrypted API keys, model configuration, text generation, embeddings, permissions, and bounded execution |
| [Hybrid Search](hybrid-search.md) | RRF/weighted RRF, typed identities, branch windows, SQL composition, explanations, and Agent/RAG examples |
| [Operators and Expressions](operators-and-expressions.md) | Precedence, NULL handling, and evaluation semantics |
| [SQL Syntax Capabilities](sql-parser-capabilities.md) | Supported syntax and explicit rejections |
| [Time Travel Queries](mysql80-compatibility.md#time-travel-queries) | Historical query semantics and support boundaries |
| [SQL Statement Index](statements/README.md) | Categories, syntax, limitations, and examples |
| [SQL Function Index](function/README.md) | Function categories and usage notes |
| [Hints](hints.md) | Index hints and optimizer hints |
| [generate_series](generate-series.md) | Table function semantics and boundaries |
| [Sequences](sequences.md) | Catalog-backed counters, caching, durability, strict native replication, failover, diagnostics, and validation |
| [DML RETURNING](design/dml-returning.md) | Row images, result sets, and compatibility boundaries |

## Configuration, Deployment, and Operations

| Document | Contents |
| --- | --- |
| [Startup Configuration](configuration.md) | Configuration file, CLI, environment variables, sysvars, and precedence |
| [Hardware and OS Requirements](hardware-and-os-requirements.md) | Supported operating systems, CPU, memory and storage expectations, and sizing guidance |
| [Server Limits and Specifications](limits.md) | Hard-coded and configurable capacity limits: schema objects, storage, connections, memory, parallelism, and subsystem caps |
| [Docker](docker.md) | Image build, deployment, and container management |
| [Backup and Recovery](backup-and-recovery.md) | Online full backups, WAL archiving, read-only queries, PITR, writable recovery, verification, export/import, and retention management |
| [Native Replication](native-replication.md) | WAL-based physical replication, role switching, and operations |
| [Performance Schema](performance-schema.md) | System tables, collection, memory, and diagnostics |
| [SHOW STATUS](show-status.md) | Status variables and compatibility matrix |
| [Error Codes](error-codes.md) | MySQL error catalog and the mydb diagnostics model |
| [ANALYZE TABLE](statements/admin/analyze-table.md) | Statistics collection and concurrency boundaries |
| [Online Column Type Change](online-column-type-change.md) | Supported scope, consistency, resources, and recovery |

## Query Execution and Storage

| Document | Contents |
| --- | --- |
| [Optimizer and Operators](optimizer-and-operators.md) | CBO, access paths, physical operators, and plan diagnostics |
| [Executor](executor.md) | Row-based, vectorized, streaming, and compiled executor constraints |
| [MVCC and Locking](mvcc-and-locking.md) | Row versions, snapshots, undo, locks, and deadlock diagnostics |
| [Storage Format Overview](storage-format.md) | Data directory, pages, indexes, logs, and recovery |
| [Table and B-tree Byte Format](table-and-btree-storage-format.md) | Field-by-field format of heap, rows, LOBs, and B-trees |
| [Format Versions](storage-format-versions.md) | Current storage format versions |

## Machine-Readable Material

- The [metadata compatibility matrix](mysql80-metadata-compat-matrix.json) and the [protocol compatibility matrix](mysql80-protocol-compat-matrix.json).
- Pinned MySQL 8.0.46 baselines: [INFORMATION_SCHEMA](fixtures/mysql80-information-schema-8.0.46.json), [Performance Schema](fixtures/mysql80-performance-schema-8.0.46.json), [SHOW STATUS](fixtures/mysql80-show-status-8.0.46.json).
- [Unicode license](UNICODE-LICENSE.txt).
