# MyDB 1.0.0 — Public Preview

MyDB is a general-purpose database written in Rust for mixed transactional and
analytical workloads, with MySQL-compatible access and native AI retrieval.
This is the first product release from the new MyDB repository.

**Release maturity:** 1.0.0 is a public preview for feature validation,
compatibility testing, and evaluation in controlled environments. It is not
yet a production-grade MySQL replacement. On-disk formats are not guaranteed
to remain backward compatible between previews.

## Highlights

- **MySQL-compatible access.** Use MySQL classic-protocol clients and drivers,
  prepared statements, TLS, and common SQL, including transactions, CTEs,
  window functions, routines, and triggers. See the
  [compatibility matrix](mysql80-compatibility.md).
- **One engine for transactional and analytical work.** Native heap storage,
  B-tree indexes, MVCC, WAL recovery, cost-based optimization, parallel hash
  joins, vectorized execution, and automatic Zone Maps support mixed workloads.
- **Native retrieval and AI access.** Store vectors and query HNSW/IVF indexes,
  combine full-text and vector retrieval, and call configured external models
  for generation and embeddings from SQL. Optional REST SQL and MCP interfaces
  share database accounts and permissions. RRF hybrid retrieval remains a
  technical preview; model inference runs outside MyDB.
- **Online schema evolution.** Supported index, primary-key, constraint, and
  column changes allow concurrent access during their online build phases.
  Preparation and publication still require barriers; supported lock modes
  depend on the operation.
- **Data protection and inspection.** Physical backups, WAL archiving and PITR,
  direct read-only backup queries, a recycle bin, and retention-based time
  travel provide recovery and inspection workflows.
- **Two replication paths.** MySQL ROW/FULL binlog integration includes the
  receiver in every server build. Native MyDB WAL replication supports
  asynchronous and semi-synchronous operation with controlled role changes.

Read the [feature overview](feature-overview.md)
for the supported scope of each capability.

## Compatibility and evaluation boundaries

- The default isolation level is `READ COMMITTED`. `REPEATABLE READ` uses
  snapshot isolation without InnoDB gap/next-key locks; application transaction
  semantics must be evaluated explicitly.
- MyDB uses its own storage engine and physical formats. InnoDB data files and
  plugin binaries cannot be reused. Use a fresh data directory for evaluation;
  check format compatibility before reopening data or restoring physical backups.
- MySQL replication requires `binlog_format=ROW` and
  `binlog_row_image=FULL`. SBR, MIXED, MINIMAL, and MySQL multi-threaded replica
  apply are unsupported.
- X Protocol, Group Replication, Router, Shell/X, plugin ABI, HANDLER,
  RESOURCE GROUP, and TABLESPACE are unsupported.
- Native replication has no automatic leader election. Role changes require
  external fencing of the former primary. Time travel requires retained undo
  history; its default retention window is zero.
- Windows evaluation targets foreground operation on local NTFS. Windows
  Service lifecycle is not implemented; other Windows versions and filesystems
  have the verification limits documented in the platform guide.

See [hardware and OS requirements](hardware-and-os-requirements.md),
[version changes](changing-versions.md),
and [storage formats](storage-format.md).

## Validation of the release baseline

All four GitHub Actions runs below completed successfully for package source
commit [`7be88e78`](https://github.com/yzsind/mydb/commit/7be88e782cfec30180672a2a4b6e728d1b276d93). The main CI's three test partitions passed 12,077 tests in total.

| Validation | Result |
| --- | --- |
| [Main CI](https://github.com/yzsind/mydb/actions/runs/43974623981) | Passed: test partitions, formatting, documentation contracts, Clippy, feature builds, and recovery checks |
| [MyDB CLI](https://github.com/yzsind/mydb/actions/runs/43974623984) | Passed: all four jobs |
| [Push Safety](https://github.com/yzsind/mydb/actions/runs/43974623975) | Passed; receiver job is excluded by the push condition |
| [Full Safety](https://github.com/yzsind/mydb/actions/runs/43975534104) | Passed: ASan fuzz targets, deterministic fuzz regressions, RC/crash/replication histories, and MySQL ROW/FULL receiver |

The full receiver run used MySQL 8.0.46 and passed both `CRC32` and `NONE`
binlog-checksum modes.

## Release-candidate packages

Assets contain these three archives and `SHA256SUMS`.

| Platform | Archive | Native validation environment |
| --- | --- | --- |
| macOS Apple Silicon | `mydb-1.0.0-macos-arm64.tar.gz` | macOS 26.5.1 ARM64 |
| Linux x86_64 | `mydb-1.0.0-linux-x86_64.tar.gz` | Ubuntu 24.04 x86_64; built against glibc 2.17 |
| Windows x64 | `mydb-1.0.0-windows-x86_64.zip` | GitHub `windows-2022` x64 hosted runner |

Every runtime archive includes the database server (`mydb`), SQL client
(`mydb-cli`), backup tool (`mydb-backup`), the Apache-2.0 license, third-party
license texts, and required corresponding MPL source archives. Windows
executables use the `.exe` suffix and require compatible Visual C++ and
Universal CRT runtimes. Other source-build targets do not imply prebuilt
archives. Container images are outside this Release's attachments.

## Documentation, feedback, and license

- [Documentation](README.md)
- [Issue reporting guide](reporting-issues.md)
- [Report an issue](https://github.com/yzsind/mydb/issues) with the exact version,
  OS, relevant logs, and reproducible SQL or scripts.
- [Apache License 2.0](../LICENSE)
