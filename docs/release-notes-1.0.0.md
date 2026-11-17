# MyDB 1.0.0 — Public Preview

MyDB is a general-purpose database written in Rust for mixed transactional and
analytical workloads, with MySQL-compatible access and native AI retrieval.
This is the first product release from the new MyDB repository.

**Release maturity:** 1.0.0 is a public preview for feature validation,
compatibility testing, and evaluation in controlled environments. It is not
yet a production-grade MySQL replacement. On-disk formats are not guaranteed
to remain backward compatible between previews.

## Draft status

This release is unpublished. The intended tag is `v1.0.0`, targeting commit
[`7be88e78`](https://github.com/yzsind/mydb/commit/7be88e782cfec30180672a2a4b6e728d1b276d93).
Three verified release-candidate packages are attached: macOS ARM64,
Linux x86_64, and Windows x64, together with `SHA256SUMS`.
Package preparation does not publish this release or create the version tag.

Documentation links include subsequent editorial corrections. These updates do
not change the package source baseline or the attached archives.

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

All four GitHub Actions runs below completed successfully for the exact target
commit. The main CI's three test partitions passed 12,077 tests in total.

| Validation | Result |
| --- | --- |
| [Main CI](https://github.com/yzsind/mydb/actions/runs/43974623981) | Passed: test partitions, formatting, documentation contracts, Clippy, feature builds, and recovery checks |
| [MyDB CLI](https://github.com/yzsind/mydb/actions/runs/43974623984) | Passed: all four jobs |
| [Push Safety](https://github.com/yzsind/mydb/actions/runs/43974623975) | Passed; receiver job is excluded by the push condition |
| [Full Safety](https://github.com/yzsind/mydb/actions/runs/43975534104) | Passed: ASan fuzz targets, deterministic fuzz regressions, RC/crash/replication histories, and MySQL ROW/FULL receiver |

The full receiver run used MySQL 8.0.46 and passed both `CRC32` and `NONE`
binlog-checksum modes. Native release-package acceptance is recorded below in
addition to these source-baseline checks.

## Release-candidate packages

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

## Package validation

All three archives passed native checks for file layout and manifest hashes,
lockfile versions, server/client version output, all three tools' help output,
TLS-required SQL, typed values, transaction rollback, process-kill crash
recovery, and secondary-index access. The Linux and Windows packages were
built and tested in the successful
[Release packages workflow](https://github.com/yzsind/mydb/actions/runs/43979385399).
Downloaded CI archives were independently checked against GitHub's artifact
SHA256 records, then their package contents and manifests were checked locally.

Additional macOS checks passed authentication and permission enforcement and
backup/recovery acceptance using the packaged server and backup tool, including
LSN/time PITR, direct backup queries, transaction/XA recovery cases, offline
export/import, and a writable restore. This used 1,000 rows and one paired
comparison for functional acceptance, not a new performance benchmark.

The Linux ABI audit passed with a maximum glibc requirement of `2.17`.
Feature-specific kernel and filesystem requirements still apply; the native
package smoke test ran on Ubuntu 24.04. The Windows result is acceptance on the
listed hosted runner, not every Windows version or filesystem. Build-date
suffixes in server version strings can differ between builders; the product
version is 1.0.0 and the source fingerprint is identical across all three packages.

The dependency lockfiles match the fixed source commit. License texts and
bundled MPL source archives were verified for every platform. Detailed build
and validation records are retained separately from the download assets.

## Check and run a package

Compare the SHA256 of your selected archive with its entry in the attached
`SHA256SUMS`. For example, use `shasum -a 256 <archive>` on macOS,
`sha256sum <archive>` on Linux, or `Get-FileHash <archive> -Algorithm SHA256`
in PowerShell. `SHA256SUMS` covers the three runtime archives.

Extract the archive and open a terminal in its top-level directory. On macOS
or Linux:

```bash
./mydb --host 127.0.0.1 --port 8738 --data-dir ./data
# In another terminal in the extracted directory:
./mydb-cli --no-config -h127.0.0.1 -P8738 -uroot -p test
```

On Windows, use `.\mydb.exe` and `.\mydb-cli.exe` with the same arguments.
The interactive client prompts for the initial password (`root` unless you
set `MYDB_ROOT_PASSWORD` before the first server start). No Rust toolchain is
needed to run a package.

## Try the source baseline

With Rust 1.95 or newer, a native C toolchain, Perl, and a MySQL 8.0 client:

```bash
git clone https://github.com/yzsind/mydb.git
cd mydb
git checkout --detach 7be88e782cfec30180672a2a4b6e728d1b276d93
cargo build --release --locked --bin mydb
./target/release/mydb --host 127.0.0.1 --port 8738 --data-dir ./data
```

Connect from another terminal:

```bash
mysql -h127.0.0.1 -P8738 -uroot -proot test
```

A fresh directory creates the loopback-only `root@localhost` account with
password `root` and the `test` database. `MYDB_ROOT_PASSWORD` selects the initial
password on first startup. TLS is advertised by default with a temporary
self-signed certificate; remote connections should use a trusted certificate
and verify server identity. See the
[configuration guide](configuration.md).

## Performance evidence

Historical measurements are identified by measurement date and retain their
original methodology and source references:

- [Sysbench, 100M rows and 300 clients](sysbench-100m-300clients-2026-11-16.md):
  November 16, 2026 measurements, including 1,327,407.37 point-select QPS on a
  16-vCPU database host; each workload had one 300-second run.
- [Cohere 1M HNSW](vector-cohere-1m-hnsw-benchmark-2026-11-14.md):
  historical November 14, 2026 development snapshot comparison, with
  source commits preserved in [mydb-archive](https://github.com/yzsind/mydb-archive).
- [Performance comparison](performance-comparison.md):
  workload-specific configurations, results, and limitations.

These are historical development measurements, not new benchmarks of the
release packages or performance guarantees across workloads.

## Documentation, feedback, and license

- [Documentation](README.md)
- [Issue reporting guide](reporting-issues.md)
- [Report an issue](https://github.com/yzsind/mydb/issues) with the exact version,
  OS, relevant logs, and reproducible SQL or scripts.
- [Apache License 2.0](../LICENSE)
