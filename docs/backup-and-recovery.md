# mydb Backup and Recovery Guide

This guide covers creating, inspecting, querying, restoring, archiving, and retaining native physical backups in mydb. It is intended for database administrators and operations staff. Current repository, image, bundle, and catalog versions are listed in the generated [storage format registry](storage-format-versions.md).

mydb is still developing its first product release. Rehearse recovery in the target deployment environment before using these procedures. For recovery across versions, check the [storage format versions](storage-format-versions.md); the product version alone does not establish physical format compatibility.

## 1. Capabilities and Recovery Scope

| Capability | Entry point | Result and purpose |
| --- | --- | --- |
| Online full backup | `BACKUP DATABASE` | Creates a complete, sealed physical base that supports direct read-only queries and subsequent WAL recovery |
| Manual WAL archiving | `WAL_ARCHIVE=MANUAL`, `BACKUP WAL` | Retains unarchived WAL on the source; an administrator or external scheduler triggers transfer |
| Continuous WAL archiving | `WAL_ARCHIVE=CONTINUOUS` | Automatically transfers subsequent WAL without periodic archive SQL commands |
| Point-in-time recovery (PITR) | `mydb-backup restore` | Uses a full base and continuous WAL to recover to a specified time, LSN, or latest confirmed position |
| Direct backup queries | `mydb --backup-repository ... --base ...` | Starts a read-only instance at the base's fixed position without copying the whole database or replaying WAL |
| Independent writable recovery | `restore --writable` or `promote` | Creates an independent instance for service recovery or development validation |
| Offline migration and retention management | `export`, `import`, `prune` | Creates a self-contained bundle, imports it into a new repository, or removes history according to a recovery window |

`BACKUP DATABASE` covers **the persistent database state of the entire instance**, regardless of the current `USE` database. It does not accept an individual database or table name. There is currently no separate incremental backup command based on changed data-file blocks; incremental recovery after a full backup relies on WAL archiving.

A full base contains the catalog, table and LOB data, undo, indexes, sequences, temporal history, and persistent system-object state needed for recovery and queries. Save server startup configuration, TLS files, runtime logs, and external keys separately. An independently restored instance does not inherit the source's node identity, replication role, or archive binding.

Native images and their state pointers have separate format versions; earlier development formats are incompatible. Promotion of a restored image commits through a single state pointer replacement, leaving the completed manifest immutable. See [storage format versions](storage-format-versions.md) for the authoritative version list.

**Completing a full backup does not automatically enable WAL archiving.** A full backup alone supports recovery to its base position. To recover subsequent business changes, first establish an archive policy and ensure that the required WAL has been archived successfully.

## 2. Preparation

### 2.1 Tools and Privileges

The server executes online backup SQL; `mydb-backup` inspects repositories and performs recovery from the operating system command line. To build the tools from source:

```bash
cargo build --release --locked --features tools --bin mydb --bin mydb-backup
./target/release/mydb-backup --help
```

The examples below assume both binaries are in `PATH`. When running from the source directory, replace the command names with `./target/release/mydb` and `./target/release/mydb-backup`.

Accounts executing backup administration SQL need `BACKUP_ADMIN` or equivalent administrative privileges. An administrator with grant authority can grant it to an existing account:

```sql
GRANT BACKUP_ADMIN ON *.* TO 'backup_operator'@'localhost';
```

The source must be a standalone instance or primary using persistent durability. Physical replicas, read-only recovery images, and `memory` durability do not support online full backups or WAL protection. Offline tools do not authenticate with SQL accounts; the operating system user must have the required file access permissions.

### 2.2 Paths and Platform Requirements

Commands in this guide use Linux/macOS shell syntax. All example directories are under `/tmp/mydb` and are intended only for rehearsals. For long-term backups, use actual paths with capacity planning and durable retention.

| Example path | Purpose |
| --- | --- |
| `/tmp/mydb/source` | Source instance data directory |
| `/tmp/mydb/repository` | Backup repository |
| `/tmp/mydb/query-runtime` | Runtime directory for direct backup queries |
| `/tmp/mydb/restored` | Independent restored data directory |
| `/tmp/mydb/verify-work` | Temporary workspace for recovery rehearsals |

SQL repository arguments accept absolute local paths or `file:///absolute/path`; paths are interpreted on **the server host**. Offline tools use absolute filesystem paths for repository, data, work, input, and output locations, not `file://` URIs. S3, HTTP, and remote database backup destinations are not currently supported.

The source directory, repository, and recovery target must not overlap or be nested within each other. The recovery target must not exist or must be empty, and cannot be inside another database directory. Backup paths cannot contain `.` or `..` components or user-supplied symbolic links; symbolic links inside the repository are also rejected.

The repository filesystem must support the file locking, durable synchronization, and atomic publication operations used by the program. Linux publication requires kernel and filesystem support for `renameat2(RENAME_NOREPLACE)`; the glibc version alone does not establish support. Unsupported operations fail explicitly rather than falling back to overwriting an existing target. See [hardware and OS requirements](hardware-and-os-requirements.md) for other platform requirements and the Windows verification scope.

### 2.3 Capacity and Online Impact

Reserve separate space for source WAL retention, full bases, backup sealing, recovery workspaces, and query temporary files. A full backup performs a checkpoint, file copying, image recovery, index preparation, and verification. Its final size and duration depend on the data and indexes; no fixed size multiplier or throughput can be promised.

Most data-copying stages allow concurrent DML, but capture boundaries pause new writes and wait for conflicting transactions to finish. DDL or maintenance statements may be rejected during capture. Long write transactions, heavy DDL, I/O pressure, or space pressure may cause backup timeouts or failures. Create full backups during quieter periods and monitor application latency and backup job errors.

## 3. Create and Confirm a Full Backup

Connect to the source instance, for example:

```bash
mysql --host=127.0.0.1 --port=4837 --user=root --password
```

Execute the following SQL:

```sql
BACKUP DATABASE TO 'file:///tmp/mydb/repository';
SHOW MYDB BACKUP JOBS;
```

A missing or empty repository can be initialized automatically. The default WAL encoding is `ADAPTIVE_LZ4`, WAL encryption is disabled, and the archive policy is `DISABLED`. An ordinary nonempty directory will not be overwritten as a repository. To specify encoding or a key, initialize it first as described in Section 9.

`BACKUP DATABASE` returns a job row asynchronously. Record its `Job_ID` and run `SHOW MYDB BACKUP JOBS` repeatedly to find the same job:

| `State` | Meaning and action |
| --- | --- |
| `RUNNING` | Still running; `Phase` identifies the current stage |
| `COMPLETED` | Publication is complete; the job's `Base_ID` is ready for use |
| `FAILED` | Did not complete successfully; inspect `Error`, resolve the cause, and resubmit |
| `CANCELLED` | Cancelled; do not use it as a successful backup |

A returned `Job_ID` or preallocated `Base_ID` does not mean the backup succeeded. Require a `COMPLETED` job and a base that can be listed in the repository:

```sql
SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repository';
SHOW MYDB BACKUP STATUS;
```

`SHOW MYDB BACKUPS` returns `Base_ID`, `Source_ID`, `Stream_ID`, `Source_End_LSN`, and `Source_End_Time`. An independent base created with archiving disabled may have no `Stream_ID`. Time fields contain source time in Unix microseconds, not formatted local time.

`BASE_ID`, `STREAM_ID`, and `JOB_ID` below are placeholders; replace them with actual output values. Each full backup creates an independent base. Creating another full backup does not automatically reset the progress of an existing archive stream.

## 4. Configure WAL Archiving for Ongoing Recovery

### 4.1 Continuous Archiving

For ongoing PITR, configure the policy before creating a full backup:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=CONTINUOUS;
SHOW MYDB BACKUP STATUS;

BACKUP DATABASE TO 'file:///tmp/mydb/repository';
SHOW MYDB BACKUP JOBS;
```

Enabling the policy initially enters `WAITING_BASE`; no base ready for WAL continuation exists yet. Wait for the full backup job to reach `COMPLETED`, confirm `Lifecycle=ACTIVE`, and record `Stream_ID`. Before releasing the capture write barrier, the first base transfers responsibility for retaining WAL after its end position to the archive stream.

The server then archives automatically. Periodic background checks do not guarantee a fixed one-second recovery point. Progress depends on job duration, load, disks, and error retries; use `Durable_Archived_LSN`, `Covered_Time`, and health status to assess it.

If previous full backups were created with archiving disabled, create another full backup after enabling archiving to establish a new archive stream.

### 4.2 Manual Archiving

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=MANUAL;
BACKUP DATABASE TO 'file:///tmp/mydb/repository';
```

After the full backup completes, run the following according to the backup schedule:

```sql
BACKUP WAL TO 'file:///tmp/mydb/repository';
SHOW MYDB BACKUP JOBS;
```

`MANUAL` retains unarchived WAL on the source but does not transfer it automatically. `BACKUP WAL` fixes the coverage boundary for this request and returns an asynchronous job. After the job reaches `COMPLETED`, check `Durable_Through_LSN`, `Covered_Time`, and `Catalog_Generation`, and save the archive completion receipt. Writes after job submission are not necessarily included in that archive operation.

`BACKUP WAL` also works with an active `CONTINUOUS` stream. It fails if no stream is active.

### 4.3 Policy Changes and Instance Restarts

An active stream can switch directly between `MANUAL` and `CONTINUOUS` while preserving the same stream and existing progress. Each source instance permits only one enabled WAL archive destination at a time.

The policy, repository binding, and WAL retention information are persisted on the source and reloaded at restart. **Do not use `--backup-repository` to enable automatic archiving on the source.** That option starts direct backup queries as described in Section 5. Restarting an encrypted archive source still requires `MYDB_BACKUP_KEY_FILE`.

### 4.4 Stop Archiving Safely

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=DISABLED;
SHOW MYDB BACKUP JOBS;
SHOW MYDB BACKUP STATUS;
```

A normal stop fixes the final archive boundary, enters `DRAINING`, and runs a `STOP` job. WAL retention is released only after archiving through that boundary and committing a closure record. Confirm job completion, `Effective_Policy=DISABLED`, and `Lifecycle=CLOSED`. If stopping fails, protection remains in place; resolve the error and repeat the stop command for the same repository.

If the repository cannot be made usable again and an administrator explicitly accepts abandoning the unarchived tail, run:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=DISABLED, DISCARD_PENDING=TRUE;
```

This releases the retention obligation for unarchived WAL; **recovery to the abandoned tail can no longer be guaranteed**. Existing complete bases and archived history remain usable. Re-enabling archiving requires a new full base and a new archive stream.

### 4.5 Cancel a Job

```sql
CANCEL MYDB BACKUP 'JOB_ID';
SHOW MYDB BACKUP JOBS;
```

Whether cancellation is allowed depends on the job phase; publication may explicitly reject it. Cancelling one job does not automatically disable the archive policy. Check job status and protection status separately.

## 5. Query a Full Backup Directly in Read-Only Mode

Start an instance on a separate port using the actual ID of a completed base:

```bash
mydb --backup-repository /tmp/mydb/repository \
  --base BASE_ID --runtime-dir /tmp/mydb/query-runtime \
  --host 127.0.0.1 --port 4838
```

Connect from another terminal:

```bash
mysql --host=127.0.0.1 --port=4838 --user=root --password
```

This instance queries the base's fixed `Source_End_LSN`; it neither follows the source nor applies additional archived WAL. To query another point in time, first perform an independent restore as described in Section 6.

Persistent backup files are opened read-only, without copying the database, replaying WAL, checkpointing, or repairing indexes. Runtime logs and query spill files go in `--runtime-dir` outside the repository. Multiple query processes can share a base using separate ports and runtime directories. Readers hold a shared repository lease, so pruning reports the repository as busy while they run.

`--backup-repository`, `--base`, and `--runtime-dir` must be supplied together and cannot be combined with `--data-dir` or an inherited `MYDB_DATA_DIR`. Remove `MYDB_DATA_DIR` from the query process's environment if it is set. Do not pass the repository's internal `bases/BASE_ID/data` directory to an ordinary `--data-dir` startup.

### 5.1 FULLTEXT Queries

Immutable backup queries support `MATCH ... AGAINST` filtering and score
projection, including boolean prefixes, phrases, and prepared query parameters.
They use the same MVCC-visible row versions as ordinary reads. If the base
contains unresolved prepared XA changes, their uncommitted text remains
invisible; matching committed text remains searchable. Querying does not modify
the sealed heap or index files or resolve the prepared branches.

Integrity verification checks stored bytes. Include representative queries and
independent content digests in recovery acceptance to check query results too.

## 6. Restore to an Independent Data Directory

### 6.1 Pre-Restore Checks

Offline tools can read published backups while the source remains running; stopping the source is unnecessary. Prepare a fresh recovery target and confirm disk capacity, binary format compatibility, and key availability:

```bash
mydb-backup list --repository /tmp/mydb/repository
mydb-backup verify --repository /tmp/mydb/repository
```

`list` outputs one base's JSON metadata per line, commonly including `id`, `stream_id`, `source_end_lsn`, and `source_end_time`. For encrypted repositories, add `--key-file /absolute/path` to operations that decrypt WAL, such as `verify`.

Recovery modifies only its private workspace and target directory, never overwriting the source instance or repository. An existing nonempty target fails immediately; in-place overwrite recovery is unsupported.

### 6.2 Restore to a Full Base

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --base BASE_ID --data-dir /tmp/mydb/restored
```

Specifying `--base` without a recovery target restores exactly to that base position without reading subsequent archived WAL.

### 6.3 Restore to the Latest Archived Position

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --stream STREAM_ID --target latest \
  --data-dir /tmp/mydb/restored-latest
```

Alternatively, use `--base BASE_ID --target latest` to recover along the specified base's archive stream. `latest` means the repository's latest confirmed coverage visible during this recovery plan, not the source instance's latest current commit.

If neither `--base` nor a target is specified, the default is the latest coverage. If the repository has multiple eligible branches, explicitly specify `--stream` or `--base`; the tool does not guess the recovery source.

### 6.4 Recover by Time

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --stream STREAM_ID \
  --target-time '2026-10-19T12:30:00+08:00' \
  --data-dir /tmp/mydb/restored-time
```

Specify the target time in RFC 3339 format with an explicit timezone, such as `+08:00` or `Z`. Time selection uses timestamps assigned in source WAL append order and published time coverage boundaries, not backup file modification times.

The base must be no later than the target time, and the archive must confirm coverage of that time. The tool replays complete WAL frames up to the target time and handles uncommitted transactions during recovery. Targets outside the confirmed time range fail. Even if no business writes occurred during that period, wall-clock time alone does not establish coverage.

### 6.5 Recover by LSN

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --stream STREAM_ID --target-lsn 12345 \
  --data-dir /tmp/mydb/restored-lsn
```

`12345` is a format example; replace it with an actual confirmed target LSN. The target must be on a complete WAL frame boundary and cannot split a physical transaction batch. Use `Durable_Through_LSN` from a completed archive job receipt to select a confirmed archive boundary.

`--target latest`, `--target-lsn`, and `--target-time` are mutually exclusive. Recovery fails if the target precedes available bases, exceeds coverage, encounters a WAL gap or file corruption, or has a source identity mismatch.

### 6.6 Check Recovery Results and Start the Instance

On successful recovery, the tool outputs a JSON report:

| Field | Meaning |
| --- | --- |
| `base_id`, `stream_id` | The selected base and archive stream |
| `base_end` | Base end LSN |
| `restored_source_lsn` | Actual source LSN reached by recovery |
| `source_time` | Source time of the recovery image, in Unix microseconds |
| `local_checkpoint` | Local checkpoint of the recovery image; do not treat it as a source LSN |
| `read_only` | Whether the image is read-only |
| `catalog_generation` | Repository catalog generation used for this recovery |

The default result is a read-only image that can be started as an ordinary data directory:

```bash
mydb --data-dir /tmp/mydb/restored \
  --host 127.0.0.1 --port 4839
```

If the tool reports that the complete recovery directory is visible but final publication durability is uncertain, retain that directory. Stop any instance using it, then run:

```bash
mydb-backup confirm --data-dir /tmp/mydb/restored
```

`confirm` takes exclusive access to the directory, checks the original recovery manifest and all contents, and synchronizes files, directories, and the parent directory. It neither replays recovery nor overwrites data. It applies only to complete independent recovery images that have not accepted subsequent business changes. Repository bases, running instances, incomplete images, and modified images are rejected. Restoring over a nonempty target remains unsupported.

After connecting, check the database and object lists, critical table row counts and content digests, business aggregates, index access results, and known business records around the target time. Server startup or a successful `SELECT 1` alone is insufficient to validate business recovery.

Recovery can retain unresolved prepared XA transactions, whose data remains invisible. PITR can apply subsequent source decisions from the archive; after making an independent instance writable, an administrator can also make local decisions using external transaction coordination information.

## 7. Use a Recovery Image for Writable Workloads

If writes are already required when creating the recovery image, add `--writable` directly:

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --stream STREAM_ID --target latest \
  --data-dir /tmp/mydb/restored-writable --writable
```

To validate the image read-only first, complete the Section 6 checks, stop the instance using that recovery directory, and then run:

```bash
mydb-backup promote --data-dir /tmp/mydb/restored
mydb --data-dir /tmp/mydb/restored \
  --host 127.0.0.1 --port 4839
```

`promote` validates the image and rejects running instances, bases inside repositories, and images with data files shared through hard links. A backup original cannot be made writable directly.

The first writable startup creates independent node and source identities. It neither automatically rejoins the original replication relationship nor inherits the source's WAL archive configuration. Before switching business traffic, check accounts, connection addresses, external configuration, and application validation results. Coordinate stopping writes on the original instance before switching connections. If backups are still needed, establish an archive policy and full base again on the new instance.

## 8. Backup Verification and Recovery Rehearsals

### 8.1 Integrity Checks

```bash
mydb-backup verify --repository /tmp/mydb/repository
```

This operation verifies published bases and archive records, data checksums, and coverage relationships. Add `--base BASE_ID` to limit which base is checked. Repository archives and coverage relationships are still checked; other archive errors are not ignored.

### 8.2 Actual Recovery Checks

```bash
mydb-backup verify --repository /tmp/mydb/repository \
  --base BASE_ID --recover-check \
  --work-dir /tmp/mydb/verify-work
```

`--recover-check` creates a temporary image outside the repository, attempts recovery to the selected branch's latest covered position, and opens a read-only database. It removes the temporary image afterward. `--work-dir` must be used with this option and needs enough recovery space.

Even with `--base`, this command's recovery-check target is `latest`. To rehearse recovery to an exact base or a particular time, run the corresponding `restore` command. Use `--base` to identify the source in repositories with multiple branches.

Include integrity checks and actual business recovery rehearsals in the backup plan. Record backup completion time, archive boundaries, recovery duration, and business validation results. Verify that bundles saved on another machine can be independently imported and restored in an environment isolated from the source.

## 9. WAL Compression and Encryption

### 9.1 Initialize a Repository

To explicitly initialize an unencrypted repository:

```bash
mydb-backup init --repository /tmp/mydb/repository \
  --wal-encoding ADAPTIVE_LZ4
```

WAL encoding supports `NONE` and `ADAPTIVE_LZ4`, with adaptive LZ4 as the default. AES-256-GCM encryption is optional. Encoding, key ID, and key binding are fixed at initialization and cannot subsequently be changed in place.

### 9.2 Configure Encrypted Archiving

Prepare a protected key file, such as `/tmp/mydb/secure/backup.keys`. The file can contain only 64 hexadecimal characters representing a 32-byte key, or use a JSON mapping:

```json
{"keys":{"daily":"<replace-with-64-hexadecimal-characters>"}}
```

This illustrates the format; do not use the placeholder as an actual key. Key IDs allow only ASCII letters, digits, `-`, and `_`, and cannot use the reserved name `plain`. On Unix, the key file must prohibit access by group and other users, for example:

```bash
chmod 600 /tmp/mydb/secure/backup.keys
mydb-backup init --repository /tmp/mydb/encrypted-repository \
  --wal-encoding ADAPTIVE_LZ4 --wal-key-id daily \
  --key-file /tmp/mydb/secure/backup.keys
```

Supply the key file path when starting the source server:

```bash
MYDB_BACKUP_KEY_FILE=/tmp/mydb/secure/backup.keys \
  mydb --data-dir /tmp/mydb/source --host 127.0.0.1 --port 4837
```

Then configure archiving and create a full backup in the encrypted repository as described in Section 4. The server reads the key ID from the repository header; no `backup_key_id` or `MYDB_BACKUP_KEY_ID` setting is needed. Key contents are not written to SQL or repository metadata.

Explicitly supply the key file for offline operations, for example:

```bash
mydb-backup restore --repository /tmp/mydb/encrypted-repository \
  --base BASE_ID --target latest \
  --data-dir /tmp/mydb/restored-encrypted \
  --key-file /tmp/mydb/secure/backup.keys
```

WAL encryption **does not encrypt the full base that can be queried directly**. Exported bundles may also contain plaintext bases. To encrypt all data at rest, use a protected encrypted filesystem or separate bundle encryption measures.

Keep keys securely and separately from repositories, and include them in disaster recovery rehearsals. Rotate keys using a new repository and a new full base. Define how recovery coverage connects while stopping protection on the old repository and establishing protection on the new one. Old backups still depend on their original keys; do not destroy those keys prematurely.

## 10. Offline Export and Import

### 10.1 Export a Recovery Target

```bash
mydb-backup export --repository /tmp/mydb/repository \
  --base BASE_ID --target latest \
  --output /tmp/mydb/offline.bundle
```

A bundle contains the required original base, sealing information, and archived WAL, with file checksums, and can be used without the source instance. The output file must not already exist. Use `--target-lsn` or `--target-time` for a fixed recovery target; specifying only `--base` without a target exports that base.

### 10.2 Export a PITR Window

```bash
mydb-backup export --repository /tmp/mydb/repository \
  --stream STREAM_ID --retain-pitr 7d \
  --output /tmp/mydb/window.bundle
```

A window export saves the bases and continuous WAL needed to cover that period. `--retain-pitr` cannot be combined with `--base`, `--target-lsn`, or `--target-time`. Window units are `s`, `h`, and `d`, for example `3600s`, `24h`, and `7d`.

### 10.3 Import and Restore

Transfer the bundle to the target host using an existing file transfer method, then run:

```bash
mydb-backup import --repository /tmp/mydb/imported \
  --input /tmp/mydb/offline.bundle
mydb-backup verify --repository /tmp/mydb/imported
mydb-backup list --repository /tmp/mydb/imported
mydb-backup restore --repository /tmp/mydb/imported \
  --base BASE_ID --target latest --data-dir /tmp/mydb/offline-restored
```

The import target must not exist or must be empty; merging into an existing repository is unsupported. Import checks the bundle and dependencies in a private workspace before publication. Truncation, trailing data, conflicting paths, and objects exceeding limits are rejected. For encrypted repositories, add the corresponding `--key-file` to export, import, verification, and WAL recovery commands. Bundles do not contain keys.

## 11. Retention Windows and History Pruning

First preview the pruning plan:

```bash
mydb-backup prune --repository /tmp/mydb/repository \
  --stream STREAM_ID --retain-pitr 7d
```

After confirming that the plan meets retention requirements, apply it:

```bash
mydb-backup prune --repository /tmp/mydb/repository \
  --stream STREAM_ID --retain-pitr 7d --apply
```

The default is a preview with no file deletion. The window is calculated backward from the selected archive stream's latest confirmed source time, **not from the local time when the command runs**. Pruning retains bases within the window, the nearest usable base before its start, and the WAL needed for recovery. Actual retained history may therefore exceed the requested window. If insufficient history exists, `7d` does not create the missing seven days of data.

Output includes `requested_window_start`, `actual_history_start`, `delete_bases`, `delete_ranges`, and `orphan_wal_blobs`. Without a retention window, pruning removes only orphan WAL blobs and does not delete bases by age.

Applying a pruning plan requires an exclusive repository lease. Direct query processes and other repository operations holding shared leases make it report busy; retry after those operations exit. Pruning stops on corrupt metadata, invalid coverage, or missing required keys. Do not manually delete bases, archives, or catalog records from the repository to free space.

Pruning uses a durable plan. After interruption, repeating `prune --apply` continues processing leftover directories from committed deletions, complete unpublished artifacts, and recognizable temporary blobs. If a new archive has referenced a formerly orphaned blob, the retry preserves it. Private recovery, import, export, and initialization workspaces record ownership and hold file locks. Retrying the same target reclaims workspaces whose owners are gone; unknown files are not deleted as ordinary orphan blobs.

## 12. Routine Monitoring and Troubleshooting

### 12.1 Monitoring Entry Points

```sql
SHOW MYDB BACKUP STATUS;
SHOW MYDB BACKUP JOBS;
SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repository';
```

Status is returned as `Name` / `Value` rows. Focus on the following fields:

| Field | Operational meaning |
| --- | --- |
| `Requested_Policy` / `Effective_Policy` | Requested and effective policies; these may differ while stopping and draining |
| `Lifecycle` | Lifecycle states such as `UNCONFIGURED`, `WAITING_BASE`, `PREPARING_BASE`, `ACTIVE`, `DRAINING`, `CLOSED`, or `BROKEN` |
| `Health` | `OK`, `WAITING_FOR_COMMAND` in manual mode, and `BROKEN`, `WAL_PRESSURE`, `ERROR`, `RETRYING`, or `CLOSED_PENDING_REPOSITORY` |
| `Latest_Base`, `Latest_Base_LSN`, `Latest_Base_Time` | Latest base and its end position |
| `QUERY_READY` / `CONTINUATION_READY` | Base readiness flags in the current control state; these do not replace integrity verification |
| `Stream_ID`, `Source_ID`, `Repository_ID` | Identify the source, repository, and stream being monitored |
| `Source_LSN` / `Durable_Archived_LSN` | Current source log position and durably archived position |
| `Covered_Time` | Confirmed source time coverage, in Unix microseconds |
| `Recoverable_From_LSN` / `Recoverable_Through_LSN` | Recoverable positions reported by the current control state; also consult the repository listing for historical branches |
| `WAL_Floor` | Lower bound of source WAL retained to protect the recovery chain |
| `Pending_WAL_Bytes` / `MAX_PENDING_WAL_BYTES` | Unarchived WAL bytes and the budget |
| `WAL_Pressure` | Whether WAL budget pressure has been reached |
| `Last_Error` / `Next_Retry_Unix_Ms` | Latest error and next retry time, with the latter in Unix milliseconds |

Status counters may change concurrently and are not a transactional snapshot. Read-only recovery images return `READ_ONLY_IMAGE` and have an empty local job list. Job history retains at most 256 entries and does not replace external long-term audit records.

### 12.2 WAL Budgets and Write Pressure

The default unarchived WAL budget is 16 GiB. It can be adjusted explicitly, for example to 32 GiB:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET MAX_PENDING_WAL_BYTES=34359738368;
```

The budget counts actual WAL frame bytes. Under pressure, new business writes fail, while reads, archiving, policy administration, and rollback retain a management path. The system neither silently discards WAL nor automatically disables protection. Writes are also subject to filesystem space admission; increasing the WAL budget does not replace adding real capacity.

First restore repository availability, resolve archive errors, or submit and await `BACKUP WAL` in manual mode. Use the explicit discard operation in Section 4.4 only if unarchived data must be abandoned.

### 12.3 Common Problems

| Symptom | Checks and action |
| --- | --- |
| No continuous archiving after a full backup | Check the policy; it defaults to `DISABLED`. Enable `MANUAL` or `CONTINUOUS`, then create a new full base |
| Prolonged `WAITING_BASE` | Check whether a full backup job was submitted and whether it failed or is still running |
| `Health=RETRYING` or `Lifecycle=BROKEN` | Inspect `Last_Error` and job errors; check repository mounts, space, permissions, keys, and identity changes. Do not delete source control files or WAL leases |
| Backup times out waiting for write transactions or a barrier | Check long write transactions, concurrent DDL, and disk latency; retry after reducing contention |
| `restore branch is ambiguous` | Select an actual `--stream` or `--base` from the repository listing |
| Target exceeds coverage or the time fence | If the source is available, continue archiving and wait for completion; otherwise choose a target within existing coverage |
| LSN splits a WAL frame or transaction batch | Use a complete valid boundary, such as the LSN in a completed archive receipt |
| Repository busy; cannot prune | Stop direct query instances and wait for recovery, export, or other repository operations to finish, then retry |
| Nonempty target, overlapping paths, or symbolic link errors | Use a fresh absolute path outside repository and database directories; do not overwrite an existing instance during recovery |
| Key permission or binding mismatch | Check file permissions, key ID, and original key material; replacing the repository header is not a repair |
| Checksum mismatch, archive gap, or identity error | Preserve error details and use another complete backup with a continuous recovery chain. Do not bypass checks or edit the manifest manually |
| `promote` reports an active instance or hard links | Stop the instance using that directory; restore a hard-linked image again with independent files before retrying |

The implementation sets explicit resource limits, such as 64 MiB per metadata object, 4 MiB per WAL blob, and controlled metadata and temporary-space budgets. File generation leases and bounded streaming have replaced the old limits of 1024 files and 512 WAL segments. Exceeding a budget fails explicitly; see Section 15 for scale verification and verifiable resumption.

WAL protection also fixes the temporal history retention window at capture time. Before changing that window, switching source lineage, or selecting a nonpersistent mode, stop protection as required to avoid breaking the recovery chain. Recovery uses strict validation; do not rely on forced tolerance or salvage settings to bypass corruption.

## 13. Command Reference and Related Documentation

| Entry point | Description |
| --- | --- |
| `mydb-backup init` | Initialize repository encoding and optional key binding |
| `mydb-backup list` | List published bases |
| `mydb-backup verify` | Check integrity, optionally including actual recovery |
| `mydb-backup restore` | Create an independent read-only or writable recovery image |
| `mydb-backup promote` | Make a stopped, independent recovery image writable |
| `mydb-backup confirm` | Verify and confirm installation durability for a complete independent image or repository |
| `mydb-backup recoverability` / `keys` / `diagnostics` | Recoverable intervals, key dependencies, and latest verification or rehearsal results |
| `mydb-backup inspect-operation` / `gc` / `checkpoint` | Ownership diagnostics, physical garbage collection, and catalog snapshots/cold history compaction |
| `mydb-backup prune` | Preview or apply retention pruning |
| `mydb-backup export` / `import` | Export a self-contained bundle or import into an empty target repository |

See `mydb-backup --help` for complete arguments. Unknown commands, duplicate options, missing arguments, and mutually exclusive targets produce errors. The tool exits with code 0 on success and 1 on failure; failures include an error message, SQLSTATE, and available diagnostics on standard error.

Related documentation:

- [Backup SQL Reference](statements/backup/README.md)
- [Startup Configuration](configuration.md)
- [Native Physical Replication](native-replication.md)
- [Storage Format Versions](storage-format-versions.md)

## 14. Recovery Planning, Diagnostics, and Interruption Handling

Before recovery, run a complete preflight check that shares the selection rules used by actual recovery:

```bash
mydb-backup restore --repository /tmp/mydb/repository \
  --base BASE_ID --target-lsn 12345 --data-dir /tmp/mydb/planned-image --dry-run
mydb-backup recoverability --repository /tmp/mydb/repository
mydb-backup recoverability --repository /tmp/mydb/repository --verify
mydb-backup verify --repository /tmp/mydb/repository --record
mydb-backup diagnostics --repository /tmp/mydb/repository
```

`--dry-run` pins the catalog generation/digest and reports the source/timeline/stream, base, requested and actual target, prepared XA, WAL dependency summary, validation level, and lower bound on space. It creates neither the target nor missing parent directories. It reads and verifies base digests and required WAL, which may incur substantial I/O. When the space upper bound is unknown, it reports a null value with an explanation; the lower bound is not a promise of the capacity needed to finish recovery. Actual recovery pins its own catalog again.

`recoverability` validates only metadata by default and lists recoverable intervals based on retained bases and continuous WAL; it does not prove payload integrity. `--verify` validates base and WAL payloads. Closure or abandonment records include the last confirmed position. `verify --record` explicitly saves the latest verification result; add `--recover-check --work-dir /tmp/mydb/drill` to save an independent rehearsal result. `diagnostics` shows each result's catalog, errors, and a stale flag relative to the current catalog. An old success record does not validate newly published objects.

`SHOW MYDB BACKUP JOBS` lists the most recent jobs first, adding creation/start/end times, failure classification, per-phase durations, actual target repository ID, completed file count, and bytes remaining in the current copy phase. Phases that cannot be estimated report NULL, and existing field order is preserved. Automatic archiving also runs through a bounded job queue. `SHOW MYDB BACKUP STATUS` distinguishes BROKEN, resource pressure, error retries, waiting for manual action, and closed states, and reports source time lag, consecutive errors, resource settings, and process-lifetime I/O counters. These count logical operations in that process, not all kernel physical I/O; child processes are counted separately. The full manifest's `capture_stats` records both freezes and their subphases, bytes successfully read stably, file counts, and sealing resource settings.

```bash
mydb-backup inspect-operation --path /tmp/mydb/restored --kind restore-work
mydb-backup inspect-operation --path /tmp/mydb/restored --kind restore-work --apply
mydb-backup confirm --data-dir /tmp/mydb/restored
mydb-backup confirm --repository /tmp/mydb/repository
mydb-backup gc --repository /tmp/mydb/repository --apply
mydb-backup checkpoint --repository /tmp/mydb/repository --gc
```

`inspect-operation` checks the ownership journal with its digest and the OS lock. `--apply` cleans only private workspaces with proven ownership and no active holder; it neither skips object validation nor deletes arbitrary unknown files. Other kinds include `repository-init`, `control-init`, `import-work`, `export-work`, `range-work`, `generation-pins`, `wal-pins`, and `catalog-checkpoint`. `confirm` fully verifies an installed image or repository and reconfirms synchronization; it does not advance archive ACKs for the source. `gc` cleans committed deletion intents and leftover artifacts with proven ownership, without implicitly setting a retention window. `checkpoint --gc` compacts the cold catalog chain into history packs backed by digest proofs and removes packed loose files under an exclusive GC lease. Old source receipts must still pass membership proofs for their original generation/digest. Automatic checkpoints run every 1024 generations.

After repairing missing or corrupt objects, explicitly reapply the original MANUAL/CONTINUOUS policy on a BROKEN source. Before restarting protection, the system checks source/timeline, ancestry of old receipts, and all live dependencies; protection is not restored while objects remain corrupt. Increasing the WAL budget alone does not constitute a repair. Abandoning the tail first persists a source-side closure intent. An offline repository may show `CLOSED_PENDING_REPOSITORY`; repository closure is recorded only after reconciliation. An abandoned position is never reported as backed up.

## 15. Fixed-Input Resumption, Resources, and Keys

```bash
mydb-backup import --repository /tmp/mydb/imported \
  --input /tmp/mydb/offline.bundle --resume
mydb-backup restore --repository /tmp/mydb/repository \
  --base BASE_ID --target-lsn 12345 --data-dir /tmp/mydb/restored --resume
mydb-backup keys --repository /tmp/mydb/repository --key-file /absolute/path/key
```

Private payloads from failed operations are retained only with explicit `--resume`. Import binds ownership to the complete bundle identity, rereads and verifies each block against the input, and finally checks per-object SHA-256, the bundle transcript digest, and object counts. Unknown objects, duplicate paths, and workspace link substitutions fail. The transcript covers, in order, the magic, object paths and lengths, verified object SHA-256 values, terminator, and counts. Payloads are bound through object SHA-256 to avoid recomputing an overall digest for the same payload. Exported base payloads also match the original manifest's chunk digests. Restore requires `--base` and a fixed base/LSN/time target, rejecting `latest`. It reuses only immutable base cache blocks validated against the original manifest, recopies bad blocks, and regenerates the WAL plan and recovery image each time. Writable resumption files must be private regular files with a single link; symbolic links, reparse points, external hard links, and special files are rejected. Cache repair cannot rewrite the same inode outside the workspace. Resumption neither reuses a changing online capture bitmap nor continues modifying a previously modified recovery image. Successful reports include `reused_base_bytes`.

File generation leases and bounded reopening support more than 1024 files and 512 WAL segments. Reopening must validate the inode/file ID, minimum EOF, and non-link attributes. Archiving durably publishes bounded batches of segments and advances acknowledgments. See [configuration](configuration.md#backup-resource-budgets) for resource parameters and their exact scopes.

`keys` lists key IDs, fingerprints, dependency counts/bytes, and key availability without exposing key material. The first version fixes one WAL key per repository. Rotation uses a new key, a new repository, and a new full backup. Retain the old repository and key until the old recovery window retires. In a handover window with business writes stopped, verify closure of the old stream, the new full backup, and continuation of the new stream. This procedure does not provide continuous PITR across repositories. For storage encryption key rotation without business downtime, use a verified filesystem encryption solution. Bases and bundles containing them may still be plaintext; WAL encryption does not encrypt full backups.

The generated [storage format registry](storage-format-versions.md) lists repository, bundle, native image, image mode, catalog checkpoint, and lineage versions. No migration from historical development formats is provided.
