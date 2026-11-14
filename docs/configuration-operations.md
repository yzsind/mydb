# Operations Configuration

These settings describe the current source checkout. See [startup configuration](configuration.md)
for option syntax, precedence and persistence, and the [option registry](configuration-registry.md)
for accepted startup names and validators.

## Server-side file access

`secure_file_priv` defaults to `NULL`: server-side `LOAD DATA INFILE`,
`SELECT ... INTO OUTFILE` / `DUMPFILE`, and `LOAD_FILE()` cannot access server
files until an administrator explicitly configures a policy. To permit an
import/export directory, run `SET GLOBAL secure_file_priv='/absolute/directory'`.
An empty value explicitly permits unrestricted paths; prefer a dedicated
directory. The existing `FILE` privilege checks still apply. `LOAD DATA LOCAL`
uses client-supplied data and is controlled separately by `local_infile`.

## Backup Resource Budgets

The following startup options also accept the corresponding `MYDB_` environment
variable (uppercase option name, for example `MYDB_BACKUP_COPY_THREADS`). Values
are fixed for the process lifetime; restart after changing them. These nine
options accept unsigned decimal integers; use raw byte counts for byte-valued
limits rather than `M`/`G` suffixes.

| Config option | Default | Accepted values / scope |
| --- | --- | --- |
| `backup_workers` | 2 | 1–8; bounded workers per database, with process-wide task admission |
| `backup_queue_capacity` | 64 | 1–256; queued tasks per database; a full queue returns an error |
| `backup_copy_threads` | Half of available CPUs, rounded up, at most 64 | 1–64, additionally capped at `backup_open_files / 4`; shared copy/hash/sync bulk slots |
| `backup_open_files` | 64 | 16–4096; backup data-handle budget, excluding database, WAL and control handles |
| `backup_bandwidth_bytes_per_sec` | 0 | 0 means unlimited; positive values throttle aggregate backup bulk bytes per process |
| `backup_chunk_memory_bytes` | 134217728 | 8388608–1073741824; aggregate copy/hash scratch, also subject to unified memory admission |
| `backup_spool_limit_bytes` | 17179869184 | At least 4194304; operation temporary-space admission, including restore plan/cache estimates |
| `backup_freeze_budget_ms` | 1000 | 1–30000; final capture gate budget, checked again after durable handoff |
| `backup_foreground_commit_target_us` | 0 | 0 disables feedback; 1–60000000 enables bounded bulk delays when recent commit EWMA exceeds the target |

The seal child receives effective memory/cache/temp configuration, including
config-file overlays, and only the copy slots reserved by its parent. The parent
releases these grants before its final sync. Bulk tasks use lower scheduling
priority where supported. Queued WAL tasks retain immutable identity/LSN tokens;
file generations are acquired when execution begins. Cancellation interrupts
resource and bandwidth waits. Rollback, WAL retention and management operations
have a separate control path; raising `MAX_PENDING_WAL_BYTES` remains possible
while an archive worker is blocked.

These limits are admission controls, not an OS-wide file descriptor or RSS cap.
`memory_target` is advisory; `backup_chunk_memory_bytes` does not bound every
metadata allocation or the whole restore process. Parent and child have separate
process-local counters and bandwidth limiters; their copy concurrency is jointly
reserved. Synchronous filesystem operations cannot always be interrupted at the
freeze deadline; an over-budget operation fails and reports the measured phases.
See [backup procedures](backup-and-recovery.md) for diagnostics and recovery.

## Recycle Bin

The [recycle bin](recycle-bin.md) is enabled by default for persistent DROP TABLE, TRUNCATE TABLE and tables removed by DROP DATABASE. It has a seven-day retention target and permits early reclamation under disk pressure. See its configuration table for GLOBAL/SESSION variables, startup option names, defaults and bounds.
