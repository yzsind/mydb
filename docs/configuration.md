# Startup Config File

mydb reads a single startup config file only when `--config` is passed explicitly:

```bash
mydb --config /etc/mydb/mydb.cnf
```

When this option is not specified, startup behavior is unchanged. mydb does not
automatically scan `/etc/my.cnf`, the user's home directory, or the data directory,
and it does not support `!include`, `!includedir`, `--defaults-file`, group suffixes,
or `print-defaults`.

The default MySQL protocol port is `8738`. Override it with `--port`,
`MYDB_PORT`, or the `port` config option.

## Format

The file must be UTF-8 and use MySQL-style INI syntax. The server reads the
`[mysqld]` and `[mydb]` groups and ignores other groups such as `[client]`. The
`[mydb]` result always overrides `[mysqld]`, regardless of the order in which the
two groups appear in the file; within a group, the last assignment of an option
wins.

```ini
[mysqld]
port = 8738
max_allowed_packet = 128M

[mydb]
data_dir = /var/lib/mydb
durability = strict
memory_target = 4G
buffer_pool_small_scan_pct = 20
general_log = OFF
```

- Group names and option names are case-insensitive, and `-` and `_` in option
  names are equivalent.
- Blank lines, `#`/`;` comments, `key=value`, bare boolean options, and single or
  double quotes are supported.
- Boolean values accept `ON/OFF`, `TRUE/FALSE`, `YES/NO`, and `1/0`.
- Byte sizes accept the `K`, `M`, `G`, and `T` binary suffixes; plain numbers with
  explicit units such as `_ms`, `_us`, and `_mb` are still interpreted according
  to the unit in their name.
- Relative paths are resolved against the working directory where mydb is
  started, not the directory containing the config file.

An unknown option, or an option that cannot be set at startup, in `[mydb]` blocks
startup so that misspelled options are not silently ignored. An unsupported MySQL
option in `[mysqld]` only produces a warning with the file line number, which
eases incremental migration of existing configurations. Test, benchmark,
crash-injection, and debug environment switches cannot be written into the config
file.

## Connection Reactors

`tokio_workers` (`MYDB_TOKIO_WORKERS`) sets the number of connection reactors
at startup, in the range 1..1024. It is a config/environment option, not a CLI
flag. On Linux, `tokio_affinity=auto` (`MYDB_TOKIO_AFFINITY`, the default) assigns
one allowed CPU to each reactor when the reactor count fits the calling
thread's effective CPU affinity mask and CPU quota. Sparse CPU masks and cgroup restrictions
are respected. Oversubscribed pools and other platforms retain scheduler-managed
placement. Use `tokio_affinity=off` to retain scheduler-managed placement on Linux.
Database-created compute and background threads retain the full server CPU mask.
Lazy pool sizing on bound reactors uses the server parallelism captured before
binding, including the startup CPU quota, rather than the single assigned CPU.
Both settings require a restart; they do not set query execution parallelism or
reserve CPUs exclusively for the database.

## TLS

The server defaults to `ssl_mode=auto`: it advertises TLS and generates an
ephemeral self-signed certificate when no certificate or key is configured.
Advertising TLS still allows plaintext connections unless
`require_secure_transport=ON` is set. The MySQL 8.0 client defaults to
`--ssl-mode=PREFERRED`: it negotiates TLS when the server offers it, so ordinary
connection examples omit this option. `PREFERRED` can fall back to plaintext
when TLS is unavailable. Use `--ssl-mode=REQUIRED` to require encryption without
verifying server identity.

Keep the default `caching_sha2_password` authentication plugin and use TLS for
client connections. A local MySQL Connector/J connection can use:

```text
jdbc:mysql://127.0.0.1:8738/test
```

Do not add `useSSL=false` or `sslMode=DISABLED` to this URL. Over TLS, SHA-2 full
authentication does not need RSA public-key retrieval, so
`allowPublicKeyRetrieval` can remain unset (Connector/J defaults it to `false`).
Connector/J defaults to `sslMode=PREFERRED`, which negotiates TLS when offered
and accepts the temporary self-signed certificate without verifying server
identity. It can fall back to plaintext when TLS is unavailable. To require
encryption, explicitly append `?sslMode=REQUIRED`; this mode fails if the server
does not offer TLS and also does not verify the certificate or hostname. The
[runnable client examples](../examples/README.md) use encrypted connections by
default.

For a stable server identity, install a CA-signed PEM certificate chain and its
matching PEM private key, then add these options to the server's config file:

```ini
[mydb]
ssl_mode = auto
ssl_cert = /etc/mydb/server-cert.pem
ssl_key = /etc/mydb/server-key.pem
require_secure_transport = ON
```

Make the key readable only by the server account and restart the server with
`--config`. Both `ssl_cert` and `ssl_key` must be supplied together; configuring
only one fails startup. CLI equivalents are `--ssl-cert` and `--ssl-key`;
environment equivalents are `MYDB_SSL_CERT` and `MYDB_SSL_KEY`.

Connect with a hostname covered by the certificate and the issuing CA:

```bash
mysql -hdb.example.com -P8738 -uapp -p \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/ca.pem test
```

For Connector/J, use
`jdbc:mysql://db.example.com:8738/test?sslMode=VERIFY_IDENTITY` and configure the
issuing CA in the JVM trust store or with `trustCertificateKeyStoreUrl`,
`trustCertificateKeyStoreType`, and `trustCertificateKeyStorePassword`.

If TLS is deliberately disabled, SHA-2 full authentication needs RSA encryption.
Connector/J then requires a preconfigured trusted `serverRSAPublicKeyFile` or an
explicit `allowPublicKeyRetrieval=true`. This is a client-side policy and cannot
be enabled by the server. Successful cached authentication does not remove this
requirement for subsequent full authentications, such as after a server restart.

`ssl_mode=disabled` disables server TLS and cannot be combined with certificate
or key options. The server's `auto`/`disabled` values differ from the client's
`REQUIRED`/`VERIFY_IDENTITY` values.

## Supported Options

<a id="data-file-io-modes"></a>
<a id="memory-used-by-wal-startup-recovery"></a>
<a id="zone-map-maintenance-and-diagnostics-supplement"></a>
See [Supported Options](configuration-resources.md#supported-options) for the detailed reference.

## Sequence Persistence

See [Sequence Persistence](configuration-resources.md#sequence-persistence) for the detailed reference.

## Performance Schema

See [Performance Schema](configuration-resources.md#performance-schema) for the detailed reference.

## Precedence and When Settings Take Effect

The override order from lowest to highest is:

1. Built-in defaults;
2. The `[mysqld]` group of the config file;
3. The `[mydb]` group of the config file;
4. Existing `MYDB_*` process environment variables;
5. Command line arguments;
6. Supported persisted global variables in
   `<data_dir>/system/mysqld-auto.cnf`.

The config file and environment variables are never written back. When `RESTART`
executes, the new process rereads the files using the original command line, so
changes to the files take effect after the restart. Static options such as
storage layout, listening address, and thread counts require a restart; dynamic
system variables can be changed with `SET GLOBAL` and persisted with
`SET PERSIST`. `mysqld-auto.cnf` is managed only by `SET PERSIST`,
`SET PERSIST_ONLY`, and `RESET PERSIST` and should not be edited by hand.

The config file may contain initialization passwords, replication passwords, or
private key paths. On Unix, mydb refuses to start if the config file allows group
or other writes; it is recommended to be owned by the service account with mode
`0600`.

### Shutdown, Corruption Recovery, and Replication Initialization

`KILL CONNECTION` closes a connection even while it is waiting for LOCAL INFILE
data. Canceling a LOCAL upload also rolls back the incomplete statement; an
interrupted upload may close the connection because a partially received frame
cannot safely become the next SQL command. During result transmission,
`KILL QUERY` completes the current frame before reporting interruption. If the
client stops reading, `net_write_timeout` bounds that wait; `KILL CONNECTION`
can close immediately.

- `SIGINT`, `SIGTERM`, SQL `SHUTDOWN`, and `RESTART` all perform a controlled
  shutdown; `SIGHUP` logs that configuration reload is unsupported and does not
  trigger shutdown. If the final checkpoint fails, the
  process exits non-zero and `RESTART` does not exec a new image. An SQL OK only
  means the shutdown request was accepted; the final on-disk outcome is
  determined by the process status and the logs.
- After shutdown begins, `SIGINT`/`SIGTERM` keep being listened on: repeated
  signals only log a single "shutting down" notice and do not interrupt
  connection draining or the final checkpoint. If these signals arrive after SQL
  `SHUTDOWN` / `RESTART` initiated the shutdown, they neither force an exit nor
  change the planned operation.
- `MYDB_SHUTDOWN_TIMEOUT_MS` controls the foreground connection drain wait
  (default 60000 ms, i.e. 60 seconds; 0 skips it). On expiry, sessions are
  canceled and the same duration (at least 1000 ms) is waited for rollback to
  finish; if still unfinished, the process force-exits non-zero, cancels the
  RESTART, and runs no checkpoint concurrent with active writes. Event and
  replication writer threads must be joined before the final checkpoint; when
  something blocks for a long time and an emergency stop is truly needed, the
  administrator explicitly uses `SIGKILL` (`kill -9`); this skips the normal
  shutdown and the next startup requires crash recovery.
- The absolute path of the executable for RESTART is cached at startup; when the
  current program path cannot be obtained, it is resolved against the working
  directory or PATH at startup. If the path cannot be determined, error 3707 is
  returned before accepting the shutdown request.
- `RESTART` does not inherit the one-shot environment variables
  `MYDB_WAL_FORCE_RECOVER`, `MYDB_UNDO_FORCE_RECOVER`, or `MYDB_REPAIR_ROLE`.
  Regular configuration environment variables are still inherited.
- On Windows, foreground `RESTART` launches the same executable with the same
  arguments, working directory, environment, and standard streams after the
  graceful shutdown sequence. The replacement has a new PID and waits for the
  old process to release the data-directory lock. Set `MYDB_RESTART_DISABLE=1`
  when an external supervisor owns process replacement. Native Windows Service
  registration and SCM lifecycle handling are not implemented.
- By default, startup is refused when the middle of the WAL is corrupted but
  record frames remain beyond it; the original is not truncated. Explicitly
  setting `MYDB_WAL_FORCE_RECOVER=1` allows lossy recovery, saving
  `*.log.force-recover-<pid>-<offset>.bak` in the same directory before
  truncating and logging the discarded byte range. This option cannot recover
  already-corrupted transactions and is not suitable for permanent configuration.
  During crash recovery it also permits a missing undo predecessor to terminate
  that transaction's undo chain after the retained suffix has been undone. A
  warning identifies the transaction and missing LSN; changes in missing history
  cannot be verified or undone. Transaction mismatches and non-backward undo
  links remain fatal. Backup restore does not permit this undo salvage mode.
- A corrupted `role.json` refuses startup by default and reports the path.
  Restoring a trusted backup is preferred; explicitly setting
  `MYDB_REPAIR_ROLE=replica` backs up the corrupted metadata and atomically writes
  an unconfigured read-only replica role. After startup, configure the source
  with `CHANGE MYDB MASTER TO`. If the existing `MYDB_ROLE` conflicts with the
  read-only replica role, change it to `auto` or `replica`; the repair does not
  automatically restore the original primary identity.
- A native replica without a configured source is allowed to start in order to
  receive administrative SQL, while business writes are still refused.
- Environment variables for logical replication are used only to initialize the
  channel the first time. Persisted configuration takes precedence; `RESET
  REPLICA ALL` stores a deletion marker so a restart does not recreate the
  channel from the same environment variables. An explicit
  `CHANGE REPLICATION SOURCE TO` can recreate the channel and clear the marker.
- `FLUSH LOGS` flushes the query logs and rotates the enabled binlogs at the same
  time. The binlog index is written atomically; when corrupted or inconsistent
  with the manifest, startup is explicitly refused without rolling back the
  active position.

### Online BTREE Index Diagnostics

`SHOW ENGINE MYDB STATUS` exposes `online_index_prepare_us`,
`online_index_replay_us`, `online_index_validation_us`,
`online_index_publish_us`, and `online_index_barrier_wait_us`, accumulating
preparation, incremental replay, initial constraint validation, publication, and
metadata lock wait durations respectively. The replay duration includes the final
increment inside publication, so these times cannot simply be added together.
`online_index_tail_entries_peak` and `online_index_tail_bytes_peak` are the peaks
of increments admitted into the publication window, bounded by 4096 entries and
1 MiB respectively; `online_index_log_bytes_peak` is the per-index memory peak
while the incremental log is being consumed. These metrics are process-cumulative
and are used together with the `index_build_*` scan, sort, tree-build, fsync, and
spill metrics.

### OPTIMIZE TABLE Online Defragmentation

`OPTIMIZE TABLE` reuses the online COPY memory budget, change log, cancellation,
and progress mechanisms. `MYDB_DDL_COPY_MAX_FINAL_MS` (default 1000 ms, at most
60000 ms) bounds the tail replay time inside the final exclusive barrier; at most
1024 changes and 4 MiB of row values are accepted, with at most 32 catch-up
retries. This setting is not a hard cap on total lock time: file syncs and
metadata commits are also subject to disk latency. `MYDB_LOB_SHRINK_DISABLE` /
`MYDB_LOB_SHRINK_MIN_FREE_PAGES` only control the extra shrink of free LOB tail
pages after replay and do not disable OPTIMIZE's full physical rebuild.

Progress/cancellation: `information_schema.MYDB_DDL_JOBS`,
`CANCEL DDL JOB <job_id>`. In `SHOW ENGINE MYDB STATUS`,
`ddl_copy_barrier_wait_us`, `ddl_copy_lock_hold_us`, `ddl_copy_lock_hold_max_us`,
and `ddl_copy_final_tail_rows` are the shared online COPY cumulative metrics; the
lock-hold metrics describe the publication barrier and do not include the whole
defragmentation duration. Only persistent storage is supported; see the
compatibility document for details.

### System Variable Scope and Persistence Protection

`SET GLOBAL` changes the global value; for GLOBAL/SESSION dual-scope variables,
new connections inherit that default while existing connections keep their
session values. `SET SESSION ... = DEFAULT` takes the current global value, and
`SET GLOBAL ... = DEFAULT` restores the built-in default. The SESSION values of
`max_allowed_packet`, `net_buffer_length`, and `max_user_connections` are
read-only, preventing session writes from polluting the globals.

Incoming packet admission consults the live GLOBAL `max_allowed_packet`, even
for existing connections; `@@SESSION.max_allowed_packet` remains its inherited
snapshot. This lets an operator change the inbound resource ceiling without
reconnecting every client.

`back_log` and `open_files_limit` are read-only startup settings and report the
effective listener backlog and operating-system file limit. `lc_messages`
currently accepts only `en_US`; other message locales return an unsupported
feature error rather than changing only the reported value.

An unknown system variable returns 1193 by default. When compatibility with
lenient clients is genuinely needed, `MYDB_SYSVAR_UNKNOWN=warn` can be set
explicitly; persistence operations always reject unknown variables.
`require_secure_transport=ON` rejects new plaintext TCP connections during the
handshake (3159); TLS connections remain usable.

SHA-2 authentication accepts plaintext passwords only over TLS. Without TLS,
`sha256_password` and the full-authentication path of `caching_sha2_password`
require RSA-encrypted credentials; the same contract applies to COM_CHANGE_USER.
Empty-password credentials remain supported. TLS uses rustls; RSA private-key
operations use the vendored OpenSSL EVP backend, with OAEP-SHA1 preferred over
PKCS#1 v1.5 and implicit rejection of invalid legacy padding.

`MYDB_AUTH_RSA_PRIVATE_KEY` selects a PKCS#1 or PKCS#8 PEM key file;
`MYDB_AUTH_RSA_PRIVATE_KEY_INLINE` takes precedence and accepts the PEM text
directly. Without either setting, startup generates an ephemeral RSA-2048 key.
Configured RSA keys must be 2048–8192 bits. Invalid/non-RSA/weak keys or keys
requiring a passphrase fail startup without an interactive prompt. OpenSSL
must be 3.2 or newer and the selected provider must pass the implicit-rejection
self-check before listeners start. Keep private keys readable only by the
server account and distribute a trusted TLS certificate for remote use.

If `system/mysqld-auto.cnf` is corrupted, empty, or has an invalid version,
startup warns explicitly and forbids `SET PERSIST`, `SET PERSIST_ONLY`, and
`RESET PERSIST` from overwriting the original file. Preserve and repair the
original file first, then restart; modifying only the in-memory GLOBAL value
remains possible. Persistence names uniformly use the canonical name, and RESET
supports case and aliases.

`auto_increment_increment` and `auto_increment_offset` support GLOBAL/SESSION and
persistence, with a range of 1..65535, and affect real auto-increment value
allocation; when the offset is greater than the increment, allocation uses offset
1.

Replication parallelism/checkpoint tuning behaviors are still unimplemented;
some variables are read-only, others only accept compatibility configuration and
echo it back, and none support persistence — the registered metadata is
authoritative. These limitations do not represent full MySQL feature
compatibility.

### Connection Initialization SQL: init_connect

`init_connect` is a dynamic GLOBAL string, defaulting to the empty string
(disabled), and can be set at startup:

```ini
[mydb]
init_connect = "SET SESSION sql_select_limit=1000; SET @connected=1"
```

It can also be set with `--init-connect='SET @connected=1'` or
`MYDB_INIT_CONNECT`, with the existing precedence CLI > environment variable >
`[mydb]` > `[mysqld]`. Runtime support:

```sql
SET GLOBAL init_connect = 'SET @connected=1; SELECT @connected';
SHOW GLOBAL VARIABLES LIKE 'init_connect';
SET PERSIST init_connect = 'SET @connected=1';
SET GLOBAL init_connect = DEFAULT;
```

Changes affect only subsequent new connections and do not alter existing
sessions; `SET GLOBAL` / persistence follow the system variable administration
privilege requirements. `NULL`, the empty string, or GLOBAL `DEFAULT` clears the
runtime configuration; numeric or boolean assignment returns a type error. `SET
SESSION` and a scope-less `SET init_connect` return 1229.

Read it with `@@init_connect` or `@@global.init_connect`; explicit
`@@session.init_connect` / `@@local.init_connect` return 1238. `SHOW SESSION
VARIABLES` still displays the global value.

After a new connection completes authentication and the default database check,
the initialization SQL runs with the privileges of the logged-in user, without
requiring the client to enable multi-statement capability; the supported SQL
surface is the same as the ordinary mydb executor. Statements run sequentially,
result sets are fully consumed and then discarded, and session settings,
transactions, and SQL side effects are preserved. Accounts holding
`CONNECTION_ADMIN` (including activated roles) or `SUPER` privileges, as well as
accounts with expired passwords, skip execution.

Assignment does not validate the SQL in advance. On execution failure, the
original cause is written to the server log, and the first client command
receives 1184 / 08S01 (`init_connect command failed`) before the connection is
closed; uncommitted transactions are rolled back and committed prefixes are kept.
Administrators can still log in to correct the configuration. `COM_RESET_CONNECTION`
and `COM_CHANGE_USER` do not rerun the initialization SQL.

See the [compatibility reference](mysql80-compatibility.md) for connection initialization semantics.

### Session Configuration and Persistence Boundaries

- `connect_timeout` controls the handshake deadline for new connections, and
  `SET GLOBAL connect_timeout=N` takes effect immediately for subsequent
  handshakes. `MYDB_HANDSHAKE_TIMEOUT` provides the startup initial value; `0`
  for the environment variable remains reserved for disabling the handshake
  timeout, and writes while refusing a connection still have a 10-second
  fallback deadline.
- Concurrent TLS connection bursts can require a longer handshake deadline.
  Some clients initialize their TLS context and load system CA certificates
  after sending the MySQL SSLRequest; contention during this work can delay
  their TLS ClientHello beyond the default 10 seconds. A server-side handshake
  timeout can therefore appear as an SSL unexpected EOF on the client. Check
  the server log for `handshake timeout exceeded`, then configure a deadline
  appropriate for the client's connection burst, for example
  `SET PERSIST connect_timeout=60`. This takes effect for new connections and
  survives restart. For a new data directory, the equivalent startup option is
  `handshake_timeout=60` in `[mydb]` (`MYDB_HANDSHAKE_TIMEOUT=60` in the
  environment). There is no `--handshake-timeout` CLI option. TLS remains enabled;
  the query execution path is unaffected.
- `SET PERSIST` applies the runtime value after the file write succeeds; a failed
  file write does not change the runtime configuration first. When the runtime
  application is rejected, the original persisted file is restored; if
  restoration also fails, both errors are reported.
- Read-only variables with no post-startup runtime backend (such as `log_bin`)
  cannot use `SET PERSIST_ONLY` and return an error immediately; use startup
  configuration instead. Dynamically settable GLOBAL variables still support
  `SET PERSIST_ONLY`.
- `sql_log_bin` supports the SESSION scope only; GLOBAL writes are explicitly
  rejected.
- During server shutdown, the drain wait timeout interrupts connections, but the
  final checkpoint must still wait for connection rollbacks and resource cleanup
  to finish, so a large rollback can push total shutdown time beyond the drain
  deadline.

### Per-Session Explicit Temporary Table Quota

`CREATE TEMPORARY TABLE` uses a dedicated per-session resource budget. The
following startup options can also be placed in the `[mydb]` option file, where
`0` disables the corresponding session cap; they are not writable SQL sysvars.
Each connection is billed independently, renaming a temporary table does not
create new quota, and CTAS, LOAD DATA, and prepared writes use the same storage
allocation checks. Exceeding a limit returns 1114 and rolls back the failing
statement.

| Option | Environment variable | Default |
|---|---|---|
| `session_temp_table_limit` | `MYDB_SESSION_TEMP_TABLE_LIMIT` | 1024 tables |
| `session_temp_storage_limit_bytes` | `MYDB_SESSION_TEMP_STORAGE_LIMIT` | 1073741824 (1 GiB) |
| `session_temp_undo_limit_bytes` | `MYDB_SESSION_TEMP_UNDO_LIMIT` | 67108864 (64 MiB) |

The storage quota counts allocated heap, LOB, and B-tree page capacity (including
index pages still resident in the cache), plus the rollback snapshot copies of
ALTERs; ALTERs therefore need extra snapshot space. The quota is checked before
allocation/copy. DELETE and transaction rollback do not shrink existing files,
and allocated space can be reused; TRUNCATE, DROP, and normal session closure
release the corresponding capacity. When file cleanup fails, the charge for
undropped capacity is conservatively retained to avoid overstating what was
released. The undo quota is released over the lifetime of the records and their
readers' references rather than simply dropping to zero at COMMIT. These caps
apply jointly with the global buffer pool, the unified memory budget, and the
temporary spill disk budget.

`tmp_table_size` and `max_heap_table_size` do not serve as quotas for user
explicit temporary tables.

## WAL write buffer

See [WAL write buffer](configuration-resources.md#wal-write-buffer) for settings and behavior.

## Server-side file access

See [Server-side file access](configuration-operations.md#server-side-file-access) for settings and behavior.

## Backup Resource Budgets

See [Backup Resource Budgets](configuration-operations.md#backup-resource-budgets) for settings and behavior.

## External AI calls

See [External AI calls](configuration-services.md#external-ai-calls) for settings and behavior.

## HTTP SQL calls

<a id="date-name-locales"></a>
See [HTTP SQL calls](configuration-services.md#http-sql-calls) for settings and behavior.

## REST SQL listener

See [REST SQL listener](configuration-services.md#rest-sql-listener) for settings and behavior.

## Recycle Bin

See [Recycle Bin](configuration-operations.md#recycle-bin) for settings and behavior.


## Interface stability

This reference describes the source revision containing it. Release users should
read documentation from the matching tag; see [changing versions](changing-versions.md).

| Interface | Support boundary |
| --- | --- |
| Registered option-file and help options | Public startup input; see [generated registry](configuration-registry.md) |
| Registered writable GLOBAL sysvars | Public SQL/config input subject to variable scope and persistence metadata |
| Documented diagnostic switches | Troubleshooting tools; require restart when process-scoped and may change in preview releases |
| Other source-only MYDB names | Internal implementation, benchmark or test controls; not a supported configuration contract |

Never infer product configuration from a grep of environment names. The registry
contains validators, not a promise of a dynamic SQL variable. Find defaults and
units in this reference and the subsystem guides; hardware-derived defaults depend
on effective memory/CPU. Inspect SHOW GLOBAL VARIABLES for registered SQL values.

## Configuration map

- [Connection reactors](#connection-reactors) and [TLS](#tls)
- [Startup names and validators](configuration-registry.md)
- [Resource settings](configuration-resources.md)
- [Operations and backup settings](configuration-operations.md)
- [AI HTTP and REST settings](configuration-services.md)
- [Precedence](#precedence-and-when-settings-take-effect) and [persistence](#system-variable-scope-and-persistence-protection)
