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

## TLS

The server defaults to `ssl_mode=auto`: it advertises TLS and generates an
ephemeral self-signed certificate when no certificate or key is configured.
Advertising TLS still allows plaintext connections unless
`require_secure_transport=ON` is set. The MySQL 8.0 client's
`--ssl-mode=REQUIRED` requires encryption but does not verify server identity.

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

`ssl_mode=disabled` disables server TLS and cannot be combined with certificate
or key options. The server's `auto`/`disabled` values differ from the client's
`REQUIRED`/`VERIFY_IDENTITY` values.

## Supported Options

The config file supports the following stable surface:

- All server, TLS, log, and binlog options listed in `mydb --help`;
- The persistence, memory, execution, vector, connection, authentication, and
  replication startup options documented on this page and in the
  [configuration template](../mydb.cnf.example);
- Registered system variables in `SHOW GLOBAL VARIABLES` that have a GLOBAL value
  and are writable, using their formal names, for example `max_connections`,
  `mydb_checkpoint_wal_bytes`, `mydb_zone_map_maintenance`.

The CLI currently exposed by `--help` is an intentionally lean startup entry
point:

| Category | CLI |
| --- | --- |
| Config/listening | `--config`, `--host`, `--port`, `--data-dir` |
| Immutable backup queries | `--backup-repository`, `--base`, `--runtime-dir` |
| Connection initialization | `--init-connect` |
| TLS | `--ssl-mode`, `--ssl-cert`, `--ssl-key` |
| Logging | `--slow-sql-ms`, `--general-log`, `--slow-query-log`, `--log-output` |
| binlog source | `--log-bin`, `--server-id`, `--binlog-checksum`, `--sync-binlog` |

The SQL service listens on `0.0.0.0` by default (all IPv4 interfaces). This can
be overridden with `--host`, the `host` option in the config file (alias
`bind_address`), or the environment variable `MYDB_HOST`; to allow only local
connections, set `--host 127.0.0.1`.

The default administrator for a fresh data directory is `'root'@'localhost'`,
which may only log in through the loopback address; this does not affect remote
connections from other authorized accounts. The default password is `root`; the
initial password can be set with `MYDB_ROOT_PASSWORD`. This default does not
modify accounts already stored in an existing data directory.

The slow query log is enabled by default (`slow_query_log=ON`) and writes to the
`mysql.slow_log` table (`log_output=TABLE`), with a default threshold of 1 second
(`long_query_time=1`). It can be overridden through the config file, the
environment variables `MYDB_SLOW_QUERY_LOG` / `MYDB_LOG_OUTPUT` /
`MYDB_LONG_QUERY_TIME`, or `SET GLOBAL`; `log_output` controls the output
destination of both the general log and the slow log, and the general log remains
disabled by default. When `long_query_time` is not set, the threshold falls back
to `slow_sql_ms` (default 1000 milliseconds).

Finer-grained storage, memory, execution, vector, and replication configuration
uses the option file, `MYDB_*` environment variables, or writable sysvars. The
MySQL→mydb replica receiver is a core capability included in every build. Build
features are not runtime configuration; tool binaries use `--features tools`.

`MYDB_EVENT_SCHEDULER` sets the startup state to `on` (default), `off`, or
`disabled`. ON/OFF can be toggled dynamically with `SET GLOBAL event_scheduler`;
starting with DISABLED creates no scheduler thread, runtime modification returns
1290, and changing it requires updating the environment variable and restarting.
There is currently no `--event-scheduler` command line option.

`event_workers` / `MYDB_EVENT_WORKERS` controls the fixed number of event
scheduler workers (1–32); the default is the smaller of the available CPU count
and 4. Pending events enter a bounded queue with twice as many slots as workers;
queue entries are charged to the unified memory budget as `SystemMetadata`; when
the budget or the queue is short, the original due-time is kept and retried
briefly instead of skipping the firing. When the engine shuts down, in-flight
event SQL is canceled and awaited for at most 2 seconds.

`plan_cache` / `MYDB_PLAN_CACHE` controls the execution plan cache, default `ON`.
When set to `OFF`, shared SELECT physical plans, prepared SELECT/DML memos, text
DML plans, and in-statement subquery plans are no longer produced or reused; the
parsed statement cache remains enabled. The parameter is read at process startup,
which suits differential verification or serves as a temporary escape hatch for
plan cache failures; it does not affect SQL semantics.

Automatic checkpoints are triggered by `MYDB_CHECKPOINT_INTERVAL_MS` or
`MYDB_CHECKPOINT_WAL_BYTES` and use fuzzy semantics: they persist a fixed-size
explicit `redo_lsn` recovery floor, reclaim only sealed WAL segments, and never
rewrite a very small active segment. `ALTER SYSTEM CHECKPOINT` and a normal
shutdown use a sharp checkpoint, flushing dirty pages in parallel and collapsing
the active WAL crash-atomically once truly quiescent. A physical replica performs
restartpoints whose trim point is always
`min(replica_applied_lsn, wal_durable_lsn)`, and all applied data files are
synced first.

`redo_enabled` / `MYDB_REDO_ENABLED` / `mydb_redo_enabled` defaults to `ON`. It
is a safe reduction switch aimed at bulk imports and offline index builds, not an
absolute zero-WAL mode: `OFF` omits per-row data redo for newly allocated heap
extents but still logs transaction boundaries, undo, file growth intent, and the
redo required for old/reused pages, and syncs data files written directly before
the commit record is published. Use `SET SESSION mydb_redo_enabled=OFF` to
control subsequent transactions on the current connection, or
`SET GLOBAL`/`SET PERSIST` to change the default for new connections; it cannot
be switched while a transaction is in progress. MySQL binlog is unaffected. XA
and mydb physical replication require this value to be `ON`. The
`reduced_redo_*` metrics in `SHOW ENGINE MYDB STATUS` show the skipped volume,
necessary fallbacks, and pre-commit fsync overhead. `strict` mode provides the
full commit crash-recovery guarantee; `relaxed`/`memory` still inherit their
respective weaker durability semantics.

External XA coordinators that require durable PREPARE acknowledgements must use
`durability=strict`. In `relaxed` mode, XA PREPARE drains WAL to the OS page cache
but does not require fsync; a power loss can erase an acknowledged prepare.
`memory` mode does not persist XA prepare decisions. Enabling binlog does not
upgrade the XA PREPARE barrier to strict durability.

The durability boundary of `strict` is the successful return of `COMMIT` (or an
autocommit write statement). For group commit, the engine first publishes
in-memory transaction state and releases row locks, then completes the WAL flush,
so other connections may observe the transaction's versions before the writer
receives the success response. A read-only query observing data does not mean the
writer has obtained durability confirmation; unconfirmed commits can still be
lost if a crash occurs within that window. A commit that returned successfully
must be recoverable.

Startup-only names use the lowercase form with the `MYDB_` prefix removed; for
example, the environment variable `MYDB_BUFFER_POOL_PAGES` corresponds to
`buffer_pool_pages`. Compatibility aliases include `datadir`/`data_dir` and
`bind-address`/`host`. The allowlist is maintained explicitly; new source-code
environment variables do not automatically become config options.

The decoded cache has been removed. If `buffer_pool_decoded_cache`,
`buffer_pool_decoded_cache_max_pct`, `MYDB_BUFFER_POOL_DECODED_CACHE`, or
`MYDB_BUFFER_POOL_DECODED_CACHE_MAX_PCT` appears in the config file or the
environment, startup fails; it is not silently ignored. `buffer_pool_pages` only
bounds the number of raw pages; short-lived storage scratch needed for page
rearrangement is charged separately to the unified process hard budget at its
actual allocation capacity.

When not configured explicitly, the raw buffer pool is derived smoothly between
20% and 60% of the total target: small targets use at least 20%, the growth
phase reserves 512 MiB preferentially for catalog/zone metadata, protocol
buffers, undo, storage scratch, thread stacks, and the allocator, and the ratio
holds once 60% is reached. For example, with `MYDB_MEMORY_TARGET=1000` the
default pool is 488 MiB, without a discontinuity at the 1 GiB boundary.
`MYDB_BUFFER_POOL_PCT` or `MYDB_BUFFER_POOL_PAGES` can still override it
explicitly.

`open_files_limit` / `MYDB_OPEN_FILES_LIMIT` defaults to `65535` and is the
process file descriptor soft limit requested at startup. The writable
`open_files_limit` SQL compatibility variable is advisory: changing it at runtime
does not change the OS limit. The effective OS value is
constrained by the operating system hard limit and the outcome of the request; a
higher inherited soft limit is never lowered. A positive integer can be set
explicitly to override the default. This value does not open files in advance and
is not a file cache capacity.

`buffer_pool_file_cache_max` defaults to `1024` and bounds the number of file
handles the buffer pool keeps long term; above the limit, idle handles are
reclaimed with second chance. Under strict/relaxed durability, handles with
pending fsync writes are kept until the sync completes, so the limit may be
exceeded briefly; `0` explicitly disables the limit. Checkpoints in both modes
sync written-back data files before releasing WAL; when history retention is
enabled, undo history is synced as well, and replica restartpoints always sync
undo. Outside of checkpoints, relaxed mode keeps its original weaker durability
semantics.

`mydb_undo_retention_ms` is both the undo retention window and the history
retention window for `FOR SYSTEM_TIME AS OF`; the default is `0` (reading past
points in time is not allowed). It can be adjusted dynamically with
`SET GLOBAL mydb_undo_retention_ms=N`; shrinking the window makes earlier targets
immediately return `MYDB_ER_SYSTEM_TIME_TOO_OLD`, while extending it only affects
history that has not yet been purged and cannot restore data that has already
been cleaned up. Enabling it from `0` persists the transaction visibility
baseline at that moment, and targets before the baseline explicitly return
`MYDB_ER_SYSTEM_TIME_TOO_OLD`; to keep the baseline atomic, 1205 is returned
while active write transactions exist, and retrying once they finish is
sufficient. Before a history query starts, the entire query graph is validated
once and a snapshot is registered; ordinary queries do not read the commit
timeline. Structural DDL, index DDL, RENAME, and TRUNCATE open a new structure
epoch; ANALYZE does not.

`memory_target` / `MYDB_MEMORY_TARGET` is an advisory working-set soft target for
buffers, caches, and executor spill; when not set explicitly it equals 50% of the
smaller of the host's physical memory and the cgroup `memory.max`, rounded down
to whole MiB (falling back to 1 GiB if memory detection fails). An explicit value is
clamped to that effective memory and only influences cache sizes and spill
watermarks, not hard boundaries. The unified hard boundary for ordinary governed
SQL admission is 80% of effective memory; 80%–90% is reserved for the
rollback/compensation path to finish WAL, undo, and lock release after a memory
pressure error, and above 90% is reserved for thread stacks, the runtime, and the
allocator. The actual process/cgroup pressure checks likewise distinguish
ordinary allocations from compensation reservations, and compensation allocations
cannot lend their reserved quota to ordinary SQL through the shared fast path.

When the default `fast-alloc` build on Linux detects a cgroup limit below
physical memory, it disables transparent huge pages for this process before
startup (without modifying system settings), preventing the thread allocator's
sparse pages from being amplified into 2 MiB of resident memory. When
`MIMALLOC_ALLOW_THP` is set explicitly, the allocator configuration is respected;
it is usually unnecessary to set it.

The first admission failure retries by reclaiming, in order, optional
SQL/derived caches, the zone map/buffer pool, and the allocator; only when a
reservation still cannot be obtained does it return 1037/HY001. Performance
Schema history and aggregation are optional state, capped at 5% of effective
memory by default; beyond the limit, samples are dropped rather than squeezing
memory needed for transaction progress. The per-query budget defaults to 42% of
the executor budget (`query_mem_quota_pct` / `MYDB_QUERY_MEM_QUOTA_PCT`), and
this derived value has a floor of 8 MiB; `query_mem_limit_mb` /
`MYDB_QUERY_MEM_LIMIT_MB` can override the derived value directly. For example,
with `MYDB_MEMORY_TARGET=1G` and the default ratios, the executor budget is about
307 MiB and the per-query soft watermark is about 129 MiB (42% of 307 MiB). Buffer raw pages,
short-lived storage scratch, undo, SQL caches, protocol/background buffers, and
executor batch memory must each obtain a categorized reservation first; the
ordinary categories combined may not cross the 80% SQL hard limit, and only the
rollback/compensation critical scope may enter the 80%–90% band. MySQL inbound
packets, outbound batches, streamed encoded results, and pending binlog row
images are charged to `ProtocolTransient` / `BinlogTransient` respectively; they
can be verified through the `controlled_memory_*_transient_bytes` and
corresponding rejection metrics in `SHOW ENGINE MYDB STATUS`. The executor,
query, and process work-memory values are soft watermarks for operator spill; the
`force` path may only cross these soft watermarks, never the SQL hard boundary of
80% of effective memory. When a non-reclaimable allocation still cannot obtain
the total budget after unified reclamation and retries, 1037/HY001 is returned
and the current SQL is terminated without closing the connection. Parallel
workers share the same terminal OOM cause, so the other workers of the same SQL
stop at the next cancellation checkpoint, and the final error is not overridden
by an ordinary 1317 interruption. Linux admission also keeps actual headroom
based on the cgroup's current unreclaimable usage; when quota is short, it
reclaims caches, buffers, and the allocator in order and returns 1037 only if
still short. An explicit buffer pool may not exceed the SQL hard limit. The
`controlled_memory_{managed,operational}_limit_bytes` and
`controlled_memory_performance_schema_limit_bytes` values in
`SHOW ENGINE MYDB STATUS` can be used to verify the three boundaries;
`executor_terminal_oom_cancellations` and `physical_memory_pressure_rejections`
record SQL-level OOM terminations and actual process pressure rejections
respectively.

In a finite Linux memory cgroup, total residency above 90% of the effective
limit can request optional clean-cache eviction towards 85%. Large incoming
allocations request room directly; smaller allocations and background samples
require recent kernel reclaim and net filesystem refault pressure. Growth of
the engine's raw buffer cache is credited as working-set migration, so full
but healthy caches are not continuously shrunk. Independently, growing
non-buffer ownership is preflighted before its pages fault, and real
unreclaimable growth catches allocator retention. These two signals are kept
separate so retained allocator pages cannot hide new mandatory reservations.
Counters are aggregated over 100 ms and pressure is retained for 250 ms;
the byte threshold is 1% of the
effective limit, capped at 64 MiB. Filesystem reclaimability alone does not
guarantee timely room for the next anonymous-page fault. The sampler remains
active during SQL; the additional OS signals have a 10 ms sampling bound to avoid repeated
filesystem reads, while logical ownership/forecast and hard admission remain
fresh on allocation samples. Allocation samplers request bounded eviction outside storage
latches. Pinned pages, dirty pages and deferred WAL changes are preserved,
and configured capacity, durability and hard admission limits do not change.
Advisory eviction preserves one fifth of configured buffer capacity, with a
16 MiB minimum and a ceiling of 10% of effective memory or 1 GiB, whichever is
smaller. Mandatory admission retains its separate smaller emergency floor.
An additional refill window of one sixteenth of that floor (at most 64 MiB)
prevents a CLOCK sweep for every newly cached page near the floor. Unlimited
or non-binding cgroup limits do not activate this cache-only policy.
Cache-only relief preserves lightweight zone-map descriptors; hard admission
can still reclaim them when mandatory memory is short.
This is a best-effort eviction policy, not an additional SQL error threshold. Existing
`memory_reclaim_attempts` and `memory_reclaim_buffer_bytes` include this work.

The default ceiling of the catalog descriptor cache is 2% when
`MYDB_MEMORY_TARGET<=1GiB` and 5% for larger targets, never exceeding 256 MiB;
the startup environment variable `MYDB_CATALOG_CACHE_MB` can override the value,
and at runtime it can be adjusted with `SET GLOBAL mydb_catalog_cache_mb=N`. At
v46 startup only object headers and the ID→name index are resident; column,
index, constraint, and statistics descriptors are assembled from the
transactional DD by object ID when a table is first resolved.
`SHOW ENGINE MYDB STATUS` exposes `catalog_cache_limit_bytes`, the catalog epoch,
and the counts of transaction commits/rollbacks and DD mutation rows.

`idle_memory_reclaim` / `MYDB_IDLE_MEMORY_RECLAIM` is enabled by default. After
all client SQL execution and result sending have been idle for 250 ms, if macOS
`phys_footprint` or Linux private RSS exceeds 110% of `MYDB_MEMORY_TARGET`, a
background thread releases pages the allocator has already freed. This idle
allocator policy does not purge query threads or evict valid raw buffer pages
just to meet its threshold. The `process_private_footprint` row of `SHOW MYDB MEMORY` and the
`idle_memory_reclaim_*` metrics in `SHOW ENGINE MYDB STATUS` can be used to
verify the effect.

`buffer_pool_small_scan_pct` accepts `0..=100`, default `20`. When the actual
physical page count of a SQL sequential scan is no greater than
`min(buffer_pool_pages, max(20, buffer_pool_pages × pct / 100))`, the first scan
still enters the cache as cold pages and only the second scan is promoted to the
low-heat working set (usage cap 6). `0` disables this policy; large table scans,
prefetch reads, and maintenance scans such as ANALYZE/index builds keep using the
cold-scan policy. At runtime it can be adjusted with
`SET GLOBAL mydb_buffer_pool_small_scan_pct = N`; the change only affects scans
that start afterwards.

Cold builds of paged HNSW default to `MYDB_HNSW_BULK_MODE=auto`, choosing among
`full-memory → chunked-paged` for unquantized FLOAT32 indexes, based on the
actual visible row count, dimensionality, adjacency capacity, and worker
scratch. Quantized HNSW shares the parallel topology builder when its temporary
construction graph is admitted; only compressed payloads survive publication. The available build controls are:

- `MYDB_HNSW_BULK_MODE=auto|memory|chunked`; `stream` is a compatibility alias
  for `chunked`. `memory` forces the full in-memory graph but still may not cross
  the SQL hard limit of 80% of effective memory; insufficient budget returns
  1037/HY001.
- `MYDB_HNSW_BULK_MEMORY_MB` is the soft ceiling of the build working set; when
  unset it is 80% of the total memory target. It does not enlarge the unified
  governor's hard target.
- `MYDB_HNSW_BUILD_CACHE_MB` is the soft ceiling of the vector LRU cache shared
  by all workers on the chunked path; the actual value is further constrained by
  the build lease.
- Finalized file-backed HNSW indexes whose `.hvec` image fits the configured
  memory target open a read-only mapping of their existing pages by default.
  This path reads stored payloads and adjacency from the authoritative file;
  payload encoding follows the index's explicit `quantization` setting.
  Final re-ranking uses MVCC-visible heap values. No separate navigation file
  is needed. When mapping is unavailable, queries use an admitted cache or
  live buffer-pool pages. Checksums are checked lazily on first page access.
  The small validation bitmap and bounded query memo are charged to the memory
  governor; clean mapped pages remain reclaimable by the OS. This does not pin
  the entire index in RAM or guarantee a warm filesystem cache after restart.
- INSERT, UPDATE and DELETE revoke the mapping before changing index pages and
  drain in-flight mapped readers. Subsequent queries use the live WAL/buffer-pool
  path and see incremental maintenance immediately; no navigation rebuild is
  required for visibility. A reopen or explicit navigation preparation can map
  a flushed, finalized image again. Physical replication receivers disable
  direct mapping because their redo writer bypasses local index DML. Runtime
  statistics for unquantized FLOAT32 HNSW report `backend=paged_mapped` while
  mapping is active, and `backend=paged` after revocation. Native and quantized
  HNSW report `paged_native` or `memory_native`; IVF reports `ivf_paged_native`
  or `ivf_native`. These native backend names do not distinguish mapping from
  page access. The FLOAT32 reverse RowId lookup is initialized on first DML
  after reopen rather than persisted separately.
- `MYDB_HNSW_MAPPED_NAVIGATION=0` disables direct mapping for differential
  diagnostics. Memory-only durability also bypasses mapping.
- `MYDB_HNSW_QUERY_CACHE_MB` controls optional HNSW navigation caches, not the
  direct file mapping. Compact graph and payload caches default to two thirds
  of the memory target, capped at 1 GiB per index; aggregate admission still
  follows the shared process limits. Dense navigation may replace raw buffer
  pages under both logical and physical memory admission.
  `0` disables the cache. Admission failure uses live persistent pages.
  Paged FLOAT32 L2/COSINE indexes can use auxiliary SQ8 navigation with
  `quantization=none`, while retaining original index vectors and exact heap
  reranking. Native compressed HNSW can cache its existing encoded payloads
  together with adjacency; this cache never changes the requested encoding.
- `MYDB_HNSW_PRELOAD=eager|lazy` controls startup opening, default `lazy`.
  `eager` opens Public vector indexes and prepares the admitted query paths
  before accepting connections; it does not fault every mapped page into RAM.
- The scratch lease on the chunked path obtains at most 30% of the soft target by
  default; the default 60% buffer pool is likewise a soft working-set ratio. The
  lease first shrinks the buffer pool in a bounded way, then requests from the
  unified governor atomically, and the combined actual residency of the two still
  may not cross the SQL hard limit of 80% of effective memory.
- `MYDB_HNSW_BUILD_TIMING=1` prints statistics, encoding, linking,
  flush-to-disk, and total durations. The corresponding HNSW lines in
  `SHOW ENGINE MYDB STATUS` also show the most recent build mode, row count,
  per-phase durations, peak scratch reservation, cache hits/misses, and the chunk
  count.

HNSW/IVF reclamation and requested rebuilds run on the
`mydb-vector-maintenance` background worker (once per second). Commit-time
incremental maintenance still keeps the candidate structures current:

- DELETE and re-linking UPDATE keep the superseded graph versions searchable
  until no snapshot that can still see them remains, then retire exactly those
  versions. Unquantized FLOAT32 HNSW can update small vector moves in place;
  native and quantized indexes use deletion and reinsertion. Queries whose
  snapshot predates an accelerator's coverage use exact scanning.
- Tombstones are reclaimed once they reach `MYDB_HNSW_COMPACT_RATIO` (default
  0.2) of a graph or `MYDB_HNSW_COMPACT_ABSOLUTE` (default 100000) nodes;
  `MYDB_HNSW_COMPACT_DISABLE=1` turns reclamation off. The memory backend
  rebuilds the unquantized FLOAT32 graph with the parallel cold-build path and
  replays DML that arrives meanwhile; the corresponding paged backend detaches
  tombstones in place (WAL-logged). Native and quantized HNSW/IVF rebuild from
  committed heap values. `OPTIMIZE TABLE` explicitly reconstructs vector indexes
  and reclaims `.hvec` file space.
- A KNN whose residual filter leaves fewer than `k` rows after the
  `mydb_hnsw_max_scan_tuples` candidate budget finishes with an exact scan
  instead of returning a short result; `mydb_hnsw_iterative_scan = off` keeps
  the single approximate pass.
- Required index loading, persistence and maintenance failures return explicit
  errors. These failures do not trigger an exact scan. A failed private
  replacement keeps the old valid object and a durable failed-job status;
  failure while modifying the active object fences further service.

Quantized indexes can request a rebuild when scalar exceptions or PQ/BQ update
growth become significant. PQ remains `PENDING_TRAINING` and queries use exact
scans until at least `2^pq_bits` committed rows are available for training.
See [quantization](vector-indexes.md#quantization) for model and payload behavior.

Stable startup options for Hash Join include:

- `hash_join_mem_limit_mb`: uses the query's remaining grant when unset, `0`
  forces spill, and a positive value is the per-operator spill watermark;
- `hash_join_grace_partitions`: `2..=256`;
- `hash_join_grace_max_depth`: `1..=8`, default `4`;
- `hash_join_parallelism`: `1..=64`;
- `hash_join_work_area`: enabled by default, can be set explicitly to `OFF` as a
  compatibility escape switch;
- `hash_join_fallback`: `chunked` (default) or `error`. When a hot partition can
  no longer be reduced, `chunked` rescans the probe side with grant-bounded build
  blocks instead of using a generic NLJ.

Hash Join grant, peak charge, spill bytes read/written, Grace
depth/repartitioning, hot partition chunking, and memory rejection sources can be
inspected with `SHOW ENGINE MYDB STATUS`. After a query completes, fails,
or is canceled, operator charges should return to zero; temporary spill files are
cleaned up by the query lifecycle.

The stable boundaries for large results and protocol buffers are as follows:
`MYDB_PACKET_WRITE_BUFFER_MAX_BYTES` defaults to 16 MiB; when exceeded it flushes
first, and a single oversized plaintext packet is written out directly in frames
without being copied once more into a batch. `MYDB_INBOUND_READ_BUFFER_BYTES`
defaults to 16 KiB. The blocking-pool hand-off for streaming cursors is
constrained by both `MYDB_STREAM_OFFLOAD_BATCH_ROWS` (default 256 rows) and
`MYDB_STREAM_OFFLOAD_BATCH_BYTES` (default 8 MiB, minimum 64 KiB). A single row
may exceed the byte threshold but then occupies a batch of its own; client rows
from `COM_STMT_FETCH` are consumed in fixed blocks, so `u32::MAX` does not drive
a preallocation of equal capacity. `MYDB_STREAM_BLOCKING_OFFLOAD=0` restores the
row-by-row oracle path. Per-connection `BufReader`, OK scratch, and compression
inflate/scratch/output are all charged to `ProtocolTransient` by allocation
capacity. On non-Unix platforms, `LOAD DATA LOCAL` retains at most 1 MiB of
controlled memory and then migrates to quota-aware spill files under
`<data-dir>/system/tmp/protocol`, deleted when the connection is canceled, errs,
or completes. Unix platforms use a bounded socket pipeline so that protocol
receiving and LOAD parsing/writing run in parallel with natural backpressure from
kernel buffers, no longer spilling fully to disk before reading back; the spill
file above remains the authoritative fallback for non-Unix platforms. When the
table is empty, autocommit is on, binlog is off, and the shape is supported, both
ordinary and AUTO_INCREMENT tables can use a crash-safe shadow generation;
explicit auto-increment values are encoded by a worker, and batches containing
NULL/0 return to the coordinator for allocation in input order.

Soft thread affinity is enabled by default when a connection submits blocking
tasks: an idle worker that last ran a task for that connection is woken first;
when the previous worker is busy, another worker takes over. Tasks can always be
taken from the shared queue; no thread is waited on for affinity's sake, and no
CPU core is pinned. `MYDB_BLOCKING_AFFINITY=0` (or `off/false/no`) disables it at
startup; this setting does not change Tokio inline execution or intra-query
parallel scheduling.

Synchronous execution uses a CPU-scaled blocking worker pool.
`MYDB_BLOCKING_POOL_SIZE` controls the number of pre-started workers,
`MYDB_BLOCKING_POOL_MAX` controls the on-demand growth ceiling, and
`MYDB_BLOCKING_STACK_MB` controls each worker's stack capacity (default 8 MiB,
range 2..=512 MiB). Whether configured explicitly or derived automatically, the
ordinary worker ceiling is limited by a stack-capacity allowance of 5% of
effective OS/cgroup memory, with a minimum of one worker. With automatic pool
sizing, the server can reserve additional managed memory for stacks to support
transaction admission; this reservation is limited by available headroom and
20% of the advisory memory target. Explicit pool settings retain the ordinary
stack allowance. Raising the connection count does not create an equal number
of permanently resident large-stack threads. Inspect the effective sizes with
`blocking_pool_base_workers`, `blocking_pool_max_workers`, and
`blocking_pool_managed_stack_bytes` in `SHOW ENGINE MYDB STATUS`.
`MYDB_TXN_ADMISSION` bounds the number of transactions that are open
concurrently and may hold row locks; when unset the requested value is 4 times
the logical CPU count (minimum 8), and the final value is additionally capped
below the blocking worker ceiling to ensure a worker is always available to run
the COMMIT/ROLLBACK that releases row locks. If the memory target can only
accommodate 1 blocking worker, admission automatically tightens to 1 so that
transactions serialize instead of making the sole worker wait on another
transaction's row locks. Connections waiting for admission suspend asynchronously
on Tokio and hold no locks yet, so they occupy no blocking worker. Setting a
positive integer overrides the requested value but remains subject to the safety
caps above; `0` disables it only as a diagnostic escape switch.
Parallel scans from standalone read-only connections and explicit read-only
transactions declare cross-session read pressure; while the pressure persists,
newly started read-write transactions additionally pass through bounded
mixed-workload admission: one slot on hosts with at most two available CPUs,
otherwise `clamp(CPUs - degree, 2, 16)`, using the startup
`MYDB_PARALLEL_DEGREE` (fallback 2) for this gate.
The decision does not look at `autocommit` alone: JDBC
connection pools often set both read-only and read-write connections to
`autocommit=0`. A connection that has not yet executed DML is still treated as
read-only; once a connection shows write intent, parallel scans in its subsequent
read-write transactions stop declaring this pressure, because the read may be a
preparatory step for later DML in the same transaction, and a transaction must
not impose analytical load shaping on itself or on similar write transactions.
Explicit read-only transactions are always treated as analytical queries. The
classification is fixed from session state at statement start, does not depend on
live CPU, RSS, or concurrency, and does not change the physical plan;
`COM_RESET_CONNECTION` clears the connection history. After the last read-only
parallel operator finishes, the pressure lease is held for 2 seconds to cover
short gaps between consecutive statements/operators of the same analytical
connection, preventing a burst of write transactions from evicting the next
analytical query's cached working set; the lease expires automatically when it
runs out and no new read-only parallel work exists. Parallel scans of read-write
transactions themselves neither open nor renew the lease, so pure TP never
permanently enters mixed-workload mode through its own scans. The
`Mydb_txn_admission_*`, `Mydb_mixed_admission_*`, and
`Mydb_parallel_read_pressure_*` counters in `SHOW GLOBAL STATUS` provide
cumulative waits and classification counts; the same metrics also appear in
`SHOW ENGINE MYDB STATUS` under lowercase names.

LOB extent allocation scans at most `MYDB_LOB_EXTENT_SCAN_PAGES=8192` page
descriptors of the free list to find a contiguous run; beyond that bounded effort
it safely falls back to ordinary allocation, so a fragmented free list cannot
amplify one large-object write into an unbounded scan. Contiguous chains record a
flag on the head page, making free-count and uncompressed-range lookups O(1);
older non-contiguous chains keep being validated by walking next pointers.

`ALTER TABLE ... ALGORITHM=COPY` on non-partitioned tables defaults to a
build-then-swap streaming rewrite. `MYDB_DDL_COPY_BATCH_PAGES` controls the
number of source heap pages scanned per batch, default `256`, minimum `1`; under
memory pressure the executor halves it automatically down to a single page, and
if a single page still cannot obtain a reservation from the unified governor's
`DdlCopy` category, 1037/HY001 is returned. Index sorting continues to use
`MYDB_PARALLEL_DEGREE` / `MYDB_PARALLEL_DISABLE` and external-sorts when the work
area reaches its watermark. The same variables also govern BTREE `CREATE INDEX`
and `ALTER TABLE ... ADD INDEX`: the default degree of parallelism is 4, and DDL
can be overridden per statement with `/*+ PARALLEL(n) */` or `/*+ NO_PARALLEL */`
placed after the leading keyword. Page range scans, sort double buffering, page
task queues, encoding buffers, and the 4 MiB per-worker stacks are all charged to
`StorageScratch` by capacity; on insufficient budget the parallelism is lowered
or spills, and 1037/HY001 is returned if even the minimum work area cannot be
obtained. See [`hints.md`](hints.md#63-b-tree-index-creation) for details.
Historical validation for `ALTER TABLE ... ADD FOREIGN KEY` also uses these two
environment variables, requesting 2 workers by default. The covering-index path
shards and merges by foreign key prefix; under concurrent writes, snapshot
projection and sort run generation proceed in parallel as page tasks while
preserving the same MVCC snapshot. The effective parallelism is limited by CPU,
index/heap page counts, and the query memory budget, and is reduced automatically
when short, never enlarging the memory ceiling. `MYDB_PARALLEL_DISABLE=1` forces
serial execution. Work sets and worker stacks are charged to `DdlOnlineLog`. In
`SHOW ENGINE MYDB STATUS`, `online_constraint_index_merge_workers_peak` and
`online_constraint_baseline_workers_peak` report the peak worker counts of the two
paths respectively; check the corresponding `*_parallel_runs` to see whether they
actually ran in parallel.

Ordinary and unique BTREE secondary indexes default to online bulk builds with
incremental catch-up; DML is allowed during the build, while preparation and final
publication require an exclusive metadata lock. An ALTER TABLE that only adds
such indexes uses the same path; explicit `LOCK=SHARED` uses the same private
bulk builder, allowing reads and blocking writes during the scan phase;
`LOCK=EXCLUSIVE` keeps exclusive access. The index incremental log is charged to
`StorageScratch` with a per-physical-index ceiling of 64 MiB, and exceeding it
aborts the DDL. This path does not depend on `MYDB_DDL_ASYNC`. Partition
REORGANIZE, HASH/KEY scaling, repartitioning, REMOVE PARTITIONING, REBUILD, and
OPTIMIZE allow concurrent reads and writes by default for tables containing only
BTREE/vector indexes and also accept `LOCK=NONE`; the private heap and bulk index
builds catch up with transactional changes afterwards, and only preparation and
publication need an exclusive barrier. `LOCK=SHARED` allows reads and blocks
writes, and `LOCK=EXCLUSIVE` retains an exclusive lock for the whole statement.
Index types that cannot yet be replayed online default to a shared build, and
explicitly specifying `LOCK=NONE` raises an error. RANGE/LIST ADD PARTITION also
supports `LOCK=NONE`; the new empty leaves are initialized privately before
publication, and failures are recovered through the intent log. TRUNCATE / REPAIR
PARTITION use a shared build with exclusive publication and do not accept
`LOCK=NONE`; CHECK/ANALYZE PARTITION also allow reads and exclude writes. Partition
truncation uses an empty shadow generation and does not reset the whole table's
AUTO_INCREMENT; ordinary tables cannot run TRUNCATE PARTITION ALL. Background
maintenance reclaims old files asynchronously; failures retain a persistent intent
for retry, and the jobs table shows a pending-cleanup state.
`MYDB_PARTITION_REORG_STREAMING=0` keeps the old path only in test builds;
production always uses shadow reorganization. Design, concurrency/recovery
verification, and performance data are archived in the project's internal
engineering records (partition-ddl-completion-2026-09-18).

`MYDB_DDL_COPY_STREAMING=0` is used only as a differential oracle in test builds;
production builds always use shadow COPY. Offline COPY also covers LOBs, vectors,
and partitions; the Memory backend publishes private memory handles to avoid
reopening empty indexes. Progress is visible in
`information_schema.mydb_ddl_jobs`; cumulative scan/convert/index/replay/swap
durations, source/shadow/spill bytes, direct heap build counts, and peak batch
bytes are provided by the `ddl_copy_*` lines of `SHOW ENGINE MYDB STATUS`.

`MYDB_DDL_ASYNC=1` still controls the internal worker/change-log path of other
existing DDL. It does not change MySQL statement completion semantics: the
connection still waits for the final catalog publish, and a background build
failure or cancellation is returned as an error of the original DDL. In strict
mode, disk tables support online column type changes (including LOBs, partitions,
and HNSW/IVF indexes), preferring online by default and also accepting
`ALGORITHM=COPY, LOCK=NONE`; this is a mydb extension and does not require
enabling asynchronous DDL. Unsupported action combinations, non-strict mode, and
Memory durability explicitly requesting `LOCK=NONE` still return 1846. The final
tail replay budget is controlled by `MYDB_DDL_COPY_MAX_FINAL_MS`, default 1000 ms
(maximum 60000 ms); system I/O provides no hard real-time ceiling.
See [Online Column Type Change](online-column-type-change.md) for details.

ALTER algorithms and lock capabilities are planned uniformly. Plain column
additions, column drops without index side effects, and compatible type widening
stay INSTANT; adding generated columns requires COPY; dropping a column that
forces a composite index rebuild requires at least INPLACE. In strict mode,
encoding-compatible NOT NULL tightening and DECIMAL precision shrinking at the
same storage width perform read-only validation (INPLACE), support
`LOCK=SHARED`, and do not rewrite the heap. Composite actions that cannot execute
atomically, and `LOCK=NONE/SHARED` without a corresponding concurrency
implementation yet, return 1846 before any modification. Verification background
is archived in the project's internal engineering records (table-ddl-hardening).

Constraint-specific CHECK/FK ALTERs are online by default and do not depend on
`MYDB_DDL_ASYNC`. Validation state is jointly bounded by the query memory quota
and a 256 MiB ceiling, returning 1037 when exceeded; the transactional
incremental log reuses the online DDL spill capability. The final tail uses the
`MYDB_DDL_COPY_MAX_FINAL_MS` time budget and is additionally limited to 1024
dirty rows, 4 MiB of row data, and 4096 entries / 1 MiB of index changes; it
fails explicitly when it cannot converge. The `online_constraint_*` lines of
`SHOW ENGINE MYDB STATUS` report scan, recomputation, and barrier durations;
progress and cancellation use `information_schema.mydb_ddl_jobs` /
`CANCEL DDL JOB`. Design and verification background is archived in the
project's internal engineering records (online-constraints-20260914).

Vector retrieval separates persisted index configuration from query-time settings:

- `MYDB_VECTOR_STORAGE=memory|paged` selects the HNSW/IVF backend, default
  `paged`;
- `MYDB_HNSW_*` controls graph building and initial session query settings;
  runtime query controls use `mydb_hnsw_*` session variables;
- `MYDB_IVF_NLIST`, `MYDB_IVF_KMEANS_ITERS`, and `MYDB_IVF_TRAIN_SAMPLE`
  supply IVF build defaults. `MYDB_IVF_NPROBE`, `MYDB_IVF_ITERATIVE_SCAN`, and
  `MYDB_IVF_MAX_SCAN_TUPLES` initialize the corresponding session variables;
- `USING HNSW|IVF WITH (...)` stores validated index configuration independently
  from COMMENT. Build defaults are captured when the index is created.
  `quantization=none` is the default; `sq8`, `sq4`, `pq`, and `bq` are explicit
  per-index choices. `MYDB_HNSW_QUANTIZE` has been removed and causes a startup
  error. See [vector indexes](vector-indexes.md) for parameters and limitations.
- Query hints `HNSW_EF` / `IVF_NPROBE` override explicitly set session values,
  which override index defaults. Explicit `mydb_hnsw_rerank_mult` /
  `mydb_ivf_rerank_mult` session values override the index `rerank_mult`.
  An explicit SET to the built-in default also takes precedence. See
  [query tuning](vector-indexes.md#query-tuning) for defaults and examples.

Zone Map is an optional skip-scan structure enabled by default for persistent
user tables; temporary and system tables do not enable it. Table creation or
alteration accepts `ZONE_MAP=ON|OFF|DEFAULT`, and `ZONE_SIZE` defaults to `1M`
with allowed values being powers-of-two multiples of the 16 KiB page within
`64K..64M`:

```sql
CREATE TABLE events (ts DATETIME, payload JSON)
  ZONE_MAP=ON ZONE_SIZE=1M;
ALTER TABLE events ZONE_MAP=OFF, ZONE_SIZE=DEFAULT;
```

The session setting `optimizer_switch='zone_map=off'` disables automatic
selection; `ZONE_MAP(table)` and `NO_ZONE_MAP(table)` provide single-statement
overrides — see [`hints.md`](hints.md) for the detailed conflict rules. The
following GLOBAL sysvars adjust the maintenance policy dynamically:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `mydb_zone_map_maintenance` | `ON` | Enables automatic maintenance. |
| `mydb_zone_map_rebuild_ratio` | `0.20` | Schedules a rebuild when the stale change count reaches `max(64, ceil(row_count_estimate × ratio))`; range `0..1`. |
| `mydb_buffer_pool_quota_zone_map_pct` | `0` | Category soft quota for Zone Map in the unified buffer pool, range `0..100`; the default disables the category eviction preference but not the caching. |

Zone Map directories and summaries use standard 16 KiB pages and share the buffer
pool with heap, index, and LOB pages. The soft quota reserves no memory and is not
a hard cap; dynamic changes take effect at subsequent eviction decisions. The same
percentage can be set at startup via `MYDB_BUFFER_POOL_QUOTA_ZONE_MAP_PCT`.
Example:

```sql
SET GLOBAL mydb_buffer_pool_quota_zone_map_pct = 0;
SHOW GLOBAL VARIABLES LIKE 'mydb_buffer_pool_quota_zone_map_pct';
SHOW ENGINE MYDB STATUS;
```

Page memory is accounted for solely by the buffer pool; lightweight storage
handles and short-lived rebuild/encoding scratch are accounted separately. There
is no longer a separate whole-table decoded summary cache.
`buffer_pool_pages_zone_map` is the resident page count; multiply it by 16384 to
get the corresponding page bytes; it does not represent handles, scratch, on-disk
file size, or process RSS. `SHOW ENGINE MYDB STATUS` also exposes cumulative
pages read by shared-pool file type: `buffer_pool_read_ios_heap`,
`buffer_pool_read_ios_index`, `buffer_pool_read_ios_overflow`,
`buffer_pool_read_ios_vector`, and `buffer_pool_read_ios_zone_map`. They use the
same accounting as the total `buffer_pool_read_ios`, covering cache-miss reads,
prefetch/bulk reads, and reads from scans that bypass the cache; cache hits are
not counted, and one bulk read accumulates by page count. These are page reads
initiated and successfully completed by the database that may hit the operating
system cache, so they must not be treated as physical disk read counts. When
comparing heap pages read before and after filtering, use the difference of counts
taken under equivalent cache conditions.

The `zone_map_resident_pages/bytes` values in `SHOW ENGINE MYDB STATUS` likewise
read the shared-pool residency statistics; `zone_map_handles`,
`zone_map_handle_bytes`, and `zone_map_handle_charged_bytes` denote the number of
cached lightweight handles, the estimated size, and the size actually charged by
the governor, respectively. `zone_map_handle_hits/misses` counts handle lookups
and is not a data page hit rate. Reading these metrics loads no cold summaries
and walks no entire zone. The old `zone_map_cache_*`, `zone_map_frame_cache_*`,
and the old frame compaction/defragmentation metrics have been removed.

The old variables `mydb_zone_map_cache_mb`, `mydb_zone_map_compact_ratio`, and
`mydb_zone_map_max_frame_versions` have been removed; setting them returns an
unknown system variable error, and old MB parameters are not reinterpreted as
percentages. The old append-frame compaction-ratio/version-count policy no longer
applies.

Background maintenance waits for a default 30-second quiet window measured from a
zone's last write. Each round checks and repairs at most 64 zones per physical
storage object and additionally inspects the base metadata of at most 64 zones to
confirm contiguous coverage. Repair cursors rotate independently, so continuously
writing one zone does not block repairs of other cold zones; regions already
completed and not stale are not rescanned from the heap. This bounds the per-round
work scope, not the disk I/O rate. Missing base summaries are rebuilt in batches
and written to shared pool pages; unfinished regions stay Unknown. Cancellation
keeps completed summaries and coverage progress; after a reset, old coverage proof
is invalidated. Explicit ANALYZE still performs a full rebuild in bounded batches.
Progress updates by covered zone; queries read summary pages on demand and never
rescan the whole table because of a cache miss. When cache admission fails or page
contents cannot be verified, queries fall back conservatively. A per-table
maintenance failure is recorded in `information_schema.mydb_zone_map_storage.last_error`,
retried after a default 30 seconds from the end of the failure, while other tables
keep being maintained. A rebuild error is not cleared by later successful sidecar
writes.

Zone Map shared cache pages, rebuild scratch, and background workers are likewise
governed by the unified memory governor; when metadata is untrusted or corrupted,
the budget is short, or a predicate cannot prune safely, the executor keeps the
original row predicate and falls back to an ordinary scan without sacrificing
result correctness. See [`storage-format.md`](storage-format.md) for the
persistent format and recovery procedures.

### Zone Map Maintenance and Diagnostics Supplement

`mydb_zone_map_enabled` is the master Zone Map switch, default `ON`, supporting
dynamic GLOBAL and `SET PERSIST` / `SET PERSIST_ONLY`. At startup use
`MYDB_ZONE_MAP_ENABLED=OFF`, or configure `zone_map_enabled=OFF` (alias
`mydb_zone_map_enabled`) in `[mydb]` / `[mysqld]`. The configuration precedence
described in this document applies; SESSION settings are not supported.

```sql
SET GLOBAL mydb_zone_map_enabled = OFF;
SELECT @@GLOBAL.mydb_zone_map_enabled;
SET PERSIST mydb_zone_map_enabled = ON;
```

Once disabled, all tables (including existing tables, partitions, and tables with
explicit `ZONE_MAP=ON`) stop summary computation, construction, incremental
maintenance, and summary file writes; neither new plans nor cached plans use Zone
Map pruning, and even a forced `ZONE_MAP` hint cannot bypass this. `ANALYZE TABLE`
still updates ordinary statistics, and background partition cleanup keeps
running. Table-level configuration, `optimizer_switch`, and
`mydb_zone_map_maintenance` keep their values and jointly constrain the
corresponding features again after re-enabling.

A dynamic disable waits for admitted summary tasks to finish, durably revokes
trust in old roots through the standard page WAL, and only then stops
maintenance. Existing summary files may remain but cannot be used to restore old
boundaries. After re-enabling, full scans are used first and pruning resumes only
once a safe rebuild completes; uncommitted transactions spanning the switch miss
no rows. min/max and membership are not computed while disabled. The
`zone_map_enabled` value in `SHOW ENGINE MYDB STATUS` reports the master switch;
when off, the Zone Map diagnostics table is empty.


- When a single bulk write reaches 256 rows, the touched zones may defer
  all-column summary maintenance and be marked Unknown to bound writer-side
  CPU/memory cost; scans are conservative during that period, and pruning resumes
  after a safe rebuild by background maintenance or `ANALYZE TABLE`. Whether a
  summary can be shrunk is still constrained by active snapshots and concurrent
  modifications.
- Zone Map paged files keep the disk high-water mark: reset/full rebuild reuses
  page numbers but currently never truncates the file automatically; ordinary
  DELETEs and checkpoints do not guarantee releasing its tail space either.
  `OBSOLETE_BYTES` is a diagnostic measure of stale space and does not mean space
  has been returned to the file system. Repeating ANALYZE at the same scale should
  not keep appending to the whole directory.
- The trigger condition of `mydb_zone_map_rebuild_ratio` is the stale change count
  reaching `max(64, ceil(row_count_estimate * ratio))`; even set to `0`, the floor
  of 64 changes remains. Maintenance replacement of a stable zone resets the
  estimate to the visible scanned row count; conservative merges never shrink the
  estimate.
- `MYDB_ZONE_MAP_REBUILD_THREADS` is a startup environment variable; when unset it
  follows `MYDB_PARALLEL_DEGREE` (which defaults to 4). The actual rebuild
  parallelism is chosen from the page count and the memory budget; changing the
  environment variable at runtime has no effect.
- `mydb_zone_map_maintenance_quiet_ms` and `mydb_zone_map_maintenance_retry_ms`
  are dynamic GLOBAL variables controlling the quiet window and the failure retry
  interval respectively; both default to 30000 ms with a range of `0..86400000`.
  Setting 0 schedules the next maintenance round immediately, but the snapshot and
  concurrent-write barriers must still be satisfied.
- In the background, each round runs at most 10 consecutive maintenance passes;
  each pass checks at most 64 zones per storage object, each batch releases its
  temporary memory independently, and deferral conditions such as cancellation,
  foreground imports, and ANALYZE are checked. Every pass gives every table a
  turn, so one large table cannot monopolize a whole round's batches; after a
  round it waits 10 seconds. The batch sizes and scheduling interval are currently
  source constants and are not controlled by the quiet-window or failure-retry
  variables above.
- `information_schema.mydb_zone_map_storage.MEMORY_FALLBACKS_GLOBAL` is a
  process-cumulative counter; the diagnostics row of every table shows the same
  global value and must not be summed per table. The remaining storage usage
  columns are still accounted per storage object.
- `information_schema.mydb_zone_map_storage.state=PENDING` means the trusted sidecar
  has Unknown zones awaiting maintenance. `mydb_zone_maps` outputs `Unknown` and NULL
  bounds for columns not yet built and no longer misreports imported data as zero
  summaries; `READY` means the base is trusted but does not guarantee a prunable
  summary for every column.
- `mydb_zone_maps.page_start/page_end` is a half-open interval `[start,end)` in pages;
  `page_end` is the zone's theoretical upper bound and may exceed the heap's
  actual page count, truncated to the actual count at execution. Time bounds are
  shown by column type, with TIMESTAMP diagnostic values in UTC; text bounds
  remain sort weights.
- `zone_map_zones_skipped` in `SHOW ENGINE MYDB STATUS` accumulates zones excluded
  during candidate generation, including planning probes;
  `zone_map_scan_pages_skipped` accumulates pages excluded at runtime. The two
  must not be directly subtracted from each other or treated as a hit rate in the
  same unit.
- Internal ENUM/SET comparisons can map ordinal matches to textual membership
  probes; at most 1024 possible values are expanded at a time (ENUM including the
  error empty value, SET including the empty set), and larger definitions still
  fall back conservatively to avoid exponential planning. OR is pruned only when
  every branch compiles safely; unsupported branches must not be dropped.


A MySQL binlog replica group-fsyncs the relay log at most every 32 source
transactions or 100 ms by default, and atomally persists channel metadata at most
every 64 progress updates or 100 ms. The corresponding startup options are
`repl_relay_sync_txns` / `MYDB_REPL_RELAY_SYNC_TXNS`,
`repl_relay_sync_interval_ms` / `MYDB_REPL_RELAY_SYNC_INTERVAL_MS`,
`repl_progress_sync_txns` / `MYDB_REPL_PROGRESS_SYNC_TXNS`, and
`repl_progress_sync_interval_ms` / `MYDB_REPL_PROGRESS_SYNC_INTERVAL_MS`. All four
have a minimum of `1`. The in-memory `SHOW REPLICA STATUS` still updates per
event; the on-disk metadata publishes only IO positions already covered by a relay
fsync, and crash recovery replays idempotently from an earlier safe position
without skipping transactions.

The single SQL applier of a MySQL binlog replica treats up to 16 consecutive
row-only source transactions totaling at most 4 MiB as one durability group by
default. Each source transaction still keeps its own MVCC commit, WAL
`CommitTxn`, GTID, and relay position; the applier publishes them strictly in
source commit order and performs a single WAL durability barrier at the group's
highest LSN. DDL, `START REPLICA UNTIL`, `SOURCE_DELAY`, apply errors, and the
tail of the current durable relay window all flush the existing group first, so
the replication position never advances across these boundaries. The
corresponding startup options are `repl_apply_batch_txns` /
`MYDB_REPL_APPLY_BATCH_TXNS` and `repl_apply_batch_bytes` /
`MYDB_REPL_APPLY_BATCH_BYTES`; both have a minimum of `1`, and setting
`repl_apply_batch_txns` to `1` restores a per-source-transaction durability
barrier as a differential oracle. The byte cap counts the actual encoded bytes of
relay records rather than estimating by row count and forms no long-term memory
reservation; the pending-commit objects' lifetime covers only one SQL apply call
and is bounded by the durable relay window, the transaction count, and the byte
cap.

Row events of the binlog source are split at
`MYDB_BINLOG_ROW_EVENT_MAX_BYTES=67108864` (64 MiB) by default; only the last
chunk of a statement carries `STMT_END_F`. A single row is never split, but both
the event size and the file position go through checked u32 conversions. Before
commit, `MYDB_BINLOG_ROTATE_BYTES` pre-judges whether the whole transaction
crosses the configured threshold, defaulting to 1 GiB when unset; when
`MYDB_BINLOG_RETENTION_BYTES` is configured, it also caps the rotation threshold.
A single transaction is never split across two files, so actual files may exceed
the soft threshold; rotation is still forced before approaching the 4 GiB file
position limit. A transaction that cannot fit into a single binlog file by itself
returns an explicit error and never truncates `event_size` / `log_pos`.

The MySQL relay stores raw row events, parsed by the SQL applier after preceding
DDL completes; errors such as a missing table or a column mismatch stop the SQL
thread while the IO thread keeps receiving. Heterogeneous-charset replication
should enable `binlog_row_metadata=FULL` on the source; mydb decodes using the
source charset carried in TABLE_MAP. When `MINIMAL` carries no source charset,
both ends must have identical schemas; invalid character bytes return an explicit
error instead of U+FFFD replacement. After fixing the replica schema,
`START REPLICA SQL_THREAD` retries the retained relay.

The relay is reclaimed once its applied prefix reaches 16 MiB and at least half
of the file, preserving the unapplied tail; the reclaim intent, the new file, and
the metadata are installed in durable order, and startup after a crash finishes an
unfinished reclaim. When the SQL thread lags or stops, unapplied events must still
be retained, so disk demand depends on the replication backlog.

After `MYDB_BINLOG_RETENTION_BYTES` or `MYDB_BINLOG_RETENTION_HOURS` is set, a
background task checks retention every 60 seconds; only inactive files are
deleted, and an hour count of 0 means no time-based expiry. After a semi-sync
wait times out, the source temporarily degrades to asynchronous to keep later
commits from waiting repeatedly; semi-sync resumes once enough replica ACKs cover
the timed-out position, and `Rpl_semi_sync_source_status` reflects the degraded
state.

`sync_binlog=0` relies on OS flushing, `1` syncs every binlog commit group,
and `N>1` syncs every N groups. Rotation closes the outstanding sync interval
when syncing is enabled. `SET GLOBAL max_binlog_size` changes the soft rotation
threshold immediately (4096–1073741824 bytes); one transaction is never split
across files. `expire_logs_days`, `max_binlog_cache_size`, and
`max_binlog_stmt_cache_size` are read-only: retention is configured through
the startup retention options, and transaction memory uses mydb's memory
governor rather than an independent MySQL binlog cache limit.

With `rpl_semi_sync_source_wait_no_replica=ON`, a commit waits for the configured
ACK quorum until timeout, including when too few replicas are connected.
With `OFF`, an unavailable quorum causes immediate asynchronous fallback;
disconnects and runtime quorum changes wake existing waiters. Connections
which negotiated semi-sync keep its packet headers after a runtime OFF;
the switch disables ACK requests/waits without changing the wire format.

Startup is still refused when a sealed WAL is corrupted or the binlog is shorter
than the WAL's committed references. This protects against silently losing
transactions or publishing an incomplete history to replicas, including
tail-loss scenarios with `sync_binlog=0`; emptying the binlog directory cannot be
relied on to preserve the original replication continuity.
For `sync_binlog=0` or values greater than `1`, a power loss may therefore require
operator recovery before startup succeeds. Preserve the data, WAL, and binlog
files; restore a mutually consistent backup and rebuild downstream replication
when the missing binlog history cannot be recovered. Do not delete or truncate
binlogs to bypass the check. Use `sync_binlog=1` to require a synchronized binlog
commit group before acknowledgement.

Binlog status, heartbeat positions, and persisted publication boundaries exclude
staged transactions. A consistent snapshot saves the file position and GTID set
together after earlier commit publication and visibility have completed; later
commits do not change those saved coordinates. The commit admission gate is
released after snapshot creation, rather than held for the whole export.
CRC32 is verified when historical events are read and
when WAL commits extend the recovered tail. NONE still checks event framing
and the format-description event, but provides no payload CRC for ordinary
events. A missing publication manifest is not sufficient evidence that complete
transaction bytes committed: startup refuses transaction history without that
manifest. Restarting an existing history with a different checksum algorithm is
also rejected. Restore consistent metadata and use its stored checksum setting.

`RESET MASTER` rejects active dump readers, including consumers that did not
send `COM_REGISTER_SLAVE`. Unrecoverable append/publication failures stop
subsequent binlog commits and wake ordering waiters with an error; restart
reconciles the surviving history against WAL.

mydb→mydb native physical replication uses a separate protocol v2. The default
listener is `127.0.0.1:3316`; non-loopback requires mTLS. See
[`native-replication.md`](native-replication.md) for configuration, watermarks,
base backup, and manual controlled switchover procedures.

Online backup and WAL archiving are configured through `BACKUP DATABASE` and
`ALTER MYDB BACKUP ... SET WAL_ARCHIVE=DISABLED|MANUAL|CONTINUOUS`. A full backup
does not enable archiving. Archive policy, repository binding and WAL retention
are persisted by the source and reloaded on restart.

`backup_repository` / `MYDB_BACKUP_REPOSITORY` selects the direct read-only
baseline query entry point. It requires `base` / `MYDB_BACKUP_BASE` and an external
`runtime_dir` / `MYDB_BACKUP_RUNTIME_DIR`, and cannot be combined with `data_dir` /
`MYDB_DATA_DIR`. It is not the source archive configuration. WAL encoding and the
optional key ID are fixed by `mydb-backup init`; the server obtains the key ID
from the repository header. Key material is supplied through the sensitive
startup option `MYDB_BACKUP_KEY_FILE`, which must also be provided on encrypted
archive source restarts, and never appears in the non-sensitive CLI help values.
WAL encryption does not encrypt the queryable baseline. See
[`backup-and-recovery.md`](backup-and-recovery.md) for the full procedures.

### Data File I/O Modes

On Linux, the startup environment variable `MYDB_DATA_FILE_DIRECT_IO` controls
the Direct I/O path for buffer pool data files (heap, indexes, LOBs, and so on);
it is off by default. Setting `0`, `off`, `false`, or `no` (case-insensitive)
disables it and uses buffered I/O with the operating system page cache; setting
`1` enables it. A process restart is required after changing it.

Buffered I/O is the default, letting index lookups and heap row fetches reuse the
operating system page cache warmed by large table scans. When enabled, applicable
page reads and writes attempt `O_DIRECT`; unsupported file systems or inapplicable
bulk paths may still fall back to buffered I/O, and there is no guarantee that
every disk operation uses Direct I/O. When disabled, no Direct I/O handles are
opened and their aligned staging copies are not performed. This variable changes
neither the I/O strategy for WAL, undo, or binlog nor the durability guarantees of
`MYDB_DURABILITY`. macOS still uses the separate `MYDB_DATA_FILE_NOCACHE` (off by
default), unaffected by this Linux switch.

Set `MYDB_DATA_FILE_DIRECT_IO=0` for buffered I/O or `1` to enable Direct I/O
at server startup. Compare both settings with your workload and file system.

### Memory Used by WAL Startup Recovery

Startup recovery validates sealed WAL segments in parallel, validates the active
segment last, and repairs its valid residual tail. The recovery snapshot keeps
the original segments' file handles and a budget-bounded logical record
directory; it no longer keeps all payloads in heap memory or re-encodes and
writes out an entire temporary WAL. Unix accesses the validated immutable prefix
through read-only mappings; the mapping residency window is managed in 64 MiB
units with a target ceiling of one third of the working-set target and at most
2 GiB. Checkpoints reclaim WAL by replacing files and unlinking the old ones; an
open recovery snapshot remains readable. Non-Unix platforms use positional
reads.

REDO preserves in-file LSN order, and UNDO follows transaction back-links.
CLR/EndTxn records generated during recovery are only appended as a validated new
tail; log initialization reuses the validated segments and transaction metadata.
Undo file repair proceeds in batches, repairing only records that must be kept,
while a missing physical prefix is handled by the existing recovery relocation
and persistent forwarding table. After transaction and log initialization, only
the Zone Map incremental directory is retained, freeing space for index
validation/rebuild.

Decoding scratch, the record directory, and navigation state are included in
controlled memory accounting; an explicit error is returned when the budget is
insufficient. The page LSN cache is enabled only during recovery and invalidated
on file rebuild, deletion, and truncation. Page numbers allocated by index
recovery never fall into existing file ranges; native recovery skips replay only
when the persisted LSNs of the whole batch of target leaf pages cover that batch,
and pages newly written during recovery, forced backup replay, and physical
replication do not use this check.

When an index's logical replay volume is significantly larger than its file page
count, startup index validation is enabled, there is no pending XA, and this is
not a physical replica, indexes may be rebuilt from heap data based on cost,
including indexes whose structure is not yet damaged. This path first writes a
persistent rebuild marker and then skips the logical replay of that tree; the
marker requires a later rebuild from the authoritative heap data and must not be
treated as repair via skipped replay. Setting `MYDB_RECOVERY_INDEX_REBUILD=0`
disables this cost-based choice. Zone Map recovery merges compatible increments;
summaries whose trusted state was revoked do not become trusted again by
replaying increments.

Recovery time depends on the volume of WAL, data pages, and index repairs.
`MYDB_MEMORY_TARGET` is a working-set target; system-level hard limits should still be set through
cgroups or the like, and it must not be taken as a guarantee that any historical
WAL can be recovered within a fixed amount of memory.

## Sequence Persistence

Sequence persistence uses `sequence_wal_batch` (`MYDB_SEQUENCE_WAL_BATCH`, default
32 extra steps). The bootstrap settings `sequence_sync_policy`
(`MYDB_SEQUENCE_SYNC_POLICY`, `local|remote_apply`) and `sequence_sync_replicas`
(`MYDB_SEQUENCE_SYNC_REPLICAS`, comma-separated UUIDs) initialize a durable catalog
configuration. Existing configuration changes require a fenced native master and
`CHANGE MYDB SEQUENCE SYNC TO '<JSON>'`; they cannot silently replace catalog
state through environment variables. See [Sequences](sequences.md) for candidate
membership, configuration epochs, acknowledgement and failover requirements.

## Performance Schema

`performance_schema` is enabled by default. `MYDB_PERFORMANCE_SCHEMA=off`
disables all PFS runtime collection and optional caches at startup. The MySQL
8.0.46 `performance_schema_*` sizing variables are exposed as GLOBAL READ ONLY
variables; at startup, positive capacities can be overridden with the same-name
uppercase `MYDB_` environment variables, for example:

```bash
MYDB_PERFORMANCE_SCHEMA_DIGESTS_SIZE=20000
MYDB_PERFORMANCE_SCHEMA_EVENTS_STATEMENTS_HISTORY_SIZE=20
MYDB_PERFORMANCE_SCHEMA_EVENTS_STATEMENTS_HISTORY_LONG_SIZE=20000
MYDB_PERFORMANCE_SCHEMA_EVENTS_WAITS_HISTORY_SIZE=20
MYDB_PERFORMANCE_SCHEMA_EVENTS_WAITS_HISTORY_LONG_SIZE=20000
```

All of these long-term capacities are reserved from the unified memory governor's
`PerformanceSchema` category, capped by default at 5% of effective memory within
the managed limits. When the budget cannot be obtained, the corresponding
optional collection is skipped. `MYDB_MEMORY_TARGET` is an advisory sizing
target. See [`performance-schema.md`](performance-schema.md) for the full
surface and data sources.

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

- `SIGINT`, `SIGTERM`, SQL `SHUTDOWN`, and `RESTART` all perform a controlled
  shutdown; `SIGHUP` does not trigger shutdown. If the final checkpoint fails, the
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

Progress/cancellation: `information_schema.mydb_ddl_jobs`,
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

Since WAL v32, segment headers include an explicit checkpoint replacement horizon.
Startup validates the replacement checkpoint before ignoring superseded files;
old and new segment generations may safely coexist after interrupted cleanup.
Rotation stages successors as `.log.pending` files. A durability barrier syncs
the preceding prefix before publishing official segment names and syncing the
WAL directory; startup discards unpublished successors.

Recovery validates complete standard-page checksums before trusting page LSNs,
coalesces consecutive updates to one page, and syncs recovered files and their
directory ancestry before a checkpoint can reclaim WAL. File lifecycle undo is
recorded as redo-only file compensation. Reduced-redo extent rollback removes
only the losing transaction's rows and truncates a tail only when it contains no
other transaction's surviving rows or later allocations. Heap page v14 marks
reduced-redo bases so strict writers emit an FPI before subsequent row redo,
even when the base page's LSN is newer than the checkpoint. The marker clears
after that image is logged, preserving the ordinary mutation fast path.

Native replication validates overlapping local WAL bytes before replay or ACK.
It appends validated source frames without re-encoding transaction backlinks.
Strict senders read only the durable prefix without draining unrelated pending
writes; batch framing is validated without materializing its row/key payloads.

`wal_write_buffer_bytes` (alias `mydb_wal_write_buffer_bytes`) controls the WAL
write-coalescing buffer. The default is 256 KiB (262144 bytes); the allowed range
is 0 through 1 GiB. Zero writes frames directly to the WAL file. This threshold
is separate from WAL segment size and does not cap total WAL-related memory.

Set it at startup using `MYDB_WAL_WRITE_BUFFER_BYTES=4M`, or in the config file:

```ini
[mydb]
wal_write_buffer_bytes = 4M
```

Environment values and config-file values accept binary K/M/G/T suffixes.
Invalid values fail startup. The environment takes precedence over the file.
The live value is also readable and writable through SQL (in bytes):

```sql
SHOW GLOBAL VARIABLES LIKE 'mydb_wal_write_buffer_bytes';
SET GLOBAL mydb_wal_write_buffer_bytes = 4194304;
```

A runtime change drains pending WAL writes before resizing the buffer and is
subject to memory admission. Larger buffers can coalesce more writes but do not
change commit durability: strict commits still synchronize WAL, while relaxed
commits drain it to the OS page cache. Frames at least as large as the threshold
bypass the buffer. Memory durability mode does not enable WAL persistence.
`SET GLOBAL` changes the running instance; use startup configuration to retain
the choice after restart. The compatibility stub `innodb_log_buffer_size` does
not control this buffer.

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

## External AI calls

`ai_enabled` / `MYDB_AI_ENABLED` defaults to ON. AI connections and models can
be configured through SQL without a local policy file. By default, configured
HTTP/HTTPS services have no outbound origin, IP address, network-range, or port
restrictions, including public HTTP and link-local addresses. IPv4, IPv6, and
DNS names are supported.
`ai_local_config` (`MYDB_AI_LOCAL_CONFIG`) is optional, for explicit outbound
origins, IP policies, private CAs, node-local account quotas, and historical
encryption keys. An omitted `egress` leaves the network unrestricted;
an explicit `egress` enables an allowlist. Invalid explicit files
return errors rather than falling back to defaults.
The startup capacity controls are `ai_max_inflight` (32), `ai_max_queue` (128),
and `ai_memory_limit_mb` (128), with corresponding `MYDB_AI_*` environment names.
`mydb_ai_enabled` is the privileged GLOBAL node switch. Set `ai_enabled=OFF`
at startup or `SET GLOBAL mydb_ai_enabled=OFF` to disable calls. AI_ADMIN and
AI_USE privileges remain required for management and model use, respectively.

API keys are encrypted database metadata. The master key comes from the
**environment-only** `MYDB_AI_ENCRYPT_KEY`, or a randomly initialized
`<data_dir>/apikey.mydb` keyring. The filename is not a default password.
Keep master keys separate from database backups and deploy matching keys to
replicas independently. See [LLM and Embedding Calls](ai.md) for the exact key
format, TLS requirements, local policy, model DDL, SQL semantics, and recovery.

## HTTP SQL calls

HTTP client SQL functions are enabled by default and require `HTTP_USE`. See
[HTTP SQL Functions](http.md#node-configuration) for the `http_*` startup options,
node egress policy, private CA bindings, account quotas, and runtime controls.
