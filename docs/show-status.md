# SHOW STATUS and Compatibility

This document records how mydb `SHOW [GLOBAL|SESSION|LOCAL] STATUS` is
collected, aggregated, and mapped to the Performance Schema. The compatibility baseline is MySQL Community 8.0.46.

## Counters and Reset Behavior

Status variables report measured values from active server subsystems.
Unsupported counters are omitted rather than filled with placeholder zeros.
An implemented but disabled subsystem may report `0` or `OFF`.

| Counter family | Reset behavior |
| --- | --- |
| Session statement, protocol, byte, and execution counters | `FLUSH STATUS` resets session counters and merges them into global totals |
| Connections, aborts, peak usage, and uptime | Reset behavior follows the corresponding MySQL counter |
| Buffer pool, WAL, undo, transaction, lock, and DML totals | Engine cumulative values are not reset by `FLUSH STATUS` |
| Binlog, semi-sync, and replica totals | Cumulative over the subsystem lifetime |
| Native `Mydb_*` counters | Reset behavior depends on the subsystem |

Account, host, and user aggregation retains disconnected-session totals within
a bounded memory budget and a maximum of 4096 groups. When optional groups
cannot be retained, `Mydb_status_aggregation_dropped` increases.

## Scope and Related Surfaces

- `SHOW SESSION/LOCAL STATUS`: Both-scope variables use the current
  connection's value, Global-scope variables use the global value.
  `Ssl_cipher`/`Ssl_version` are Both-scope in MySQL 8.0.46, so GLOBAL
  also returns the current connection's value.
- `SHOW GLOBAL STATUS`: disconnected-session cardinality plus all active
  sessions, without missing or double-counting disconnecting connections.
- `performance_schema.global_status/session_status` reuse the same
  snapshot but, following MySQL rules, do not collect SQL/protocol
  `Com_*` command counters; `Com_stmt_reprepare` is a special MySQL
  global variable, and mydb has no reprepare subsystem yet, so it is not
  provided.
- `status_by_thread` contains only active foreground connections;
  `status_by_account/host/user` merge active connections with
  disconnected-session cardinality. host does not carry the client
  ephemeral port.
- `sys.metrics` uses the PFS global-status visible set, then merges
  global variables.

## Implemented MySQL Core Families

- Connections/threads: `Connections`, `Aborted_*`,
  `Connection_errors_max_connections`, `Threads_*`,
  `Max_used_connections`, `Uptime*`.
- Protocol/statements: network bytes, Questions/Queries, all classified
  DML/transaction/DDL/SHOW/admin `Com_*`, plus prepared protocol
  commands.
- Storage: buffer pool requests/pages/I/O, data OS calls, WAL
  logical/physical/fsync, row lock current/waits/total/avg/max/deadlocks,
  InnoDB rows.
- Execution: Handler write/update/delete/commit/rollback/read-rnd-next,
  Slow_queries, Prepared_stmt_count, external sort
  `Created_tmp_files`/`Sort_merge_passes`,
  `Table_locks_{immediate,waited}`. Set operation external sorts are
  reported through
  `Mydb_setop_spill_{count,bytes_written,bytes_read,partitions,repartitions,max_depth,file_opens,merge_passes}`;
  these are process-lifetime cumulative values that share their source
  of truth with the lowercase items of the same names in
  `SHOW ENGINE MYDB STATUS`.
- Replication: source/master semi-sync compatible names, replica/slave
  status, and stable `Mydb_*` channel/binlog metrics.

`Sort_rows`, `Select_*`, the remaining Handler read breakdowns, and
complete temporary-table counters are not provided yet: the current
execution paths have no unified, double-count-free real collection
points for them. They are omitted rather than padded with constant zeros.

## Filtering and Result Metadata

Result names are sorted and unique. Use `LIKE` or `WHERE` to select variables.
An invalid `WHERE` expression returns an error rather than silently excluding
rows. Results have the fixed columns `Variable_name` and `Value`, including
when the statement is prepared.
