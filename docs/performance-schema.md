# Performance Schema

Performance Schema exposes runtime statements, waits, locks, memory, sessions,
and replication diagnostics. Its compatibility baseline is MySQL 8.0.46 Community.

## Compatibility Boundaries

- The 111 Community table definitions follow MySQL 8.0.46 across `DESCRIBE`, `SHOW CREATE`,
  and `information_schema.tables/columns/statistics`.
- 14 optional plugin/enterprise extension tables are retained by default; they are
  separate from the Community-equivalent set.
- Tables report measured data when the corresponding subsystem is available.
  Tables for unavailable subsystems return empty results, with no placeholder rows.
- Performance Schema state lives only in-process: it is cleared on restart and never
  changes persistent formats.

## Collection and Memory

- Collection is controlled by setup consumers and instruments.
- Current, history, and summary tables expose statement and wait events;
  account, host, and user summaries include retained disconnected-session data.
- Statement digests have a default total capacity of 10000. Latency histograms
  provide 95th, 99th, and 99.9th percentiles.
- Setup data, event history, digests, error summaries, and retained connection
  groups share the Performance Schema memory budget. When capacity is exhausted,
  optional collection is skipped and the relevant lost counters increase.

`MYDB_PERFORMANCE_SCHEMA=off` disables runtime collection. MySQL PFS sizing variables
are all exposed as GLOBAL READ ONLY variables; startup capacities can be overridden
with same-name uppercase `MYDB_` environment variables, for example:

```bash
MYDB_PERFORMANCE_SCHEMA_DIGESTS_SIZE=20000
MYDB_PERFORMANCE_SCHEMA_EVENTS_STATEMENTS_HISTORY_LONG_SIZE=20000
```

## Data Sources

- Sessions: connection lifecycle, session status, sysvars/user variables, prepared
  statements, connection attributes, and memory peaks.
- Statements: unified statement begin/end, scanned rows-examined, diagnostics area,
  lock waits, result row counts, digests, and histograms.
- Locks: read-only snapshots of MDL, transaction row/table locks, and the wait-for
  graph.
- Memory: allocations, frees, current values, and peaks per category from the unified
  governor.
- Logs, keyring, and replication: their respective runtime registries/snapshots.

Subsystems that mydb does not have — Group Replication, InnoDB redo, clone, firewall,
NDB, and the enterprise thread pool — keep their exact structure and are explicitly
empty.
