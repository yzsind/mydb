# MVCC and Locking

This guide describes transaction visibility, isolation, lock waits, and
concurrency diagnostics. The default isolation level is `READ COMMITTED`.
For historical reads, see
[Time Travel Queries](mysql80-compatibility.md#time-travel-queries).

A plain SELECT uses a consistent snapshot and does not wait for another
transaction's row write lock. Older visible values are reconstructed from undo
when necessary. UPDATE, DELETE, and locking reads check the current row after
locking. Covering, full-text, spatial, vector, and ordinary index access all
respect the same transaction visibility rules.

## Commit Visibility

A successful COMMIT makes the transaction visible to the next RC statement.
Existing RC statement snapshots and RR transaction snapshots remain stable.
With strict durability, a successful COMMIT also completes the required WAL
synchronization. A durability or compensation failure can quarantine the
database until restart recovery; applications must handle an unsuccessful
commit explicitly. See [persistence and recovery](storage-format.md).

## Snapshot Lifecycle and Isolation Levels

| Isolation level | Plain consistent read | Current read/write | Key boundaries |
| --- | --- | --- | --- |
| `READ UNCOMMITTED` | Each statement reads the latest heap version and sees other transactions' uncommitted INSERT/UPDATE/DELETE | Locks and reads the latest current version | Dirty reads allowed |
| `READ COMMITTED` (default) | Each statement takes a fresh committed snapshot | Re-reads the latest committed version after locking | Non-repeatable reads and phantoms allowed within one transaction |
| `REPEATABLE READ` | The first plain consistent read fixes the transaction snapshot, which is reused afterwards | Does not use the fixed old snapshot; reads the latest committed version after locking | Snapshot results are stable, but there are no gap/next-key/predicate locks; range inserts and write skew are not prevented |
| `SERIALIZABLE` | Multi-statement transactions take transaction-duration table S on the persistent user tables involved, then run the current statement view | DML takes table IX and row X | Table S blocks later DML on the same table; an autocommit single read-only statement needs no cross-statement locks |

Two kinds of reads must be distinguished:

- A plain `SELECT` is a consistent read. It takes no row locks; when it meets another transaction's uncommitted version it follows undo back to the older committed version, so it never queues behind a writer.
- `SELECT ... FOR UPDATE`, `FOR SHARE`, `UPDATE`, and `DELETE` are current reads. Candidate rows may first be found via snapshot/index, but after acquiring locks the current heap version must be re-read and WHERE re-evaluated, avoiding lost updates or modification of rows that no longer match after changes during the wait.

`START TRANSACTION` by itself does not fix the RR snapshot; the first consistent read does. `START TRANSACTION WITH CONSISTENT SNAPSHOT` is effective only under RR; other isolation levels produce a warning. Both READ ONLY and READ WRITE RR transactions establish the snapshot on the first consistent read; WITH CONSISTENT SNAPSHOT pins it at transaction start. `READ ONLY` is only an access mode: RC read-only transactions still refresh the snapshot per statement, and all DML returns 1792.

The absence of InnoDB-style gap/next-key locks in RR is an intentional concurrency
design choice. Such locks can block concurrent inserts into scanned index ranges,
increase lock contention, and introduce deadlock cycles involving range locks
and inserts. mydb instead combines MVCC transaction snapshots for stable
consistent reads with locks on actual heap rows and unique keys for writes and
constraint enforcement. This design avoids gap-lock overhead and the deadlock
patterns caused by those locks, favoring concurrency without locking index gaps.
Row-lock and unique-key-lock conflicts can still cause waits and deadlocks.

RR does not use row locks to prevent phantoms, nor does it automatically serialize the "read A, read B, then write different records" write-skew pattern. Workloads that depend on range invariants should use `SERIALIZABLE`, explicitly lock a summary row, or enforce the constraint as a UNIQUE/FK/CHECK constraint the database can atomically check.

Under `SERIALIZABLE`, table S and the IX needed for writes merge into SIX; requesting S from IX in the reverse direction also merges into SIX. The upgrade must re-check the other holders — S must not be treated as implicitly including IX. When two transactions first read the same table and then write different rows, the upgrades wait or go through deadlock/timeout handling; neither can bypass the other's S. See [Current Explicit Boundaries](#current-explicit-boundaries) for table-lock granularity and the boundaries around system tables and temporary tables.

## Lock Hierarchy and Compatibility

### Metadata Locks (MDL)

MDL protects `GLOBAL`, `SCHEMA`, `TABLE`, `FUNCTION`, `PROCEDURE`, `TRIGGER`, `EVENT`, and `COMMIT` objects. The main modes are:

- `SHARED_READ`: ordinary reads.
- `SHARED_WRITE`: allows ordinary concurrent DML.
- `SHARED_NO_WRITE`: reads allowed, writes blocked; used by `LOCK TABLES ... READ`.
- `SHARED_UPGRADABLE`: a shared mode that can be upgraded.
- `SHARED_NO_READ_WRITE`, `EXCLUSIVE`: block both reads and writes; used by DDL and explicit write table locks.
- `INTENTION_EXCLUSIVE`: global/hierarchical intention.

MDL tickets have three durations: `STATEMENT`, `TRANSACTION`, and `EXPLICIT`. Ordinary object MDL in an explicit transaction is retained until COMMIT/ROLLBACK; autocommit statements usually release it at end of statement; `LOCK TABLES` holds until `UNLOCK TABLES`, a new explicit transaction boundary, or connection close.

MDL uses its own ticket queues and does not enter the wait-for graph of the transaction data locks below; its waits are governed by `lock_wait_timeout`.

### Table-Level Data Locks

Transactional data locks support `IS`, `IX`, `S`, `SIX`, and `X`. DML/`FOR UPDATE` uses IX, `FOR SHARE` uses IS, and a plain `SERIALIZABLE` SELECT uses S. DDL mainly takes MDL X first and confirms in the storage layer that there are no active table data-lock holders.

Compatibility matrix (✓ means the modes can coexist):

| Held / Requested | IS | IX | S | SIX | X |
| --- | :---: | :---: | :---: | :---: | :---: |
| IS | ✓ | ✓ | ✓ | ✓ |  |
| IX | ✓ | ✓ |  |  |  |
| S | ✓ |  | ✓ |  |  |
| SIX | ✓ |  |  |  |  |
| X |  |  |  |  |  |

### Row Locks

A row resource is identified by `(storage/table id, RowId)` with modes S/X:

| Held / Requested | S | X |
| --- | :---: | :---: |
| S | ✓ |  |
| X |  |  |

Re-acquiring a held lock within the same transaction is idempotent. An S→X upgrade completes immediately only when the requester is the sole S owner; otherwise it must wait for all other S owners. The wait-for graph retains all blockers, never just the first owner.

### Unique-Key Locks

A unique-key resource is identified by `(table id, normalized index name, logical IndexKey)` and always uses mode X. It protects the unique-key reservation that has not yet become a stable index fact and is a distinct resource from the heap row lock; it is released when its holder commits, rolls back, or finishes XA. A reservation left behind by an already-finalized transaction can be cleaned up safely, while an `Active`/`Prepared` owner must be waited on.

### Explicit Table Locks and User Locks

`LOCK TABLES t READ` maps to explicit-duration MDL `SHARED_NO_WRITE`, `WRITE` maps to `EXCLUSIVE`, and an allow-list of tables/aliases accessible to the current session is established. It implicitly commits any existing transaction first; accessing an undeclared table inside the locked region returns 1100, and writing while holding a READ lock returns 1099. `UNLOCK TABLES` releases the explicit MDL and performs the necessary transaction boundary.

`GET_LOCK()`/`RELEASE_LOCK()` are separate in-process, connection-level named user locks: they allow reentrancy within the same connection, are released according to the reentrancy count, and are reclaimed when the connection closes. They are not part of MVCC, MDL, or the transactional data locks, are not released automatically by ordinary COMMIT/ROLLBACK, and do not participate in the transaction deadlock graph.

## Locking Reads, Wait Strategies, and Deadlocks

`SELECT ... FOR UPDATE` takes row X and `FOR SHARE` takes row S, with support for:

- Default wait: wait for conflicting locks to be released, subject to the session timeout and cancellation.
- `NOWAIT`: a single attempt; a conflict immediately returns 3572/HY000 and no long-lived wait is created.
- `SKIP LOCKED`: skips rows currently conflicting with other transactions and returns only the subset that was locked successfully. It suits task queues and does not provide a fully consistent result.

Deadlock detection covers transactional row, table, and unique-key locks.
A detected deadlock returns 1213/40001; retry the transaction. MDL has separate
wait handling.

Data-lock waits of ordinary SQL sessions are governed by `innodb_lock_wait_timeout`, default 50 seconds; the direct storage API falls back to `MYDB_LOCK_WAIT_MS`, default 30000 ms, when no session override is installed. MDL uses `lock_wait_timeout`, default 31536000 seconds. Cancellation/KILL is checked between short wait slices, so it never has to wait for the full timeout.

A 1213 deadlock rolls back and ends the current transaction. A 1205 lock timeout or 1317 query interruption rolls back the current statement in an explicit transaction; earlier successful statements remain pending. An autocommit statement failure ends its implicit transaction. A 3572 NOWAIT failure only rolls back the current statement and is not one of the transaction-terminating errors above.

## Savepoints, Statement Atomicity, and XA

A DML error in an explicit transaction rolls back that statement and releases
its newly acquired row and unique-key locks. Transaction-duration table
intention locks and MDL remain until transaction end. An autocommit statement
error rolls back its implicit transaction. Deadlocks end the transaction as
described above.

`ROLLBACK TO SAVEPOINT` undoes later data changes and later savepoints, but
ordinary row, unique-key, and table locks remain until transaction end.
Savepoints do not shorten the hold time of hot row locks. `RELEASE SAVEPOINT`
removes only the named savepoint; later savepoints remain valid.

`XA PREPARE` keeps row, unique-key, table, and transaction-duration metadata
locks until `XA COMMIT` or `XA ROLLBACK`. Prepared state and the required lock
set survive recovery. Required binlog intent is also retained; restarting with
binary logging disabled does not silently discard it.

## Undo Retention, Purge, and Space Reclamation

`mydb_undo_retention_ms=0` disables an extra time-travel window; it does not
disable undo. Active writes, prepared transactions, and consistent snapshots
still retain the versions required for rollback and visibility. Long-running
readers can delay cleanup and increase storage use.

Purge reclaims eligible old versions and large values only after they are no
longer needed. Segment compaction can reclaim partially used space. Eviction
from the in-memory undo cache does not itself remove retained history from disk.
An unavailable required version returns `MYDB_ER_SNAPSHOT_TOO_OLD`
(50043/HY000); corruption returns an explicit error rather than a current value
in place of an old one.

The commonly used parameters follow. Unless stated otherwise, `0` means no limit:

| Parameter | Default | Purpose |
| --- | ---: | --- |
| `mydb_undo_retention_ms` | 0 | Extra historical window retained beyond the normal safety horizon; also the `FOR SYSTEM_TIME` window |
| `mydb_undo_segment_bytes` | 64 MiB | Target size of a single undo segment |
| `mydb_undo_buffer_bytes` | 1 MiB | Frame buffer per write shard; 0 means write-through |
| `mydb_undo_purge_interval_ms` | 1000 | Background purge period |
| `mydb_undo_purge_batch_records` | 4096 | Maximum number of records cleaned per batch |
| `mydb_undo_mem_cache_limit` | 64 MiB | Cap of the resident payload cache; payloads are evicted in batches of one eighth of the cap, so the cap is soft by one batch; the metadata floor is non-evictable, cold payloads are read from disk |
| `mydb_undo_txn_max_bytes` | 0 | Logical undo cap per transaction; exceeding it returns 50021 |
| `mydb_undo_tablespace_max_bytes` | 0 | Shared disk pressure threshold |
| `mydb_undo_retention_guarantee` | 0 | When 1, the disk threshold is a retention guarantee; failed admission returns 50020 |
| `mydb_undo_disk_cap_softfail_warn` | 0 | Whether crossing the disk threshold issues a warning when the guarantee is off |

The resident undo catalog reserves allocation capacity with the unified memory governor. When the payload cache exceeds its limit, it evicts already-persisted before images first; when non-reclaimable memory needed by metadata and rollback cannot be admitted within the relevant operational or compensation hard budget, 1037/HY001 is returned. `MYDB_MEMORY_TARGET` sizes working sets and is not a hard allocation ceiling.

## Diagnostics and Troubleshooting

### Inspecting Transactions and Blocking Chains

```sql
SELECT trx_id, trx_state, trx_started, trx_wait_started,
       trx_requested_lock_id, trx_mysql_thread_id
FROM information_schema.innodb_trx
ORDER BY trx_started;

SELECT requesting_trx_id, requested_lock_id,
       blocking_trx_id, blocking_lock_id
FROM information_schema.innodb_lock_waits;

SELECT engine_transaction_id, thread_id, object_schema, object_name,
       index_name, lock_type, lock_mode, lock_status, lock_data
FROM performance_schema.data_locks
ORDER BY engine_transaction_id, object_schema, object_name;

SELECT *
FROM performance_schema.data_lock_waits;
```

`performance_schema.data_locks` reports `ENGINE='MYDB'`. Row locks appear as heap `RECORD` with `INDEX_NAME` NULL and `LOCK_DATA` as the physical `page:slot`; unique-key reservations report the corresponding index name. `ENGINE_TRANSACTION_ID` is the real storage-transaction identity, and the PFS producer then maps `THREAD_ID` through the session registry.

### Inspecting MDL

```sql
SELECT object_type, object_schema, object_name,
       lock_type, lock_duration, lock_status,
       owner_thread_id, source
FROM performance_schema.metadata_locks
WHERE lock_status IN ('GRANTED', 'PENDING')
ORDER BY object_type, object_schema, object_name, lock_status;
```

When a DDL waits for a long time, check transaction-duration MDL, explicit `LOCK TABLES`, and long-running transactions together, not just the row-lock tables.

### Inspecting Undo and Overall State

```sql
SELECT txn_id, table_id, record_count, UNDO_BYTES,
       chain_depth, oldest_created_unix_ms
FROM information_schema.MYDB_UNDO_HISTORY
ORDER BY record_count DESC;

SHOW ENGINE MYDB STATUS;
SHOW GLOBAL STATUS LIKE 'Innodb_row_lock%';
SHOW GLOBAL STATUS LIKE 'Table_locks%';
SHOW MYDB MEMORY;
```

Key items in `SHOW ENGINE MYDB STATUS`:

- `active_transactions`, `oldest_snapshot_scn`, `oldest_active_txn_ms`;
- `lock_waits`, `active_lock_waits`, `oldest_lock_wait_ms`, `deadlocks`, `lock_wait_histogram_ms`;
- `row_s_lock_acquires`, `row_x_lock_acquires`, `lock_upgrades`, `lock_upgrade_failures`, `table_lock_acquires`;
- `undo_disk_bytes`, `undo_resident_bytes`, `undo_evicted_records`, `undo_largest_txn_id`, `undo_largest_txn_bytes`, `undo_retention_pressure`, `undo_purge_*`;
- `snapshot_too_old_count`, `index_page_max_trx_shortcut_hits`, `index_page_max_trx_shortcut_misses`.

Setting `MYDB_LOCK_DIAGNOSTICS=1` at startup makes 1205 timeout logs include active transactions, lock counts, wait-for edges, and a row-FIFO summary. `MYDB_MAX_LOCKS=N` caps the instance-wide number of row + unique-key locks, default 0 (unlimited); exceeding it returns 1206, and table/MDL tickets do not count toward the cap.

## Common Concurrency Patterns

### Non-Blocking Consistent Read

```sql
-- session A
START TRANSACTION;
UPDATE account SET balance = balance - 10 WHERE id = 1;

-- session B (default RC)
SELECT balance FROM account WHERE id = 1;
-- Does not wait on A's X lock; reads the pre-update committed value via undo.
```

### Safely Claiming Queue Tasks

```sql
START TRANSACTION;
SELECT id, payload
FROM jobs
WHERE state = 'ready'
ORDER BY id
LIMIT 1
FOR UPDATE SKIP LOCKED;

UPDATE jobs SET state = 'running' WHERE id = ?;
COMMIT;
```

Multiple workers do not wait on each other, but any given SELECT may skip locked rows, so the application must not interpret the result as a global snapshot of the queue.

### RR Ranges Do Not Block Inserts

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
SELECT * FROM booking WHERE room_id = 7 AND day = '2026-09-01';
-- Repeated queries within this snapshot are stable, but no gap lock stops
-- another transaction from inserting a matching row.
```

When "no rows found" must itself exclude concurrent inserts, use `SERIALIZABLE` (accepting whole-table write blocking), lock a stable business mutex/summary row, or use a UNIQUE constraint so the race is arbitrated by the unique-key lock.

## Current Explicit Boundaries

- RR is snapshot isolation and does not aim for gap/next-key lock compatibility with InnoDB RR.
- Multi-statement `SERIALIZABLE` transactions use persistent user-table-level S/IX concurrency control, which is coarse-grained; system tables and session temporary tables do not take this S, and an autocommit single read-only statement does not establish a cross-statement table S either. S and IX merge automatically into SIX, and conflicts are checked against the merged mode.
- Row locks lock heap rows, not index records; there are no record+gap, insert-intention-gap, or similar modes.
- MDL, transaction data locks, `LOCK TABLES`, and `GET_LOCK` are distinct subsystems; their diagnostics and release boundaries must not be mixed.
- Undo retention is a time-window target, not an unlimited-history promise for arbitrarily slow queries; resource caps, purge that has already run, and the baseline created when retention is enabled from 0 can all make older targets fail explicitly.
- The lock tables and wait structures are in-process concurrency state; only the lock set needed by XA PREPARE is written to WAL and rebuilt during recovery. Ordinary crash recovery first processes incomplete transactions with WAL/undo and only then accepts connections.
