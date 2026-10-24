# MVCC and Locking

This document describes mydb transaction visibility and locking. For physical row
and index layouts, see [Table and B-tree Storage Format](table-and-btree-storage-format.md);
for historical reads, see [Time Travel Queries](mysql80-compatibility.md#time-travel-queries).

mydb combines heap current versions, transaction slots, commit SCNs, segmented undo, and pessimistic write locks to deliver transactional concurrency:

- A plain `SELECT` is an MVCC consistent read and never waits because another transaction holds a row lock; when the current heap version is invisible, the visible older version is reconstructed by walking the undo chain.
- `INSERT`, `UPDATE`, `DELETE`, and locking reads are current reads. They acquire the appropriate locks before writing or returning rows, and never keep operating on candidate rows that became stale while waiting.
- Row-level S/X locks protect specific heap rows, unique-key locks serialize uniqueness races, table-level intention locks coordinate DML, `SERIALIZABLE`, and DDL, and metadata locks (MDL) protect object definitions and explicit `LOCK TABLES`.
- The default isolation level is `READ COMMITTED`. `REPEATABLE READ` is snapshot isolation and explicitly does not implement InnoDB gap, next-key, or predicate locks; use the current table-level `SERIALIZABLE` implementation when concurrent range writes must be blocked.

## 1. Transaction Identity, States, and Commit Ordering

Every storage transaction has a monotonically increasing `txn_id`, and its transaction slot records one of the following states:

| State | Meaning | Visibility to other transactions |
| --- | --- | --- |
| `Active` | Currently executing | Invisible under RC/RR/SERIALIZABLE; RU sees the latest heap version |
| `Prepared` | XA PREPARE completed, awaiting XA COMMIT/ROLLBACK | Same as `Active`; still holds locks and undo |
| `Committed` | Commit result published with a `commit_scn` | Visible when `commit_scn <= snapshot_scn` |
| `RolledBack` | Rolled back | Invisible; physical content has been compensated by undo |

A `txn_id` only identifies the writer; it does not by itself express commit order. The actual visibility order is determined by `commit_scn`. Concurrent commits may allocate a larger SCN first and finish publishing a smaller SCN later, so mydb maintains a contiguous `commit_frontier`: an SCN can become the `snapshot_scn` of a new snapshot only when every commit from the earliest gap up to that SCN has been published. Snapshots must not read the SCN allocation counter directly; otherwise a row in a fixed snapshot could flip from invisible to visible while a commit is still being published.

A successful COMMIT return additionally requires the contiguous visibility watermark to cover this transaction's `commit_scn`. When a concurrent commit encounters an earlier publication gap, it first completes its own WAL persistence and lock release, then waits for the watermark to advance; commits that finish in normal order never wait. As a result, the next RC SELECT after a successful COMMIT sees that commit, while the stability of existing RC statement snapshots and RR transaction snapshots is preserved.

A compiled unique-key point lookup reads the physical index and collects historical key-repair candidates inside a single transaction-manager read-lock section, preventing missed reads caused by concurrent registration or rollback-driven cleanup of repair entries. When historical candidates exist, MVCC visibility is checked for each one and the index key of the reconstructed row is verified; the first physical candidate must not be taken blindly. When a unique key is reused by a new row, the original row visible to the older snapshot must still be returned.

The high-level commit sequence is:

```text
Generate undo / heap and index changes / WAL
        ↓
Append CommitTxn WAL record
        ↓
Publish transaction slot as Committed(commit_scn)
        ↓
Release row / unique-key / table data locks and advance the visibility frontier
        ↓
Complete the WAL durability barrier and post-commit maintenance
```

Locks can be released once the commit WAL has entered the ordered log and the transaction slot has been published; release does not wait across the group-commit fsync. Strict durability mode relies on the monotonic WAL LSN order for its guarantee: a transaction that commits after this commit has been observed cannot reach the durability watermark before it. If the post-publication durability barrier fails, the database enters MVCC fail-stop/quarantine instead of continuing to serve a published state whose durability cannot be proven.

Read-only transactions are still assigned a transaction identity so they participate in lifecycle management, but they write no `BeginTxn`/`EndTxn` WAL; an ordinary autocommit read-only statement can directly use a guard-protected read-only statement snapshot.

## 2. Row Versions and the Undo Chain

### 2.1 The Current Version Lives in the Heap

mydb is not a "separate version table per version" model. The heap row stores the current version, and the MVCC row v7 header is logically:

```text
row_version | flags | last_txn_id | undo_ptr | layout_id | current values
```

- `last_txn_id` is the transaction that last created or modified the current version.
- `undo_ptr` points to the undo record for the row's previous version; `0` means there is no predecessor.
- The deleted flag marks a logical delete. As long as an older snapshot may still read the pre-delete version, the slot cannot be physically reused.
- `layout_id` allows row layouts from before and after online/instant DDL to coexist and is orthogonal to MVCC visibility.

An ordinary INSERT currently keeps the full 18 B MVCC header even when `undo_ptr` is temporarily 0, reserving room for an in-place patch by the first UPDATE. Row migration changes only physical placement, never version determination; a `RowId` is `page_id:u32 + slot_id:u16`, and lock resources additionally carry the owning table/storage id so RowIds from different tables can never collide.

### 2.2 Undo Records

Every transaction owns a private linear undo stack, and the transaction slot stores only the head. Appenders perform "read old head → persist record → CAS-publish new head" under the transaction-private append latch, so parallel write workers of the same transaction cannot fork the undo chain, while different transactions can still append in parallel.

| undo kind | Stored content | Effect when rolling back to an older snapshot |
| --- | --- | --- |
| `Insert` | Location of the new row | Reaching it proves the target snapshot predates the INSERT; return "row does not exist" |
| `InsertBatch` | Locations of a batch of new rows | Same as `Insert`, and reduces undo overhead for bulk inserts |
| `Update` | Full before header/value | Replaces the current working version with the full old version |
| `UpdateDelta` | Changed column numbers and their before values | Reverse-overwrites only those columns; used for safe shapes such as fixed-width columns with an unchanged NULL bitmap |
| `Delete` | Full pre-delete header/value | Reconstructs the pre-delete row |

Large values are not copied into undo as multi-GiB payloads. The record stores the old LOB-chain descriptor; reading an old version materializes it on demand, and purge frees only the chains genuinely owned by that historical version. The descriptor carries an allocation generation to prevent ABA-style mistaken frees after page-number reuse.

In persistent mode undo lives in `system/undo/undo-<segment-id>.seg`, and records use CRC-protected append-only frames. There are multiple write shards by default and the current hot tail may be buffered; undo frames also enter the WAL so that crash recovery can roll back incomplete transactions. Memory mode retains the same logical undo semantics but creates no persistent segments.

### 2.3 Visible Version Lookup

For an ordinary current snapshot the decision order is:

1. `last_txn_id == reader.txn_id`: the reader sees its own writes; if the version carries the deleted flag, the result is still "row does not exist".
2. `READ UNCOMMITTED`: the latest heap version is visible for `Active`, `Prepared`, and `Committed`; `RolledBack` is invisible.
3. Other isolation levels: `Active`, `Prepared`, and `RolledBack` are invisible; `Committed` is visible only when `commit_scn <= snapshot_scn`.
4. When the current heap version is invisible, before images/deltas are applied in reverse starting from `undo_ptr` until a visible version is found, an INSERT boundary is reached, or it is confirmed that no earlier version exists.

Visibility first consults the lock-free transaction-state cache, then the transaction slots. Safely GC'd transaction slots sit below the contiguous `min_retained_txn_id` watermark: row versions produced by them can be treated directly as visible to all current snapshots. Historical `FOR SYSTEM_TIME` snapshots cannot use this simplification; when a transaction slot is missing they must query the persistent commit ledger and the historical baseline.

Eviction of an undo payload from the in-memory cache does not mean it has been purged: cold reads page it back in from disk by segment/offset. Only when the physical record has already been purged while an active snapshot still needs it does mydb return `MYDB_ER_SNAPSHOT_TOO_OLD` (50043/HY000); it never returns the current version masquerading as an old one. Chain cycles and missing required fields are reported explicitly as undo corruption. Valid chains have no fixed record-count limit: the shared walker uses constant-space Brent cycle detection. Rollback bounds transient compensation work in batches.

## 3. Snapshot Lifecycle and Isolation Levels

A transaction view records the transaction ID, snapshot boundary, isolation level, and read mode. A statement registers its snapshot synchronously when it selects the visibility boundary; when purge samples all snapshot shards it either sees the complete registration or completes horizon sampling first, so the "SCN chosen but not yet registered, then purged" window does not exist. The guard of a streaming cursor follows the cursor lifecycle and is deregistered when consumption completes or it is dropped.

| Isolation level | Plain consistent read | Current read/write | Key boundaries |
| --- | --- | --- | --- |
| `READ UNCOMMITTED` | Each statement reads the latest heap version and sees other transactions' uncommitted INSERT/UPDATE/DELETE | Locks and reads the latest current version | Dirty reads allowed |
| `READ COMMITTED` (default) | Each statement takes a freshly published frontier | Re-reads the latest committed version after locking | Non-repeatable reads and phantoms allowed within one transaction |
| `REPEATABLE READ` | The first plain consistent read fixes the transaction snapshot, which is reused afterwards | Does not use the fixed old snapshot; reads the latest committed version after locking | Snapshot results are stable, but there are no gap/next-key/predicate locks; range inserts and write skew are not prevented |
| `SERIALIZABLE` | Multi-statement transactions take transaction-duration table S on the persistent user tables involved, then run the current statement view | DML takes table IX and row X | Table S blocks later DML on the same table; an autocommit single read-only statement needs no cross-statement locks |

Two kinds of reads must be distinguished:

- A plain `SELECT` is a consistent read. It takes no row locks; when it meets another transaction's uncommitted version it follows undo back to the older committed version, so it never queues behind a writer.
- `SELECT ... FOR UPDATE`, `FOR SHARE`, `UPDATE`, and `DELETE` are current reads. Candidate rows may first be found via snapshot/index, but after acquiring locks the current heap version must be re-read and WHERE re-evaluated, avoiding lost updates or modification of rows that no longer match after changes during the wait.

`START TRANSACTION` by itself does not fix the RR snapshot; the first consistent read does. `START TRANSACTION WITH CONSISTENT SNAPSHOT` is effective only under RR; other isolation levels produce a warning. Both READ ONLY and READ WRITE RR transactions establish the snapshot on the first consistent read; WITH CONSISTENT SNAPSHOT pins it at transaction start. `READ ONLY` is only an access mode: RC read-only transactions still refresh the snapshot per statement, and all DML returns 1792.

RR does not use row locks to prevent phantoms, nor does it automatically serialize the "read A, read B, then write different records" write-skew pattern. Workloads that depend on range invariants should use `SERIALIZABLE`, explicitly lock a summary row, or enforce the constraint as a UNIQUE/FK/CHECK constraint the database can atomically check.

Under `SERIALIZABLE`, table S and the IX needed for writes merge into SIX; requesting S from IX in the reverse direction also merges into SIX. The upgrade must re-check the other holders — S must not be treated as implicitly including IX. When two transactions first read the same table and then write different rows, the upgrades wait or go through deadlock/timeout handling; neither can bypass the other's S. See Section 12 for table-lock granularity and the boundaries around system tables and temporary tables.

## 4. Index Access Still Defers to Heap MVCC

Primary-key and secondary BTREEs both store `IndexKey + RowId`; they are not clustered indexes. Index entries are only candidates: ordinary, covering, index-only, full-text, spatial, and vector paths must ultimately defer to heap MVCC visibility; vector indexes additionally re-rank with the exact vectors from the heap.

After an index key changes or a DELETE, older snapshots may still need the old-key-to-RowId mapping. Transaction-level index repair/history entries retain those candidates until both the active snapshots and the historical retention horizon have passed the corresponding commit; the current contiguous visibility frontier is also an upper bound for reclamation — even with no active readers, old keys needed by commits published beyond the gap cannot be reclaimed. Queries merge the current index with the required old keys, deduplicate by RowId, and then re-verify against heap/undo.

A BTREE leaf's `page_max_trx_id` is a conservative accelerator, not a visibility map and not the source of truth. Only when:

- the maximum writer txn id on the page is no greater than the contiguous `min_retained_txn_id`, or the page has never been written by a transaction; and
- no pending index repair exists

can a whole page be proven visible to all current snapshots. When it cannot be proven, heap headers are read row by row; disabling the short-circuit affects performance only, never results.

## 5. Write and Locking-Read Flow

The typical UPDATE/DELETE flow is:

```text
Acquire IX on the target logical table
        ↓
snapshot / index produces candidate RowIds
        ↓
Acquire X locks by RowId (wait on conflict)
        ↓
Re-read the current heap row, confirm writer state, re-evaluate WHERE
        ↓
Acquire the required unique-key / FK protection locks
        ↓
Append undo, modify heap, LOBs, indexes, and derived maintenance state, write WAL
        ↓
COMMIT publishes, or ROLLBACK applies undo in reverse
```

INSERT first takes table IX, then a transaction-duration exclusive reservation on the logical key of each unique index; after completing the heap insert it acquires the X lock on the new RowId and appends `Insert`/`InsertBatch` undo. The unique-key lock prevents two uncommitted INSERTs from both passing the uniqueness pre-check; the foreign-key path also locks the referenced parent key/row, preventing concurrent deletes from producing orphans.

Write locks, unique-key locks, and table data locks are held until transaction COMMIT/ROLLBACK by default. After the commit is published they are released together and waiters on the affected resources are woken; a rollback first compensates heap/index/LOB along the undo chain, then marks the transaction `RolledBack` and releases the locks. If compensation fails midway, the system does not hastily release locks or mark the slot as rolled back; it enters MVCC quarantine and waits for restart recovery to finish the work along the persistent undo.

## 6. Lock Hierarchy and Compatibility

### 6.1 Metadata Locks (MDL)

MDL protects `GLOBAL`, `SCHEMA`, `TABLE`, `FUNCTION`, `PROCEDURE`, `TRIGGER`, `EVENT`, and `COMMIT` objects. The main modes are:

- `SHARED_READ`: ordinary reads; an autocommit single-table read can take the ticket-free atomic fast path.
- `SHARED_WRITE`: allows ordinary concurrent DML.
- `SHARED_NO_WRITE`: reads allowed, writes blocked; used by `LOCK TABLES ... READ`.
- `SHARED_UPGRADABLE`: a shared mode that can be upgraded.
- `SHARED_NO_READ_WRITE`, `EXCLUSIVE`: block both reads and writes; used by DDL and explicit write table locks.
- `INTENTION_EXCLUSIVE`: global/hierarchical intention.

MDL tickets have three durations: `STATEMENT`, `TRANSACTION`, and `EXPLICIT`. Ordinary object MDL in an explicit transaction is retained until COMMIT/ROLLBACK; autocommit statements usually release it at end of statement; `LOCK TABLES` holds until `UNLOCK TABLES`, a new explicit transaction boundary, or connection close. Multi-object requests are sorted by object key and mode before being taken, reducing the risk of internal lock-order inversion; a waiting writer-like ticket keeps later compatible requests that could starve writers from jumping the queue.

MDL uses its own ticket queues and does not enter the wait-for graph of the transaction data locks below; its waits are governed by `lock_wait_timeout`.

### 6.2 Table-Level Data Locks

Transactional data locks support `IS`, `IX`, `S`, `SIX`, and `X`. DML/`FOR UPDATE` uses IX, `FOR SHARE` uses IS, and a plain `SERIALIZABLE` SELECT uses S. DDL mainly takes MDL X first and confirms in the storage layer that there are no active table data-lock holders.

Compatibility matrix (✓ means the modes can coexist):

| Held / Requested | IS | IX | S | SIX | X |
| --- | :---: | :---: | :---: | :---: | :---: |
| IS | ✓ | ✓ | ✓ | ✓ |  |
| IX | ✓ | ✓ |  |  |  |
| S | ✓ |  | ✓ |  |  |
| SIX | ✓ |  |  |  |  |
| X |  |  |  |  |  |

Table locks are sharded by table id, and the transaction side keeps a membership record of which tables this transaction holds, so commit/rollback walks only the actually-held set instead of scanning all tables. The common IX/IX acquisition never enters the global lock manager.

### 6.3 Row Locks

A row resource is identified by `(storage/table id, RowId)` with modes S/X:

| Held / Requested | S | X |
| --- | :---: | :---: |
| S | ✓ |  |
| X |  |  |

Re-acquiring a held lock within the same transaction is idempotent. An S→X upgrade completes immediately only when the requester is the sole S owner; otherwise it must wait for all other S owners. The wait-for graph retains all blockers, never just the first owner.

The row-lock table uses 64 shards by default (8 in test builds); uncontended acquisitions for different RowIds never pass through a single global lock. Each transaction keeps an ordered held-lock log used for transaction end and internal statement-savepoint cleanup. Bulk locking first acquires all uncontended rows within one shard/manager critical section, then waits for the remaining rows one by one.

### 6.4 Unique-Key Locks

A unique-key resource is identified by `(table id, normalized index name, logical IndexKey)` and always uses mode X. It protects the unique-key reservation that has not yet become a stable index fact and is a distinct resource from the heap row lock; it is released when its holder commits, rolls back, or finishes XA. A reservation left behind by an already-finalized transaction can be cleaned up safely, while an `Active`/`Prepared` owner must be waited on.

### 6.5 Explicit Table Locks and User Locks

`LOCK TABLES t READ` maps to explicit-duration MDL `SHARED_NO_WRITE`, `WRITE` maps to `EXCLUSIVE`, and an allow-list of tables/aliases accessible to the current session is established. It implicitly commits any existing transaction first; accessing an undeclared table inside the locked region returns 1100, and writing while holding a READ lock returns 1099. `UNLOCK TABLES` releases the explicit MDL and performs the necessary transaction boundary.

`GET_LOCK()`/`RELEASE_LOCK()` are separate in-process, connection-level named user locks: they allow reentrancy within the same connection, are released according to the reentrancy count, and are reclaimed when the connection closes. They are not part of MVCC, MDL, or the transactional data locks, are not released automatically by ordinary COMMIT/ROLLBACK, and do not participate in the transaction deadlock graph.

## 7. Locking Reads, Wait Strategies, and Deadlocks

`SELECT ... FOR UPDATE` takes row X and `FOR SHARE` takes row S, with support for:

- Default wait: join the per-row FIFO queue; on release the head of that row's queue is woken first, and a bounded 50 ms retry is the fallback for missed notifications and for re-running deadlock detection.
- `NOWAIT`: a single attempt; a conflict immediately returns 3572/HY000 and no long-lived wait is created.
- `SKIP LOCKED`: skips rows currently conflicting with other transactions and returns only the subset that was locked successfully. It suits task queues and does not provide a fully consistent result.

The transactional data locks (row/table/unique key) share one wait-for graph. Every real conflict writes a `waiter -> blockers[]` edge, and the full graph is traversed starting from the current waiter; if the walk returns to the waiter itself, the current request returns 1213/40001 and the error text asks the client to retry the transaction. Row queues follow FIFO: even if a resource has just become free, a non-head request cannot jump the queue; multiple compatible S waiters can acquire shared ownership one after another on later retries.

Data-lock waits of ordinary SQL sessions are governed by `innodb_lock_wait_timeout`, default 50 seconds; the direct storage API falls back to `MYDB_LOCK_WAIT_MS`, default 30000 ms, when no session override is installed. MDL uses `lock_wait_timeout`, default 31536000 seconds. Cancellation/KILL is checked between short wait slices, so it never has to wait for the full timeout.

A 1213 deadlock rolls back and ends the current transaction. A 1205 lock timeout or 1317 query interruption rolls back the current statement in an explicit transaction; earlier successful statements remain pending. An autocommit statement failure ends its implicit transaction. The internal `LockWouldBlock` marker used for async worker scheduling is not 1205: it only moves the statement to the blocking pool for replay, is not exposed to the client, and does not end an explicit transaction. A 3572 NOWAIT failure only rolls back the current statement and is not one of the transaction-terminating errors above.

## 8. Savepoints, Statement Atomicity, and XA

Every DML inside an explicit transaction creates an internal statement savepoint recording the undo head, row/global lock log lengths, index repair, binlog length, and statistics deltas. A statement error rolls back that statement's data changes and releases the row/unique-key locks newly logged by the statement; transaction-duration table intention locks and MDL remain until the transaction ends. An autocommit statement error rolls back the entire implicit transaction.

A user `SAVEPOINT` records the same set of data boundaries, but the public semantics of `ROLLBACK TO SAVEPOINT` follow InnoDB: the data changes and later savepoints after the savepoint are undone, while ordinary row/unique/table locks are still held until the transaction ends — it is not treated as an "unlock point" for concurrency control. Do not use savepoints to shorten the hold time of hot row locks.

`RELEASE SAVEPOINT` only removes the named savepoint; later savepoints remain valid, and the storage transaction and the session temporary-table journal follow the same rule. After compaction, savepoints confirm their boundaries via undo forwarding instead of comparing raw physical addresses. A rollback with no data changes does not flush the transaction WAL buffer early.

When the actual compensation completes, a `RollbackTo` WAL control record is written so recovery can skip already-compensated forward records. Savepoints record the boundaries of immediate WAL and buffered WAL separately, handling interleaving between the two and mid-flight flushes; otherwise, after a statement rollback releases its locks, crash recovery could overwrite data committed later by other transactions. Compensated old LOB/DELETE undo keeps record-level resource state, so purge does not re-free resources already returned to surviving rows. That state and the forwarding directory are persisted at checkpoint/compaction, and the compensation WAL uses the original record identity to support the replication side.

Physical compensation failures — statement, named savepoint, or whole transaction — all enter quarantine; user errors such as a nonexistent savepoint that changed no data return normally. For rows that were inserted and then deleted, idempotent cleanup of leftover index entries is allowed, but a genuine heap delete failure must propagate.

XA PREPARE persists the branch’s binlog intent, including table maps and row events, in bounded WAL-protected chunks when logging is required. The PREPARE record carries the chunk count, length and checksum. Recovery validates and restores that intent before COMMIT; restarting with binary logging disabled cannot silently waive it. The lock snapshot records effective modes, including S-to-X upgrades and shared batches.

XA PREPARE moves the transaction state to `Prepared`; the held row, unique-key, and table data locks plus transaction-duration MDL all remain in effect. The PREPARE WAL stores a lock snapshot; crash recovery rebuilds it and migrates it into the current sharded lock store until `XA COMMIT` or `XA ROLLBACK` finishes the branch.

## 9. Undo Retention, Purge, and Space Reclamation

`mydb_undo_retention_ms=0` only means that no additional time-travel window is provided; it does not mean "undo disabled". Rollback of active write transactions, active statement/RR snapshots, and the unclosed commit frontier still retain the required records. Purge eligibility considers:

- private undo chains reachable from all `Active`/`Prepared` transactions;
- the oldest registered snapshot SCN;
- the current contiguous visibility frontier, preventing mistaken deletion during a commit gap;
- the configured historical retention window and the commit ledger;
- creator transaction state and the verified-safe transaction-slot GC watermark.

Purge first plans a bounded victim batch under the shared undo latch, then briefly takes the write latch to publish the deletions. Once a deleted record is processed, the old LOB chains it owned are freed; only after DELETE undo is safely cleaned up does the heap tombstone move from deleted-pending to free, making the slot reusable. Fully dead segments are unlinked directly, while older segments still holding a few live records can be rewritten by build-then-swap compaction; old pointers are resolved through the forwarding map, and active transaction heads are atomically remapped under their private append latch.

The commonly used parameters follow. Unless stated otherwise, `0` means no limit:

| Parameter | Default | Purpose |
| --- | ---: | --- |
| `mydb_undo_retention_ms` | 0 | Extra historical window retained beyond the normal safety horizon; also the `FOR SYSTEM_TIME` window |
| `mydb_undo_segment_bytes` | 64 MiB | Target size of a single undo segment |
| `mydb_undo_buffer_bytes` | 1 MiB | Frame buffer per write shard; 0 means write-through |
| `mydb_undo_purge_interval_ms` | 1000 | Background purge period |
| `mydb_undo_purge_batch_records` | 4096 | Maximum number of records cleaned per batch |
| `mydb_undo_mem_cache_limit` | 64 MiB | Cap of the resident payload cache; the metadata floor is non-evictable, cold payloads are read from disk |
| `mydb_undo_txn_max_bytes` | 0 | Logical undo cap per transaction; exceeding it returns 50021 |
| `mydb_undo_tablespace_max_bytes` | 0 | Shared disk pressure threshold |
| `mydb_undo_retention_guarantee` | 0 | When 1, the disk threshold is a retention guarantee; failed admission returns 50020 |
| `mydb_undo_disk_cap_softfail_warn` | 0 | Whether crossing the disk threshold issues a warning when the guarantee is off |

The resident undo catalog reserves allocation capacity with the unified memory governor. When the payload cache exceeds its limit, it evicts already-persisted before images first; when non-reclaimable memory needed by metadata and rollback cannot be admitted within the relevant operational or compensation hard budget, 1037/HY001 is returned. `MYDB_MEMORY_TARGET` sizes working sets and is not a hard allocation ceiling.

## 10. Diagnostics and Troubleshooting

### 10.1 Inspecting Transactions and Blocking Chains

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

### 10.2 Inspecting MDL

```sql
SELECT object_type, object_schema, object_name,
       lock_type, lock_duration, lock_status,
       owner_thread_id, source
FROM performance_schema.metadata_locks
WHERE lock_status IN ('GRANTED', 'PENDING')
ORDER BY object_type, object_schema, object_name, lock_status;
```

When a DDL waits for a long time, check transaction-duration MDL, explicit `LOCK TABLES`, and long-running transactions together, not just the row-lock tables.

### 10.3 Inspecting Undo and Overall State

```sql
SELECT txn_id, table_id, record_count, bytes,
       chain_depth, oldest_created_unix_ms
FROM information_schema.mydb_undo_history
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

## 11. Common Concurrency Patterns

### 11.1 Non-Blocking Consistent Read

```sql
-- session A
START TRANSACTION;
UPDATE account SET balance = balance - 10 WHERE id = 1;

-- session B (default RC)
SELECT balance FROM account WHERE id = 1;
-- Does not wait on A's X lock; reads the pre-update committed value via undo.
```

### 11.2 Safely Claiming Queue Tasks

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

### 11.3 RR Ranges Do Not Block Inserts

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
SELECT * FROM booking WHERE room_id = 7 AND day = '2026-09-01';
-- Repeated queries within this snapshot are stable, but no gap lock stops
-- another transaction from inserting a matching row.
```

When "no rows found" must itself exclude concurrent inserts, use `SERIALIZABLE` (accepting whole-table write blocking), lock a stable business mutex/summary row, or use a UNIQUE constraint so the race is arbitrated by the unique-key lock.

## 12. Current Explicit Boundaries

- RR is snapshot isolation and does not aim for gap/next-key lock compatibility with InnoDB RR.
- Multi-statement `SERIALIZABLE` transactions use persistent user-table-level S/IX concurrency control, which is coarse-grained; system tables and session temporary tables do not take this S, and an autocommit single read-only statement does not establish a cross-statement table S either. S and IX merge automatically into SIX, and conflicts are checked against the merged mode.
- Row locks lock heap rows, not index records; there are no record+gap, insert-intention-gap, or similar modes.
- MDL, transaction data locks, `LOCK TABLES`, and `GET_LOCK` are distinct subsystems; their diagnostics and release boundaries must not be mixed.
- Undo retention is a time-window target, not an unlimited-history promise for arbitrarily slow queries; resource caps, purge that has already run, and the baseline created when retention is enabled from 0 can all make older targets fail explicitly.
- The lock tables and wait structures are in-process concurrency state; only the lock set needed by XA PREPARE is written to WAL and rebuilt during recovery. Ordinary crash recovery first processes incomplete transactions with WAL/undo and only then accepts connections.
