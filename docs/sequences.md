# Sequence Usage and Durability

Sequences are a mydb extension supporting signed SMALLINT, INTEGER, and BIGINT values. Definitions are stored in the internal `mysql.sequences` table, and recovery state is stored in `mysql.sequence_values`; no per-object `.seq` files are created. AUTO_INCREMENT uses separate counters and semantics.

## Creation and Usage

```sql
CREATE SEQUENCE order_ids AS BIGINT START 1 INCREMENT 1 CACHE 32 NO CYCLE;
SELECT nextval('order_ids');
SELECT currval('order_ids');
SELECT lastval();

CREATE TABLE orders (
    id BIGINT PRIMARY KEY DEFAULT (nextval('order_ids')),
    payload JSON
);
ALTER SEQUENCE order_ids OWNED BY orders.id;
INSERT INTO orders(payload) VALUES ('{"item":"book"}');

CREATE SEQUENCE descending_ids INCREMENT -7 START -1 CACHE 16;
SELECT nextval('descending_ids');
```

`nextval` and `setval` are not rolled back by transactions, savepoints, or XA. These functions can be used in ordinary expressions, DEFAULT expressions, views, stored functions, triggers, prepared statements, and LOAD DATA SET expressions. Execution involving sequences keeps session state on its owning thread and does not distribute value allocation to parallel workers.

`nextval` and `setval` require UPDATE privilege on the corresponding object; `currval` and `lastval` require SELECT or UPDATE. Read-only transactions, `super_read_only`, physical replicas, and stopped primary writes all prohibit advancing counters, including allocations from the session CACHE. Expression contexts that require side-effect-free evaluation, such as generated columns and CHECK constraints, cannot allocate sequence values.

`currval` is the last value used for that object in the current session; `lastval` refers to the object most recently used by nextval. Both fail explicitly if uninitialized. Recreating an object with the same name after DROP does not inherit its old session values. Session history is neither persisted nor replicated.

## Caching and Resets

CACHE defaults to 1. When CACHE is greater than 1, each connection preallocates a contiguous range; unused values are discarded when the connection closes. Values returned by different connections may interleave and are not guaranteed to increase in response order.

```sql
SELECT setval('order_ids', 1000, false);
SELECT nextval('order_ids');
ALTER SEQUENCE order_ids START WITH 2000;
ALTER SEQUENCE order_ids RESTART WITH 2000;
ALTER SEQUENCE order_ids OWNED BY NONE;
```

`setval(x, false)` makes the next fresh allocation start at x; true advances one step before the next allocation and updates currval in the calling session. Other sessions may continue using their existing CACHE allocations. setval does not change the object referenced by lastval; when it modifies that object, lastval follows the updated session currval.

START changes only the starting point for a future RESTART. RESTART and changes to generation settings are published atomically through a new counter generation. Sessions then discard their old caches of future values while retaining existing currval history. DDL follows mydb's implicit commit boundaries.

OWNED BY uses stable table identity and column IDs. Dropping the table or dependent column drops the sequence in the same catalog transaction; table rewrites and renames preserve the dependency. A permanent sequence cannot depend on a temporary table, and cross-database moves involving dependencies are explicitly rejected.

## Local Durability

In disk mode, the WAL reservation for a new window must be synchronized locally before any value in that window can be issued. Ordinary nextval calls do not update catalog rows; checkpoints merge state in batches and retain records that have not yet been materialized. Recovery orders records by generation and WAL LSN rather than taking the largest numeric value, so descending sequences, CYCLE, and backward setval calls recover correctly.

`MYDB_SEQUENCE_WAL_BATCH` controls the number of additional WAL lookahead steps, defaulting to 32 with a range of 1 to i64::MAX; its startup configuration key is `sequence_wal_batch`. It is independent of SQL CACHE. Increasing it reduces reservation synchronization frequency but widens the range of values that may be skipped after a failure. Very large CACHE ranges are calculated in constant time without iterating over individual values or allocating arrays.

Under a normal configuration without CYCLE, explicit resets, or direction changes, local restarts do not reissue successfully allocated values. Gaps can result from transaction rollback, failed requests, connection closure, caching, and crashes; gap-free numbering is not guaranteed. Memory mode provides no durability across restarts.

## Native Primary/Replica Synchronization

| Policy | Condition before a new window can be used | Failover boundary |
| --- | --- | --- |
| local, the default | WAL is durable on the primary | Planned switchover preserves reservations after catching up to the fence; forced promotion of a lagging replica may reuse values that were not replicated |
| remote_apply | WAL is durable locally, and the durable-applied watermark of every remote node in the fixed candidate set covers the reservation | Preserves reservations for successfully allocated values when using designated candidates, valid history, and external fencing |

If remote_apply cannot be satisfied, the operation fails explicitly rather than falling back to local. Existing semi-synchronous replication that allows fallback after a timeout does not provide this guarantee. Confirmation occurs for new windows and setval; allocating values within an already confirmed window requires no network round trip per call.

Initial setup can use `sequence_sync_policy` / `MYDB_SEQUENCE_SYNC_POLICY` and `sequence_sync_replicas` / `MYDB_SEQUENCE_SYNC_REPLICAS`; replicas is a comma-separated list of node UUIDs. After initialization, the catalog configuration is authoritative, and conflicting old environment settings cannot overwrite it. The initial configuration epoch is 1.

Use [CHANGE MYDB SEQUENCE SYNC TO](statements/replication/change-mydb-sequence-sync.md) to change an existing primary. Obtain actual node UUIDs from `SHOW MYDB REPLICA STATUS` or `SHOW MYDB REPLICAS`, and increment the known current configuration epoch by one:

```sql
STOP MASTER WRITES WAIT 30;
CHANGE MYDB SEQUENCE SYNC TO '{"remote_apply":true,"replicas":["11111111-1111-4111-8111-111111111111"],"epoch":2}';
RESUME MASTER WRITES;
```

Replace the example UUID with the actual remote node UUID. The member list must be sorted, contain no duplicates, and have at most 64 entries. The change requires stopping writes and draining existing writes. Writes may resume only after the new set has caught up with all old reservations and the configuration commit. On failure, the primary keeps writes stopped. Operations tooling must retain the configuration epoch and revoke and fence the promotion eligibility of nodes removed from the set.

Promotion in strict mode also requires the current configuration epoch confirmed by the operator:

```sql
PROMOTE TO MASTER FORCE FENCED SEQUENCE_SYNC_EPOCH=2;
```

For planned switchover, append the same parameter after the existing AT LSN, FENCE_TOKEN, and WAIT parameters. External fencing of the old primary remains necessary; mydb does not provide consensus leader election. Waiting for any N ACKs and then promoting an arbitrary replica does not provide this guarantee.

In a strict configuration with one primary and one replica, promoting the replica leaves no remote candidate and causes new reservation windows to be rejected. Establish and confirm a replacement replica, then update the candidate set under controlled conditions before continuing value allocation with the strong guarantee. Memory mode and writable standalone instances do not support remote_apply.

MySQL binlog does not replicate independent sequence runtime state. Use native WAL replication to synchronize sequences between primary and replica.

## Status, Backups, and Verification

```sql
SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, COUNTER_ID,
       ALLOCATED_VALUE, RESUME_VALUE, IS_CALLED, STATE_LSN,
       CHECKPOINTED_STATE_LSN, LOCAL_DURABLE_STATE_LSN,
       SYNC_POLICY, PENDING, SYNC_CONFIG_EPOCH
FROM information_schema.mydb_sequences;

SHOW GLOBAL STATUS LIKE 'Mydb_sequence%';
SHOW SESSION STATUS LIKE 'Mydb_sequence%';
```

ALLOCATED_VALUE includes unused values allocated to session CACHE ranges; RESUME_VALUE is a safe recovery position and may be ahead of it. A replica cannot know the last value actually returned by the primary, so ALLOCATED_VALUE is NULL there. Direct access to the raw DD tables is prohibited.

Full backups, incremental recovery, WAL archiving, and LSN- and time-based recovery include the state table and dedicated records. Reserved windows may be ahead of the values actually used at the target time, so recovery may leave gaps. Recovery to earlier history or cloning does not guarantee freedom from conflicts with all IDs already consumed by external systems.

The required catalog and WAL versions are listed in the generated [storage format registry](storage-format-versions.md). Old data directories, old backups, and nodes with mixed formats are explicitly rejected without automatic migration.
