# mydb Native Physical Replication

Native replication is a mydb→mydb WAL stream independent of the MySQL binlog. Protocol v2 only connects nodes whose physical formats match exactly and distinguishes four progress watermarks:

- `received_lsn`: network frames have passed envelope/WAL CRC and range validation;
- `durable_lsn`: frames have entered the replica's durable WAL;
- `applied_lsn`: frames have been projected into the online Database;
- `durable_applied_lsn`: satisfies both durable and applied — the only watermark allowed for semi-sync ACKs, restartpoints, and planned promotion.

The local WAL tail cannot be inferred as applied within the process. After an apply failure, reconnection requests data from the last `durable_applied_lsn + 1`; a Database still alive within the same process skips the prefix it has already applied successfully, preventing a repeated BEGIN from temporarily overriding the visibility of already-committed transactions. This never raises the durable-applied or ACK watermarks. After a process restart, only a real redo of the local WAL can correct the throttle-written progress metadata.

## Configuration

The master requires at least `MYDB_ROLE=master`; a new replica is configured with `MYDB_ROLE=replica`, `MYDB_REPL_MASTER_HOST`, and `MYDB_REPL_MASTER_PORT`. Existing data directories should use `MYDB_ROLE=auto` instead, with roles changed only through administrative SQL; startup fails when the explicit environment role conflicts with the persisted role. `FORCE_RESYNC` uses the persisted role for its controlled restart, overriding a stale initial role in the original environment. Ordinary startup still rejects conflicting explicit roles.

The replication listener binds to `127.0.0.1:3316` by default. TLS configuration:

```text
MYDB_REPL_TLS_MODE=auto|disabled|verify_identity
MYDB_REPL_TLS_CA=/path/ca.pem
MYDB_REPL_TLS_CERT=/path/node.pem
MYDB_REPL_TLS_KEY=/path/node-key.pem
MYDB_REPL_TLS_SERVER_NAME=replica-source.example.com   # optional
```

`auto` allows plaintext only on pure loopback with no certificates configured; any non-loopback bind/source requires a fully configured CA, certificate, and key and performs mutual TLS with hostname/IP SAN verification. `disabled` cannot bypass the non-loopback safety gate either.

Capacity settings: `MYDB_REPL_MAX_CONNECTIONS=16`, `MYDB_REPL_MAX_BASE_BACKUPS=1`, `MYDB_REPL_APPLY_QUEUE_BYTES=16777216` (16 MiB), `MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT=10`. These environment settings use decimal integers, not size suffixes. Chunk, in-flight, TLS, and base-backup buffers are accounted under `NativeReplicationTransient` and subject to unified memory admission; `MYDB_MEMORY_TARGET` is an advisory sizing target, not their hard cap. The effective minimum of `MYDB_REPL_SEMI_SYNC_TIMEOUT_MS` is 1 ms (a configured 0 is clamped to 1 ms).

## Base Backup

A new replica, or a replica carrying the `resync_required` marker, performs a base backup before the Database opens. During capture the master freezes new writers, drains existing writers, runs a sharp checkpoint, and captures private copies of the manifest files; once capture completes it resumes writes and then transfers the backup. Capture also fences background purge, undo compaction, and checkpoints. The source needs temporary disk space for the private copies; the write pause scales with local copying, not network transfer. The backup includes heap/LOB/sequence/auto-increment, the physical allocation baseline of segmented undo, the temporal ledger/history, the data dictionary, and paginated Zone Maps. User B-trees and HNSW/IVF candidate files (`.hnsw`/`.hvec`) are rebuilt by the replica from its committed heap; data-dictionary B-trees are retained. Vector accelerators are revoked when replay changes their coverage, and queries can fall back to exact search. See [vector storage and recovery](vector-indexes.md#storage-and-recovery).

Zone Map v6 uses 16 KiB standard pages, and the current physical page version is 3 (compatibility identifier `zone=6/3`). The `.zm` baseline must be transferred with the backup; subsequent increments are maintained by replaying physical FPIs and standard-page PageDeltas (WAL v28). A PageDelta uses the full baseline page as its LZ4 dictionary and must match the baseline LSN and whole-page checksum, as well as the decoded 16 KiB length, new LSN, and checksum; a missing or corrupted baseline raises an explicit error instead of being padded with zero pages. The first modification, or a modification that cannot be proven to retain the full baseline, still records an FPI; recovery repairs torn pages with FPIs first and then applies the deltas in order. The capturing side flushes dirty data pages clean once more inside the summary write gate and then makes private copies of every streamed mutable file, including `.zm`, heaps, undo, and catalog files. This avoids a post-checkpoint rebuild producing a mixed baseline where the new Ready root is already on disk while the leaves are still old, and prevents in-place updates during the transfer from changing the captured summary. The WAL start point of the base backup remains the `backup_lsn + 1` taken after the initial checkpoint; the extra flush does not advance this start point.

A physical replica does not rebuild or allocate Zone Map pages locally; when a summary is missing, untrusted, or corrupted it conservatively scans the heap and waits for the source to replay valid pages. Both queries and cost sampling compare a zone's `rebuild_scn` against the safe-SCN lower bound of registered read views: a rebuild summary newer than that bound must not exclude the corresponding zone, so rows that an older RR snapshot on the replica still needs to read are not missed after a rebuild shrinks on the source. Final row visibility is still decided by MVCC.

Every file must strictly match its manifest path, contiguous offsets, declared length, EOF, and SHA-256. The replica writes into a staging tree on the same filesystem and replaces the data through a persistent install journal after `fsync` completes; a network interruption, verification failure, or insufficient disk never installs an incomplete backup. Data and replication progress commit together: a crash before the install-complete marker is persisted rolls back to the old directory, while a crash afterwards keeps the new directory and continues cleaning up the old one. Role/TLS/node-local configuration is never overridden from the master.

When the master is temporarily unreachable, bootstrap keeps the process alive and retries with exponential backoff (the initial interval is set by `MYDB_REPL_RECONNECT_MS`, clamped to 100 ms–30 s, at most 30 s afterwards). It does not listen for SQL until seeding completes. The backup concurrency permit covers the entire transfer, and the backup freeze is independent of STOP/RESUME MASTER WRITES.

## Status and Administration

```sql
SHOW MYDB MASTER STATUS;
SHOW MYDB REPLICA STATUS;
SHOW MYDB REPLICAS;
CHANGE MYDB MASTER TO HOST='127.0.0.1', PORT=3316;
START MYDB REPLICATION;
STOP MYDB REPLICATION;
RESET MYDB REPLICATION;
```

`SHOW MYDB REPLICAS` separates connection state from WAL retention state: a disconnected slot may still retain WAL but immediately leaves the semi-sync quorum. `Replica_Acked_LSN` is the per-slot watermark maintained by the master; MASTER STATUS additionally lists the number of invalidated and abandoned slots. An invalidated slot regains WAL retention protection at an approved new backup or a valid resume boundary; an ordinary ACK does not unconditionally revive an invalidated slot. An ACK must satisfy `applied ≤ durable ≤ received ≤ highest_sent ≤ source durable/current`, and an ACK that runs ahead causes a disconnect.

After RESET, a node can re-enter replica mode via CHANGE MYDB MASTER TO. Entering replica mode or changing the source always requires re-seeding, preventing locally diverged data from being stitched in. Trigger it explicitly with `START MYDB REPLICATION FORCE_RESYNC`: it requires `SYSTEM_VARIABLES_ADMIN` and `SHUTDOWN`, reuses the RESTART drain/restart flow, disconnects clients, and automatically resumes replication once the backup is installed. Platforms that do not support RESTART return an explicit error; seeding can still be completed by a manual restart.

### Old Read Snapshots and Physical Reclamation

An old RR transaction or a statement still executing on the replica may need rows the master has already deleted. When the master sends the physical slot reclamation `SlotReclaim`, the replica keeps the page and the undo access entry and defers replay of that record if the delete is still invisible to the replica's oldest registered snapshot. `Last_Status` in `SHOW MYDB REPLICA STATUS` reports `physical replication replay blocked by active snapshot` together with the snapshot SCN, the reclamation LSN, and the page-slot location.

The failed record does not advance `Applied_LSN` or `Durable_Applied_LSN`; the received and durable-WAL watermarks may run ahead. The receiver retries automatically with the existing reconnection backoff and catches up once the old transactions finish and the statements release their read views; no manual replication restart is needed. Replication lag may grow in the meantime, so long-running transactions should COMMIT or ROLLBACK promptly.

This mechanism does not report replica snapshots back to the master and does not change the master's undo/WAL retention policy; do not assume from it that history is retained indefinitely. Reclaimed WAL, invalidated slots, and divergent histories are still handled by the existing resume/re-seed rules.

## Planned Switchover

1. The old master runs `STOP MASTER WRITES WAIT 30` and obtains `Fence_LSN`, `Timeline_ID`, and `Fence_Token`. The freeze is persisted, so the old master remains non-writable after a restart.
2. The replica runs:

   ```sql
   PROMOTE TO MASTER AT LSN <Fence_LSN>
     FENCE_TOKEN='<Fence_Token>' WAIT 30;
   ```

   The replica requires a recent heartbeat proving the source is still frozen and that the token hash and LSN match, and it must have caught up to the durable-applied watermark; it then stops the receiver, rolls back unfinished source transactions, checkpoints, and opens writes only after creating a new timeline with an explicit parent/fork.
   If sequence synchronization uses `remote_apply`, append the confirmed current
   `SEQUENCE_SYNC_EPOCH=<epoch>` after WAIT. Omitting it or supplying another
   epoch blocks promotion. Record the persisted epoch during preparation; see
   [sequence synchronization administration](statements/replication/change-mydb-sequence-sync.md).
3. The old master runs `DEMOTE TO REPLICA WITH MASTER='new-primary:3316'`. If the old timeline is a parent of the new master and has not passed the fork LSN, streaming resumes directly; otherwise a divergence/resync is returned and WAL is never stitched together.

Unplanned promotion only permits `PROMOTE TO MASTER FORCE FENCED`, with `SEQUENCE_SYNC_EPOCH=<epoch>` appended when strict sequence synchronization requires it. This command asserts that the operator has isolated the old master through external systems; the promotion point is the last `durable_applied_lsn`, and data after that point may be lost. mydb provides no automatic master election or consensus and cannot prevent split-brain without external fencing.

Before promotion, complete replicated undo frames beyond missing source physical allocation prefixes are persisted at local offsets with durable source-ID aliases. Unlogged allocations lost in a source crash therefore cannot permanently block failover. Transaction undo chains are preserved, and missing logical before-images still fail rollback and prevent promotion.

Native promotion does not yet guarantee GTID or file-position continuity for existing MySQL binlog downstream consumers; the downstream must be reconfigured.

### Connection Recovery and Wait Cancellation

Connection failures without progress (including an already-connected identical node UUID) back off exponentially from the configured initial reconnect interval, up to 60 seconds; the backoff resets once the persisted apply position advances. An identical UUID does not preempt an existing connection, preventing mistakenly started clones from kicking each other offline; stop the surplus instance or reinitialize the clone's replication identity. The master closes a connection and releases its online membership when no complete frame has arrived for more than `max(6 × heartbeat, 30s)`.

If opening the master-side WAL hits a transient IO error, the connection is dropped and reconnection is allowed without generating a permanent resync marker; a reclaimed position or divergent history still requires a rebuild. Frame reading preserves progress across heartbeats and new-WAL notifications, so brief fragmentation does not break protocol parsing.

`PROMOTE … WAIT` and `STOP MASTER WRITES … WAIT` support cancellation via `KILL QUERY`. Canceling STOP MASTER WRITES does not automatically unfreeze the master, preserving the already-persisted fencing state. `RESET MYDB REPLICATION` also clears the storage engine's physical-replica flag, allowing the event scheduler to resume execution.

## Logical downstreams and read catch-up

A physical replica cannot serve a MySQL binlog downstream. Physical WAL replay
does not regenerate logical binlog events, including when `log_replica_updates`
is enabled. `COM_BINLOG_DUMP` and `COM_BINLOG_DUMP_GTID` return error 1235 on such
a node instead of streaming an incomplete history. Use a source that generates
logical binlog events; native physical replication remains the supported path
for replicating the received physical history.

Physical replication is asynchronous. Heartbeat and chunk publication intervals
can delay status/read observation after a source commit; a fixed 100 ms sleep
is not a consistency barrier. For verification, record the desired source LSN,
wait for the replica's `Durable_Applied_LSN` to reach it, and start a new read
statement/transaction. Existing repeatable-read snapshots retain their original
view. Planned promotion already enforces the explicit fence-LSN barrier.

## Sequence reservations

Sequence definitions, catalog recovery states and compact nontransactional reservations all travel in
the native WAL stream. Replicas apply the runtime state online, pin unmaterialized reservations at
restartpoints and reconstruct safe counters before promotion opens writes. Nodes must match the
current catalog and WAL versions in the [storage format registry](storage-format-versions.md).
No `.seq` files are involved.

`remote_apply` sequences require every explicitly configured remote candidate to acknowledge a
durable-applied reservation before its window becomes usable. Timeout never degrades this policy.
Strict promotion additionally requires the operator's current `SEQUENCE_SYNC_EPOCH=<n>`; candidate
changes require STOP MASTER WRITES, a new persisted configuration epoch and full catch-up. This
policy is independent of best-effort semi-sync. See [Sequences](sequences.md) for operational SQL,
replacement replicas and guarantee conditions.
