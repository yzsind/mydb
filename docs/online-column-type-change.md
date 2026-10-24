# Online Column Type Change

When changing a column type requires rewriting data, mydb by default uses a consistency snapshot, shadow storage, and incremental catch-up, applying the tail delta and publishing the new storage only inside a short barrier. The SQL statement still returns only after the ALTER succeeds or fails.

## Supported Scope

- Strict SQL mode and disk-based storage (Strict / Relaxed). No dependency on `MYDB_DDL_ASYNC`.
- `MODIFY COLUMN` and `CHANGE COLUMN`, multiple columns changed in one statement, `FIRST` / `AFTER`, and character set conversion.
- Regular tables, tables without a primary key, partitioned tables, and subpartitioned storage; partitions are re-routed using the converted values.
- Regular/unique BTREE indexes, HNSW, and IVF_SQ8; both the in-memory and paged backends of vector indexes are supported.
- Out-of-row data such as TEXT/BLOB is included. Conversion, generated columns, NOT NULL, CHECK, and index key computation reuse the existing general-purpose implementations; conversions or dependencies that were already unsupported still raise errors.

`LOCK=DEFAULT` prefers the online path, and `LOCK=NONE` requires reads and writes to be allowed during the build; the final publication still needs a brief exclusive barrier. `LOCK=SHARED` keeps the semantics of blocking writes during the build, and `LOCK=EXCLUSIVE` uses the offline path. Supported physical column changes allow `ALGORITHM=COPY, LOCK=NONE`, which is a mydb extension.

Non-strict mode, Memory durability, FULLTEXT/SPATIAL indexes, and ALTER action combinations not yet covered by the unified candidate schema retain their explicitly supported SHARED/EXCLUSIVE paths; unsupported `LOCK=NONE` combinations return 1846. `MYDB_DDL_COPY_STREAMING=0` is a differential entry point only in test builds; production builds always use shadow COPY.

## Execution and Consistency

### Validation without a storage rewrite

In strict SQL mode, encoding-compatible NOT NULL changes, DECIMAL precision
reductions within the same physical width/scale, numeric signed-to-unsigned domain
narrowing without an encoding change, and VARBINARY/TEXT/BLOB limit
reductions use snapshot validation and incremental catch-up. `LOCK=DEFAULT` and
`LOCK=NONE` permit writes during validation; prepare and publish use brief
writer barriers that continue to allow readers. `LOCK=SHARED/EXCLUSIVE` retains
the requested write exclusion. The storage generation stays unchanged, and the
base scan projects only the constrained columns.

Concurrent writes continue to follow the published definition. If a committed
row still violates the candidate at certification, the ALTER fails and keeps
that business commit and the original definition. Old snapshots retain their
historical NULL facts. VARCHAR shrinking still uses COPY because shrinking may
legally trim trailing spaces; fixed-width, scale, charset, and other physical
encoding changes continue through the rewrite path. FLOAT/DOUBLE scale changes
and M reductions use COPY so that rounding and range checks affect stored values
and index keys together; a pure M widening with unchanged scale remains INSTANT.

Supported compound rewrites bind later column changes and functional indexes
to the evolving candidate, rather than the original table. For example:

```sql
ALTER TABLE t ADD c VARCHAR(20) DEFAULT '7',
  MODIFY c BIGINT, ADD INDEX ci((c+1)), LOCK=NONE;
```

The candidate is validated and published once. Unsupported action mixtures
continue to return an explicit error before any mutation.

### Physical rewrites

1. An initial exclusive MDL barrier drains existing access, registers change capture, and pins the source schema and MVCC snapshot; once table-lock protection is in place, the barrier is released.
2. Snapshot scanning and type conversion run during the phase where concurrent reads and writes are allowed. Regular tables keep streaming, parallel conversion and batched index builds; LOBs use controlled page-by-page materialization. Partitioned tables route shadow data by the new definition and then batch-build each physical index.
3. The change log records `(txn_id, storage_id, RowId)`. The log is rotated after reading a consistency watermark, and only transactions visible at that watermark are processed; uncommitted transactions are kept for the next round and rolled-back transactions are discarded. Savepoint rollbacks are handled naturally through the final visible row versions. Log processing does not hold the capture mutex for long periods.
4. The source snapshot is retained throughout, preventing version reclamation and source RowId reuse. Tables without a primary key likewise maintain shadow rows through physical identity mapping. Each replay batch deletes old keys before inserting new ones, avoiding transient conflicts from unique-key permutation. Tail mapping space is reserved before the final barrier and flushed early when necessary, so the entire mapping never spills inside the critical section.
5. Catch-up continues with early data flushes. The final barrier accepts at most 1024 log records; if the backlog is too large, the barrier is released to keep catching up, for at most 32 rounds, failing with an error when it cannot converge. Tail conversion data is capped at 4 MiB, and the replay time budget is controlled by `MYDB_DDL_COPY_MAX_FINAL_MS`, defaulting to 1000 ms with a maximum of 60000 ms. The budget is not a hard real-time guarantee for operating-system I/O.
6. The new heap, BTREE, LOB, and vector files are published as one storage generation; a partitioned table atomically replaces its entire leaf storage set. Inside the final lock there is no rescanning, no full-table rewrite, and no vector graph rebuild. Old vector graph release and old-generation file deletion for regular tables are deferred until after the barrier is released.

The source table still enforces the old schema's constraints. If a concurrent transaction commits data that cannot be converted to the target type, the ALTER fails while the source table and the business commits remain valid. A column definition must not break existing constraints such as foreign key or generated-column dependencies.

## Resources and Recovery

- When row-mapping memory reaches 8 MiB, or the global memory controller refuses further growth, it switches to a sparse on-disk mapping; the temporary file is unlinked immediately after creation, so a process exit leaves no file behind.
- The incremental log is cached in small batches, flushed to disk automatically, with a per-round queue cap of 64 MiB. Capture failure fails the DDL, never business writes. The replay working set is governed by memory control.
- Regular tables reuse the ALTER COPY intent and catalog transaction; partitioned tables reuse the partition-reorganization journal. Before commit the source table is restored; after commit the new generation is kept and cleanup completes. Recovery decisions consider both the generation and the schema version, so a stale intent left by delayed cleanup cannot misclassify later metadata changes.
- The catalog commit and the logical DDL binlog event use the same transaction; shadow data produces no duplicate business row events.
- The new generation stores the publishing transaction ID. An old read view accessing the new generation returns 1412, avoiding empty results or mixed versions.
- Vector cache keys include the storage generation; background compaction and persistence never write an old graph into the new generation's path.

## Observability

Progress: `information_schema.mydb_ddl_jobs`.

BTREE index jobs expose their index subjects and support `CANCEL DDL JOB`.
Column-validation and constraint jobs have distinct object kinds. A bounded
history retains terminal results in memory; a completed job can remain visible
with `CANCEL_ALLOWED=0`. An acknowledged cancellation before the finalization
gate prevents publication, including when the final replay batch is empty.

`SHOW ENGINE MYDB STATUS` adds:

- `ddl_copy_barrier_wait_us`: cumulative time spent waiting in the final barrier.
- `ddl_copy_lock_hold_us` / `ddl_copy_lock_hold_max_us`: cumulative/maximum time of the publish critical section.
- `ddl_copy_final_tail_rows`: number of tail records accepted in the final barrier.

The existing `ddl_copy_*` counters for scanning, conversion, indexing, replay, and building remain available. All metrics are process-lifetime cumulative values; for single-run comparisons use before/after deltas and avoid interference from other DDL.
