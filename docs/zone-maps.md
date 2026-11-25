# Automatic Zone Maps

Zone Maps summarize blocks of persistent user tables so eligible scans can skip
regions that cannot match a predicate. They are enabled by default and require
no CREATE INDEX statement. The benefit depends on data layout and selectivity;
a small table, randomly mixed values, or a broad predicate can still need a full
scan. B-trees remain useful for selective point lookups and ordered access.

## Query normally

This disposable example verifies query behavior; its few rows do not demonstrate
a pruning speedup:

```sql
DROP TABLE IF EXISTS test.zone_map_demo;
CREATE TABLE test.zone_map_demo(id INT PRIMARY KEY, region INT, amount INT);
INSERT INTO test.zone_map_demo VALUES (1, 1, 10), (2, 2, 20), (3, 2, 30);

EXPLAIN SELECT id, amount FROM test.zone_map_demo WHERE region = 2;
SELECT id, amount FROM test.zone_map_demo WHERE region = 2 ORDER BY id;
```

The result is `(2, 20)` and `(3, 30)`. Use EXPLAIN on representative data to
inspect the chosen path; the optimizer may prefer a B-tree or ordinary scan.
Typed min/max, NULL, string-prefix, and Exact/Bloom membership summaries can
reduce the candidate pages. Every surviving row still evaluates the original
SQL predicate. Bloom membership can admit extra candidates without admitting
incorrect final rows.

## Transactions and fallback

Serial, parallel and vectorized scans preserve MVCC visibility and
read-your-writes. A transaction's own INSERT/UPDATE must remain visible even
when an older persisted summary would exclude its new values. Snapshot reads
must likewise not lose older visible versions. The engine combines applicable
transaction changes with summaries or chooses a conservative scan.

After the setup above, this query must see the writer's updated region:

```sql
START TRANSACTION;
UPDATE test.zone_map_demo SET region = 2 WHERE id = 1;
SELECT id FROM test.zone_map_demo WHERE region = 2 ORDER BY id;
ROLLBACK;
```

The SELECT returns `1`, `2`, and `3`; after rollback the original membership
is restored. This result must hold for every supported execution mode.

Missing, stale, type-incompatible, untrusted, or inadmissible optional summaries
cannot authorize pruning. AS OF queries do not treat current summaries as
historical evidence. This fallback policy does not mean incompatible storage
formats or arbitrary physical corruption are ignored; storage admission and
recovery checks still apply.

## Maintenance and controls

Writes maintain summaries and bounded changes; background maintenance repairs
eligible zones. Work is incremental and budgets scratch memory and worker
capacity. A hot zone need not block maintenance of other zones. `ANALYZE TABLE`
can explicitly rebuild summaries while refreshing statistics, subject to its
locking and resource boundaries.

The node switch `mydb_zone_map_enabled` defaults to ON and supports GLOBAL and
persistence operations. `mydb_zone_map_maintenance` controls automatic
maintenance separately. Turning off the master switch preserves stored files
and subordinate settings; it does not drop summaries or change query results.
`mydb_buffer_pool_quota_zone_map_pct` defaults to `0`, meaning no special category
eviction preference. Zone Map pages still use the shared buffer pool; the quota
is not a separate memory reservation or hard cap.

Use `ZONE_MAP(table)` and `NO_ZONE_MAP(table)` hints only within their documented
semantic and conflict rules. See [hints](hints.md) and the full
[maintenance configuration](configuration-resources.md#automatic-zone-maps) for ranges,
startup names, quiet/retry periods, and rebuild thresholds.

## Verify and diagnose

```sql
SELECT @@GLOBAL.mydb_zone_map_enabled;
SELECT * FROM information_schema.MYDB_ZONE_MAP_STORAGE;
SELECT * FROM information_schema.MYDB_ZONE_MAPS;
SELECT METRIC_NAME, METRIC_VALUE FROM information_schema.MYDB_ENGINE_STATUS
WHERE METRIC_NAME LIKE 'zone_map_%';
```

`PENDING` and Unknown summaries can mean coverage is awaiting maintenance.
Inspect `MYDB_ZONE_MAP_STORAGE.LAST_ERROR` and the maintenance settings before
assuming a table has useful pruning. Metadata queries are diagnostic snapshots,
not catch-up barriers.

Compare identical ordered results with and without eligible pruning, then
compare EXPLAIN ANALYZE and counter deltas under equivalent cache conditions.
`zone_map_zones_skipped` and `zone_map_scan_pages_skipped` have different units.
Buffer-pool read counters measure database page reads, which can hit the OS
cache; they are not physical disk I/O counts or a direct measure of wall time.

Zone Map `.zm` files use shared 16 KiB pages and participate in WAL, checkpoints,
backups and native replication. Do not remove them manually. See
[storage and recovery](storage-format.md), [optimizer access paths](optimizer-and-operators.md),
and [resource diagnostics](configuration-resources.md#zone-map-maintenance-and-diagnostics-supplement).
