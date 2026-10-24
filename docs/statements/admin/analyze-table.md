# `ANALYZE TABLE`

## Description

Samples table statistics, or creates/drops column histograms.

## Syntax

```ebnf
<analyze-table> ::= 'ANALYZE TABLE' <table-list>
                    | 'ANALYZE TABLE' <qualified-name>
                      ( 'UPDATE HISTOGRAM ON' <identifier-list>
                        [ 'WITH' <unsigned-integer> 'BUCKETS' ]
                      | 'DROP HISTOGRAM ON' <identifier-list> )
```

## Behavior, Privileges, and Limitations

- The plain form re-samples each table and updates optimizer statistics; the histogram form allows only one table at a time.
- Background automatic statistics checks run every 60 seconds by default; the check interval can be adjusted
  via the environment variable `MYDB_AUTO_ANALYZE_INTERVAL_S`
  or at runtime with `SET GLOBAL mydb_auto_analyze_interval_s = 60` (minimum 1 second).
  The check interval does not include statistics collection time; whether collection actually runs still
  depends on trigger conditions such as the amount of table modification.
- Column value frequencies and histogram buckets are stored independently: `buckets=[]` does not mean there
  are no value frequencies. Low-cardinality columns are fully counted within the sampled pages; a full scan
  yields exact frequencies, while page sampling still carries sampling error. Equality predicates use value
  frequencies directly and do not depend on histogram buckets. For statistics sources and estimation rules,
  see [Optimizer](../../optimizer-and-operators.md#42-common-estimation-rules).
- Bucket counts and column types are validated by the statistics module; after the catalog is updated, related plan caches are invalidated.
- Returns a per-table status result; no user rows are modified. During collection, a `SHARED_UPGRADABLE` metadata lock is held, allowing ordinary SELECT and INSERT/UPDATE/DELETE on the same table; conflicting DDL and other explicit ANALYZE/histogram maintenance on the same table will wait. A queued exclusive DDL may still block subsequent requests according to MDL scheduling rules.
- Statistics collection uses an MVCC snapshot protected from purge; heap scans release internal read locks in bounded page batches. Collection does not hold the catalog write lock throughout; it briefly acquires it only at publication time and validates the table definition.
- Modifications made during collection are preserved. Before choosing the snapshot, a brief shared data lock is attempted non-blockingly to align the row-count delta, then released immediately; if alignment is impossible because a transaction is actively writing or finishing its commit, the original row-count baseline and committed deltas are kept while the other snapshot statistics are updated. The row count remains an estimate; re-running ANALYZE once the table is quiet will calibrate it.
- Background/implicit statistics skip a table that is currently being analyzed, so queries do not indirectly wait on maintenance. Explicit ANALYZE also maintains the zone map; when changes happen concurrently, a conservative merge is used to guarantee that filtered queries miss no rows.

## Examples

```sql
ANALYZE TABLE orders, customer;
ANALYZE TABLE orders UPDATE HISTOGRAM ON customer_id WITH 128 BUCKETS;
ANALYZE TABLE orders DROP HISTOGRAM ON customer_id;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
