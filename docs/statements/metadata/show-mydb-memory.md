# `SHOW MYDB MEMORY`

## Description and Syntax

Shows host, process, buffer pool, storage, executor, cache, and session retained memory.

```sql
SHOW MYDB MEMORY;
SHOW MYDB MEMORY\G
```

The global summary requires the PROCESS privilege; insufficient privileges return 1227. Regular accounts can use
`SHOW MYDB MEMORY SESSIONS` to view their own connection. Reading statistics does not evict caches.

## Columns and Statistics Semantics

| Column | Meaning |
| --- | --- |
| CATEGORY / COMPONENT | Functional category and component name |
| CURRENT_BYTES | Current accounted amount, estimate, or OS observation; `rss_minus_tracked` is a signed difference |
| PEAK_BYTES | Historical peak; NULL when not collected, and per-component peaks must not be summed as a simultaneous peak |
| LIMIT_BYTES / USED_PCT | The budget applicable to this row and the current amount/budget percentage; NULL when no budget applies |
| HITS / MISSES / HIT_PCT | Cumulative cache hits, misses, and hit rate; the hit rate is NULL when there are no samples |
| ENTRIES | Number of cache entries, pages, or active work area handles, depending on the component |
| EVICTIONS | Actual eviction count; NULL when not collected, never replaced by fallback counts |
| ACCOUNTING | The measurement method listed in the table below |
| IN_TRACKED_TOTAL | Only YES rows can be summed to obtain tracked_engine; detail/summary rows must not be double-counted |
| SCOPE | Scope of byte counts and entry counts: host, cgroup, process, or thread |
| COUNTER_SCOPE | Scope of hit/miss/eviction counters; NULL when no such counters exist |
| PARENT_COMPONENT | Owning or related total item, helping identify double attribution; child items are not guaranteed to exhaust the parent |
| REJECTIONS | Cumulative rejections of controlled memory; NULL when not collected |

Percentages keep 4 decimal places. Values that cannot be collected return NULL instead of masquerading as 0.

| ACCOUNTING | Meaning |
| --- | --- |
| OS | Operating system observation |
| CHARGED | Accounting value precisely maintained by the governor; the accounting basis itself may be an estimate or a reservation and does not represent the allocator's actual physical usage |
| EXACT_BREAKDOWN / SUBSET | Detail with an explicit basis such as pages, or an execution-usage sub-item |
| ESTIMATE | Object memory estimate |
| ESTIMATE_SUBSET | Estimated detail already counted in the controlled total, such as undo and function caches |
| SHARED_ESTIMATE | Logical cache size including shared objects; must not be summed across caches |
| RESERVATION | Work area budget reservation; does not mean an equal amount of physical memory has been allocated |
| ROLLUP / DERIVED | Rollup or computed value |

## Totals, Attribution, and Limitations

- `controlled_memory` is the governor's total accounted amount; each controlled category is counted only once.
- `tracked_engine` is the sum of the controlled categories plus independent process cache estimates; it is neither a complete physical memory census nor a dedicated hard limit.
- `undo_resident` is already accounted for in `system_metadata` and is not summed again. Its resident amount includes a catalog overhead estimate.
- `controlled_derived` includes function caches, expression programs, column metadata, and storage/vector derived caches. The individual cache rows serve attribution and are not summed again; they do not exhaust all derived caches.
- Expression L1/L2 may share the same Arc object; logical sizes are shown separately but not double-counted in totals.
- `thread` refers to the worker thread executing the SHOW, not all threads, and not a cache exclusive to the current connection. Its hit counters may be process-scoped; check COUNTER_SCOPE as well. Thread-local estimates outside the governor (such as the time zone cache) are not added to the process total.
- `prepared_total` and `cte_result_total` roll up into `retained_total`, which only represents session retained caches, not all session memory.
- `rss_minus_tracked = process_rss - tracked_engine`; a negative value only means accounting/estimates exceed RSS and must not be attributed directly to non-resident pages, and a positive value does not equal the leak amount either.
- Concurrent sampling of multiple data sources may have time skew; the IN_TRACKED_TOTAL summation uses the values shown here.

## Shared Budgets

LIMIT_BYTES is not necessarily a component-exclusive quota; USED_PCT across rows must not be summed to infer remaining capacity.

| Row | LIMIT_BYTES meaning |
| --- | --- |
| storage.scratch, partition.dml_batch/ddl/stats, ddl.alter_copy_batch, system.controlled_memory | The same process operational budget |
| system.process_private_footprint | The process managed budget |
| buffer_pool.raw_total/raw_active/raw_recycled | The same buffer pool capacity |
| executor.query_execution | The overall execution memory budget |
| executor.work_area_reserved/work_area_used | The same work area target |
| storage.undo_resident | The undo cache upper bound; NULL when not configured |
| Other controlled categories | No component-exclusive quota; NULL |

## OS and Recycler Diagnostics

Linux host_memory uses MemTotal − MemAvailable. macOS uses
`(active_count + wire_count + compressor_page_count) × page_size`, capped at the physical total;
this is an approximation of usage, not equivalent to the memory_pressure pressure percentage, and inactive pages are not counted.
Compressed pages use the actual physical storage page count.

The recycler is not a memory pool; its statistics are no longer mixed into PEAK_BYTES, HITS, or EVICTIONS.
Use the dedicated status metrics to query occupied memory before/after reclamation, bytes freed, attempts/successes, active statements, and duration:

```sql
SHOW ENGINE MYDB STATUS;
```

Focus on the `idle_memory_reclaim_*` metrics in the output.

## See Also

- [Session memory](show-mydb-memory-sessions.md)
- [Metadata and diagnostics index](README.md)
