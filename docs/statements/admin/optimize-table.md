# `OPTIMIZE TABLE`

## Description

Rebuilds a table online to reclaim heap, index, and LOB space, refresh statistics, and rebuild Zone Maps and special indexes.

## Syntax

```ebnf
<optimize-table> ::= 'OPTIMIZE' [ 'LOCAL' | 'NO_WRITE_TO_BINLOG' ]
                     'TABLE' <table-list>
```

## Behavior, Privileges, and Limitations

- The synchronous operation builds private shadow storage, catches up concurrent DML, then publishes a new generation. Preparation and publication need exclusive barriers; scans and main catch-up allow ordinary reads and writes. Publication can wait for long transactions and disk syncs.
- Ordinary persistent tables, partitions and subpartitions are supported. Memory durability, temporary/system tables, and partitioned tables with FULLTEXT indexes are unsupported. This statement rebuilds every physical leaf of each target table; targeted ALTER TABLE OPTIMIZE PARTITION has separate semantics.
- Logical table identity, definitions, constraints, and auto-increment counters are retained. Old RR snapshots accessing a replaced generation return 1412 and must reopen their transaction. Extra disk space is needed for old and new storage to coexist.
- Returns per-table status; failed tables report errors while later targets continue, and cancellation aborts the statement. Only successful targets are binlogged; LOCAL/NO_WRITE_TO_BINLOG suppresses that logging.
- Progress and cancellation use information_schema.mydb_ddl_jobs and CANCEL DDL JOB. See [operational limits](../../mysql80-compatibility.md#optimize-table) and [publication budgets](../../configuration.md#optimize-table-online-defragmentation).

## Examples

```sql
OPTIMIZE TABLE orders, customer;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
