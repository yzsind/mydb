# `CHECK TABLE`

## Description

Checks the implemented LOB and index consistency invariants for the named tables.

## Syntax

```ebnf
<check-table> ::= 'CHECK TABLE' <table-list>
                  { 'QUICK' | 'FAST' | 'MEDIUM' | 'EXTENDED'
                    | 'CHANGED' | 'FOR UPGRADE'
                    | 'LOCAL' | 'NO_WRITE_TO_BINLOG' }
```

## Behavior, Privileges, and Limitations

- Returns `Table`, `Op`, `Msg_type`, and `Msg_text`. A clean table produces a status/OK row. Each detected problem produces an error row, so one table can produce multiple rows.
- Index scans allow concurrent writes and retry when their mutation generation changes. After bounded retries, persistent write pressure produces an explicit error row; retry after writes subside. LOB structure uses bounded page-lock batches, and row/chain checks pin an MVCC snapshot.
- mydb is a transactional MVCC engine; tables do not accumulate MyISAM-style repairable corruption. For severe damage, recover with replica rebuilds or PITR.
- The listed trailing options are currently accepted without selecting different check algorithms or emitting option-specific warnings. Do not interpret QUICK/EXTENDED as guarantees of different verification depth.

## Privileges, Transaction, and Result Details

Requires SELECT and implicitly commits the caller's earlier DML. Per-table failures are reported in the four-column result; inspect every Msg_type, because a successful protocol result set can contain error rows. CHECK does not repair data; it covers the implemented LOB and index consistency invariants rather than a full storage and catalog integrity certification, and compatibility modifiers do not select deeper algorithms.

## Examples

```sql
CHECK TABLE orders EXTENDED;
CHECK TABLE customer, orders QUICK;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
