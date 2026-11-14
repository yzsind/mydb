# `CHECK TABLE`

## Description

Checks the implemented LOB and index consistency invariants for the named tables. An OK result is not a complete storage or catalog integrity certification.

## Syntax

```ebnf
<check-table> ::= 'CHECK TABLE' <table-list>
                  { 'QUICK' | 'FAST' | 'MEDIUM' | 'EXTENDED'
                    | 'CHANGED' | 'FOR UPGRADE'
                    | 'LOCAL' | 'NO_WRITE_TO_BINLOG' }
```

## Behavior, Privileges, and Limitations

- Returns `Table`, `Op`, `Msg_type`, and `Msg_text`. A clean table produces a status/OK row. Each detected problem produces an error row, so one table can produce multiple rows.
- mydb is a transactional MVCC engine; normal operation should not produce MyISAM-style repairable corruption. Severe recovery should use replica rebuilds or PITR.
- The listed trailing options are currently accepted without selecting different check algorithms or emitting option-specific warnings. Do not interpret QUICK/EXTENDED as guarantees of different verification depth.

## Privileges, Transaction, and Result Details

Requires SELECT and implicitly commits the caller's earlier DML. Per-table failures are reported in the four-column result; inspect every Msg_type, because a successful protocol result set can contain error rows. CHECK does not repair data and does not certify storage invariants that its current LOB/index validators do not inspect. Compatibility modifiers do not select deeper algorithms.

## Examples

```sql
CHECK TABLE orders EXTENDED;
CHECK TABLE customer, orders QUICK;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
