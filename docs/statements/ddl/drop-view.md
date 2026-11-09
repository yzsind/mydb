# `DROP VIEW`

## Description

Drops one or more view definitions.

## Syntax

```ebnf
<drop-view> ::= 'DROP VIEW' [ 'IF EXISTS' ]
                <qualified-name> { ',' <qualified-name> }
                [ 'RESTRICT' | 'CASCADE' ]
```

## Behavior and Limitations

- Drops only the view, not the underlying tables or data.
- Missing objects produce an error or a Note depending on IF EXISTS. RESTRICT and CASCADE are currently accepted compatibility spellings with no distinct execution effect: dependent views are not dropped and can become invalid, reporting 1356 when queried. Do not rely on CASCADE for dependency cleanup.
- Acquires the view metadata lock and invalidates dependent plans.

## Privileges, Transactions, and Dependencies

Requires DROP on each view. Persistent DROP implicitly commits preceding DML and atomically validates the multi-object catalog change. Only view definitions are removed, never base rows. Dependents can remain present but invalid (1356); explicitly drop dependent views if that is intended. RESTRICT/CASCADE does not choose a different dependency algorithm in the current implementation.

## Examples

```sql
DROP VIEW active_customer, order_summary;
DROP VIEW IF EXISTS old_report;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
