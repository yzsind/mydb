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
- Missing objects produce an error or a Note depending on IF EXISTS; dependent views are validated per the current DROP behavior.
- Acquires the view metadata lock and invalidates dependent plans.

## Examples

```sql
DROP VIEW active_customer, order_summary;
DROP VIEW IF EXISTS old_report;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
