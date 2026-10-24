# `SHOW CREATE TRIGGER`

## Description

Returns a trigger's canonical CREATE statement plus character set/sql_mode metadata.

## Syntax

```ebnf
<show-create-trigger> ::= 'SHOW CREATE TRIGGER' <qualified-name>
```

## Behavior, Privileges, and Results

- The output includes definer, timing, event, table, order, and the routine body.
- Returns an error if the object does not exist or privileges are insufficient.

## Examples

```sql
SHOW CREATE TRIGGER orders_bi;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
