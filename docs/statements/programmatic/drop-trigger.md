# `DROP TRIGGER`

## Description

Drops a named trigger.

## Syntax

```ebnf
<drop-trigger> ::= 'DROP TRIGGER' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior and Limitations

- IF EXISTS controls the missing-object diagnostic.
- The drop affects only subsequent DML and does not modify rows already written.

## Examples

```sql
DROP TRIGGER IF EXISTS orders_bi;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
