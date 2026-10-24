# `SHOW CREATE SEQUENCE`

## Description

Returns the SQL that recreates a sequence's current definition.

## Syntax

```ebnf
<show-create-sequence> ::= 'SHOW CREATE SEQUENCE' <qualified-name>
```

## Behavior, Privileges, and Results

- The output includes the start point, increment, bounds, cache, and cycle options; it neither exposes nor advances the session currval.
- Returns an error if the object does not exist or is not visible.

## Examples

```sql
SHOW CREATE SEQUENCE invoice_seq;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
