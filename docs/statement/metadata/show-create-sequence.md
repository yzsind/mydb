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

## Result and Visibility Details

Requires SELECT on the sequence. The returned definition contains configured START, increment, bounds, cache and cycle, with a non-default backing type when applicable. START is a definition value, not the current physical counter. This SQL alone does not capture allocated CACHE windows or the session CURRVAL; use a physical backup for state recovery.

See [ordered columns and wire types](result-columns.md#show-create-sequence). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE SEQUENCE invoice_seq;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
