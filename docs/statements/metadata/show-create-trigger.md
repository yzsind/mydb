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

## Result and Visibility Details

Requires TRIGGER on the subject table. SQL Original Statement is the canonical trigger body/definition; sql_mode, character sets and Created are separate result metadata. A missing trigger is an error.

See [ordered columns and wire types](result-columns.md#show-create-trigger). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE TRIGGER orders_bi;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
