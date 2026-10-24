# `NEXTVAL`

## Description

Obtains and advances the next value of the specified sequence.

## Syntax

```sql
NEXTVAL(sequence_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sequence_name` | Yes | String | The 1st argument; the sequence name |

## Return Value

Obtains and advances the next value of the specified sequence. The return type is determined by the function semantics and the input type.

## Examples

```sql
CREATE SEQUENCE function_doc_seq START WITH 1;
SELECT NEXTVAL('function_doc_seq');
DROP SEQUENCE function_doc_seq;
```

Result: NEXTVAL returns 1 and advances the sequence; the final statement cleans up the example object.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
