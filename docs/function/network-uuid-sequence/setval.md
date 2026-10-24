# `SETVAL`

## Description

Sets the current value of the specified sequence.

## Syntax

```sql
SETVAL(sequence_name, value [, is_called])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sequence_name` | Yes | String | The 1st argument; the sequence name |
| `value` | Yes | Numeric or integer | The 2nd argument; the input value |
| `is_called` | No | Address, UUID, sequence name, integer, or flag value | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Sets the current value of the specified sequence. The return type is determined by the function semantics and the input type.

## Examples

```sql
CREATE SEQUENCE function_doc_seq;
SELECT SETVAL('function_doc_seq', 42);
DROP SEQUENCE function_doc_seq;
```

Result: SETVAL returns and records the new value 42.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
