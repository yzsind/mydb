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
| `sequence_name` | Yes | String | Existing sequence, optionally database-qualified. |
| `value` | Yes | Numeric expression | New sequence value, within its bounds. |
| `is_called` | No | Boolean/numeric expression | Default true; false makes the next NEXTVAL start at value. |

## Return Value

Returns the signed BIGINT value after a nontransactional counter reset. is_called defaults to true: true makes the next allocation advance from value and updates session CURRVAL; false makes the next allocation start at value and does not define a new CURRVAL. Existing caches in other sessions may continue issuing their reserved values.

## Examples

```sql
CREATE SEQUENCE function_doc_seq START WITH 1 CACHE 1;
SELECT SETVAL('function_doc_seq',42,FALSE);
SELECT NEXTVAL('function_doc_seq');
SELECT SETVAL('function_doc_seq',100,TRUE);
SELECT NEXTVAL('function_doc_seq');
DROP SEQUENCE function_doc_seq;
```

Result:

```text
42
42
100
101
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.

- See [sequence state and transactions](README.md#sequence-state-and-transactions) and [Sequences](../../sequences.md) for privileges, session caches, rollback gaps and durability.

## Related Functions

[`NEXTVAL`](nextval.md), [`CURRVAL`](currval.md), [`LASTVAL`](lastval.md).
