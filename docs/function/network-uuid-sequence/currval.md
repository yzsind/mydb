# `CURRVAL`

## Description

Returns the most recent value obtained from the specified sequence in the current session.

## Syntax

```sql
CURRVAL(sequence_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sequence_name` | Yes | String | Sequence name, optionally database-qualified. |

## Return Value

Returns the signed BIGINT most recently recorded for this sequence in the current session. An undefined session value raises an explicit sequence error; another session's NEXTVAL does not define this session's CURRVAL.

## Examples

```sql
CREATE SEQUENCE function_doc_seq START WITH 1;
SELECT NEXTVAL('function_doc_seq');
SELECT CURRVAL('function_doc_seq');
DROP SEQUENCE function_doc_seq;
```

Result: CURRVAL returns 1, the most recent value obtained in the current session.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.
- Returns a clear error when the current session has not yet successfully called the corresponding NEXTVAL.

- See [sequence state and transactions](README.md#sequence-state-and-transactions) and [Sequences](../../sequences.md) for privileges, session caches, rollback gaps and durability.

## Related Functions

[`NEXTVAL`](nextval.md), [`LASTVAL`](lastval.md), [`SETVAL`](setval.md).
