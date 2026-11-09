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
| `sequence_name` | Yes | String | Sequence name, optionally database-qualified. |

## Return Value

Returns a signed BIGINT allocated from the sequence, advances the counter and records the session CURRVAL/LASTVAL state. Allocation is nontransactional; rollback does not return a value to the sequence.

## Examples

```sql
CREATE SEQUENCE function_doc_seq START WITH 1;
SELECT NEXTVAL('function_doc_seq');
DROP SEQUENCE function_doc_seq;
```

Result: NEXTVAL returns 1 and advances the sequence; the final statement cleans up the example object.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.

- See [sequence state and transactions](README.md#sequence-state-and-transactions) and [Sequences](../../sequences.md) for privileges, session caches, rollback gaps and durability.

## Related Functions

[`CURRVAL`](currval.md), [`LASTVAL`](lastval.md), [`SETVAL`](setval.md).
