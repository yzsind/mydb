# `LASTVAL`

## Description

Returns the most recent value obtained from any sequence in the current session.

## Syntax

```sql
LASTVAL()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the signed BIGINT from the sequence selected by the most recent successful NEXTVAL in this session. Undefined/dropped or unauthorized sequence state raises an error. SETVAL does not select a new LASTVAL target, but changing the current target's CURRVAL can affect the returned value.

## Examples

```sql
CREATE SEQUENCE function_doc_seq START WITH 1;
SELECT NEXTVAL('function_doc_seq');
SELECT LASTVAL();
DROP SEQUENCE function_doc_seq;
```

Result: LASTVAL returns 1, the most recent value obtained from any sequence in the current session.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.
- Returns a clear error when the current session has not yet successfully called the corresponding NEXTVAL.

- See [sequence state and transactions](README.md#sequence-state-and-transactions) and [Sequences](../../sequences.md) for privileges, session caches, rollback gaps and durability.

## Related Functions

[`NEXTVAL`](nextval.md), [`CURRVAL`](currval.md), [`SETVAL`](setval.md).
