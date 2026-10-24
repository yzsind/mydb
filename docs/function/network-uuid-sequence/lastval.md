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

Returns the most recent value obtained from any sequence in the current session. The return type is determined by the function semantics and the input type.

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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires a sequence created in advance in the current database, called from a statement with database and session context; the call reads or changes session/sequence state.
- Returns a clear error when the current session has not yet successfully called the corresponding NEXTVAL.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
