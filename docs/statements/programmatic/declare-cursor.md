# `DECLARE CURSOR`

## Description

Declares a mydb SQL cursor at session top level and associates it with a read-only query.

## Syntax

```ebnf
<declare-cursor> ::= 'DECLARE' <identifier> 'CURSOR FOR' <select-statement>
```

## Behavior and Limitations

- This is a mydb session-level extension; standard MySQL only allows DECLARE CURSOR inside stored programs.
- The query establishes a streaming ResultCursor and retains the necessary snapshot/scan state, subject to MYDB_CURSOR_* and the unified memory limits.
- Cursor names belong to the session and are closed automatically when the connection drops.

## Context, Errors, and Lifecycle

DECLARE executes the query and opens the session cursor immediately; do not issue OPEN for this extension. Source SELECT permissions apply and the query must produce a result set. Names belong to one connection. The cursor retains its execution state and can pin read history; use CLOSE promptly. COMMIT alone does not invalidate it. Inspect information_schema.mydb_cursors and the configured MYDB_CURSOR_* memory/idle limits; memory admission still applies.

## Examples

```sql
DECLARE recent_orders CURSOR FOR
SELECT id, total FROM orders ORDER BY created_at DESC;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
