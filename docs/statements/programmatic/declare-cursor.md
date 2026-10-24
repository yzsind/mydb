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

## Examples

```sql
DECLARE recent_orders CURSOR FOR
SELECT id, total FROM orders ORDER BY created_at DESC;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
