# `CLOSE SQL CURSOR`

## Description

Closes a session-level SQL cursor and releases its snapshot, scan state, and memory reservation.

## Syntax

```ebnf
<close-cursor> ::= 'CLOSE' <identifier>
```

## Behavior and Limitations

- An unknown name returns an error; FETCH cannot continue after the cursor is closed.
- Session SQL cursors are not closed by COMMIT alone; fetching can continue afterward. Explicit CLOSE, connection reset, reauthentication, and disconnect release session cursors. This lifecycle differs from stored-program block cursors.

## Context, Errors, and Lifecycle

Unknown/already closed cursor returns 1326. Closing releases retained scan/snapshot state and memory; it returns OK without a result set. COMMIT alone does not close this session-level cursor. See the [full session example](../examples.md#complete-stored-cursor-example); routine and binary-protocol cursors are separate resources.

## Examples

```sql
CLOSE recent_orders;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
