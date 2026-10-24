# `CLOSE SQL CURSOR`

## Description

Closes a session-level SQL cursor and releases its snapshot, scan state, and memory reservation.

## Syntax

```ebnf
<close-cursor> ::= 'CLOSE' <identifier>
```

## Behavior and Limitations

- An unknown name returns an error; FETCH cannot continue after the cursor is closed.
- Cursors that are no longer valid are also cleaned up when the transaction ends or the connection disconnects.

## Examples

```sql
CLOSE recent_orders;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
