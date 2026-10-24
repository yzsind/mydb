# `FETCH SQL CURSOR`

## Description

Reads the next row, or the next N rows, from a session-level SQL cursor.

## Syntax

```ebnf
<fetch-cursor> ::= 'FETCH' [ 'NEXT' | <unsigned-integer> ]
                   [ 'FROM' ] <identifier>
```

## Behavior and Limitations

- With the count omitted, or with NEXT, one row is read; a count gives the maximum number of rows to return.
- Reaching the end returns an empty result; an unknown or already closed cursor returns an error.
- Each FETCH continues the original cursor's snapshot and result schema.

## Examples

```sql
FETCH NEXT FROM recent_orders;
FETCH 100 FROM recent_orders;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
