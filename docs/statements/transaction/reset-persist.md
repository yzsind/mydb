# `RESET PERSIST`

## Description

Removes one or all persisted system variable entries.

## Syntax

```ebnf
<reset-persist> ::= 'RESET PERSIST' [ 'IF EXISTS' ] [ <identifier> ]
```

## Behavior and Limitations

- When a variable is specified, only that entry is removed; when omitted, the managed entries in the persistence file are cleared.
- Removing a persisted value does not automatically restore the current runtime value to its default.
- A nonexistent variable yields an error or a Note depending on IF EXISTS; the corresponding system variable administration privilege is required.

## Examples

```sql
RESET PERSIST mydb_buffer_pool_pages;
RESET PERSIST IF EXISTS obsolete_variable;
RESET PERSIST;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
