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
- A name absent from persisted_variables returns an error unless IF EXISTS is specified; IF EXISTS succeeds without adding a persisted entry. Requires global SYSTEM_VARIABLES_ADMIN.

## Transaction and Error Details

The managed file is mysqld-auto.cnf under the data directory. RESET PERSIST without a name clears managed persisted entries, not all configuration sources or environment settings. The runtime value is unchanged until another SET or subsequent startup configuration applies. IF EXISTS suppresses a missing persisted entry; it does not validate a new value or invent a variable.

## Examples

```sql
RESET PERSIST mydb_buffer_pool_pages;
RESET PERSIST IF EXISTS obsolete_variable;
RESET PERSIST;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
