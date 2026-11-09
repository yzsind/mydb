# `RESET RLS CONTEXT`

## Description

Changes trusted row-security context for the current session.

## Syntax

```ebnf
<reset-rls-context> ::= 'RESET RLS CONTEXT' [ <string-literal> ]
```

## Behavior, Privileges, and Results

Requires global `RLS_CONTEXT_ADMIN` and a top-level statement; stored routines and triggers cannot change trusted context. Returns OK. Omitting the key clears all keys; a specified key alone is cleared. Connection reset and reauthentication also start with empty context.

## Example

```sql
RESET RLS CONTEXT;
```

## See Also

- [Connection-pool context](../../row-level-security.md#connection-pools)
- [Security index](README.md)
