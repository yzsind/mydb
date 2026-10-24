# `SHOW ENGINE`

## Description

Shows the status or mutex information of a given engine.

## Syntax

```ebnf
<show-engine> ::= 'SHOW ENGINE' <identifier>
                  ( 'STATUS' | 'MUTEX' )
```

## Behavior, Privileges, and Results

- SHOW ENGINE MYDB STATUS returns engine diagnostics; MYDB MUTEX returns the synchronization information supported by the implementation.
- Non-mydb engines return 1286 per the compatibility probe instead of an ambiguous syntax error.

## Examples

```sql
SHOW ENGINE MYDB STATUS;
SHOW ENGINE MYDB MUTEX;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
