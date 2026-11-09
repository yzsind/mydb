# `SHOW ENGINE`

## Description

Shows the status or mutex information of a given engine.

## Syntax

```ebnf
<show-engine> ::= 'SHOW ENGINE' <identifier>
                  ( 'STATUS' | 'MUTEX' )
```

## Behavior, Privileges, and Results

- SHOW ENGINE MYDB STATUS returns engine diagnostics; MYDB MUTEX currently returns the compatible three-column schema with zero rows, without per-latch measurements.
- Non-mydb engines return 1286 per the compatibility probe instead of an ambiguous syntax error.

## Result and Visibility Details

Requires global PROCESS. STATUS and MUTEX use Type/Name/Status diagnostic rows. Only the native MYDB engine is handled; another engine name returns 1286, even though InnoDB is accepted as a CREATE TABLE compatibility engine alias.

See [ordered columns and wire types](result-columns.md#show-engine). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW ENGINE MYDB STATUS;
SHOW ENGINE MYDB MUTEX;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
