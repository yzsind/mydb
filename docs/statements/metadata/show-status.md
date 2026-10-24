# `SHOW STATUS`

## Description

Shows session or global runtime status counters.

## Syntax

```ebnf
<show-status> ::= 'SHOW' [ 'GLOBAL' | 'SESSION' | 'LOCAL' ]
                 'STATUS'
                 [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- SESSION is the default; GLOBAL returns process-level aggregates.
- Counters come from status registration and real-time collection, and reading does not reset them; see FLUSH STATUS for the reset capability.

## Examples

```sql
SHOW GLOBAL STATUS LIKE 'Connections';
SHOW SESSION STATUS LIKE 'Rows_%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
