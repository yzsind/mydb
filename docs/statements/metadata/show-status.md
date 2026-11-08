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

## Result and Visibility Details

Both columns are strings; Value can encode counts, durations, bytes or booleans according to Variable_name. SESSION and GLOBAL have different counter sources and should not be added together. FLUSH STATUS resets selected peaks/session counters but preserves many cumulative storage counters.

See [ordered columns and wire types](result-columns.md#show-status). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW GLOBAL STATUS LIKE 'Connections';
SHOW SESSION STATUS LIKE 'Rows_%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
