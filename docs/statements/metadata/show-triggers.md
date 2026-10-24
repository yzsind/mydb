# `SHOW TRIGGERS`

## Description

Lists the visible triggers in a database.

## Syntax

```ebnf
<show-triggers> ::= 'SHOW TRIGGERS'
                   [ ( 'FROM' | 'IN' ) <identifier> ]
                   [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns columns such as Trigger, Event, Table, Statement, Timing, Created, sql_mode, and Definer.
- Filtering applies to the trigger metadata result.

## Examples

```sql
SHOW TRIGGERS FROM app LIKE 'orders%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
