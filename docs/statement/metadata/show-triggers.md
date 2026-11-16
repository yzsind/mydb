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

## Result and Visibility Details

Requires TRIGGER visibility on each subject table. LIKE matches the subject Table, not the Trigger name. Use WHERE Trigger=... for a trigger-name filter. Character-set and sql_mode fields are captured definition metadata.

See [ordered columns and wire types](result-columns.md#show-triggers). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW TRIGGERS FROM app LIKE 'orders%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
