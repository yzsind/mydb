# `REPLACE`

## Description

Inserts a new row; on a primary-key or unique-key conflict, it first deletes all conflicting old rows, then inserts the new row. It is not an alias of UPDATE.

## Syntax

```ebnf
<replace> ::= 'REPLACE'
              [ 'LOW_PRIORITY' | 'DELAYED' ]
              [ 'INTO' ] <qualified-name>
              ( [ '(' <identifier-list> ')' ]
                ( 'VALUES' | 'VALUE' ) <row-value> { ',' <row-value> }
              | 'SET' <assignment> { ',' <assignment> }
              | [ '(' <identifier-list> ')' ] <query-expression> )
              [ <returning-clause> ]
```

## Behavior and Limitations

- Requires the INSERT and DELETE privileges on the target table, and fires DELETE/INSERT-related triggers and foreign key actions.
- There may be more than one conflicting old row; affected rows typically includes both the deleted and inserted counts.
- Columns that are not provided are handled as a newly inserted row and do not inherit the old row's values.
- RETURNING returns NEW by default; OLD/NEW can be referenced explicitly.
- `DELAYED` is compatibility syntax and produces a deprecation or not-implemented warning.

## Examples

```sql
REPLACE INTO settings (name, value) VALUES ('theme', 'dark');

REPLACE INTO snapshot (id, payload)
SELECT id, payload FROM staging_snapshot
RETURNING OLD.id, NEW.id;
```

If a row with `name='theme'` already exists, the first statement deletes the old row and then inserts the new one; foreign key and trigger effects also occur under this two-step semantics.

## Related Statements

- [INSERT](insert.md)
- [DELETE](delete.md)
