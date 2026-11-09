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
- RETURNING returns NEW by default. OLD is always SQL NULL for REPLACE because one new row can replace several conflicting old rows; NEW is the final inserted image.
- `DELAYED` is compatibility syntax and produces a deprecation or not-implemented warning.

## Privileges, Results, and Boundaries

Each new row can delete several conflicting rows across unique keys. The affected count is inserted plus deleted rows. It does not retain old defaults or old auto-increment values unless explicitly supplied. DELETE/INSERT triggers and FK actions execute under that replacement's statement transaction. A projection/constraint error rolls back the entire statement, including deletions.

OLD is NULL in RETURNING even when a conflict occurred. Indirect FK-cascade rows are not returned. Partition selection uses the target table's routing rules; rows outside an explicit PARTITION set cannot be silently redirected. See [row-image definitions](../../dml-returning.md).

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
