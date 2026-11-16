# `ALTER VIEW`

## Description

Replaces the query definition or attributes of an existing view.

## Syntax

```ebnf
<alter-view> ::= 'ALTER'
                 [ 'ALGORITHM' [ '=' ] ( 'UNDEFINED' | 'MERGE' | 'TEMPTABLE' ) ]
                 [ 'DEFINER' [ '=' ] <account> ]
                 [ 'SQL SECURITY' ( 'DEFINER' | 'INVOKER' ) ]
                 'VIEW' <qualified-name>
                 [ '(' <identifier-list> ')' ]
                 'AS' <query-expression>
                 [ 'WITH' [ 'CASCADED' | 'LOCAL' ] 'CHECK OPTION' ]
```

## Behavior and Limitations

- The target must be an existing view; a regular table is not converted into a view.
- The new definition is parsed, column-derived, and dependency-checked before commit; on failure, the old definition is kept.
- After the catalog is updated, dependent plan caches are invalidated.

## Privileges, Transactions, and Dependencies

Uses CREATE VIEW plus DROP on the view and source privileges; definer changes are checked independently. It implicitly commits preceding DML. The same recursive CTE, variable, placeholder, locking-clause, updatability and CHECK OPTION restrictions as CREATE VIEW apply. A validated replacement keeps its logical name and revalidates dependents; a rejected definition leaves the old definition intact. See [CREATE VIEW boundaries](create-view.md).

## Examples

```sql
ALTER SQL SECURITY INVOKER VIEW active_customer AS
SELECT id, email, last_login FROM customer WHERE disabled = 0;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
