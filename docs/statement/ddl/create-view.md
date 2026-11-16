# `CREATE VIEW`

## Description

Saves a query definition and creates a logical view.

## Syntax

```ebnf
<create-view> ::= 'CREATE' [ 'OR REPLACE' ]
                  [ 'ALGORITHM' [ '=' ] ( 'UNDEFINED' | 'MERGE' | 'TEMPTABLE' ) ]
                  [ 'DEFINER' [ '=' ] <account> ]
                  [ 'SQL SECURITY' ( 'DEFINER' | 'INVOKER' ) ]
                  'VIEW' [ 'IF NOT EXISTS' ] <qualified-name>
                  [ '(' <identifier-list> ')' ]
                  'AS' <query-expression>
                  [ 'WITH' [ 'CASCADED' | 'LOCAL' ] 'CHECK OPTION' ]
```

## Behavior and Limitations

- Saves the parsed semantic query along with explicit column names, definer, security mode, and check option. Execution uses the saved AST; normalized SHOW CREATE text is for display.
- When the view is queried, execution follows SQL SECURITY and the privileges on the underlying objects; the reference chain participates in planning, locking, and dependency resolution.
- ALGORITHM=TEMPTABLE forces materialization; MERGE/UNDEFINED follow the supported view-planning rules. LOCAL/CASCADED CHECK OPTION is enforced on supported updatable direct-column views (failure 1369); non-updatable or computed-projection shapes that cannot enforce it return 1368.
- Conflicts between OR REPLACE and IF NOT EXISTS, and already existing objects, are validated per MySQL compatibility rules.

## Privileges, Transactions, and Dependencies

Requires CREATE VIEW and the necessary source read privileges; OR REPLACE also requires DROP. The default security mode is DEFINER, with the creating authorization account as definer when omitted. Naming another definer has a separate privilege check. Captured SQL mode/character sets and normalized output are definition metadata, not runtime variables in the view body.

Direct-column single-table views, including supported nested projections, support INSERT/UPDATE/DELETE. Aggregation, set operations, DISTINCT, windows and materialization can make a view non-updatable. Join-view UPDATE can target one supported direct base projection, while cross-base assignments and unsupported outer/complex shapes fail. CHECK OPTION must be enforceable; violation is 1369 and a non-updatable checked shape is 1368. See [view compatibility boundaries](../../mysql80-compatibility.md#views-view).

Creation implicitly commits preceding DML. It rejects recursive CTE bodies, mutable session variables, parameters and locking clauses in the definition. Replacing a view revalidates dependent definitions; a missing dependency can subsequently produce 1356.

## Examples

```sql
CREATE VIEW active_customer AS
SELECT id, email FROM customer WHERE disabled = 0;

CREATE SQL SECURITY INVOKER VIEW order_summary (customer_id, amount) AS
SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)

## Known Query Body Boundaries

The view query body supports plain SELECT, set operations, and non-recursive CTEs, including shared materialization for multiple references. Recursive CTE view definitions are unsupported and return 1235 at creation time.
The view body cannot contain user variables, system variables, or prepared-statement parameters (including LIMIT/OFFSET parameters); a plain parameterless CREATE VIEW can be executed through SQL PREPARE/EXECUTE.
