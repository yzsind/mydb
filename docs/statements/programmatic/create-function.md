# `CREATE FUNCTION`

## Description

Creates or replaces an SQL stored function, declaring parameters, the return type, characteristics, and the RETURN path.

## Syntax

```ebnf
<create-function> ::= 'CREATE' [ 'OR REPLACE' ] [ 'DEFINER' [ '=' ] <account> ]
                      'FUNCTION' [ 'IF NOT EXISTS' ] <qualified-name>
                      '(' [ <function-param> { ',' <function-param> } ] ')'
                      'RETURNS' <data-type>
                      { <routine-characteristic> } <routine-body>
<function-param> ::= <identifier> <data-type>
```

## Behavior and Limitations

- Function parameters are all input parameters; the RETURNS type governs call result conversion and metadata.
- A function must execute RETURN to produce a result. Creation does not prove that every path returns: reaching the end without RETURN raises 1321 at execution. A body with no RETURN anywhere is rejected at creation with 1415. DETERMINISTIC and data-access characteristics are declared metadata and do not prove purity or enable constant folding.
- When a stored function has the same name as a built-in function, the qualified name and resolution rules distinguish them.
- OR REPLACE is a mydb extension to MySQL 8.0. It creates the function if absent, or replaces its complete definition, including parameters, RETURNS, body, characteristics, definer, and creation-time session settings. Omitted characteristics take CREATE defaults; an omitted DEFINER defaults to the current account.
- CREATE ROUTINE is required; replacing an existing function additionally requires ALTER ROUTINE on that function. Existing routine grants are preserved, and automatic_sp_privileges applies as for CREATE. Normal definer and binary-logging checks still apply.
- The new definition is validated and compiled before catalog replacement; validation or authorization failure leaves the existing definition available. Replacement uses exclusive routine metadata locking and the normal DDL implicit-commit rules. Subsequent queries and prepared executions use the new definition and return metadata; dependent views are revalidated, and calls with an obsolete argument count return an error.
- OR REPLACE cannot be combined with IF NOT EXISTS. Only the same kind of routine in the specified database is replaced; a procedure with the same name is unaffected. Built-in sys functions cannot be replaced.

## Context, Errors, and Lifecycle

Requires CREATE ROUTINE on the schema; specifying a different definer and binary-log safety settings can impose additional checks. Captured SQL mode/character sets govern body evaluation and result conversion. Stored-function recursion is rejected (1424); procedure recursion has a separate configurable limit.

A stored function cannot return arbitrary SELECT result sets, execute explicit/implicit commits, or use dynamic SQL. The owning statement provides atomic DML scope; external effects remain subject to their own context restrictions. A declared DETERMINISTIC/NO SQL characteristic is not a proof about effects. When execution reaches the end of a conditional-return path, error 1321 is returned.

## Examples

```sql
DELIMITER //
CREATE FUNCTION add_tax(p_amount DECIMAL(20,2))
RETURNS DECIMAL(20,2)
DETERMINISTIC
NO SQL
BEGIN
  RETURN p_amount * 1.06;
END//
DELIMITER ;
```

Replace the return type and body directly:

```sql
CREATE OR REPLACE FUNCTION add_tax(p_amount DECIMAL(20,2))
RETURNS DECIMAL(22,4)
DETERMINISTIC NO SQL
RETURN p_amount * 1.08;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
