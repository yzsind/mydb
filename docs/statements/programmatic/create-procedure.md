# `CREATE PROCEDURE`

## Description

Creates or replaces a stored procedure and saves its parameters, characteristics, definer, and routine body.

## Syntax

```ebnf
<create-procedure> ::= 'CREATE' [ 'OR REPLACE' ] [ 'DEFINER' [ '=' ] <account> ]
                       'PROCEDURE' [ 'IF NOT EXISTS' ] <qualified-name>
                       '(' [ <procedure-param> { ',' <procedure-param> } ] ')'
                       { <routine-characteristic> } <routine-body>
<procedure-param> ::= [ 'IN' | 'OUT' | 'INOUT' ] <identifier> <data-type>
<routine-characteristic> ::= 'COMMENT' <string-literal>
                             | 'LANGUAGE SQL'
                             | [ 'NOT' ] 'DETERMINISTIC'
                             | 'CONTAINS SQL' | 'NO SQL'
                             | 'READS SQL DATA' | 'MODIFIES SQL DATA'
                             | 'SQL SECURITY' ( 'DEFINER' | 'INVOKER' )
```

## Behavior and Limitations

- When the parameter mode is omitted it defaults to IN; the defaults are NOT DETERMINISTIC, CONTAINS SQL, and SQL SECURITY DEFINER.
- The routine body is fully parsed at CREATE time; an unsupported inner statement fails the DDL and nothing partial is saved.
- IF NOT EXISTS produces a Note for an existing procedure; call privileges and definer/invoker privileges are validated at CALL time.
- OR REPLACE is a mydb extension to MySQL 8.0. It creates the procedure if absent, or replaces its complete definition, including parameters, parameter modes/types, body, characteristics, definer, and creation-time session settings. Omitted characteristics take CREATE defaults; an omitted DEFINER defaults to the current account.
- CREATE ROUTINE is required; replacing an existing procedure additionally requires ALTER ROUTINE on that procedure. Existing routine grants are preserved, and automatic_sp_privileges applies as for CREATE.
- The new definition is validated and compiled before catalog replacement; validation or authorization failure leaves the existing definition available. Replacement uses exclusive routine metadata locking and the normal DDL implicit-commit rules; subsequent calls and prepared calls use the new definition.
- OR REPLACE cannot be combined with IF NOT EXISTS. Built-in sys procedures cannot be replaced.

## Examples

```sql
DELIMITER //
CREATE PROCEDURE transfer(IN p_from BIGINT, IN p_to BIGINT, IN p_amount DECIMAL(20,2))
MODIFIES SQL DATA
BEGIN
  UPDATE account SET balance = balance - p_amount WHERE id = p_from;
  UPDATE account SET balance = balance + p_amount WHERE id = p_to;
END//
DELIMITER ;
```

Replace the parameter list and body directly:

```sql
CREATE OR REPLACE PROCEDURE transfer(IN p_account BIGINT, IN p_amount DECIMAL(20,2))
MODIFIES SQL DATA
UPDATE account SET balance = balance + p_amount WHERE id = p_account;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
