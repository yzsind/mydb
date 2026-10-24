# `CREATE PROCEDURE`

## Description

Creates a stored procedure and saves its parameters, characteristics, definer, and routine body.

## Syntax

```ebnf
<create-procedure> ::= 'CREATE' [ 'DEFINER' [ '=' ] <account> ]
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

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
