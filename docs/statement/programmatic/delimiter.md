# `DELIMITER` (client directive)

## Description

Changes the statement separator used by the mysql command-line client when reading a script. It is not a server SQL statement and does not create server session state.

## Syntax

```text
DELIMITER <delimiter-token>
```

The token is interpreted by the client. Sending `DELIMITER //` as a raw SQL protocol packet to mydb returns 1235, `DELIMITER is a client script directive, not a server SQL statement`.

## Examples

Run this script with the mysql command-line client:

```sql
DELIMITER //
CREATE PROCEDURE p() BEGIN SELECT 1 AS answer; END//
DELIMITER ;
CALL p();
DROP PROCEDURE p;
```

An API client instead sends the complete `CREATE PROCEDURE p() BEGIN SELECT 1 AS answer; END` as one statement, without either DELIMITER line or a trailing `//`. Semicolons within the body remain part of that statement. Ordinary COM_QUERY multi-statement capability does not turn DELIMITER into SQL.

## See Also

- [CREATE PROCEDURE](create-procedure.md)
- [Example setup and client contexts](../examples.md)
