# `SIGNAL`

## Description

Actively raises an SQLSTATE/named condition and sets diagnostic information.

## Syntax

```ebnf
<signal> ::= 'SIGNAL' <condition-value>
             [ 'SET' <signal-item> { ',' <signal-item> } ]
<condition-value> ::= 'SQLSTATE' [ 'VALUE' ] <string-literal>
                      | <condition-name>
<signal-item> ::= 'MESSAGE_TEXT' '=' <expr>
                  | 'MYSQL_ERRNO' '=' <expr>
```

## Behavior and Limitations

- The SQLSTATE and errno are validated; MESSAGE_TEXT becomes the client error text.
- The signal can be caught by an outer handler; otherwise it terminates the current statement/call.

## Examples

```sql
SIGNAL SQLSTATE '45000'
SET MYSQL_ERRNO = 1644, MESSAGE_TEXT = 'invalid account state';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
