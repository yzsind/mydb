# `SIGNAL`

## Description

Actively raises an SQLSTATE/named condition and sets diagnostic information.

## Syntax

```ebnf
<signal> ::= 'SIGNAL' <condition-value>
             [ 'SET' <signal-item> { ',' <signal-item> } ]
<condition-value> ::= 'SQLSTATE' [ 'VALUE' ] <string-literal>
                      | <condition-name>
<signal-item> ::= <condition-information-item> '=' <expr>
<condition-information-item> ::= 'MESSAGE_TEXT' | 'MYSQL_ERRNO'
                  | 'CLASS_ORIGIN' | 'SUBCLASS_ORIGIN'
                  | 'CONSTRAINT_CATALOG' | 'CONSTRAINT_SCHEMA' | 'CONSTRAINT_NAME'
                  | 'CATALOG_NAME' | 'SCHEMA_NAME' | 'TABLE_NAME'
                  | 'COLUMN_NAME' | 'CURSOR_NAME'
```

## Behavior and Limitations

- The SQLSTATE and errno are validated; MESSAGE_TEXT becomes the client error text.
- The signal can be caught by an outer handler; otherwise it terminates the current statement/call.

## Context, Errors, and Lifecycle

SQLSTATE must be five ASCII alphanumeric characters and cannot begin with 00. A named condition must have been declared with SQLSTATE, not merely an errno. MYSQL_ERRNO is 1 through 65535; MESSAGE_TEXT cannot be NULL. Duplicate information items fail parsing; additional condition-source names are limited to 64 characters and cannot be NULL.

The current executor propagates SIGNAL as a raised condition, including an unhandled 01-class signal (default 1642); callers must not assume an unhandled warning-class SIGNAL always continues the body. A matching handler can control continuation/exit. See [handler rules](declare-handler.md) and [GET DIAGNOSTICS fields](get-diagnostics.md#diagnostic-items).

## Examples

```sql
SIGNAL SQLSTATE '45000'
SET MYSQL_ERRNO = 1644, MESSAGE_TEXT = 'invalid account state';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
