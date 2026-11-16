# `DECLARE CONDITION`

## Description

Declares a local condition name for an SQLSTATE or MySQL errno.

## Syntax

```ebnf
<declare-condition> ::= 'DECLARE' <identifier> 'CONDITION FOR'
                        ( 'SQLSTATE' [ 'VALUE' ] <string-literal>
                        | <unsigned-integer> )
```

## Behavior and Limitations

- Condition names can be used by handlers, SIGNAL, and RESIGNAL.
- The SQLSTATE must be a valid five-character value; the errno must be within the allowed range.

## Context, Errors, and Lifecycle

SQLSTATE must be five ASCII alphanumeric characters and cannot start with 00. An errno condition must be nonzero. A name is block-scoped; handlers can use either errno or SQLSTATE conditions, but SIGNAL/RESIGNAL may only use a named condition defined with SQLSTATE, not an errno alias. Illegal combinations fail routine validation rather than silently changing the condition category.

## Examples

```sql
DECLARE duplicate_key CONDITION FOR 1062;
DECLARE bad_state CONDITION FOR SQLSTATE '45000';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
