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

## Examples

```sql
DECLARE duplicate_key CONDITION FOR 1062;
DECLARE bad_state CONDITION FOR SQLSTATE '45000';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
