# `IF`

## Description

Executes stored program statement branches conditionally.

## Syntax

```ebnf
<if> ::= 'IF' <expr> 'THEN' { <routine-statement> }
         { 'ELSEIF' <expr> 'THEN' { <routine-statement> } }
         [ 'ELSE' { <routine-statement> } ]
         'END IF'
```

## Behavior and Limitations

- Conditions use SQL three-valued logic; TRUE enters the branch, FALSE/NULL continue matching.
- Only the first matching branch executes.

## Examples

```sql
IF v_total > 1000 THEN
  SET v_level = 'large';
ELSEIF v_total > 0 THEN
  SET v_level = 'normal';
ELSE
  SET v_level = 'empty';
END IF;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
