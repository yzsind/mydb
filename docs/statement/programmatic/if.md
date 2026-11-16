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

## Context, Errors, and Lifecycle

This is an inner stored-program statement, not the IF(condition,a,b) expression. Conditions convert under SQL truth rules; NULL does not select the branch. ELSEIF is one keyword and nesting requires matching END IF. Place the snippet in a procedure/function/trigger/event body with its variables declared first. See [body contexts](README.md#execution-contexts).

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
