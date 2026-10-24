# `CASE Statement`

## Description

Selects a stored program branch by value or search condition.

## Syntax

```ebnf
<case> ::= 'CASE' [ <expr> ]
           { 'WHEN' <expr> 'THEN' { <routine-statement> } }
           [ 'ELSE' { <routine-statement> } ]
           'END CASE'
```

## Behavior and Limitations

- With an operand, the operand is compared against each WHEN; without an operand, each WHEN is a Boolean condition.
- When nothing matches and there is no ELSE, a condition is raised following the stored program CASE rules.

## Examples

```sql
CASE v_state
  WHEN 'new' THEN SET v_code = 1;
  WHEN 'done' THEN SET v_code = 2;
  ELSE SET v_code = 0;
END CASE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
