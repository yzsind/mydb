# `CASE Statement`

## Description

Selects a stored program branch by value or search condition.

## Syntax

```ebnf
<case> ::= 'CASE' [ <expr> ]
           'WHEN' <expr> 'THEN' { <routine-statement> }
           { 'WHEN' <expr> 'THEN' { <routine-statement> } }
           [ 'ELSE' { <routine-statement> } ]
           'END CASE'
```

## Behavior and Limitations

- With an operand, the operand is compared against each WHEN; without an operand, each WHEN is a Boolean condition.
- When nothing matches and there is no ELSE, error 1339 (CASE not found) is raised.

## Context, Errors, and Lifecycle

This is a stored-program statement ending in END CASE, distinct from a CASE expression ending in END. The operand form uses equality comparison; NULL does not equal NULL, so use a searched WHEN ... IS NULL for that branch. No match and no ELSE returns 1339. Examples require declared v_state/v_code in a surrounding body.

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
