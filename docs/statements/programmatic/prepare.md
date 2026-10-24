# `PREPARE`

## Description

Creates a session-level named prepared statement from a string or user variable.

## Syntax

```ebnf
<prepare> ::= 'PREPARE' <identifier> 'FROM'
              ( <string-literal> | <user-variable> )
```

## Behavior and Limitations

- The source must be exactly one preparable statement; placeholders use ?.
- Parsing, parameter numbering, and the prepared statement limit are validated at PREPARE time; no text substitution is used.
- The name belongs to the current session; re-PREPARING the same name replaces or errors according to lifecycle rules.

## Examples

```sql
PREPARE find_customer FROM 'SELECT id, name FROM customer WHERE id = ?';
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
