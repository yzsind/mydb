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
- The name belongs to the current session; names are case-insensitive. Preparing an existing name first deallocates its old definition. If parsing or validation of the replacement fails, the old definition is not restored; EXECUTE then returns 1243.

## Context, Errors, and Lifecycle

SQL PREPARE names are distinct from numeric COM_STMT_PREPARE handles. The source must contain one preparable statement; LOAD DATA is rejected with 1295 and multi-statements with 1064. The count is subject to max_prepared_stmt_count and memory admission. PREPARE captures the selected database/parse mode and describes result metadata; EXECUTE still performs privilege and dependency checks.

A prepared statement cannot capture an expiring routine local-variable frame by textual variable name. Supply parameters through USING @variables. Re-preparation deletes the old name before validating the replacement, including when validation fails.

## Examples

```sql
PREPARE find_customer FROM 'SELECT id, name FROM customer WHERE id = ?';
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
