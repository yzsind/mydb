# `SHOW WARNINGS / ERRORS`

## Description

Reads the warnings/errors from the current session's diagnostics area, or returns their count.

## Syntax

```ebnf
<show-diagnostics> ::= 'SHOW' ( 'WARNINGS' | 'ERRORS' )
                       [ 'LIMIT' ( <row-count>
                         | <offset> ',' <row-count>
                         | <row-count> 'OFFSET' <offset> ) ]
                       | 'SHOW COUNT' '(' '*' ')'
                         ( 'WARNINGS' | 'ERRORS' )
```

## Behavior, Privileges, and Results

- The plain form returns Level, Code, and Message; ERRORS keeps only Error-level conditions.
- The COUNT form returns the count; reading the diagnostics area does not clear it.
- A new statement usually replaces/updates the current statement diagnostics, so this should run immediately after the target statement.

## Examples

```sql
INSERT IGNORE INTO customer(id, email) VALUES (1, NULL);
SHOW WARNINGS LIMIT 10;
SHOW COUNT(*) WARNINGS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
