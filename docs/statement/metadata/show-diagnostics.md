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

## Result and Visibility Details

SHOW ERRORS keeps Error-level conditions; SHOW WARNINGS also includes Warning and Note. Reading diagnostics does not clear them. max_error_count limits retained rows, while COUNT(*) and warning/error counters can be larger. Result-set maintenance note rows need not appear in SHOW WARNINGS.

See [ordered columns and wire types](result-columns.md#show-warnings-and-errors). Reading this metadata does not itself commit user DML.

## Examples

```sql
INSERT IGNORE INTO customer(id, email) VALUES (1, NULL);
SHOW WARNINGS LIMIT 10;
SHOW COUNT(*) WARNINGS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
