# `GET DIAGNOSTICS`

## Description

Reads the current diagnostics area at top level or in a stored program; STACKED reads the active handler condition and is only valid inside a handler.

## Syntax

```ebnf
<get-diagnostics> ::= 'GET' [ 'CURRENT' | 'STACKED' ] 'DIAGNOSTICS'
                      ( <diag-assignment> { ',' <diag-assignment> }
                      | 'CONDITION' <expr>
                        <condition-assignment> { ',' <condition-assignment> } )
<diag-assignment> ::= <routine-target> '=' ( 'ROW_COUNT' | 'NUMBER' )
```

## Behavior and Limitations

- CURRENT reads the current diagnostics area; STACKED, valid only inside a handler, reads the condition being handled.
- Condition items support MYSQL_ERRNO, RETURNED_SQLSTATE, MESSAGE_TEXT, and source fields such as catalog/schema/table/column/cursor.

## Diagnostic Items

```ebnf
<condition-assignment> ::= <routine-target> '=' <condition-item>
<condition-item> ::= 'MYSQL_ERRNO' | 'RETURNED_SQLSTATE' | 'MESSAGE_TEXT'
                   | 'CLASS_ORIGIN' | 'SUBCLASS_ORIGIN'
                   | 'CONSTRAINT_CATALOG' | 'CONSTRAINT_SCHEMA' | 'CONSTRAINT_NAME'
                   | 'CATALOG_NAME' | 'SCHEMA_NAME' | 'TABLE_NAME'
                   | 'COLUMN_NAME' | 'CURSOR_NAME'
```

ROW_COUNT and NUMBER are statement-area items and cannot be requested as CONDITION fields. Condition items require a valid CONDITION number. Unspecified source fields are empty diagnostic strings, not evidence that the corresponding object was checked.

```sql
GET CURRENT DIAGNOSTICS @rows=ROW_COUNT, @conditions=NUMBER;
```

## Context, Errors, and Lifecycle

Top-level targets are @user variables. Within a routine, declared local variables are also valid. No global administration privilege is required to read this session's diagnostics. STACKED outside an active handler is 3004; an invalid/nonpositive/out-of-range condition number is 1758. Condition numbers are one-based. Diagnostics themselves do not read another session or execute the failed SQL again.

## Examples

```sql
GET DIAGNOSTICS v_rows = ROW_COUNT, v_count = NUMBER;
GET STACKED DIAGNOSTICS CONDITION 1
  v_errno = MYSQL_ERRNO, v_message = MESSAGE_TEXT;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
