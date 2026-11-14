# `DROP TABLESPACE / DROP UNDO TABLESPACE`

## Description

mydb does not currently support TABLESPACE; valid statements return error 1235. The syntax below exists to recognize the statement and return an explicit error.

## Syntax

```ebnf
<drop-tablespace> ::= 'DROP' [ 'UNDO' ] 'TABLESPACE' <identifier>
                      { <tablespace-option> }
```

## Behavior and Limitations

- With sufficient statement privileges, recognized valid forms return 1235 (ER_NOT_SUPPORTED_YET), including a nonexistent target. Authentication/authorization errors can precede that unsupported error.
- No names, attributes, or dedicated catalog views are registered; no compatibility success or warnings are produced.
- Syntax errors still return parse errors.

## Examples

```sql
DROP TABLESPACE ts_archive ENGINE=InnoDB;
DROP UNDO TABLESPACE undo_1;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [Support and compatibility status](../README.md#support-and-compatibility-status)
