# `DROP TABLESPACE / DROP UNDO TABLESPACE`

## Description

mydb does not currently support TABLESPACE; valid statements return error 1235. The syntax below exists to recognize the statement and return an explicit error.

## Syntax

```ebnf
<drop-tablespace> ::= 'DROP' [ 'UNDO' ] 'TABLESPACE' <identifier>
                      { <tablespace-option> }
```

## Behavior and Limitations

- Valid statements uniformly return 1235 (ER_NOT_SUPPORTED_YET), including the case where the target name does not exist.
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
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
