# `CREATE TABLESPACE / CREATE UNDO TABLESPACE`

## Description

mydb does not currently support TABLESPACE; valid statements return error 1235. The syntax below exists to recognize the statement and return an explicit error.

## Syntax

```ebnf
<create-tablespace> ::= 'CREATE' [ 'UNDO' ] 'TABLESPACE' <identifier>
                        [ 'ADD DATAFILE' <string-literal> ]
                        { <tablespace-option> }
<tablespace-option> ::= 'ENGINE' [ '=' ] <identifier>
                        | 'FILE_BLOCK_SIZE' [ '=' ] <size>
                        | 'AUTOEXTEND_SIZE' [ '=' ] <size>
                        | 'MAX_SIZE' [ '=' ] <size>
                        | 'COMMENT' [ '=' ] <string-literal>
                        | 'ENCRYPTION' [ '=' ] <string-literal>
```

## Behavior and Limitations

- With sufficient statement privileges, recognized valid forms return 1235 (ER_NOT_SUPPORTED_YET), including a nonexistent target. Authentication/authorization errors can precede that unsupported error.
- No names, attributes, or dedicated catalog views are registered; no compatibility success or warnings are produced.
- Syntax errors still return parse errors.

## Examples

```sql
CREATE TABLESPACE ts_archive ADD DATAFILE 'archive.ibd' ENGINE=InnoDB;
CREATE UNDO TABLESPACE undo_1 ADD DATAFILE 'undo_1.ibu';
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [Support and compatibility status](../README.md#support-and-compatibility-status)
