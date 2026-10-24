# `SELECT ... INTO OUTFILE / DUMPFILE`

## Description

Has the server write query results directly to a file. `OUTFILE` exports with row and field formatting; `DUMPFILE` concatenates the raw fields of at most one row without delimiters.

## Syntax

```ebnf
<select-into-file> ::= <query-expression> 'INTO'
                       ( 'OUTFILE' <string-literal> <outfile-options>
                       | 'DUMPFILE' <string-literal> )
<outfile-options>  ::= [ 'CHARACTER SET' <identifier> ]
                       [ 'FIELDS' <field-option> { <field-option> } ]
                       [ 'LINES' <line-option> { <line-option> } ]
<field-option>     ::= 'TERMINATED BY' <string-literal>
                       | [ 'OPTIONALLY' ] 'ENCLOSED BY' <string-literal>
                       | 'ESCAPED BY' <string-literal>
<line-option>      ::= 'STARTING BY' <string-literal>
                       | 'TERMINATED BY' <string-literal>
```

## Behavior and Limitations

- Requires both the `SELECT` privilege on the queried objects and the global `FILE` privilege.
- The target must be a new file the server can create; existing files are never overwritten.
- `secure_file_priv`, parent-directory normalization, and symlink checks restrict writable paths. `character_set_filesystem` affects filename interpretation.
- Both regular and streaming queries write directly to a cursor without full materialization; after success, affected rows and `ROW_COUNT()` report the number of rows written.
- `DUMPFILE` accepts multiple columns in one row and writes one NUL byte for a SQL NULL field. More than one row returns 1172; a failure may leave a partially written file.
- The writer buffer is billed by capacity to StorageScratch under unified hard memory admission. `MYDB_MEMORY_TARGET` is an advisory sizing target.

## Examples

```sql
SELECT id, name
FROM customer
ORDER BY id
INTO OUTFILE '/var/lib/mydb-files/customer.tsv'
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';

SELECT payload
INTO DUMPFILE '/var/lib/mydb-files/object.bin'
FROM blobs WHERE id = 7;
```

Paths must fall inside the server-allowed export directory; the examples do not create files on the client machine.

## Related Statements

- [SELECT](select.md)
- [LOAD DATA](../dml/load-data.md)
