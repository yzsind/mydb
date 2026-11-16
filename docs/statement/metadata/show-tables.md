# `SHOW TABLES`

## Description

Lists the visible tables and views in a database.

## Syntax

```ebnf
<show-tables> ::= 'SHOW' [ 'FULL' ] 'TABLES'
                 [ ( 'FROM' | 'IN' ) <identifier> ]
                 [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- The current database is used when the database is omitted. Plain SHOW TABLES returns one `Tables_in_<db>` column. FULL adds `Table_type` and the mydb extension columns `TABLE_ROWS`, `DATA_LENGTH`, `INDEX_LENGTH`, `CREATE_TIME`, and `TABLE_COMMENT` (seven columns in total). This extended shape applies to user and system schemas; clients requiring MySQL's two-column listing can query `information_schema.TABLES` with an explicit projection.
- Session temporary tables are not enumerated by the persistent catalog listing; they can still shadow names during normal object lookup.

## Result and Visibility Details

LIKE matches Tables_in_<db>. Plain SHOW TABLES returns only the first column. The user-schema listing is the persistent table/view catalog; it does not enumerate session temporary tables. A temporary table can shadow a persistent name for SELECT/DESCRIBE without changing this listing. View physical metrics are NULL.

See [ordered columns and wire types](result-columns.md#show-full-tables). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW FULL TABLES FROM app WHERE Table_type = 'BASE TABLE';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
