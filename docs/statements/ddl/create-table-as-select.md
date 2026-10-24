# `CREATE TABLE AS SELECT`

## Description

Creates a table and loads the query result into it; columns, constraints, table options, and partitions can be declared explicitly.

## Syntax

```ebnf
<ctas> ::= 'CREATE' [ 'TEMPORARY' ] 'TABLE'
           [ 'IF NOT EXISTS' ] <qualified-name>
           [ '(' <table-element> { ',' <table-element> } ')' ]
           [ <table-option> { <table-option> } ]
           [ <partition-clause> ]
           [ 'IGNORE' | 'REPLACE' ]
           'AS' <query-expression>
```

## Behavior and Limitations

- Bare CTAS infers column names, types, origin, and nullability from the query projection; explicit definitions follow MySQL column-merge rules.
- A regular SELECT source is loaded in streamed chunks; persistent CTAS uses intents and a deferred dictionary generation to guarantee crash atomicity.
- For persistent CTAS, the CREATE and the row events enter the same GTID/XID binlog transaction.
- IGNORE/REPLACE control load conflicts; on query or write failure, no half-built visible table is left behind.

## Examples

```sql
CREATE TABLE active_customer AS
SELECT id, email FROM customer WHERE disabled = 0;

CREATE TABLE daily_total (
  day DATE PRIMARY KEY,
  amount DECIMAL(20,2) NOT NULL
) AS
SELECT DATE(created_at), SUM(total) FROM orders GROUP BY DATE(created_at);
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
