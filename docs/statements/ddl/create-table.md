# `CREATE TABLE`

## Description

Creates a persistent table or a session temporary table, defining columns, constraints, indexes, partitions, and table options.

## Syntax

```ebnf
<create-table> ::= 'CREATE' [ 'TEMPORARY' ] 'TABLE'
                   [ 'IF NOT EXISTS' ] <qualified-name>
                   '(' <table-element> { ',' <table-element> } ')'
                   [ <table-option> { <table-option> } ]
                   [ <partition-clause> ]
                   [ 'ON COMMIT' ( 'PRESERVE ROWS' | 'DELETE ROWS' | 'DROP' ) ]
<table-element> ::= <column-definition> | <table-constraint> | <index-definition>
<column-definition> ::= <identifier> <data-type>
                        { <column-option> }
<column-option> ::= 'NULL' | 'NOT NULL' | 'DEFAULT' <expr>
                    | 'AUTO_INCREMENT' | 'INVISIBLE' | 'VISIBLE'
                    | 'GENERATED ALWAYS AS' '(' <expr> ')' [ 'VIRTUAL' | 'STORED' ]
                    | 'CHECK' '(' <expr> ')'
```

## Behavior and Limitations

- Supports primary keys, unique keys, CHECK, foreign keys, generated columns, invisible columns, column/table character sets, and the implemented forms of B-tree, FULLTEXT, SPATIAL, vector, and multi-valued indexes.
- Table options and partitions are accepted only within the documented syntax and limitations. `ENGINE=InnoDB` and `ENGINE=mydb` select the same native engine; unknown engines return 1286. Named TABLESPACE options are unsupported and return 1235. Accepted metadata-only options such as ROW_FORMAT are persisted and diagnosed; see [compatibility](../../mysql80-compatibility.md#table-ddl).
- TEMPORARY tables belong to the session and take precedence over persistent tables of the same name; ON COMMIT behavior applies only to supported temporary table forms.
- Functional string indexes preserve the expression result collation fixed at creation. Numeric keys preserve DECIMAL precision and scale. Expressions referencing TIMESTAMP are maintained but excluded from query access and ordering because their result may depend on the reader's time zone; FORCE INDEX does not bypass this restriction.
- Long-lived indexes and table data go into the unified storage and memory governor; once creation succeeds, the data dictionary is the authoritative source of the catalog.

## Examples

```sql
CREATE TABLE customer (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_customer_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TEMPORARY TABLE work_queue (
  id BIGINT PRIMARY KEY,
  payload JSON
) ON COMMIT DELETE ROWS;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
