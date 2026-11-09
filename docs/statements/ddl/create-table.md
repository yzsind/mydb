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

## Definition Elements

```ebnf
<column-definition> ::= <identifier> <data-type> { <column-option> }
<column-option> ::= 'NULL' | 'NOT NULL' | 'DEFAULT' <expr>
                 | 'AUTO_INCREMENT' | 'VISIBLE' | 'INVISIBLE'
                 | 'COMMENT' <string-literal>
                 | [ 'CHARACTER SET' <charset> ] 'COLLATE' <collation>
                 | 'ON UPDATE' <expr>
                 | [ 'GENERATED ALWAYS' ] 'AS' '(' <expr> ')' [ 'VIRTUAL' | 'STORED' ]
                 | 'PRIMARY KEY' | 'UNIQUE' [ 'KEY' ] | 'CHECK' '(' <expr> ')'
<table-constraint> ::= [ 'CONSTRAINT' <identifier> ]
                      ( 'PRIMARY KEY' '(' <index-key-list> ')'
                      | 'UNIQUE' [ 'KEY' ] [ <identifier> ] '(' <index-key-list> ')'
                      | 'CHECK' '(' <expr> ')' [ [ 'NOT' ] 'ENFORCED' ]
                      | 'FOREIGN KEY' [ <identifier> ] '(' <identifier-list> ')'
                        'REFERENCES' <qualified-name> '(' <identifier-list> ')'
                        { 'ON' ( 'DELETE' | 'UPDATE' ) <referential-action> } )
<referential-action> ::= 'RESTRICT' | 'NO ACTION' | 'CASCADE' | 'SET NULL'
```

See [index keys and options](create-index.md#index-keys-and-options) for inline INDEX/KEY definitions and [Data Types](../../data-types.md) for legal type/default/generated-expression combinations. Generated/CHECK expressions cannot allocate sequence values or invoke external side effects. Character set can also be specified without COLLATE; each collation must match its charset.

## Table Options

| Option group | Current effect |
| --- | --- |
| ENGINE=InnoDB or mydb | The same native engine; unknown names return 1286 |
| DEFAULT CHARACTER SET / CHARSET / COLLATE | Stored defaults used when deriving columns |
| AUTO_INCREMENT, COMMENT | Actual counter start/next value and catalog comment |
| PCTFREE/PCTUSED, Zone Map controls | Native storage/planning options within their validated ranges |
| ROW_FORMAT, STATS_*, KEY_BLOCK_SIZE, MAX_ROWS and other documented compatibility options | Persisted/echoed metadata; no separate InnoDB physical layout |
| Named TABLESPACE | Unsupported, 1235; no physical tablespace or name catalog |
| Partition/subpartition definition | Supported RANGE/LIST/HASH/KEY families and validated actions; see [partitioning](../../mysql80-compatibility.md#partitioning) |

An accepted metadata option must not be interpreted as a new physical layout, encryption guarantee, or lock algorithm. Use [compatibility details](../../mysql80-compatibility.md#table-ddl) for exact retained options.

## Privileges, Transactions, and Dependencies

Persistent CREATE needs CREATE; temporary CREATE needs CREATE TEMPORARY TABLES in the schema. Declared foreign keys also need REFERENCES on their referenced tables. Permanent table DDL implicitly commits. Successful temporary-table DDL has its own transaction boundary but is not undone by a later user ROLLBACK.

Identifiers are limited to 64 characters and tables to 1017 live columns. Duplicate objects return 1050; IF NOT EXISTS preserves the existing object and emits a Note, without validating that it matches the requested shape. Temporary tables shadow persistent names in ordinary object resolution. The session temporary table default is ON COMMIT PRESERVE ROWS; DELETE ROWS and DROP are explicit lifecycle requests.

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
