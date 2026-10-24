# `LOAD DATA`

## Description

Parses text records in bulk from a server-side file or a client LOCAL stream and inserts them into a table, with support for field/line delimiters, character sets, column mapping, user variables, and SET transformations.

## Syntax

```ebnf
<load-data> ::= 'LOAD DATA' [ 'LOW_PRIORITY' | 'CONCURRENT' ]
                [ 'LOCAL' ] 'INFILE' <string-literal>
                [ 'REPLACE' | 'IGNORE' ]
                'INTO TABLE' <qualified-name>
                [ 'PARTITION' '(' <identifier-list> ')' ]
                [ 'CHARACTER SET' <identifier> ]
                [ 'FIELDS' <field-option> { <field-option> } ]
                [ 'LINES' <line-option> { <line-option> } ]
                [ 'IGNORE' <unsigned-integer> ( 'LINES' | 'ROWS' ) ]
                [ '(' <load-target> { ',' <load-target> } ')' ]
                [ 'SET' <assignment> { ',' <assignment> } ]
<load-target> ::= <identifier> | <user-variable>
```

## Behavior and Limitations

- Non-LOCAL paths reside on the server, require the FILE privilege, and are restricted by `secure_file_priv`; LOCAL data is uploaded over the protocol and controlled by client capability negotiation.
- `REPLACE` and `IGNORE` handle conflicts with replace or warning-and-skip semantics, respectively.
- Fields and SET expressions go through type conversion, DEFAULT, generated columns, CHECK, foreign keys, and triggers.
- Large files are read and written in batches and are never required to reside fully in memory; protocol buffers and batches are billed by capacity to the unified governor.
- A successful OK packet reports Records, Deleted, Skipped, and Warnings; degradation of row-level errors depends on IGNORE and sql_mode.
- `LOW_PRIORITY/CONCURRENT` takes effect only to the extent expressible in the current single-engine lock model; any part that cannot be honored produces an explicit warning.

## Examples

```sql
LOAD DATA INFILE '/var/lib/mydb-files/customer.csv'
INTO TABLE customer
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name, @created)
SET created_at = STR_TO_DATE(@created, '%Y-%m-%d');

LOAD DATA LOCAL INFILE 'orders.tsv'
REPLACE INTO TABLE orders
FIELDS TERMINATED BY '\t';
```

## Related Statements

- [SELECT INTO OUTFILE](../query/select-into-file.md)
- [INSERT](insert.md)
- [LOAD XML](load-xml.md)
