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
- `LOW_PRIORITY/CONCURRENT` are compatibility modifiers and do not provide MyISAM scheduling/concurrent-insert guarantees in the native engine.

## Privileges, Results, and Boundaries

Server-side INFILE requires INSERT plus global FILE; REPLACE additionally requires DELETE. LOCAL instead requires CLIENT_LOCAL_FILES and local_infile enabled on the server; a disabled server setting returns 3948. LOCAL without REPLACE follows IGNORE semantics for eligible duplicate/conversion errors. It still rejects undecodable input, foreign-key violations and rows outside an explicit partition set (1366/1452/1748); the statement rolls back.

LOAD DATA is not preparable. Non-LOCAL input uses the server's secure_file_priv and path checks (1290 when disallowed). Data changes participate in the current transaction; user-variable SET targets remain session state. Inspect Records/Deleted/Skipped/Warnings and SHOW WARNINGS after loading. Field-count diagnostics are 1261/1262. [Bulk-load compatibility](../../mysql80-compatibility.md#load-data--load-xml) records the retained integrity policy.

## Field and Line Options

```ebnf
<field-option> ::= 'TERMINATED BY' <string-literal>
                 | [ 'OPTIONALLY' ] 'ENCLOSED BY' <string-literal>
                 | 'ESCAPED BY' <string-literal>
<line-option> ::= 'STARTING BY' <string-literal>
                | 'TERMINATED BY' <string-literal>
```

FIELDS and COLUMNS are synonyms. Defaults are field terminator tab, no enclosure, backslash escaping, empty line prefix and newline terminator. ENCLOSED BY and ESCAPED BY take one character or the empty string; empty escape disables escape processing. `\N` is SQL NULL with backslash escaping, distinct from an empty field. CHARACTER SET describes file data; the file name is a server path or a negotiated LOCAL client name, not a statement-text charset declaration. IGNORE n LINES/ROWS skips input records before normal mapping.

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
