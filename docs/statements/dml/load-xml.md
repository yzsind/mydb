# `LOAD XML`

## Description

Extracts fields from row elements in an XML file and bulk-inserts them into a table, reusing the LOAD DATA paths for privileges, conflict handling, and DML constraints.

## Syntax

```ebnf
<load-xml> ::= 'LOAD XML' [ 'LOW_PRIORITY' | 'CONCURRENT' ]
               [ 'LOCAL' ] 'INFILE' <string-literal>
               [ 'REPLACE' | 'IGNORE' ]
               'INTO TABLE' <qualified-name>
               [ 'CHARACTER SET' <identifier> ]
               [ 'ROWS IDENTIFIED BY' <string-literal> ]
               [ 'IGNORE' <unsigned-integer> ( 'LINES' | 'ROWS' ) ]
               [ '(' <load-target> { ',' <load-target> } ')' ]
               [ 'SET' <assignment> { ',' <assignment> } ]
```

## Behavior and Limitations

- The default row element and field mapping follow the current LOAD XML parser; `ROWS IDENTIFIED BY` can specify the element name.
- LOCAL, the FILE privilege, `secure_file_priv`, REPLACE/IGNORE, type conversion, and trigger semantics are identical to LOAD DATA.
- Unsupported XML structures or options return an explicit error; the entire document is never silently imported as a single column.
- Large inputs are processed in batches and are subject to the unified memory target.

## Examples

```sql
LOAD XML INFILE '/var/lib/mydb-files/customer.xml'
INTO TABLE customer
ROWS IDENTIFIED BY '<customer>';

LOAD XML LOCAL INFILE 'customer.xml'
IGNORE INTO TABLE customer (id, name);
```

## Related Statements

- [LOAD DATA](load-data.md)
- [INSERT](insert.md)
