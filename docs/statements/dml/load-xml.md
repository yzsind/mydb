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

- The default row element is `<row>`. ROWS IDENTIFIED BY chooses the row element name; the attribute, child-element and named-field forms are described below.
- LOCAL, the FILE privilege, `secure_file_priv`, REPLACE/IGNORE, type conversion, and trigger semantics are identical to LOAD DATA.
- This is a row-element loader, not a general validating XML/XSD processor. It extracts recognized row elements; no matching row elements produces no imported rows. An incomplete trailing row can be left unimported rather than reported as a document-validation error. Validate input XML before relying on a complete import.
- XML loading currently retains the complete document before row extraction, unlike the record-streaming LOAD DATA path. Input growth and batches are subject to unified memory admission; MYDB_MEMORY_TARGET is an advisory working-set target, not a hard allocation ceiling.

## Privileges, Results, and Boundaries

The same INSERT/FILE/DELETE and LOCAL capability rules as LOAD DATA apply. The default tag supports these equivalent row shapes:

```xml
<rows>
  <row id="1" name="Ada"/>
  <row><id>2</id><name>Lin</name></row>
  <row><field name="id">3</field><field name="name">Sam</field></row>
</rows>
```

Map the fields to a target with matching names or an explicit column/user-variable list. The loader decodes the supported entity/escape forms and recognizes supported xsi:nil NULL cases; it is not an external-entity fetcher or namespace-aware schema validator. Nested same-name row elements and incomplete input are outside a complete-document validation guarantee. Compare expected source record counts with Records/Skipped/Warnings rather than assuming a successful scan validates the document.

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
