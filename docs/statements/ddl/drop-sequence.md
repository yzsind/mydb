# `DROP SEQUENCE`

## Description

Drops one or more sequence objects.

## Syntax

```ebnf
<drop-sequence> ::= 'DROP SEQUENCE' [ 'IF EXISTS' ]
                    <qualified-name> { ',' <qualified-name> }
```

## Behavior and Limitations

- After the drop, calls such as nextval/currval return an object-not-exist error.
- IF EXISTS turns missing objects into Notes; tables with the same name are not dropped.
- Sequence values already written to business tables are unaffected.

## Examples

```sql
DROP SEQUENCE invoice_seq, legacy_seq;
DROP SEQUENCE IF EXISTS missing_seq;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
