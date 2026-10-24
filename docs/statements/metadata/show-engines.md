# `SHOW ENGINES`

## Description

Lists the storage engine compatibility catalog. `SHOW STORAGE ENGINES` is a synonym.

## Syntax

```ebnf
<show-engines> ::= 'SHOW' [ 'STORAGE' ] 'ENGINES'
```

## Behavior, Privileges, and Results

- mydb has a single native physical engine; names such as InnoDB are shown as compatibility metadata.
- Support, Comment, Transactions, XA, and Savepoints reflect actual capabilities and do not imply that separate engine implementations can be created.

## Examples

```sql
SHOW ENGINES;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
