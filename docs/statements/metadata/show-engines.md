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

## Result and Visibility Details

Engine is part of the six-column output. InnoDB is a compatibility alias for the same native MVCC engine. Support and transaction flags do not mean a separate storage implementation is installed.

See [ordered columns and wire types](result-columns.md#show-engines). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW ENGINES;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
