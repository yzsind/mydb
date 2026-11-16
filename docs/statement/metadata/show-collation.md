# `SHOW COLLATION`

## Description

Lists the supported collations.

## Syntax

```ebnf
<show-collation> ::= 'SHOW COLLATION'
                     [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns Collation, Charset, Id, Default, Compiled, Sortlen, Pad_attribute.
- Only collations that are registered and implement comparison semantics appear as available entries.

## Result and Visibility Details

LIKE matches Collation; WHERE can filter Charset or Id. Default/Compiled are catalog flags, Sortlen is a compatibility sort-length value, and Pad_attribute distinguishes PAD SPACE from NO PAD.

See [ordered columns and wire types](result-columns.md#show-collation). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW COLLATION WHERE Charset = 'utf8mb4';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
