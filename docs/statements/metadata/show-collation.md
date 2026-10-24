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

## Examples

```sql
SHOW COLLATION WHERE Charset = 'utf8mb4';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
