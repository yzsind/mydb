# `SHOW CHARACTER SET`

## Description

Lists the supported character sets; CHARSET is a synonym.

## Syntax

```ebnf
<show-character-set> ::= 'SHOW'
                         ( 'CHARACTER SET' | 'CHARSET' )
                         [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns Charset, Description, Default collation, Maxlen.
- The list comes from the character set registry and does not imply that every external MySQL charset is supported.

## Examples

```sql
SHOW CHARACTER SET LIKE 'utf8%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
