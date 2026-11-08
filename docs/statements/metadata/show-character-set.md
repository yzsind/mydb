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

## Result and Visibility Details

LIKE matches Charset. Maxlen is maximum bytes per character. This registry listing is readable without PROCESS and does not allocate a new connection charset.

See [ordered columns and wire types](result-columns.md#show-character-set). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CHARACTER SET LIKE 'utf8%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
