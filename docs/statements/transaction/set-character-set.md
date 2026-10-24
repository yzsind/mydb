# `SET CHARACTER SET / SET CHARSET`

## Description

Sets the client and result character sets, and makes the connection charset follow the current database default.

## Syntax

```ebnf
<set-character-set> ::= 'SET'
                         ( 'CHARACTER SET' | 'CHARSET' )
                         ( 'DEFAULT' | <identifier> )
```

## Behavior and Limitations

- The difference from SET NAMES is that character_set_connection takes the current database's character set instead of the specified value directly.
- An unknown character set returns an error; DEFAULT restores the default combination.

## Examples

```sql
SET CHARACTER SET latin1;
SET CHARSET DEFAULT;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
