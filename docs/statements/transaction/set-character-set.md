# `SET CHARACTER SET / SET CHARSET`

## Description

Sets the client and result character sets. The current implementation preserves character_set_connection and collation_connection; it does not rebind them to the selected database defaults.

## Syntax

```ebnf
<set-character-set> ::= 'SET'
                         ( 'CHARACTER SET' | 'CHARSET' )
                         ( 'DEFAULT' | <identifier> )
```

## Behavior and Limitations

- Unlike SET NAMES, this statement leaves character_set_connection and collation_connection unchanged. This is a current compatibility boundary, including after USE selects a database with other defaults.
- An unknown or disallowed client character set returns an error. DEFAULT selects utf8mb4 for client/results only; the connection charset/collation still remain unchanged.

## Transaction and Error Details

Client/results change together; connection charset/collation remain unchanged. This statement does not implicitly commit. To explicitly change all connection settings, use SET NAMES. Read back the four variables rather than assuming USE or SET CHARSET changed connection collation.

## Examples

```sql
SET CHARACTER SET latin1;
SET CHARSET DEFAULT;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
