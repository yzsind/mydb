# `SET NAMES`

## Description

Sets the client, connection, and result character sets in one statement, optionally with a connection collation.

## Syntax

```ebnf
<set-names> ::= 'SET NAMES'
                ( 'DEFAULT' | <identifier> )
                [ 'COLLATE' <identifier> ]
```

## Behavior and Limitations

- DEFAULT restores the server/session default character set combination.
- COLLATE must belong to the chosen character set; a mismatch returns an error.
- Affects SQL text decoding, string literals, and result encoding, and participates in the relevant semantic cache key.

## Examples

```sql
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET NAMES DEFAULT;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
