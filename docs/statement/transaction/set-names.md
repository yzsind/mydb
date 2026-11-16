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

- DEFAULT selects the built-in utf8mb4 client character set and its registered default collation (unless a valid COLLATE is supplied).
- COLLATE must belong to the chosen character set; a mismatch returns an error.
- Affects SQL text decoding, string literals, and result encoding, and participates in the relevant semantic cache key.

## Transaction and Error Details

The selected charset is assigned to character_set_client, character_set_connection and character_set_results; the selected/default collation becomes collation_connection. No current data transaction is committed. Read back all four values to verify a connection setup:

```sql
SET NAMES utf8mb4 COLLATE utf8mb4_bin;
SELECT @@character_set_client, @@character_set_connection,
       @@character_set_results, @@collation_connection;
```

## Examples

```sql
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET NAMES DEFAULT;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
