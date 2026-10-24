# `SET AUTOCOMMIT`

## Description

Enables or disables autocommit for the session.

## Syntax

```ebnf
<set-autocommit> ::= 'SET' [ 'SESSION' | 'LOCAL' ]
                     [ '@@' [ 'SESSION' '.' ] ] 'autocommit'
                     '=' ( '0' | '1' | 'ON' | 'OFF' )
```

## Behavior and Limitations

- After setting it to 0, transactional statements enter an explicit commit cycle; setting it back to 1 commits the current transaction following MySQL rules.
- GLOBAL autocommit is not supported by this dedicated statement; the general system variable rules validate the scope.
- An invalid value returns 1231; there is no silent conversion of non-zero strings.

## Examples

```sql
SET autocommit = 0;
UPDATE counter SET value = value + 1 WHERE id = 1;
COMMIT;
SET autocommit = 1;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
