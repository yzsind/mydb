# `SET AUTOCOMMIT`

## Description

Enables or disables autocommit for the session.

## Syntax

```ebnf
<set-autocommit> ::= 'SET' [ 'SESSION' | 'LOCAL' ]
                     [ '@@' [ 'SESSION' '.' ] ] 'autocommit'
                     '=' ( 'DEFAULT' | <expr> )
```

## Behavior and Limitations

- After setting it to 0, transactional statements enter an explicit commit cycle; setting it back to 1 commits the current transaction following MySQL rules.
- GLOBAL autocommit is not supported by this dedicated statement; the general system variable rules validate the scope.
- An invalid value returns 1231; there is no silent conversion of non-zero strings.

## Transaction and Error Details

Values are validated as the boolean autocommit system variable; DEFAULT and accepted numeric/string boolean spellings use the general SET path. Invalid values such as 2 return 1231. `SET GLOBAL autocommit=...` uses the general registry/scope path, not a distinct transaction command.

Changing 0 to 1 commits; COMMIT while autocommit remains 0 leaves the next transaction to start lazily. Ordinary SET autocommit is prohibited during a live XA branch. Check `SELECT @@session.autocommit` before relying on a transaction boundary.

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
