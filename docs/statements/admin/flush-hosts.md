# `FLUSH HOSTS`

## Description

Clears host failure state and unblocks hosts blocked due to connection errors.

## Syntax

```ebnf
<flush-hosts> ::= 'FLUSH HOSTS'
```

## Behavior, Privileges, and Limitations

- mydb has no separate MySQL host cache; the command resets the account's failed_login_count to provide an equivalent unlock.
- It does not modify passwords, SSL, or account lockout policy.

## Examples

```sql
FLUSH HOSTS;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
