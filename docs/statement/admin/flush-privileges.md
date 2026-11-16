# `FLUSH PRIVILEGES`

## Description

Refreshes privilege-related runtime state.

## Syntax

```ebnf
<flush-privileges> ::= 'FLUSH PRIVILEGES'
```

## Behavior, Privileges, and Limitations

- The authoritative source of accounts and grants is transactionally written to the data dictionary; this command advances the authorization epoch, clears authentication caches, and resets runtime failed-login state. It does not reload grants from MySQL system tables.
- Requires the global `RELOAD` privilege. It implicitly commits the current user transaction before refreshing authorization state. A later `ROLLBACK` cannot undo that transaction.

## Privileges, Transaction, and Result Details

This statement implicitly commits before changing authorization state. The following sequence leaves the inserted row committed:

```sql
START TRANSACTION;
INSERT INTO counter(id,value) VALUES(100,1);
FLUSH PRIVILEGES;
ROLLBACK;
SELECT * FROM counter WHERE id=100; -- Row remains.
```

GRANT/REVOKE already publishes catalog state; FLUSH is not required after supported account SQL. It does not reload edited mysql.* rows.

## Examples

```sql
FLUSH PRIVILEGES;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
