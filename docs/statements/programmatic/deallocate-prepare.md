# `DEALLOCATE PREPARE`

## Description

Releases a named prepared statement. `DROP PREPARE` is a synonymous form.

## Syntax

```ebnf
<deallocate> ::= ( 'DEALLOCATE' | 'DROP' ) 'PREPARE' <identifier>
```

## Behavior and Limitations

- After release the name can no longer be executed; a missing name returns the dedicated prepared statement error.
- Disconnecting a session automatically releases all of its named prepared statements.

## Examples

```sql
DEALLOCATE PREPARE find_customer;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
