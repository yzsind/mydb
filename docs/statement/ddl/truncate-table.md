# `TRUNCATE TABLE`

## Description

Quickly empties a table and resets state tied to the table lifecycle.

## Syntax

```ebnf
<truncate-table> ::= 'TRUNCATE' [ 'TABLE' ] <qualified-name>
```

## Behavior and Limitations

- Requires the `DROP` privilege on the table and acquires the exclusive metadata lock; `ALTER` alone is not a substitute.
- Unlike DELETE without WHERE, it does not return rows one by one and offers no RETURNING; auto-increment state is handled per the table-rebuild semantics.
- States that do not permit truncation, such as foreign key references, return an explicit error.
- This is a DDL boundary; do not rely on an outer ROLLBACK to recover.
- Persistent tables publish a fresh empty physical identity before retiring the old files. Retried cleanup for the old identity cannot remove rows inserted after TRUNCATE succeeds.

Persistent tables enter the [recycle bin](../../recycle-bin.md) before their new empty physical identity is published when recycling is enabled. Restore the retained version to an unused table name; it does not overwrite the current table.

## Privileges, Transactions, and Dependencies

TRUNCATE resets AUTO_INCREMENT with the table lifecycle and does not run row-by-row DELETE triggers or provide RETURNING. It implicitly commits, including a session temporary target. Enforced referencing foreign keys can block it. Views and protected system tables are not ordinary truncate targets; supported performance/log tables have their own clear semantics. Old files are retired by physical identity after publication, so cleanup cannot remove data inserted into the new generation.

## Examples

```sql
TRUNCATE TABLE staging_event;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
