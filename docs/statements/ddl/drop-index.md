# `DROP INDEX`

## Description

Drops a named index from a table.

## Syntax

```ebnf
<drop-index> ::= 'DROP INDEX' <identifier> 'ON' <qualified-name>
```

## Behavior and Limitations

- Requires the ALTER/INDEX privilege and acquires the DDL metadata lock.
- The primary key cannot be dropped with this statement; use ALTER TABLE DROP PRIMARY KEY instead.
- A missing index returns the corresponding error; after the drop, plan caches and statistics are invalidated.
- Index retirement manifests commit atomically with the definition. Failed physical cleanup is retried during maintenance and restart; recreating the same index name uses a different stable identity. Cleanup failure after commit does not turn the successful DROP into a statement failure.
- Relaxed durability defers physical cleanup until the retirement manifest is durable. Physical replicas reclaim old files without creating local WAL or deleting source-owned retirement records.

## Examples

```sql
DROP INDEX idx_order_customer ON orders;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
