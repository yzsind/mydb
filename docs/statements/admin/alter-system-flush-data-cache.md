# `ALTER SYSTEM FLUSH DATA_CACHE`

## Description

Reclaims evictable data page cache.

## Syntax

```ebnf
<flush-data-cache> ::= 'ALTER SYSTEM FLUSH DATA_CACHE'
```

## Behavior, Privileges, and Limitations

- Honors pinned pages, active transactions, and the unified memory governor; it never discards dirty pages that have not been safely written back.
- The operation affects performance-related heat; it does not change table data or physical plan structures.

## Examples

```sql
ALTER SYSTEM FLUSH DATA_CACHE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
