# `CHECKSUM TABLE`

## Description

Computes a stable checksum digest over the binary serialization of table rows.

## Syntax

```ebnf
<checksum-table> ::= 'CHECKSUM TABLE' <table-list>
                     [ 'QUICK' | 'EXTENDED' ]
```

## Behavior, Privileges, and Limitations

- The default/EXTENDED mode computes a CRC32 per row and then aggregates; the result is used for same-implementation data consistency checks.
- QUICK uses the fast path only when the table has the CHECKSUM option enabled; otherwise it returns NULL, matching MySQL's probing semantics.
- Under concurrent modifications, the result corresponds to the snapshot visible to the statement.

## Examples

```sql
CHECKSUM TABLE orders EXTENDED;
CHECKSUM TABLE customer QUICK;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
