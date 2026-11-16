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
- QUICK is currently accepted without selecting a separate fast path; it uses the same scan as the default/EXTENDED form. Do not rely on CHECKSUM=1 metadata to avoid a scan.
- Under concurrent modifications, the result corresponds to the snapshot visible to the statement.

## Privileges, Transaction, and Result Details

Requires SELECT on every target. It is a consistent read, not an implicit-commit maintenance operation; all scanned tables share the statement's protected read view, including under RC. Output is Table (VARCHAR) and Checksum (BIGINT). A missing table yields NULL checksum; a scan failure yields NULL with a warning.

The checksum aggregates serialized row checksums in the native scan order/representation. Use it for comparable mydb data/layout semantics; no numeric equality with MySQL CHECKSUM is promised. QUICK currently does not avoid the scan. An ordinary ROLLBACK does not need to undo this read.

## Examples

```sql
CHECKSUM TABLE orders EXTENDED;
CHECKSUM TABLE customer QUICK;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
