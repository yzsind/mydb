# SQL ROWID

`ROWID` is a read-only `BIGINT UNSIGNED` pseudocolumn that locates a heap row
within the table named by the SQL statement. It works with partitioned and
nonpartitioned base tables, including tables without a primary key.

```sql
SELECT u.ROWID AS rid, u.* FROM users AS u;
SELECT * FROM users WHERE ROWID = ?;
SELECT * FROM users WHERE ROWID IN (?, ?, ?);
UPDATE users SET name = ? WHERE ROWID = ?;
DELETE FROM users WHERE ROWID = ?;

INSERT INTO users(name) VALUES ('Alice') RETURNING ROWID, id;
UPDATE users SET name = 'Bob' WHERE ROWID = ?
RETURNING old.ROWID AS old_rid, new.ROWID AS new_rid;
DELETE FROM users WHERE ROWID = ? RETURNING ROWID;
```

Bind addresses as exact unsigned 64-bit integers. Drivers must preserve values
above `2^53` and `i64::MAX`; converting them through floating point or signed
64-bit integers can change their values. Result metadata uses MySQL LONGLONG
with the UNSIGNED flag, including prepared statements and empty results.

## Address layout

The high 16 bits contain a stable table-local partition tag, the next 32 bits
contain the page number, and the low 16 bits contain the slot number:

```text
ROWID = (partition_tag << 48) | (page_id << 16) | slot_id
```

Nonpartitioned tables use tag zero. Partitioned tables use a nonzero tag for
each physical leaf; subpartitions have their own tags. Tags are persisted in
the catalog and do not depend on partition order or truncated storage IDs.
The existing limit of 8192 physical leaves remains unchanged.

Integer equality and eligible IN predicates can address the corresponding
heap directly. They do not require a primary or secondary index. Explicit
`PARTITION` selections still restrict access. Missing addresses return no rows;
MVCC, locks, permissions, and storage errors follow the ordinary SQL rules.
Other comparisons and conversions retain the usual integer expression semantics.
For large address sets the optimizer compares page-ordered lookup costs with
a heap scan. EXPLAIN reports `RowIdLookup` or `RowIdBatchLookup` when selected;
ANALYZE includes candidate, duplicate, fetch, and miss counts.

## Names and projections

- A real column named `rowid` takes precedence, including an INVISIBLE column.
  That table cannot access the pseudocolumn through the same name.
- Names and qualifiers follow normal case and alias rules. Ambiguous
  unqualified references in joins return an error.
- `*`, `t.*`, and `RETURNING *` do not automatically include the pseudocolumn.
- The unmatched side of an outer join has a NULL ROWID.
- Views, CTEs, and derived tables expose ROWID only through an explicit
  projection. Intermediate rows do not receive new physical addresses.
- System views and table functions do not have an implicit physical ROWID.
- The pseudocolumn is not listed by SHOW COLUMNS, DESCRIBE, or
  information_schema.columns. It cannot be assigned, indexed, or referenced
  in a generated column, default, CHECK, or partition definition.

Reading the pseudocolumn requires table-level SELECT access, including access
inherited from database/global grants and roles. Write statements also require
the usual write privileges; temporary-table and view authorization rules apply.

## Lifetime and transactions

ROWID is a reusable physical address, not a permanent business identifier.
It has no table ID, database ID, storage generation, UUID, or checksum.
The same integer can identify unrelated rows in different tables or instances.

Ordinary updates and forwarding within one heap preserve the root address.
An update that moves a row to another partition produces a new address.
UPDATE RETURNING exposes the actual OLD and NEW addresses; DELETE RETURNING
exposes the old address, and INSERT RETURNING exposes the inserted address.

Deleted rows remain subject to MVCC and undo retention. After reclamation, a
new row can reuse the same slot. TRUNCATE, OPTIMIZE, COPY, repartitioning,
partition-tag reuse, and temporary-table recreation can also reuse addresses.
An old value is not guaranteed to continue identifying its original row, nor
is it guaranteed to remain invalid forever. Use a primary or unique key when
an application needs a persistent logical identity.

Surviving partition leaves retain their tags during unrelated partition DDL.
Renames and index-only changes preserve heap addresses. Restart, crash recovery,
and physical backups preserve addresses when the physical layout is unchanged.
Native physical replicas retain the published tag mapping, subject to replay
progress and visibility. Logical import and MySQL ROW/FULL replication do not
guarantee identical addresses. The pseudocolumn is not part of logical row
events unless its value was explicitly stored in an ordinary user column.

This extension does not add Oracle ROWID/UROWID data types, text address
formats, CHARTOROWID/ROWIDTOCHAR, or trigger OLD/NEW pseudocolumn fields.

Persisted partition tags require catalog format 69. Earlier development data
directories must be rebuilt; see [Format Versions](storage-format-versions.md).
