# `ALTER TABLE`

## Description

Modifies table structure, constraints, indexes, partitions, and table options.

## Syntax

```ebnf
<alter-table> ::= 'ALTER TABLE' <qualified-name>
                  <alter-action> { ',' <alter-action> }
<alter-action> ::= 'ADD' [ 'COLUMN' ] <column-definition>
                   | 'DROP' [ 'COLUMN' ] <identifier>
                   | 'MODIFY' [ 'COLUMN' ] <column-definition>
                   | 'CHANGE' [ 'COLUMN' ] <identifier> <column-definition>
                   | 'RENAME COLUMN' <identifier> 'TO' <identifier>
                   | 'ADD' <table-constraint-or-index>
                   | 'DROP' ( 'PRIMARY KEY' | 'INDEX' <identifier>
                              | 'FOREIGN KEY' <identifier>
                              | 'CHECK' <identifier> )
                   | 'RENAME' [ 'TO' | 'AS' ] <qualified-name>
                   | <partition-action>
                   | <table-option>
```

## Behavior and Limitations

- `FORCE` performs a physical rebuild through the COPY generation pipeline. It preserves the logical definition and data; unsupported storage/lock combinations fail explicitly.
- Supported actions are strictly validated by the executor; unknown actions return 1235/1064 and are never silently dropped.
- Online DDL uses jobs to manage progress; COPY uses a shadow generation, and online BTREE index builds retain the heap while building a private index. Progress can be monitored through information_schema.mydb_ddl_jobs.
- `ADD PRIMARY KEY` supports DML during the build phase by default and also accepts `ALGORITHM=INPLACE, LOCK=NONE`; prepare and publish use brief writer barriers that allow readers. It can be built together with regular and unique BTREE indexes, all published together once every validation passes.
- While the primary key is being built, the old table constraints remain in effect; on successful publication, the primary key and NOT NULL take effect together. If NULLs remain at the end, 1138 is returned; duplicate keys return 1062; on failure, already committed DML and the old definition are preserved, NULLs are not converted to default values, and the behavior is unaffected by non-strict SQL mode.
- `LOCK=SHARED/EXCLUSIVE` blocks writes as requested; mixed operations such as primary key plus column changes, or replacing the primary key, do not support `LOCK=NONE` and explicitly return 1846. Additive index/primary-key publication preserves older snapshots through snapshot-specific optimizer facts; physical generation changes may still return 1412 to an older snapshot.
- In strict mode, encoding-compatible NOT NULL, same-width/scale DECIMAL precision, numeric signed-to-unsigned domain narrowing, and VARBINARY/TEXT/BLOB limit validation support `LOCK=NONE` without rebuilding the heap. Integer signedness changes still require COPY because the physical encoding changes. VARCHAR shrink remains COPY because trailing-space trimming can change stored values.
- Compound column rewrites resolve newly added or changed columns before binding later actions and functional indexes, then publish one candidate atomically.
- An unqualified `RENAME TO` destination inherits the source table's schema, including for temporary tables. Permissions and metadata locks use that same destination.
- CREATE, LIKE, ALTER and RENAME enforce the same 64-character identifier and 1017-live-column limits; dropped column tombstones do not count toward the live-column limit.
- Multiple actions form the atomic intent of a single DDL statement; on failure, no half-completed new schema is exposed.
- Explicit ALGORITHM and LOCK requests are validated against the actual execution capability; unsupported combinations fail before mutation.

## Examples

```sql
ALTER TABLE customer ADD COLUMN last_login TIMESTAMP NULL;
ALTER TABLE orders ADD INDEX idx_orders_customer_created (customer_id, created_at);
ALTER TABLE sales DROP PARTITION p2024;
ALTER TABLE customer ADD PRIMARY KEY(id), ALGORITHM=INPLACE, LOCK=NONE;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
