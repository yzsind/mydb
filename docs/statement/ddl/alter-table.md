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
- Online DDL uses jobs to manage progress; COPY uses a shadow generation, and online index builds retain the heap while building private trees or ANN graphs. Progress can be monitored through information_schema.MYDB_DDL_JOBS.
- `ADD INDEX` and `ADD UNIQUE INDEX` support a trailing `WHERE <expr>` for [partial membership](../../partial-indexes.md), including the supported special-index families. The index method still determines online/locking support. Renaming a predicate column rewrites its references; drop dependent indexes before dropping that column.
- `ADD PRIMARY KEY` supports DML during the build phase by default and also accepts `ALGORITHM=INPLACE, LOCK=NONE`; prepare and publish use brief writer barriers that allow readers. It can be built together with regular and unique BTREE indexes, all published together once every validation passes.
- While the primary key is being built, the old table constraints remain in effect; on successful publication, the primary key and NOT NULL take effect together. If NULLs remain at the end, 1138 is returned; duplicate keys return 1062; on failure, already committed DML and the old definition are preserved, NULLs are not converted to default values, and the behavior is unaffected by non-strict SQL mode.
- Supported index-only BTREE/FULLTEXT/SPATIAL/HNSW/IVF additions accept `LOCK=NONE`, including atomic mixed-index batches. Vector construction, initial image logging and model training run outside writer exclusion; only bounded catch-up and publication remain inside. `LOCK=SHARED/EXCLUSIVE` preserves the requested blocking behavior. Supported primary-key replacements and column rewrites use the [online COPY path](../../online-column-type-change.md); unsupported mixes return 1846. Additive index/primary-key publication preserves older snapshots through snapshot-specific optimizer facts; physical generation changes may still return 1412 to an older snapshot.
- In strict mode, encoding-compatible NOT NULL, same-width/scale DECIMAL precision, numeric signed-to-unsigned domain narrowing, and VARBINARY/TEXT/BLOB limit validation support `LOCK=NONE` without rebuilding the heap. Integer signedness changes still require COPY because the physical encoding changes. VARCHAR shrink remains COPY because trailing-space trimming can change stored values.
- Compound column rewrites resolve newly added or changed columns before binding later actions and functional indexes, then publish one candidate atomically.
- An unqualified `RENAME TO` destination inherits the source table's schema, including for temporary tables. Permissions and metadata locks use that same destination.
- CREATE, LIKE, ALTER and RENAME enforce the same 64-character identifier and 1017-live-column limits; dropped column tombstones do not count toward the live-column limit.
- Multiple actions form the atomic intent of a single DDL statement; on failure, no half-completed new schema is exposed.
- Explicit ALGORITHM and LOCK requests are validated against the actual execution capability; unsupported combinations fail before mutation.

## Actions and Requested Algorithms

Column add/modify/change accepts FIRST or AFTER column where valid. Index actions include ADD/DROP/RENAME INDEX, visibility changes and primary-key changes. Constraint actions include CHECK enforcement and FOREIGN KEY replacement. Partition actions include supported ADD/DROP/TRUNCATE/REORGANIZE/EXCHANGE/maintenance forms. FORCE rebuilds through COPY. ALGORITHM and LOCK are comma-separated ALTER options, not unvalidated hints. [RLS enable/disable](../security/alter-row-security.md) is a separate whole-statement extension.

| Action family | Nonblocking write capability |
| --- | --- |
| Add ordinary/unique BTREE indexes, including supported ADD PRIMARY KEY batches | Online INPLACE; LOCK=NONE allowed during build, brief writer barriers at prepare/publication |
| Supported constraint-only CHECK/FK validation | Online validated path, subject to its action compatibility checks |
| Supported encoding-compatible column domain checks | Validation without heap rebuild; restrictions in the behavior section apply |
| Supported AUTO_INCREMENT additions, primary-key replacements and column COPY rewrites | Online COPY accepts LOCK=NONE within its documented scope; unsupported combinations fail explicitly |
| Index-only FULLTEXT, SPATIAL, HNSW and IVF additions, including supported mixed-index batches | Online INPLACE; LOCK=NONE allows writes during private construction; brief writer barriers at prepare/publication |
| Other mixed physical actions | Capability depends on the complete atomic plan; unsupported combinations return 1846 or an explicit unsupported error |
| Requested SHARED / EXCLUSIVE | Requests the corresponding blocking behavior, even when an online path exists |

INSTANT is only available for implemented metadata actions; it is not a synonym for forcing a physical rewrite to be fast. Unknown actions/options or action mixes with no atomic plan fail explicitly. [DDL compatibility](../../mysql80-compatibility.md#table-ddl) lists concrete supported online column and partition forms.

## Privileges, Transactions, and Dependencies

Requires ALTER on the table. Foreign-key addition also requires REFERENCES; RENAME uses DROP on source and CREATE+INSERT on destination in addition to ALTER. The ordinary persistent ALTER is an implicit-commit boundary. All actions are checked before choosing an atomic publication path; an error does not establish that earlier user DML remains pending.

DDL can wait for long transactions at preparation/publication even with LOCK=NONE. Track supported jobs through information_schema.MYDB_DDL_JOBS and CANCEL DDL JOB. A cancellation accepted before finalization prevents publication; poll the job for its terminal result. Snapshot readers can receive 1412 after physical generation replacement and must reopen their transaction. See [limits](../../limits.md) for naming/enforcement boundaries.

## Examples

```sql
ALTER TABLE customer ADD COLUMN last_seen_at TIMESTAMP NULL;
ALTER TABLE orders ADD INDEX idx_orders_customer_created (customer_id, created_at);
ALTER TABLE sales DROP PARTITION p2024;
CREATE TABLE customer_without_pk(id BIGINT NOT NULL);
ALTER TABLE customer_without_pk ADD PRIMARY KEY(id), ALGORITHM=INPLACE, LOCK=NONE;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
