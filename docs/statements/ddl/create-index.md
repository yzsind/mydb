# `CREATE INDEX`

## Description

Creates the supported forms of regular, unique, full-text, spatial, functional, covering, or vector indexes on an existing table.

## Syntax

```ebnf
<create-index> ::= 'CREATE'
                   [ 'UNIQUE' | 'FULLTEXT' | 'SPATIAL' ]
                   'INDEX' <identifier>
                   [ 'USING' ( 'BTREE' | 'HASH' ) ]
                   'ON' <qualified-name>
                   '(' <index-key> { ',' <index-key> } ')'
                   [ 'INCLUDE' '(' <identifier-list> ')' ]
                   [ 'VISIBLE' | 'INVISIBLE' ]
                   { <index-option> }
                   [ 'ALGORITHM' [ '=' ] ( 'DEFAULT' | 'INPLACE' ) ]
                   [ 'LOCK' [ '=' ] ( 'DEFAULT' | 'NONE' | 'SHARED' | 'EXCLUSIVE' ) ]
```

## Behavior and Limitations

- Vector indexes use `CREATE INDEX name ON table_name(vector_column) USING HNSW` or `USING IVF`; metric and build/search parameters go in the index COMMENT. See [vector index configuration](../../configuration.md).
- Regular/unique indexes are persisted as B-trees; if the HASH name is only a compatibility hint, it is mapped to the implementation and produces a diagnostic.
- FULLTEXT, SPATIAL, and HNSW/IVF only accept the column types and options implemented by their respective modules.
- Regular and unique BTREE secondary indexes are built online by default; setting `MYDB_DDL_ASYNC` is not required. Prepare and publish briefly exclude writers while allowing readers; `INSERT`, `UPDATE`, and `DELETE` are allowed during the scan, sort, bulk-load, and main incremental catch-up phases.
- `CREATE INDEX ... ALGORITHM=INPLACE LOCK=NONE` explicitly selects the online build; `LOCK=SHARED/EXCLUSIVE` selects a blocking build. `ALGORITHM=INSTANT/COPY` returns 1846 on CREATE INDEX — use ALTER TABLE for COPY; `LOCK=NONE` on non-BTREE indexes returns 1846.
- `ALTER TABLE ... ADD INDEX` (including adding multiple BTREE indexes and the primary key at once) uses the same path; `LOCK=NONE` allows DML during the build, and `LOCK=SHARED/EXCLUSIVE` preserves the explicit blocking behavior. Mixed ALTERs that include column changes, and special indexes, are not part of this online path.
- The statement returns success only after waiting for the full index publication. A unique index does not prematurely enforce the new constraint on concurrent DML before publication; if duplicate values are found during validation, the creation fails while already committed business writes are preserved. Nullable unique keys allow multiple `NULL`s.
- Incremental logs count toward the memory budget, with at most 64 MiB per physical index; exceeding the limit aborts the candidate index build. Long transactions may delay the prepare, publish, or failure cleanup phases.
- BTREE builds expose cancellable job IDs in `information_schema.mydb_ddl_jobs`; terminal results remain in bounded in-memory history. Catch-up fails explicitly if its bounded publication attempts cannot converge.
- Index definitions, statistics, and visibility enter the catalog; optimizer hints can explicitly select or exclude indexes.

## Examples

```sql
CREATE INDEX idx_order_customer ON orders (customer_id, created_at DESC);
CREATE INDEX idx_order_created ON orders (created_at) ALGORITHM=INPLACE LOCK=NONE;
CREATE UNIQUE INDEX uk_customer_email ON customer (email);
CREATE INDEX idx_payload_cover ON event (kind) INCLUDE (created_at, payload);
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
