# `CREATE SEQUENCE`

## Description

Creates a PostgreSQL-style sequence object for use with nextval/currval/lastval/setval.

## Syntax

```ebnf
<create-sequence> ::= 'CREATE' [ 'TEMPORARY' ] 'SEQUENCE'
                      [ 'IF NOT EXISTS' ] <qualified-name>
                      { <sequence-option> }
<sequence-option> ::= 'AS' <integer-type>
                      | 'INCREMENT' [ 'BY' ] <signed-integer>
                      | 'MINVALUE' <signed-integer> | 'NO MINVALUE'
                      | 'MAXVALUE' <signed-integer> | 'NO MAXVALUE'
                      | 'START' [ 'WITH' ] <signed-integer>
                      | 'CACHE' <unsigned-integer>
                      | 'CYCLE' | 'NO CYCLE'
                      | 'OWNED BY' ( <qualified-name> '.' <identifier> | 'NONE' )
<integer-type> ::= 'SMALLINT' | 'INT2' | 'INTEGER' | 'INT' | 'INT4' | 'BIGINT' | 'INT8'
```

## Behavior and Limitations

- The start point, increment, bounds, cycle, and cache are validated uniformly in the executor; the increment must not be zero. CACHE must be positive and defaults to 1; use `CACHE 1` for single-value session allocation. The spelling `NO CACHE` is currently rejected with 1064.
- Sequence values have independent persistent state; values already allocated are not reclaimed when the calling transaction rolls back.
- TEMPORARY sequences belong to the session; IF NOT EXISTS produces a Note.
- This is a PostgreSQL-style extension of mydb, not a native MySQL 8.0 object.
- Text-protocol SELECT, prepared statements, defaults, views, routines, triggers and LOAD DATA SET expressions can call sequence functions. Evaluation keeps sequence/session state on the connection thread and allocates values serially.
- `OWNED BY table.column` binds stable table/column identities; dropping that table or column also drops the sequence. Renames and table rewrites preserve the dependency. `OWNED BY NONE` removes it. Permanent sequences cannot depend on temporary tables; cross-database dependency moves are rejected.
- NEXTVAL/SETVAL require UPDATE on the sequence; CURRVAL/LASTVAL require SELECT or UPDATE. Advancing values is rejected on physical replicas, in read-only transactions, under super_read_only or while master writes are stopped. See [Sequences](../../sequences.md) for cache, durability and synchronization rules.

## Examples

```sql
CREATE SEQUENCE invoice_seq START WITH 1000 INCREMENT BY 1 CACHE 32;
SELECT NEXTVAL('invoice_seq'); -- 1000
SELECT CURRVAL('invoice_seq'); -- 1000
SHOW CREATE SEQUENCE invoice_seq;

CREATE TABLE invoice_owner(id BIGINT);
ALTER SEQUENCE invoice_seq OWNED BY invoice_owner.id;
ALTER SEQUENCE invoice_seq OWNED BY NONE;
DROP TABLE invoice_owner;
DROP SEQUENCE invoice_seq;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
