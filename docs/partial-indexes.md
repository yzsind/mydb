# Partial indexes

A partial index stores entries only for rows whose `WHERE` predicate evaluates to SQL TRUE. FALSE and NULL do not qualify.

```sql
DROP TABLE IF EXISTS test.partial_users;
CREATE TABLE test.partial_users (
    id INT PRIMARY KEY,
    email VARCHAR(120),
    username VARCHAR(40),
    active INT
);
INSERT INTO test.partial_users VALUES
    (1, 'a@example.com', 'ada', 1),
    (2, 'old@example.com', 'ada', 0),
    (3, 'pending@example.com', 'ada', NULL);

CREATE INDEX active_email ON test.partial_users(email) WHERE active = 1;
CREATE UNIQUE INDEX active_username ON test.partial_users(username) WHERE active = 1;

DROP TABLE IF EXISTS test.partial_tasks;
CREATE TABLE test.partial_tasks (
    id BIGINT PRIMARY KEY,
    owner_id BIGINT,
    active INT,
    INDEX active_owner(owner_id) WHERE active = 1
);

ALTER TABLE test.partial_tasks ADD INDEX active_id(id) WHERE active = 1;
```

All three users may have the same username because only row 1 qualifies.
Changing row 2 or 3 to `active=1` while row 1 is active returns duplicate-key
error 1062; the rejected statement leaves membership and table rows unchanged.

Put `WHERE` after the other options of the individual index definition. Parentheses are optional. A comma outside the predicate's parentheses starts the next index or ALTER action.

The predicate is available for ordinary and unique B-tree indexes, FULLTEXT, SPATIAL, HNSW and IVF indexes. Existing restrictions on prefix keys, functional keys, multi-valued keys, INCLUDE columns and special index configurations still apply. A primary key cannot be partial. Foreign keys require complete supporting indexes.

## Membership and writes

An UPDATE can add a row to the index, remove it, or change its indexed keys. Updating only a predicate column still maintains the index. Unique constraints apply only to qualifying rows, with the existing NULL handling for unique keys. Nonqualifying rows may share the same key.

Index-specific domains, such as the nonzero-vector requirement for COSINE, apply only to qualifying rows. Table column types and CHECK constraints still apply to every row.

Predicate expressions may use local columns, constants and deterministic scalar expressions. Subqueries, aggregates, window functions, variables, parameters, stored functions and volatile functions are rejected. Expression settings are fixed at index creation so writers, background workers and recovery agree on membership.

The definition is retained by `SHOW CREATE TABLE` and `CREATE TABLE ... LIKE`. Renaming a referenced column rewrites the predicate; dropping one requires removing the dependent index first.

## Query planning

The optimizer may use a partial index only when the query guarantees its predicate. Recognized proofs include matching expressions, conjunctions, disjunctions, integer comparisons and ranges, BETWEEN, IN and non-NULL deductions. Unproven cases use another access path. Equality and range inference is conservative about data types and expression settings.

```sql
-- Eligible for an index WHERE active = 1:
SELECT email FROM test.partial_users WHERE active = 1 AND email = 'a@example.com';

-- Requires a complete access path:
SELECT email FROM test.partial_users WHERE email = 'a@example.com';
```

`FORCE INDEX` cannot override the membership requirement. Forcing only an inapplicable partial index returns an explicit error. Parameter-dependent access paths are planned again with the current values; prepared statement semantic templates remain reusable.

FULLTEXT scoring uses the whole visible table as its corpus. Document count, total token length and query-term document frequencies are maintained incrementally in persisted MVCC counters shared by compatible indexes. Cold-cache and write-first scoring read bounded counter ranges without scanning the user heap. Counters use the data transaction, including rollback, savepoints, recovery and physical replication. Filtering eligibility does not switch the logical tokenizer for a column list. A partial index must not change MATCH membership outside its predicate or make scores depend on the chosen index. Vector indexes retain their existing approximate search semantics and snapshot fallbacks.

## Storage format

This checkout uses catalog format 75, including partial-index predicates and
transactional FULLTEXT metadata. Released 1.0.2 uses format 72; use a fresh data
directory and logical transfer when moving between them. See
[current source changes](current-source.md), [changing versions](changing-versions.md),
and [storage format versions](storage-format-versions.md).
