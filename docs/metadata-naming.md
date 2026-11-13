# Metadata naming

System metadata has a canonical spelling independent of the spelling used in a
query. MySQL-compatible objects retain their MySQL names, including intentional
case differences. Native mydb views in `information_schema` use uppercase
`MYDB_` names and uppercase column names separated by underscores.

Schema and table lookup remains ASCII case-insensitive. For example,
`information_schema.mydb_caches` resolves to `information_schema.MYDB_CACHES`.
Metadata listings publish the canonical name. String comparisons against those
listings follow the column collation or an explicitly requested binary comparison.

## Result column names

An unaliased system metadata column reference returns the name published by its source relation.
An explicit alias retains its exact spelling. Derived tables, CTEs and views
publish their own column names, including declared column lists and aliases.
These rules apply to empty results and prepared statements as well as ordinary
queries. Computed expressions retain their expression labels. Ordinary user-table
column references retain their existing query spelling.

Computed results derived from system metadata retain their published aliases
through derived tables, CTEs and views. A user-created table in `mysql` follows
ordinary-table naming rules; its location does not make it a system metadata table.

```sql
SELECT table_name FROM information_schema.tables LIMIT 1;
-- Result column: TABLE_NAME

SELECT table_name AS table_name FROM information_schema.tables LIMIT 1;
-- Result column: table_name

SELECT HOST FROM mysql.user LIMIT 1;
-- Result column: Host

SELECT TRX_ID FROM information_schema.innodb_trx LIMIT 1;
-- Result column: trx_id
```

MySQL-compatible exceptions such as `mysql.user.Select_priv`,
`information_schema.INNODB_TRX.trx_id`, `sys.metrics.Variable_name` and the
`sys.x$` views retain their existing spelling. MySQL `SHOW` result labels also
retain the spelling defined by their individual statements.

## Native views

All names below belong to `information_schema`.

| View | Row meaning |
| --- | --- |
| `MYDB_AI_MODELS` | An AI model visible to the current account |
| `MYDB_AI_CONNECTIONS` | An AI provider connection |
| `MYDB_AI_SECRETS` | An AI secret and local key availability, without credential material |
| `MYDB_AI_RUNTIME_STATUS` | A node-local AI runtime metric |
| `MYDB_ROW_SECURITY_POLICIES` | A row security policy |
| `MYDB_SEQUENCES` | A sequence and its recovery and synchronization state |
| `MYDB_DDL_JOBS` | A tracked DDL job |
| `MYDB_PREPARED_XA_TRANSACTIONS` | A prepared XA transaction |
| `MYDB_CURSORS` | A live server-side cursor |
| `MYDB_CACHES` | A cache summary |
| `MYDB_PLAN_CACHE_ENTRIES` | A resident cached execution plan |
| `MYDB_MEMORY_ADVISOR` | A memory or work-area metric |
| `MYDB_VECTOR_INDEXES` | A vector index and available runtime statistics |
| `MYDB_UNDO_HISTORY` | An undo summary for a transaction and storage table |
| `MYDB_TEMPORAL_HISTORY` | Available temporal history for a table or index |
| `MYDB_STATS_HEALTH` | A table's optimizer statistics health |
| `MYDB_ZONE_MAPS` | A zone's column summary |
| `MYDB_ZONE_MAP_STORAGE` | A zone map storage object |

The native view names `AI_MODELS`, `AI_CONNECTIONS`, `AI_SECRETS`,
`AI_RUNTIME_STATUS`, `MYDB_PLAN_CACHE` and `MYDB_XA_RECOVER` are not aliases.
Use the names in the table above. The `MYDB_PLAN_CACHE` environment variable
continues to control plan caching; it is separate from the metadata view name.

Use the column descriptions to inspect names, units and value semantics:

```sql
SHOW FULL COLUMNS FROM information_schema.MYDB_VECTOR_INDEXES;

SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'information_schema'
  AND TABLE_NAME LIKE 'MYDB\_%'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT SQL_TEXT, DEFAULT_SCHEMA_NAME, HITS, TABLE_DEPENDENCY_COUNT
FROM information_schema.MYDB_PLAN_CACHE_ENTRIES;

SELECT INDEX_NAME, ALGORITHM, DISTANCE_METRIC, IS_LOADED,
       DECLARED_DIMENSIONS, RUNTIME_DIMENSIONS
FROM information_schema.MYDB_VECTOR_INDEXES;
```

Row visibility and management privileges are defined by each feature. Naming
rules do not grant access to additional objects or expose secrets.

## Native column conventions

| Convention | Meaning |
| --- | --- |
| `IS_`, `HAS_`, `CAN_` | State, existence or capability predicate, respectively |
| `_ID`, `_NAME` | An identity or a name in the documented object domain |
| `_UNIX_MS`, `_UNIX_US` | An absolute time since the Unix epoch in milliseconds or microseconds |
| `_AT` | A calendar time represented by a SQL temporal type |
| `_DURATION_MS`, `_DURATION_US` | An elapsed duration, distinct from an absolute time |
| `_BYTES` | A size in bytes |
| `_RATIO` | A documented numerator divided by a denominator |
| `_PCT` | A percentage, with precision described by the column |
| `_COUNT`, or an unambiguous plural such as `TOTAL_NODES` | A quantity of objects or events |

Public algorithm terms such as `M`, `EF_CONSTRUCTION`, `NLIST`, `NPROBE` and
`SQ8` retain their established spelling. General identifiers such as `ID`,
`SQL`, `DDL`, `XA`, `LSN`, `SCN` and `URL` are also retained.

`MYDB_DDL_JOBS.REPLAY_WATERMARK_SCN` is a snapshot SCN reached by incremental
replay. It is `NULL` when no replay watermark exists. `PROGRESS_ROWS` reports
copied or processed rows separately.

`MYDB_CACHES.CAPACITY_ENTRIES` is measured in cache entries; zero means
unbounded or not applicable. `HIT_PCT` is currently an integer percentage rounded
to the nearest whole percentage point. `MYDB_STATS_HEALTH.STALE_RATIO` is the
number of modified rows divided by the analyzed row count and can exceed 1.
`MYDB_VECTOR_INDEXES.STALE_RATIO_PCT` is a percentage of stale vector nodes.
These quantities have different denominators and must not be treated as the
same metric.

`MYDB_AI_CONNECTIONS.IS_CREDENTIAL_AVAILABLE` is true when no credential is
required or the referenced key is locally available. It does not verify the
credential with the remote provider. `MYDB_AI_SECRETS.IS_AVAILABLE` describes
local encryption-key availability.

`MYDB_ZONE_MAP_STORAGE.MEMORY_FALLBACKS_GLOBAL` is a process-wide cumulative
counter repeated in each storage row. Do not sum it across rows.

Internal dictionary names use lowercase words separated by underscores. Their
tables are private storage metadata and are not a SQL administration interface.
