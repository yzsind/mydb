# Error Codes and Diagnostics

mydb uses MySQL 8.0.46 as the compatibility baseline for client-visible errors,
warnings, and notes. The MySQL protocol, stored-routine diagnostics, and
`performance_schema.events_errors_summary_*` use consistent error numbers and
SQLSTATEs.

## Reading a Diagnostic

Each condition has a numeric error code, a five-character SQLSTATE, and a
message. Error codes are non-zero 16-bit numbers; SQLSTATEs contain uppercase
ASCII letters or digits. MySQL-compatible conditions use the corresponding
MySQL code and SQLSTATE. The catalog covers 1,642 active MySQL 8.0.46
server-to-client conditions; a catalog entry alone does not imply support for
the feature associated with that condition.

Messages describe the failure and relevant parameters in English. Use error
codes and SQLSTATEs to classify failures rather than depending on exact message
text. Client-side `CR_*` errors, such as errors from an upstream replication
connection, are distinct from server errors.

SQL `SIGNAL` and `RESIGNAL` can supply runtime diagnostics. They require a
valid non-success SQLSTATE and, when supplied, a non-zero MySQL error number.

## MYDB Reserved Range

`50000..=50999` is reserved exclusively for mydb extensions and does not
overlap the MySQL 8.0.46 catalog:

<!-- BEGIN GENERATED MYDB ERRORS -->
| errno | symbol | SQLSTATE |
| ---: | --- | --- |
| 50001 | `MYDB_ER_INVALID_VECTOR_VALUE` | `22018` |
| 50002 | `MYDB_ER_VECTOR_DIMENSION_MISMATCH` | `22023` |
| 50003 | `MYDB_WARN_VECTOR_FALLBACK` | `01000` |
| 50010 | `MYDB_ER_SEQUENCE_EXISTS` | `42S01` |
| 50011 | `MYDB_ER_SEQUENCE_NOT_FOUND` | `42S02` |
| 50012 | `MYDB_ER_SEQUENCE_LIMIT_REACHED` | `22003` |
| 50013 | `MYDB_ER_SEQUENCE_VALUE_UNDEFINED` | `55000` |
| 50014 | `MYDB_ER_INVALID_SEQUENCE` | `22023` |
| 50015 | `MYDB_ER_SEQUENCE_CONTEXT_UNSUPPORTED` | `0A000` |
| 50020 | `MYDB_ER_UNDO_RETENTION_EXHAUSTED` | `HY000` |
| 50021 | `MYDB_ER_UNDO_TXN_LIMIT` | `HY000` |
| 50022 | `MYDB_WARN_UNDO_CAPACITY` | `01000` |
| 50030 | `MYDB_ER_ROLE_CHANGE_BLOCKED` | `55000` |
| 50031 | `MYDB_ER_REPLICATION_STATE` | `HY000` |
| 50032 | `MYDB_ER_REPLICATION_DIVERGED` | `HY000` |
| 50033 | `MYDB_ER_REPLICATION_SECURITY` | `28000` |
| 50034 | `MYDB_ER_BASE_BACKUP_FAILED` | `HY000` |
| 50035 | `MYDB_ER_BACKUP_FAILED` | `HY000` |
| 50040 | `MYDB_ER_SYSTEM_TIME_TOO_OLD` | `HY000` |
| 50041 | `MYDB_ER_SYSTEM_TIME_SCHEMA_CHANGED` | `HY000` |
| 50042 | `MYDB_ER_INVALID_SYSTEM_TIME` | `22007` |
| 50043 | `MYDB_ER_SNAPSHOT_TOO_OLD` | `HY000` |
| 50050 | `ER_AI_ACCESS_DENIED` | `42000` |
| 50051 | `ER_AI_BUDGET_EXCEEDED` | `HY000` |
| 50052 | `ER_AI_CREDENTIAL_UNAVAILABLE` | `HY000` |
| 50053 | `ER_AI_DIMENSION_MISMATCH` | `HY000` |
| 50054 | `ER_AI_DISABLED` | `HY000` |
| 50055 | `ER_AI_EGRESS_DENIED` | `42000` |
| 50056 | `ER_AI_ENCRYPTION_KEY_INVALID` | `HY000` |
| 50057 | `ER_AI_ENCRYPTION_KEY_MISSING` | `HY000` |
| 50058 | `ER_AI_INCOMPLETE` | `HY000` |
| 50059 | `ER_AI_INVALID_ARGUMENT` | `22023` |
| 50060 | `ER_AI_INVALID_RESPONSE` | `HY000` |
| 50061 | `ER_AI_MODEL_NOT_FOUND` | `HY000` |
| 50062 | `ER_AI_OUTPUT_SCHEMA` | `HY000` |
| 50063 | `ER_AI_QUEUE_FULL` | `HY000` |
| 50064 | `ER_AI_REFUSED` | `HY000` |
| 50065 | `ER_AI_SECRET_DECRYPT_FAILED` | `HY000` |
| 50066 | `ER_AI_TIMEOUT` | `HY000` |
| 50067 | `ER_AI_UNSUPPORTED_CONTEXT` | `0A000` |
| 50068 | `ER_AI_UPSTREAM_` | `HY000` |
| 50069 | `ER_AI_UPSTREAM_AUTH` | `HY000` |
| 50070 | `ER_AI_UPSTREAM_ERROR` | `HY000` |
| 50071 | `ER_AI_WRONG_MODEL_KIND` | `HY000` |
| 50090 | `ER_AI_CIRCUIT_OPEN` | `HY000` |
| 50091 | `ER_AI_KEY_ROTATION_INCOMPLETE` | `HY000` |
| 50140 | `MYDB_ER_RECYCLEBIN` | `HY000` |
<!-- END GENERATED MYDB ERRORS -->

The legacy 4301, 4901–4908, and 30036–30037 entries do not get compatibility
aliases.

## Diagnostic Details

OS failures, corruption details, and worker failures can be associated with
stable `MYDB-DIAG-*` identifiers in server diagnostics. Clients receive the
corresponding MySQL condition and a message suitable for applications.
Preserve the diagnostic identifier and relevant server logs when reporting a
problem.

Recycle-bin administration errors use `50140 / HY000` (`MYDB_ER_RECYCLEBIN`), with a specific diagnostic for an invalid entry, unavailable state, read-only operation, missing dependency or cleanup failure. Authorization and SQL syntax retain the existing MySQL error codes. See [Recycle Bin](recycle-bin.md).

## Operational response

Use [AI](ai.md), [HTTP](http.md), [RLS](row-level-security.md), and
[REST credentials](rest-api.md#create-list-reveal-and-revoke-api-keys) for feature-specific
handling. Access errors require a privilege or policy correction; budget errors
require reducing work or reviewing capacity; corruption and recovery errors require
preserving data and logs before repair. A timeout of an external call does not prove
that the remote side effect was rolled back. REST/MCP string error codes are a
transport contract distinct from SQL errno and SQLSTATE.
