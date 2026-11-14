# REST SQL API

The optional REST listener executes SQL against the same engine, accounts,
privileges, row security policies, transactions, and resource limits as the
MySQL listener. It supports HTTP/1.1 and HTTPS HTTP/2. SQL execution is always
POST; SQL and credentials are never accepted in URL parameters.

## Enable the listener

```ini
[mydb]
rest_enabled = ON
rest_host = 127.0.0.1
rest_port = 8739
rest_tls_mode = required
rest_ssl_cert = /etc/mydb/server-cert.pem
rest_ssl_key = /etc/mydb/server-key.pem
```

REST defaults to disabled. HTTPS is required by default. Without dedicated
REST certificates the listener uses the configured server certificate or
an ephemeral self-signed certificate. `rest_tls_mode=disabled` is permitted
only on loopback. When `ssl_ca` is configured, verified client certificate
identity is available to account `REQUIRE X509` checks.

## Create, list, reveal, and revoke API keys

Use a TLS-authenticated MySQL connection:

```sql
CREATE API KEY 'backend';
CREATE API KEY 'temporary' EXPIRES AT '2027-01-01 00:00:00';
SHOW API KEYS;
SHOW API KEY 'backend';
DROP API KEY 'temporary';
```

Each account can retain **1024 keys**, including expired keys. Names are unique
within an account. Deleting a key releases its slot. Keys **never expire by
default**; `expires_at` is NULL. Explicit expiration timestamps are UTC.

CREATE and singular SHOW return `key_id`, `name`, `owner`, `created_at`,
`expires_at`, `status`, and the complete `api_key`. The complete credential can
be retrieved repeatedly. Plural SHOW returns metadata without credentials.

Accounts manage their own credentials. `API_KEY_ADMIN` permits management of
other accounts using `FOR 'user'@'host'`, and master-key rotation. Managing a
system account also follows `SYSTEM_USER` restrictions. Roles and anonymous
accounts cannot own keys. Administration is limited to top-level MySQL
sessions; creating or revealing credentials and rotating master keys require
TLS. REST SQL, stored programs, triggers and events cannot administer keys.

API keys authenticate as their exact owning account and inherit its current
privileges, default roles, Host restrictions, TLS requirements and quotas.
Privilege revocations and row security policies apply normally. Locked,
deleted or password-expired accounts cannot use keys. Changing a password does
not delete keys. Deleting an account deletes its keys; renaming an account
moves key ownership. Revocation and expiration close affected REST sessions
and cancel their running work; already committed work remains committed.

## Credential encryption and recovery

Authentication uses an indexed key ID and constant-time digest verification.
Full credential recovery uses an independently encrypted credential envelope.
The REST master key comes from environment-only `MYDB_REST_ENCRYPT_KEY`
(`base64:` or `hex:` encoding of 32 random bytes), or an automatically created
`<data_dir>/rest-keys.mydb`. File keyrings require server ownership, restrictive
permissions and a platform with Unix permission enforcement. Other platforms
must supply the environment key.

Ciphertext and credential metadata follow WAL, native physical replication
and backups. Master keys must be provisioned separately to replicas and
recovery hosts; they are not included in database backups. Backup manifests
list required REST master-key IDs. A missing or incorrect master key is an
error, never a reason to generate a replacement for existing credentials.

To rotate, deploy a new candidate while retaining the old keys. With an
environment candidate, `rest_previous_keys_file` points to the previous
keyring. Run from a privileged TLS MySQL session:

```sql
ALTER INSTANCE ROTATE API KEY MASTER KEY;
```

Rotation atomically rewraps data keys without changing API key strings.
Retain keys needed by older backups. AK management is local credential
administration: it is not exported through MySQL logical binlog replication.

## Execute SQL

```bash
curl --cacert /path/to/ca.pem https://localhost:8739/api/v1/sql \
  -H "Authorization: Bearer $MYDB_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"database":"test","sql":"SELECT ? AS id","params":[42]}'
```

Specify either `sql` and optional `params`, or `statements`, an array of
`{"sql":"...","params":[]}` objects. Each item must contain exactly one
SQL statement. Unknown fields, duplicate JSON fields, incorrect parameter
counts, invalid types and unsupported protocol operations produce errors.
Batches contain 1 through 1024 items and stop at the first failure.

`timeout_ms` defaults to 30000 and cannot exceed `rest_max_timeout_ms`.
`database` selects the default database for an independent request. Omitting it
leaves no database selected, including when creating a persistent session. Each
independent request owns a temporary SQL session which is closed afterward.
Explicit transaction control and disabling autocommit require a persistent
session; use `atomic=true` for a transaction confined to one request.

### Parameters

Use `?` placeholders. Values are bound without SQL interpolation. Shorthand
values support SQL NULL, booleans, strings, safe integers and exact decimals.
Integers outside ±9007199254740991 require a typed representation.

| Type | Value representation |
| --- | --- |
| `int64`, `uint64`, `decimal` | Decimal string |
| `float32`, `float64` | Finite JSON number |
| `string` | UTF-8 string |
| `binary` | Standard padded Base64 |
| `date`, `time`, `datetime`, `timestamp`, `year` | SQL text string |
| `json` | JSON text string; `"null"` is native JSON null |
| `geometry` | Base64 of SRID + WKB bytes |
| `bit` | Unsigned decimal string and `bits` from 1 through 64 |
| `vector` | Array text string plus `datatype` and `dimension` |

Temporal parameters use the engine's existing SQL conversion rules. In
particular, a TIME beyond the supported range is clamped to
`±838:59:59`; parameter conversion does not currently add a statement warning.

```json
{
  "sql": "SELECT ?, ?, ?",
  "params": [
    {"type":"uint64","value":"18446744073709551615"},
    {"type":"decimal","value":"12345678901234567890.123456789"},
    {"type":"vector","value":"[1,2,3]","datatype":"FLOAT32","dimension":3}
  ]
}
```

### Batches and transactions

`atomic=false` is the default: each item follows normal SQL transaction and
autocommit rules. Successful prefix operations are not automatically undone.
`atomic=true` wraps transactional queries and DML in BEGIN/COMMIT. DDL,
transaction control, CALL and unknown statement categories are rejected
before execution. Persistent sessions must have autocommit enabled and no
active transaction to use an atomic batch. Database rollback does not undo
external HTTP or other external SQL effects.

## Persistent sessions

| Method and path | Behavior |
| --- | --- |
| `POST /api/v1/sessions` | Create with `{"database":"test"}` or `{}`; returns 201 and Location |
| `GET /api/v1/sessions/{id}` | Get session and transaction state |
| `POST /api/v1/sessions/{id}/sql` | Execute with the same SQL request shape, without `database` |
| `POST /api/v1/sessions/{id}/touch` | Refresh session idle deadline |
| `DELETE /api/v1/sessions/{id}` | Close and roll back unfinished transactions; returns 204 |

Use SQL BEGIN, COMMIT, ROLLBACK, SAVEPOINT and USE on the persistent session.
Sessions are independent of TCP connections. One session accepts one SQL
execution/result transmission at a time; concurrent execution returns
409 `SESSION_BUSY`. Different sessions execute concurrently.

Default session idle lifetime is 300 seconds. Idle transactions expire after
60 seconds; touch cannot extend that transaction deadline. Disconnecting while
execution or result transmission is incomplete cancels work and closes its
session. SQL errors otherwise retain the engine's existing transaction rules.

Resources belong to the creating AK. A different key, even from the same
account, receives 404. Administrators use SQL PROCESSLIST and KILL for
cross-account management.

## JSON and streaming results

JSON responses include `request_id`, `session_id`, `results`, `session`,
`error`, `execution_outcome` and `transaction_state`. Each result identifies
its `statement_index` and `result_index`; procedures can return multiple
results. Results use column metadata and row arrays, preserving duplicate
column names. DML results include affected rows, matched rows, last insert
ID, informational text and diagnostics.

Integers, DECIMAL, row counts and insert IDs use decimal strings. Finite
floating values use JSON numbers; nonfinite values use a `non_finite` object.
SQL NULL is JSON null, while native JSON is encoded as JSON text. Binary,
BIT and geometry use Base64. Temporal values preserve SQL precision and
zero-date semantics; TIMESTAMP results use the session time zone. VECTOR uses
canonical array text with element type and dimension metadata.

For large results send `Accept: application/x-ndjson`. Event sequence:

```text
request_start
result_start
rows
rows
result_end
complete
```

An OK-only result (such as INSERT without RETURNING) emits a single
`result_end` containing `result.kind: "ok"`, without a preceding
`result_start`. CALL may append such an OK result after its result sets.
Accept media types are case-insensitive and support lists, wildcards, and
quality weights; JSON wins ties. A quality of zero excludes that format.

There may be multiple results. Failure terminates with `error` instead of
`complete`. HTTP 200, partial rows, or EOF do not establish successful
completion. After headers are sent, SQL errors are represented in the stream.
Buffered JSON exceeding the complete-response ceiling returns an error rather than
silently truncating. Stream buffering is bounded and slow consumers are
cancelled.

If buffered results exceed the ceiling, the server drains the result and
finishes execution before reporting `RESULT_TOO_LARGE` with
`results_omitted: true`. Check `execution_outcome` and transaction state:
`succeeded` confirms execution, including the commit of an atomic batch.
Later SQL errors in a batch still take precedence and may cause rollback.

## Status, cancellation, and retries

Provide an optional `X-Request-ID` with 16 through 64 ASCII letters, digits,
`-` or `_` to identify work before it completes. Otherwise the server generates
an ID. Retained duplicate IDs return 409 without starting SQL.

| Method and path | Behavior |
| --- | --- |
| `GET /api/v1/requests/{id}` | Execution state and summary, without result rows |
| `POST /api/v1/requests/{id}/cancel` | Request cancellation; 202 is not rollback confirmation |
| `GET /api/v1/status` | Capacity and execution counters; requires `API_KEY_ADMIN` |
| `GET /health/live` | Unauthenticated process liveness |
| `GET /health/ready` | Unauthenticated readiness |

Finished request summaries remain for up to five minutes in a bounded cache.
IDs are unique within the creating API key. Restart or eviction can remove them. A 404 does not establish that SQL did not
execute. Request IDs provide correlation, not durable idempotency.

Errors include a symbolic code, message, MySQL error number/SQLSTATE when
available, failure index, execution outcome and transaction state. Batch
errors retain completed prefix results. Confirmed rollback is reported
separately. A failed response or timeout does not establish that a write was
not committed. Neither the server nor examples automatically retry SQL POST.

Status codes: 400 invalid request; 401 authentication failure; 403 privilege
failure; 404 inaccessible resource; 406/415 unsupported format; 409 busy,
duplicate ID or SQL conflict; 413 oversized request; 422 SQL/typing errors;
429 account quota; 500 internal failure; 503 unavailable/capacity; 504 deadline.

All business responses are non-cacheable. CORS and cookies are not enabled.
Proxy identity headers are ignored; account Host/TLS checks use the actual
transport connection.

The machine-readable interface is available in [OpenAPI 3.1](rest-openapi.json).
Error messages are bounded and include `message_truncated` when shortened.
When MySQL logical binlogging is active, grant or revoke the local
`API_KEY_ADMIN` privilege with `sql_log_bin=OFF`.

Temporal strings retain all runtime fractional digits even when expression
metadata provides a conservative precision. Oversized JSON responses mark
`results_omitted=true`; execution and transaction state still describe the SQL
which ran. The JSON response ceiling must be at least 1024 bytes.

Completed request summaries are evicted before active work under protocol
memory pressure. A transport deadline can close the affected session; SQL
resources remain owned until the engine actually finishes cleanup. Allow
a short network grace period for terminal timeout diagnostics.

Silent connections must begin HTTP within five seconds; HTTP/2 peers must answer
keepalive pings. Completed SQL responses have a 30-second delivery deadline,
including when network backpressure stops body polling. Expiry closes the HTTP
connection and its affected SQL session; other HTTP/2 streams on that connection
may also be interrupted. Memory and execution slots remain charged until their
actual owners release them. Buffered result metadata and OK packets count toward
the protocol memory budget, even when there are no result rows.

The [OpenAPI schema](rest-openapi.json) defines result-set/OK variants, column
encodings, sessions, request state and each NDJSON event. It describes canonical
JSON number values: use typed decimal/int64/uint64 for integral values outside
the safe JSON integer range, including numbers written with a decimal point.
The engine additionally enforces decimal precision/scale, finite floating ranges,
vector shape and SQL conversion rules. SQL FLOAT nonfinite output is JSON null;
DOUBLE nonfinite output uses the non_finite object. A schema cannot encode every
SQL type conversion or the lexical distinction between 1 and 1.0.
