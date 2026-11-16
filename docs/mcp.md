# MCP Server

mydb provides an optional Model Context Protocol (MCP) endpoint for AI clients.
The endpoint executes SQL with the same accounts, roles, row security policies,
transactions and resource limits as MySQL and the [REST SQL API](rest-api.md).
It supports MCP `2026-07-28` and `2025-11-25` over Streamable HTTP.

## Enable MCP

```ini
[mydb]
mcp_enabled = ON
rest_enabled = OFF
rest_host = 127.0.0.1
rest_port = 8739
rest_tls_mode = required
rest_ssl_cert = /etc/mydb/server-cert.pem
rest_ssl_key = /etc/mydb/server-key.pem
```

Connect to `https://localhost:8739/mcp`. The listener starts when either REST
or MCP is enabled; their routes are enabled independently. Health endpoints
are available whenever the listener is running. Settings prefixed `rest_`
control the shared listener, TLS, SQL sessions, execution concurrency, request
body limits, timeouts and memory. MCP has no separate port or certificate.

HTTPS is the default. Without dedicated certificates the listener uses the
server certificate or an ephemeral self-signed certificate. Install a trusted
certificate for remote clients. `rest_tls_mode=disabled` permits HTTP only on
an actual loopback listener. HTTP/1.1 and HTTPS HTTP/2 are supported.

## Credentials and clients

Create an API key through a TLS-authenticated MySQL connection:

```sql
CREATE API KEY 'ai-client';
```

Configure the MCP client with the endpoint URL and this request header:

```text
Authorization: Bearer <api_key>
```

For clients accepting a JSON connection definition, the relevant fields are:

```json
{
  "url": "https://db.example.com:8739/mcp",
  "headers": {"Authorization": "Bearer <api_key>"}
}
```

Client configuration containers and transport labels depend on the client.
The client must trust the endpoint certificate and support custom request
headers. This release uses preconfigured Bearer API keys, not an OAuth login
flow. It does not provide stdio or the legacy two-endpoint HTTP+SSE transport.

Keys inherit the owning account's current privileges, roles, Host and TLS
requirements, row policies and quotas. Revocation, expiry and account lock or
deletion invalidate access and close affected sessions. Already committed
changes remain committed. Session and request handles belong to the creating
key, not merely to its database account. Another key receives `NOT_FOUND`.
The same key may use its handles through either REST or MCP.

API key administration remains restricted to top-level authenticated MySQL
sessions. MCP cannot create, reveal or drop keys, or rotate their master keys.
Other SQL administration follows normal database privileges. HTTP, AI and
other externally acting SQL functions retain their own permissions and limits.

## Tools

| Tool | Arguments and behavior |
| --- | --- |
| `list_databases` | `{}`; list visible databases |
| `list_tables` | `database`; list visible tables and views |
| `describe_table` | `database`, `table`; return columns, indexes, CREATE definition and constraints |
| `explain_sql` | `sql`, optional `params`, `database` or `session_id`, `timeout_ms`, `request_id`; ordinary EXPLAIN only |
| `execute_sql` | SQL request described below |
| `open_session` | Optional `database`; create a SQL session and return its `id` |
| `get_session` | `session_id`; inspect state without extending idle deadlines |
| `close_session` | `session_id`; cancel active work, close and roll back unfinished transactions |
| `get_request` | `request_id`; inspect a retained execution summary without result rows |
| `cancel_request` | `request_id`; request cancellation, without promising rollback |

`execute_sql` accepts:

```json
{
  "database": "test",
  "sql": "SELECT ? AS id",
  "params": [{"type": "uint64", "value": "18446744073709551615"}],
  "timeout_ms": 30000,
  "request_id": "client_request_000001"
}
```

Alternatively use `statements: [{"sql": "...", "params": []}]` instead of
`sql` and its parameters. Batches contain 1 through 1024 SQL items, each with
exactly one statement. Batches stop on the first failure. Unknown fields,
invalid parameters and unsupported SQL or protocol operations return errors.
Values are bound to `?` placeholders without SQL interpolation. Parameter
representations and result encodings follow [REST SQL](rest-api.md#parameters),
including exact decimal and 64-bit integer values, binary, JSON, temporal,
geometry and vector values. Native SQL JSON is represented as JSON text.
Pass large integers and exact decimals as typed decimal strings when the
client's JSON number implementation cannot retain their precision.

Omitting `database` leaves no default database selected.
Use `session_id` instead of `database` to reuse a SQL session. Use SQL `USE`
to change that session's database. BEGIN, COMMIT, ROLLBACK, SAVEPOINT, session
variables, roles and temporary tables work through the session. Without a
handle each call uses a temporary session; explicit transaction control and
disabling autocommit require a persistent session.

`atomic: true` wraps a transactional query/DML batch in BEGIN/COMMIT. DDL,
CALL and transaction control are rejected before execution. The persistent
session must be in autocommit mode without an active transaction. Rejecting
an atomic request under these preconditions preserves the caller's existing
transaction. Internal rollback remains possible after key/privilege revocation;
a failed rollback closes the session so a partial batch cannot later be committed.
Default
non-atomic batches follow ordinary SQL commit rules; successful prefix writes
are not automatically undone. Database rollback does not undo external effects.

The same session accepts only one execution/result delivery at a time;
concurrent calls return `SESSION_BUSY`. Sessions are independent of MCP or TCP
connections. Default session idle expiry is 300 seconds and transaction idle
expiry is 60 seconds. Closing a session, expiry or disconnecting an unfinished
SQL request closes the session and rolls back its unfinished transaction.

## Results, cancellation and retries

Tool results contain `structuredContent` and a short text summary. SQL results
include `request_id`, `session_id`, `results`, `session`, `error`,
`execution_outcome` and `transaction_state`. Result sets use column metadata
and row arrays, preserving duplicate column names. CALL can return multiple
results. DML reports affected/matched rows, insert ID and diagnostics.
SQL and tool execution failures set `isError: true`; malformed protocol
messages use JSON-RPC errors. Authentication failures return HTTP 401.
A successful `get_request` or `cancel_request` lookup has `isError: false` even
when the inspected SQL request failed; its stored `error` is execution status
data. `cancel_requested` is false when no cancellation was issued, including
when the request was already complete.

The delivery deadline begins after tool completion; it does not shorten the
SQL execution timeout. Completed responses have a 30-second delivery deadline independent of body
polling. An expired or abandoned response closes its SQL session and rolls back
unfinished transactions. A stalled network response closes its HTTP connection,
which also interrupts other streams on that connection. Bytes retained by the
network keep their memory charge until they are released.

MCP responses may use request-scoped SSE carrying the final result, not a
stream of database rows. Client disconnects cancel unfinished work. A reverse
proxy must forward streaming responses and allow the configured SQL timeout.
A cancellation response confirms only that cancellation was requested; use
`get_request` to check the final execution and transaction state.

The entire SQL call is bounded by row and byte ceilings. Exceeding either
returns `RESULT_TOO_LARGE`, `results_omitted: true` and no result rows. The
server drains results and finishes SQL/COMMIT before reporting this output
failure. `execution_outcome: "succeeded"` may therefore confirm a successful
write despite `isError: true`. A later SQL error takes precedence and can
roll back an atomic batch. Shrink the query to retrieve a bounded result;
there is no result cursor or pagination tool in this release.

Request IDs require 16 through 64 ASCII letters, digits, `_` or `-`. Duplicate
retained IDs do not execute again. Summaries are retained for at most five
minutes in a bounded cache and can disappear after restart or eviction. An ID
is a correlation handle, not a durable idempotency guarantee. `NOT_FOUND`
does not prove that SQL did not run. Never automatically retry a write after
an unknown outcome. Cancellation cannot undo committed or external effects.

## MCP configuration

All options support configuration files, `MYDB_MCP_*` environment variables
and `--mcp-*` command-line arguments.

| Option | Default and meaning |
| --- | --- |
| `mcp_enabled` | `OFF`; enable `/mcp` |
| `mcp_max_inflight` | `16`; concurrent SQL calls, including result delivery |
| `mcp_max_result_rows` | `1000`; total result rows across the call |
| `mcp_max_result_bytes` | `1048576`; complete SQL tool response budget; minimum 8192 |
| `mcp_allowed_origins` | `[]`; JSON array of exact HTTP(S) origins |

SQL collection reserves 4096 bytes of the response budget for the MCP envelope
and bounded diagnostics. MCP concurrency is additionally bounded by
`rest_max_inflight`. SQL sessions count toward shared session and account
connection limits. Request bodies use `rest_max_request_bytes`, headers use
`rest_max_header_bytes`, and all protocol buffers count toward the shared
`rest_memory_limit_mb` and global memory governor. SQL input buffers and
execution admission slots remain owned by background workers until they
actually exit, even after a client disconnects or cancels its request.
Saturation returns an
explicit capacity error; session inspection and cancellation do not consume
MCP SQL execution slots. With MCP enabled, HTTP/2 connections allow eight
additional streams beyond the shared SQL concurrency ceiling so cancellation
and status calls can proceed while all SQL slots are occupied. SQL admission
limits remain unchanged. SQL deadlines use the shared REST timeout settings.

For a nonempty origin list in an option file, quote the complete JSON value
so the option-file parser preserves its inner double quotes:

```ini
mcp_allowed_origins = '["https://app.example.com:443"]'
```

Requests without Origin headers are accepted after authentication. A present
Origin must match `mcp_allowed_origins`; the default rejects every browser
origin. Matching compares scheme, hostname and effective port. Wildcards,
credentials, fragments, malformed or out-of-range ports, and non-root paths
are rejected. Allowing an origin does not
enable CORS response headers. SQL and credentials are not accepted in URL
parameters, and compressed request bodies are unsupported.

Logs correlate tools, Key IDs, accounts, request IDs, execution time and outcome.
Credentials are not logged. SQL uses the existing query logging and redaction
path. Database values, comments and object definitions are untrusted content,
not instructions to the client. This release advertises Tools only, with no
Resources, Prompts, subscriptions or asynchronous Tasks extension.
