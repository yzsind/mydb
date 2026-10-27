# HTTP SQL Functions

mydb can call HTTP and HTTPS endpoints from SQL expressions. These functions
return native JSON and require the global `HTTP_USE` privilege. HTTP calls are
enabled by default and can reach public networks, private networks, loopback,
and link-local addresses unless an administrator supplies an explicit policy.

```sql
GRANT HTTP_USE ON *.* TO 'integration'@'localhost';

SELECT HTTP_GET('https://example.com/api/status');
SELECT HTTP_POST('https://example.com/api/items', JSON_OBJECT('name', 'sample'));
SELECT HTTP_REQUEST('DELETE', 'https://example.com/api/items/1');
```

## Functions

| Function | Signature |
| --- | --- |
| HTTP_REQUEST | `HTTP_REQUEST(method, url [, options])` |
| HTTP_GET | `HTTP_GET(url [, options])` |
| HTTP_HEAD | `HTTP_HEAD(url [, options])` |
| HTTP_OPTIONS | `HTTP_OPTIONS(url [, options])` |
| HTTP_DELETE | `HTTP_DELETE(url [, options])` |
| HTTP_POST | `HTTP_POST(url, body [, options])` |
| HTTP_PUT | `HTTP_PUT(url, body [, options])` |
| HTTP_PATCH | `HTTP_PATCH(url, body [, options])` |

Names are case insensitive. The generic method must be a valid HTTP method
token; use uppercase for standard methods. CONNECT tunnels and protocol
upgrades are unsupported. Only HTTP and HTTPS URLs are accepted. URL userinfo,
fragments, control characters, and URLs exceeding 16 KiB are rejected.

A SQL NULL method or URL returns SQL NULL without sending a request. Arguments
still follow normal SQL evaluation rules. An omitted or SQL NULL options
argument means `{}`. Options must otherwise be a JSON object or JSON text.
Unknown fields, duplicate object keys, invalid types, and conflicting settings
return errors.

## Request options

| Option | Type and behavior |
| --- | --- |
| `headers` | Object mapping names to strings or arrays of strings. Arrays preserve repeated fields. |
| `query` | Object mapping names to strings or arrays of strings. Values are URL encoded and appended to the original query string. |
| `body` | Raw UTF-8 string. |
| `json` | Any JSON value, including JSON null. Serialized as JSON. |
| `body_base64` | Standard padded Base64 representing arbitrary bytes. |
| `form` | Ordered array of `{"name":"...","value":"..."}` objects. Repeated names are preserved. |
| `multipart` | Ordered array of parts, described below. |
| `auth` | `{"type":"none"}`, `{"type":"basic","username":"...","password":"..."}`, or `{"type":"bearer","token":"..."}`. |
| `timeout_ms` | Total call deadline including queuing, DNS, redirects, and reading. Default and maximum: 30000. |
| `connect_timeout_ms` | Connection timeout. Default and maximum: 5000. |
| `read_timeout_ms` | Read inactivity timeout. Default and maximum: 10000. |
| `follow_redirects` | Boolean, default false. |
| `max_redirects` | Integer from 0 through 5, default 5 when following redirects. |
| `max_response_bytes` | Positive integer, at most the node response ceiling. |
| `response_encoding` | `auto` (default), `utf8`, or `base64`. |
| `on_error` | `raise` (default) or `return`. |

Exactly one request body representation may be specified. The body argument of
HTTP_POST, HTTP_PUT, or HTTP_PATCH accepts SQL text, binary bytes, or native
JSON. SQL NULL means no body; a native JSON null sends the JSON text `null`.
A non-NULL positional body cannot be combined with a body option. Native JSON
numbers retain their exact text, including high-precision DECIMAL values; the
`json` option also preserves numeric lexemes. The 64 KiB options limit includes
embedded body fields; use a positional body for larger raw or JSON payloads. Numbers and
other SQL types must be converted explicitly.

Each multipart part has `name` and exactly one of `text` or `base64`, with
optional `filename` and `content_type`. Filenames are metadata; mydb never
opens a server file for an HTTP upload.

```sql
SELECT HTTP_REQUEST('POST', 'http://127.0.0.1:8080/upload', JSON_OBJECT(
  'multipart', JSON_ARRAY(
    JSON_OBJECT('name', 'description', 'text', 'sample'),
    JSON_OBJECT('name', 'file', 'filename', 'sample.bin',
                'content_type', 'application/octet-stream', 'base64', 'AP8=')
  )
));
```

JSON, form, and multipart encoding generate Content-Type. Transport-owned
headers such as Host, Content-Length, Transfer-Encoding, Connection, Upgrade,
and proxy headers cannot be supplied. Header names are case insensitive;
repeat a header through one array rather than differently cased keys. CR/LF
injection and an `auth` option conflicting with Authorization are rejected.

Use prepared placeholders to pass credentials and request data. Server logs,
PROCESSLIST, audit events, and optimizer traces redact HTTP SQL. Client query
histories remain under the client's control. A credential deliberately stored
in a user table, routine, view, trigger, or event follows that object's normal
persistence rules; this API does not provide an encrypted credential catalog.

## Response contract

All methods return the same JSON shape:

```json
{
  "status": 200,
  "headers": {"content-type": ["application/json"]},
  "body": "{\"ok\":true}",
  "body_encoding": "utf8",
  "url": "https://example.com/api/status",
  "http_version": "HTTP/1.1",
  "redirect_count": 0,
  "elapsed_ms": 12,
  "error": null,
  "content_encoding": "",
  "content_decoded": false
}
```

Header names are lowercase and values are arrays. Non-UTF-8 header bytes are
represented as `{"encoding":"base64","value":"..."}` array entries.
`auto` returns valid UTF-8 text verbatim and otherwise uses Base64. Explicit
`utf8` rejects invalid UTF-8; `base64` always preserves the bytes through
Base64. Empty responses, including HEAD and 204, have an empty body.

HTTP 4xx and 5xx responses are data, not SQL errors. Inspect `status` explicitly.
To read JSON returned by an endpoint, extract and parse the response's body:

```sql
SET @response = HTTP_GET('http://127.0.0.1:8080/status');
SELECT JSON_EXTRACT(@response, '$.status');
SELECT JSON_EXTRACT(JSON_UNQUOTE(JSON_EXTRACT(@response, '$.body')), '$.ok');
```

HTTP/1.1, HTTPS HTTP/2, chunked responses, gzip, and deflate are supported.
Both compressed and decompressed body sizes are bounded. Other content
encodings, including compound coding lists, are returned unchanged. Headers describe the original response;
`content_encoding` and `content_decoded` identify body decoding explicitly.

DNS, connection, TLS, timeout, transport, and response-decoding failures
normally raise SQL errors. With `on_error='return'`, these failures produce an
error object containing `code`, `stage`, and `may_have_executed`; unavailable
response fields are null. The latter flag means the upstream may have applied
the request even though mydb could not obtain a complete result.

Argument, authorization, policy, resource, KILL, and SQL deadline failures
always raise errors. An HTTP request deadline can be returned as data; a SQL
statement deadline cannot. Errors and diagnostics omit request credentials,
URL parameters, and body content.

## SQL execution and transactions

HTTP functions are volatile external effects. Each actual evaluation sends a
request, subject to explicit redirect handling. SQL does not guarantee one
call per source row or per statement. Predicates, joins, grouping, sorting,
subqueries, and repeated expression references can change evaluation counts.
CASE, IF, and COALESCE retain their existing short-circuit behavior. Functions
are not constant-folded or served from a result cache.

Calls can appear in runtime SELECT expressions, DML, SET, transactions, views,
stored programs, triggers, and events. Permissions follow the effective
SQL SECURITY DEFINER/INVOKER context. Declaring a routine DETERMINISTIC does
not make HTTP deterministic. Generated columns, defaults, CHECK constraints,
partition expressions, and functional indexes reject HTTP effects.

PREPARE, ordinary EXPLAIN, and creation of a stored definition never send
requests. EXPLAIN ANALYZE executes its target and can send requests.

Network waits inside writes or transactions can extend database lock lifetimes.
Keep endpoint deadlines bounded and avoid callbacks that require those same
locked rows.

An external side effect cannot be undone by ROLLBACK, a savepoint rollback,
constraint failure, connection loss, or KILL. No HTTP request is automatically
retried. After a request has been sent, mydb does not automatically replay the
owning SQL statement. A lock conflict requiring a full replay returns
`ER_HTTP_REPLAY_FORBIDDEN`; callers must inspect the external outcome before
choosing to retry. Use endpoint idempotency keys when the business operation
requires deduplication across application retries.

Replication and recovery replay database changes rather than HTTP requests.
CTAS records its concrete table definition and actual inserted rows. Stored
object definitions may contain HTTP expressions, but installing those
definitions does not execute them.

## Node configuration

```ini
[mydb]
http_enabled = ON
# http_local_config = /etc/mydb/http-local.json
http_max_inflight = 32
http_max_queue = 128
http_memory_limit_mb = 128
http_max_request_bytes = 4194304
http_max_response_bytes = 4194304
http_max_header_bytes = 65536
http_max_calls_per_statement = 1024
```

Each setting has the equivalent uppercase environment variable, for example
`MYDB_HTTP_MAX_INFLIGHT`. Capacity settings are startup settings. Per-call
options can only tighten the applicable ceilings. The default account
concurrency limit is four. Redirects count against the statement send limit.
HTTP and AI have separate policy, permission, admission, and enable controls.

HTTP grants are mydb extensions. With MySQL binlogging enabled, issue such
grants with session `sql_log_bin=OFF`; attempting to binlog them returns an
explicit unsupported-context error. Native replication still carries catalog
changes.

An optional policy file uses version 1:

```json
{
  "version": 1,
  "egress": {
    "allowed_origins": ["http://127.0.0.1:8080"],
    "allow_plaintext_origins": ["http://127.0.0.1:8080"],
    "private_destinations": ["127.0.0.1/32"]
  },
  "ca_bindings": {},
  "user_limits": [
    {"user":"integration","host":"localhost","max_inflight":2,
     "requests_per_minute":120,"max_http_attempts_per_statement":100}
  ]
}
```

Omitting `egress` permits all HTTP/HTTPS destinations. Supplying it requires an
allowed canonical origin; private destinations additionally need an explicit
IP network, and HTTP origins need an explicit plaintext entry. An empty
`allowed_origins` denies all requests. DNS answers are checked and pinned to
the client connection. IPv4-mapped IPv6 is checked as IPv4.

`ca_bindings` maps canonical origins to administrator-owned PEM CA files.
Normal certificate-chain and hostname validation remain enabled. There is no
call-level TLS verification bypass. Environment proxies are ignored and no
Cookie jar is shared between calls.

Redirect following is opt-in and limited to the original origin. HTTPS
never redirects to HTTP. A followed 301/302 converts POST to GET; 303 converts
non-HEAD methods to GET; 307/308 preserve method and body. Converted requests
have no body or body Content-Type. Every destination is checked again.

```sql
GRANT HTTP_ADMIN ON *.* TO 'operator'@'localhost';
ALTER SYSTEM RELOAD HTTP CONFIG;
SET GLOBAL mydb_http_enabled = OFF;
SET GLOBAL mydb_http_enabled = ON;
SHOW HTTP STATUS;
```

Setting the global switch also requires the usual global-system-variable
permission. A failed policy reload retains the previous policy. Successful
reload or disable cancels queued and in-flight local waits without resending.
It cannot revoke operations already received by an upstream service.

SHOW HTTP STATUS requires HTTP_ADMIN and exposes aggregate calls, attempts,
status classes, error classes, elapsed time, queue/inflight counts, and charged
memory. It contains no endpoint URLs or payloads.

## Error families

The stable extension codes are ER_HTTP_INVALID_ARGUMENT (50100),
ER_HTTP_ACCESS_DENIED (50101), ER_HTTP_DISABLED (50102), ER_HTTP_EGRESS_DENIED
(50103), ER_HTTP_BUDGET_EXCEEDED (50104), ER_HTTP_QUEUE_FULL (50105), ER_HTTP_DNS
(50106), ER_HTTP_CONNECT (50107), ER_HTTP_TLS (50108), ER_HTTP_TIMEOUT (50109),
ER_HTTP_INVALID_RESPONSE (50110), ER_HTTP_TRANSPORT (50111),
ER_HTTP_UNSUPPORTED_CONTEXT (50112), and ER_HTTP_REPLAY_FORBIDDEN (50113).
Authorization and egress errors use SQLSTATE 42000; invalid arguments use
22023; the remaining HTTP conditions use HY000. Existing SQL cancellation and
memory conditions retain their native codes.

Error numbers are unsigned 16-bit values on the MySQL wire. A client that
exposes extension codes as signed integers can normalize them with
`code & 0xffff` before comparing the numbers above.
