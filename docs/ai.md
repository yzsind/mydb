# LLM and Embedding Calls

mydb calls text-generation and text-embedding services through SQL. It supports
Ollama, OpenAI Responses, OpenAI Chat Completions-compatible deployments, and
OpenAI Embeddings-compatible deployments. Model weights and inference processes
remain outside the database.

`AI_GENERATE` returns text. `AI_EMBED` returns a native FLOAT32 `VECTOR`. Direct results use the native
VECTOR wire type with little-endian FLOAT32 coordinates; use
`VECTOR_TO_STRING` for readable SQL-client output.
Their `_DETAIL` variants return JSON containing the result, model revision,
usage when reported by the provider, timing, and errors when explicitly requested.

## Enable a node

AI calls are disabled by default. Configure an explicit outbound policy:

```ini
[mydb]
ai_enabled = ON
ai_local_config = /etc/mydb/ai-local.json
ai_max_inflight = 32
ai_max_queue = 128
ai_memory_limit_mb = 128
```

Equivalent startup environment variables are `MYDB_AI_ENABLED`,
`MYDB_AI_LOCAL_CONFIG`, `MYDB_AI_MAX_INFLIGHT`, `MYDB_AI_MAX_QUEUE`, and
`MYDB_AI_MEMORY_LIMIT_MB`. Capacity settings are startup settings.

Example policy for a local Ollama service:

```json
{
  "version": 1,
  "encryption": {"previous_keys_file": null},
  "ca_bindings": {},
  "user_limits": [],
  "egress": {
    "allowed_origins": ["http://127.0.0.1:11434"],
    "private_destinations": ["127.0.0.1/32"],
    "allow_plaintext_origins": ["http://127.0.0.1:11434"]
  }
}
```

Private destinations require both an allowed origin and an explicit IP network.
Plain HTTP additionally requires an entry in `allow_plaintext_origins`.
Public HTTPS services also require their origin in `allowed_origins`.
DNS results are checked and pinned to the resulting client connection;
redirects and environment-configured proxies are disabled. Connection clients
are pooled and periodically refreshed.

Node policies are not database metadata. Deploy them separately on each node.
`ALTER SYSTEM RELOAD AI CONFIG` validates and atomically replaces the local
policy. A failed reload keeps the preceding policy. In-flight calls can fail
when the policy generation changes; retry the statement after checking its
external execution status. Policy reload never resends a model request.

```sql
SET GLOBAL mydb_ai_enabled = OFF;
SET GLOBAL mydb_ai_enabled = ON;
```

The node switch requires `AI_ADMIN` and the normal authorization for setting
global system variables. Turning it off stops queued calls and cancels local
HTTP waits. A request already received by a provider may still execute and
incur charges.

## Configure Ollama

Install the desired models in Ollama before creating database model objects.
The following examples use `qwen3:0.6b` and `all-minilm:latest`:

```sql
CREATE AI CONNECTION local_models WITH CONFIG '{
  "protocol":"ollama",
  "base_url":"http://127.0.0.1:11434",
  "auth":{"type":"none"}
}';

CREATE AI MODEL test.local_chat USING local_models WITH CONFIG '{
  "kind":"text_generation",
  "remote_model":"qwen3:0.6b",
  "model_revision":"qwen3-local-v1",
  "capabilities":{"messages":true,"temperature":true,"json_schema":true},
  "defaults":{"max_output_tokens":256,"provider_options":{"think":false}},
  "provider_options_allowed":["think"]
}';

CREATE AI MODEL test.local_embed USING local_models WITH CONFIG '{
  "kind":"embedding",
  "remote_model":"all-minilm:latest",
  "model_revision":"all-minilm-local-v1",
  "embedding":{"dimensions":384,"normalize":"none"}
}';

SELECT AI_GENERATE('test.local_chat', 'Describe MVCC in one sentence.');
SELECT VECTOR_DIM(AI_EMBED('test.local_embed', 'Database transactions'));
```

Capability declarations must match the actual model and deployment. They do
not make an unsupported provider parameter work. Ollama requests use
`stream=false`; embedding requests also use `truncate=false`.

## Encrypted API keys

API keys are persisted as encrypted database metadata. Each Secret payload
uses a new random data key and AES-256-GCM authenticated encryption. The data
key is wrapped with the instance master key using AES-256-KWP. The payload
is bound to the catalog encryption identity, Secret ID, and generation.

The preferred master-key source is `MYDB_AI_ENCRYPT_KEY`. Accepted formats:

- `base64:` followed by the standard Base64 encoding of exactly 32 random bytes.
- `hex:` followed by exactly 64 hexadecimal characters.

For example, provision a random environment key before starting mydb:

```bash
export MYDB_AI_ENCRYPT_KEY="base64:$(openssl rand -base64 32)"
```

An empty or malformed environment value is an error and never falls back to a
file. Short passwords are not padded, repeated, truncated, or hashed into keys.
Do not place the master key in SQL or ordinary configuration files.

Without the environment variable, first-time Secret initialization creates a
random master key in `<data_dir>/apikey.mydb`, using restrictive Unix file
permissions. Platforms without Unix permission enforcement require the environment key provider. `apikey.mydb` is a filename, not a shared default password.
If encrypted metadata already exists and its key is missing, mydb never
creates a replacement key automatically. Ordinary SQL remains available,
while calls requiring the missing key fail explicitly.

Write API keys using a binary prepared statement over a TLS connection:

```python
import os
import mysql.connector

connection = mysql.connector.connect(
    host="127.0.0.1", port=8738, user="root",
    password=os.environ["MYDB_PASSWORD"], database="test",
    ssl_disabled=False,
)
cursor = connection.cursor(prepared=True)
cursor.execute("CREATE AI SECRET cloud_key VALUE %s",
               (os.environ["MODEL_API_KEY"],))
connection.close()
```

Configure normal certificate verification for remote database connections.
Secret DDL requires TLS. Prepared Secret DDL requires a placeholder rather than
an embedded key literal. Direct textual Secret DDL accepts a string literal,
but client-side SQL histories and tracing are outside the server's control.

```sql
CREATE AI CONNECTION cloud_text WITH CONFIG '{
  "protocol":"openai_responses",
  "base_url":"https://api.openai.com/v1",
  "auth":{"type":"bearer","secret":"cloud_key"}
}';

CREATE AI MODEL test.cloud_chat USING cloud_text WITH CONFIG '{
  "kind":"text_generation",
  "remote_model":"YOUR_TEXT_MODEL_ID",
  "model_revision":"text-deployment-v1",
  "defaults":{"max_output_tokens":1024}
}';
```

`auth.type` also supports `header`, with `header_name` and `secret`, for
trusted API gateways. Authentication headers cannot be supplied as fixed
connection headers or call options. Fixed header names must be unique without
regard to case and cannot conflict with the configured authentication header.
Transport-owned headers such as Content-Type, Content-Length, Connection, and
Transfer-Encoding are managed by mydb and cannot be configured as fixed or
authentication headers. SQL has no function to export decrypted
Secrets, including for administrators. SHOW and system views expose metadata
and readiness, without key bytes or ciphertext.

Catalog, WAL, native replication, and physical backups contain encrypted
Secret envelopes. They never contain the master key, plaintext data key, or
plaintext Secret VALUE. Physical backup and native base-backup file sets
exclude `apikey.mydb`; back up the keyring separately with appropriate access
controls. A generic filesystem copy of the entire data directory may include
both keys and ciphertext, so it does not provide that separation.

### Rotation and recovery

Use prepared `ALTER AI SECRET name VALUE ?` to replace a provider API key.
Revoke a compromised key with the provider as well. To stop its use immediately,
disable the relevant model or connection before replacing it.

For master-key rotation, securely provision a keyring containing the candidate
active key and all still-needed historical keys, then run:

```sql
ALTER SYSTEM ROTATE AI ENCRYPTION KEY;
```

Environment-key deployments may supply historical keys through the node
policy's `encryption.previous_keys_file`. Changing an external shell environment
does not update a running process; deploy its environment and restart or use
an appropriate deployment mechanism. Rotation rewraps data keys and preserves
the API-key payload ciphertext and business generation. Retain old keys for
older WAL, PITR targets, backups, and replicas until their retention windows end.
Losing the only applicable master key makes its encrypted Secrets unrecoverable.

Native replicas need the corresponding master keys through an independent
secure deployment channel. Applying ciphertext WAL does not require decryption;
serving AI calls or taking over the AI workload does. Restores preserve the
catalog encryption identity used by authenticated encryption.

## Calling functions

```sql
AI_GENERATE(model, input [, options])
AI_GENERATE_DETAIL(model, input [, options])
AI_EMBED(model, text [, options])
AI_EMBED_DETAIL(model, input [, options])
```

Model names are literals, placeholders, or user variables. Unqualified names
resolve in the executing database. A model is fixed for each call position;
it cannot be selected from a data row.

A SQL string is a prompt, including one that happens to contain JSON text.
A JSON array is a text message history for generation, or an explicit batch of
strings for `AI_EMBED_DETAIL`. Messages accept `system`, `user`, and `assistant`
roles and text content. At least one user message is required. Numeric, binary,
date, geometry, and vector inputs need an explicit text conversion when used
as prompts. SQL NULL inputs make no HTTP request; a non-NULL model is still
resolved and authorized. Omitted or SQL NULL options mean an empty object;
a JSON null options value is invalid.

```sql
SELECT AI_GENERATE_DETAIL(
  'test.local_chat',
  JSON_ARRAY(
    JSON_OBJECT('role','system','content','Summarize database documentation.'),
    JSON_OBJECT('role','user','content', ?)
  ),
  JSON_OBJECT('max_output_tokens',128,'timeout_ms',10000)
);

SELECT AI_EMBED_DETAIL(
  'test.local_embed', JSON_ARRAY('first document','second document')
);
```

Generation options include `temperature`, `top_p`, `max_output_tokens`, `stop`,
`seed`, and `response_format`, subject to declared capabilities. Execution
options include `timeout_ms`, `max_retries`, `retry_ambiguous`, and `on_error`.
Unknown, duplicate, conflicting, unsupported, or out-of-range parameters fail.
Defaults are merged with call options; arrays replace arrays. An optional
public parameter can be removed with JSON null. Execution controls cannot be
removed this way. `provider_options` admits only model-allowlisted paths with
an adapter-known type or an explicit local JSON Schema. Reserved request,
authentication, routing, streaming, tool, and budget fields cannot be overridden.

For structured text output:

```sql
SELECT AI_GENERATE_DETAIL('test.local_chat', ?, JSON_OBJECT(
  'response_format', JSON_OBJECT(
    'type','json_schema', 'name','summary', 'strict',TRUE,
    'schema',JSON_OBJECT(
      'type','object',
      'properties',JSON_OBJECT('summary',JSON_OBJECT('type','string')),
      'required',JSON_ARRAY('summary'), 'additionalProperties',FALSE
    )
  ),
  'max_output_tokens',256,
  'on_error','return'
));
```

The generated value is parsed and validated locally before `output_json` is
returned. Refusal, truncation, missing text, and schema mismatch are errors. Ollama and
Chat Completions replies must identify the assistant role; tool/function call
outputs are rejected even when accompanied by text.
`on_error='return'` is available only on DETAIL functions and returns
`ok=false` with structured call errors. It does not catch invalid SQL, access
denials, unsupported contexts, resource-budget failures, KILL, or the query's
execution deadline. Missing usage is JSON null, rather than an invented zero.

## Execution boundaries

AI functions run in the outermost final SELECT projection or in a single
`SET @variable = AI_...` assignment. The relational phase applies ordinary
filtering, joins, sorting, OFFSET, and LIMIT before any model call. Owned input
rows pass into a separate external cursor; table metadata locks are released
before it waits on model HTTP. Prepared cursor fetches retain statement
parameters, model definitions, variables, cancellation, and deadlines.

```sql
SELECT id, AI_GENERATE('test.local_chat', body) AS summary
FROM articles
WHERE tenant_id = ?
ORDER BY id
LIMIT 10;

SET @query_vector = AI_EMBED('test.local_embed', ?);
SELECT id, COSINE_DISTANCE(embedding, @query_vector) AS distance
FROM chunks
WHERE tenant_id = ? AND embedding_model_revision = 'all-minilm-local-v1'
ORDER BY distance
LIMIT 10;
```

Call positions are independent. Equal prompts do not imply result deduplication
or a shared generation. CASE/IF/COALESCE preserve their normal branch behavior.
Direct embedding projections can batch compatible ready inputs, preserving
input order and duplicate occurrences. Calls inside conditions are evaluated
only for reached branches. Text generation is not implicitly deduplicated or
batched into an unverified provider API.

Place aggregation, windows, and DISTINCT in a pure inner query. AI calls are
rejected in predicates, joins, ordering, aggregate/window inputs, DML, CTAS,
persistent views, generated columns, defaults, checks, indexes, stored programs,
triggers, and events. Explicit transactions, autocommit=0, XA, and locking reads
cannot contain model calls. Use explicit projection columns. Schema deletion
requires dropping its AI models first.

External effects are not transactional. A failed statement, rollback, timeout,
disconnect, or local cancellation cannot undo a provider's work or charges.
Some rows can arrive before a later SQL error; consume the final statement
status before treating the whole result as successful. Save generated values
with ordinary parameterized DML in a subsequent short transaction, checking
content versions to avoid storing embeddings for stale text.

`EXPLAIN` binds and displays the external stage without contacting a provider.
`EXPLAIN ANALYZE` executes it and can incur charges. Optimizer folding,
common-expression elimination, cached results, and query fallback do not
replay model calls. Updating model identity, revision, embedding dimensions,
or normalization requires a new model object. Equal dimensions alone do not
make two embedding spaces compatible.

## Limits and deployment profiles

Default hard bounds include 100 logical call items and 100 HTTP attempts per
statement, 64 items per explicit embedding batch, 256 KiB per input text, 64 KiB
options JSON, JSON depth 32, and 8 MiB request/response bodies. FLOAT32 embeddings
must have their configured dimension in 1..16383 and finite coordinates.
Memory reservations, result-window limits, model limits, and provider limits
can reject a smaller request. Embedding dimensions and normalization are
model properties, not per-call overrides. Inputs are never truncated by mydb;
provider-side text context policies still require deployment validation.

Each statement has a 60-second AI deadline; individual calls default to
30 seconds, including queueing and retries. Captured statement deadlines and
KILL apply while waiting. Cursor time budgets are absolute and are not reset
by FETCH. Model output budgets default to 1024 tokens and have a 4096-token
ceiling. Reasoning tokens are governed according to the chosen protocol.

The instance defaults to 32 in-flight HTTP calls, a queue of 128, and 128 MiB
of governed AI memory. Instance, connection, model, and account concurrency
and RPM budgets are admitted together. A request waiting for any budget does
not hold another budget's slot or spend its tokens. Runnable older waiters get
priority without blocking requests whose independent budgets are available.
Concurrency/RPM changes use the current model and connection limits, while
existing in-flight use remains counted across limit changes. `user_limits` entries match the authenticated `(user,host)` account,
not a new client-address wildcard search. Entries support `max_inflight`,
`requests_per_minute`, `max_call_items_per_statement`, and
`max_http_attempts_per_statement`. Strict token limits require a trusted
tokenizer or gateway; unsupported token-limit settings fail explicitly.

Automatic retries default to zero. A model can permit up to eight retries and
optionally allow ambiguous retries. Each attempt consumes the statement budget;
backoff and valid Retry-After values cannot extend its deadline. Resending an
ambiguous request can repeat work and charges. There is no automatic model
fallback or cross-statement response cache. Connection health failures open a
bounded circuit breaker; authentication failures require a credential or
configuration generation change before retrying. Each logical connection uses
one versioned health entry; responses from an older version cannot change the
new version's state. Queued requests recheck health before sending. Health
results are recorded before releasing slots, so waiting requests observe a
newly opened circuit or authentication latch. Reservations that never reach
dispatch return their rate credit when the quota version still matches;
already dispatched attempts keep their charge. Closing and reopening a database instance starts a new
runtime identity and does not inherit the previous instance's health cache.

Connection `transport` can name a `ca_file` from local `ca_bindings` and set
`connect_timeout_ms`. The default is 5000 ms with certificate and hostname
verification. `profile='openai_chat_legacy'` uses a deployment's older
`max_tokens` field; the default Chat profile uses `max_completion_tokens`.
Protocol and profile support do not imply support for every provider model.

## Administration and visibility

`AI_ADMIN` controls AI objects and local administration. `AI_USE` controls
model calls, independently of table/column SELECT privileges:

```sql
GRANT AI_ADMIN ON *.* TO 'ai_admin'@'localhost';
GRANT AI_USE ON AI MODEL test.local_chat TO 'app'@'%';
GRANT AI_USE ON AI MODEL test.local_embed TO 'app'@'%';
REVOKE AI_USE ON AI MODEL test.local_chat FROM 'app'@'%';
```

Global `GRANT AI_USE ON *.*` explicitly includes future models. Bootstrap root
has explicit AI grants; SUPER alone is not an AI capability. Roles and grant
options follow normal authorization rules. Deleting a model retires its grants;
recreating the same name does not restore them. Execution and dispatch recheck
revocations, including prepared executions.

Management syntax includes CREATE/ALTER/DROP AI SECRET, CONNECTION, and MODEL;
SHOW AI SECRETS/CONNECTIONS/MODELS; SHOW CREATE AI objects; node-policy reload;
and master-key rotation. Connection and model ALTER replace their complete
configuration rather than applying a JSON patch. A connection configuration
change cancels its queued and in-flight local waits, preventing queued requests
from sending superseded credentials or routing settings. Requests already
received upstream may still execute and incur charges. Referenced Secrets and
connections cannot be dropped.

Read-only metadata is available in `information_schema.AI_MODELS`,
`AI_CONNECTIONS`, `AI_SECRETS`, and `AI_RUNTIME_STATUS`. Models are filtered by
model privileges; sensitive management metadata requires AI_ADMIN. AI SQL is
redacted in server SQL logs, process metadata, prepared-statement snapshots,
and optimizer trace. The server never automatically executes generated SQL,
tools, remote file fetches, or model-supplied instructions.

AI catalog changes, AI-specific grants/revokes, and master-key rotation are
rejected while outgoing MySQL binary logging is enabled. Native WAL replication
replicates encrypted metadata and stored results, and never replays an external
model call. Ordinary row replication of saved results follows existing MySQL
type-compatibility boundaries.
