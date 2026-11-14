# AI HTTP and REST Configuration

These settings describe the current source checkout. See [startup configuration](configuration.md)
for option syntax, precedence and persistence, and the [option registry](configuration-registry.md)
for accepted startup names and validators.

## External AI calls

`ai_enabled` / `MYDB_AI_ENABLED` defaults to ON. AI connections and models can
be configured through SQL without a local policy file. By default, configured
HTTP/HTTPS services have no outbound origin, IP address, network-range, or port
restrictions, including public HTTP and link-local addresses. IPv4, IPv6, and
DNS names are supported.
`ai_local_config` (`MYDB_AI_LOCAL_CONFIG`) is optional, for explicit outbound
origins, IP policies, private CAs, node-local account quotas, and historical
encryption keys. An omitted `egress` leaves the network unrestricted;
an explicit `egress` enables an allowlist. Invalid explicit files
return errors rather than falling back to defaults.
The startup capacity controls are `ai_max_inflight` (32), `ai_max_queue` (128),
and `ai_memory_limit_mb` (128), with corresponding `MYDB_AI_*` environment names.
`mydb_ai_enabled` is the privileged GLOBAL node switch. Set `ai_enabled=OFF`
at startup or `SET GLOBAL mydb_ai_enabled=OFF` to disable calls. AI_ADMIN and
AI_USE privileges remain required for management and model use, respectively.

API keys are encrypted database metadata. The master key comes from the
**environment-only** `MYDB_AI_ENCRYPT_KEY`, or a randomly initialized
`<data_dir>/apikey.mydb` keyring. The filename is not a default password.
Keep master keys separate from database backups and deploy matching keys to
replicas independently. See [LLM and Embedding Calls](ai.md) for the exact key
format, TLS requirements, local policy, model DDL, SQL semantics, and recovery.

## HTTP SQL calls

HTTP client SQL functions are enabled by default and require `HTTP_USE`. See
[HTTP SQL Functions](http.md#node-configuration) for the `http_*` startup options,
node egress policy, private CA bindings, account quotas, and runtime controls.

### Date name locales

`lc_time_names` supports `en_US`, `de_DE`, `fr_FR`, `es_ES`, `it_IT`,
`pt_BR`, `zh_CN`, `ja_JP`, and `ko_KR`. Unsupported names return error 1649;
assignments never silently fall back to English. It controls date name output.
`STR_TO_DATE` recognizes English month and weekday names independently of this
output setting, matching MySQL 8.0.

## REST SQL listener

The shared REST SQL/MCP listener is disabled by default and shares the SQL engine and
account resource limits with MySQL. See [REST SQL API](rest-api.md) for
credentials, full key retrieval, transactions, result formats and recovery.
All options below also support CLI `--rest-...` and `MYDB_REST_*` variables.
The listener starts when either `rest_enabled` or `mcp_enabled` is ON; REST
and MCP routes are enabled independently. The `rest_*` listener, TLS, SQL
session and memory settings below apply to both protocols. See [MCP Server](mcp.md)
for the `/mcp` endpoint, authentication and `mcp_*` limits.

| Option | Default |
| --- | --- |
| `rest_enabled` | `OFF` |
| `rest_host`, `rest_port` | `127.0.0.1`, `8739` |
| `rest_tls_mode` | `required`; `disabled` requires loopback |
| `rest_ssl_cert`, `rest_ssl_key` | Unset; supply both or neither |
| `rest_max_connections` | `512` |
| `rest_max_sessions` | `256`; also subject to SQL connection limits |
| `rest_max_inflight` | `64` |
| `rest_max_request_bytes` | `8388608` |
| `rest_max_header_bytes` | `16384` |
| `rest_max_json_response_bytes` | `16777216` |
| `rest_memory_limit_mb` | `128` |
| `rest_default_timeout_ms`, `rest_max_timeout_ms` | `30000`, `300000` |
| `rest_session_idle_timeout_ms` | `300000` |
| `rest_transaction_idle_timeout_ms` | `60000` |
| `rest_previous_keys_file` | Unset; old master-key keyring for rotation |

API keys default to no expiration and have a fixed limit of 1024 records per
account. Complete keys can be retrieved through TLS MySQL `SHOW API KEY`.
The master key is environment-only `MYDB_REST_ENCRYPT_KEY` or the independently
managed `<data_dir>/rest-keys.mydb`. Master-key values cannot be supplied in
ordinary startup configuration or command-line arguments.

