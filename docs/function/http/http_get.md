# `HTTP_GET`

```sql
HTTP_GET(url [, options])
```

Calls an HTTP endpoint and returns the [common response JSON](../../http.md#response-contract).
Requires `HTTP_USE`. The function is volatile and external effects are not
rolled back with the database transaction. SQL NULL URL returns SQL NULL.

```sql
SELECT HTTP_GET('http://127.0.0.1:8080/echo');
```

This example requires the [local test service and egress policy](README.md#local-example-setup). Merely starting a loopback service does not allow access under the default policy. Status, headers,
body, timing, and errors depend on that service. HTTP error status codes are
returned as data; transport failures normally raise SQL errors.

See [HTTP SQL Functions](../../http.md) for request options, body and binary
encoding, authentication, execution semantics, limits, and node policy.

## Arguments and result

The URL must be HTTP/HTTPS text. Omitted/SQL NULL options means an empty
options object; otherwise use a JSON object or JSON text. Unknown fields, invalid
types, duplicate keys and conflicting body representations are errors.

Returns native response JSON. SQL NULL URL returns SQL NULL. Inspect status
explicitly: HTTP 4xx/5xx are response data; transport failures normally raise SQL
errors. `{"on_error":"return"}` can capture convertible transport failures, but does
not catch authorization, egress-policy, resource, KILL or SQL-deadline failures.

Requires global HTTP_USE and the node egress policy. Calls are volatile, can have
external side effects, are not rolled back and are not automatically retried.
See [HTTP response contract](../../http.md#response-contract) and
[request options](../../http.md#request-options) for authentication, binary data,
limits and the complete response schema.

## Reading a response

After the local example setup, inspect the JSON fields rather than treating
an HTTP response as a SQL success/failure boolean:

```sql
SET @response = HTTP_GET('http://127.0.0.1:8080/echo');
SELECT JSON_EXTRACT(@response,'$.status') AS status,
       JSON_UNQUOTE(JSON_EXTRACT(@response,'$.body_encoding')) AS encoding;
```

The supplied test service returns status 200 and encoding utf8. The body is
text inside the response envelope; parse it separately if the endpoint returns
JSON. Timing and headers vary; see the shared guide for binary response handling.
