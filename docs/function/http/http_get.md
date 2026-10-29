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

This example requires an HTTP service at the given address. Status, headers,
body, timing, and errors depend on that service. HTTP error status codes are
returned as data; transport failures normally raise SQL errors.

See [HTTP SQL Functions](../../http.md) for request options, body and binary
encoding, authentication, execution semantics, limits, and node policy.
