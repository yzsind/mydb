# `SHOW HTTP STATUS`

## Description

Reads aggregate HTTP SQL runtime counters.

## Syntax

```ebnf
<show-http-status> ::= 'SHOW HTTP STATUS'
```

## Behavior, Privileges, and Results

Requires global `HTTP_ADMIN`. The result has `Variable_name` (VARCHAR) and `Value` (BIGINT) columns. The output contains aggregate calls, attempts, HTTP status/error classes, elapsed time, queued/in-flight work and charged memory. It does not expose URLs, credentials, request bodies or response payloads. Counters are sampled during concurrent execution and are not a transactional snapshot. Reading does not send requests or clear counters.

## Example

```sql
SHOW HTTP STATUS;
```

## See Also

- [HTTP node policy](../../http.md#node-configuration)
- [Integration index](README.md)
