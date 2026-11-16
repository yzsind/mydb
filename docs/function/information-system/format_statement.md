# `FORMAT_STATEMENT`

## Description

Truncates and formats an SQL statement into text suitable for diagnostic display.

## Syntax

```sql
FORMAT_STATEMENT(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | SQL text to parse, normalize, or format. |

## Return Value

Returns SQL text truncated to sys.sys_config statement_truncate_len characters (default 64), including an ellipsis when truncated. SQL NULL returns NULL. A limit at most 3 yields that many dots; it is display formatting, not executable SQL rewriting.

## Examples

```sql
SELECT FORMAT_STATEMENT('SELECT 1');
```

Result:

```text
SELECT 1
```

Assumes statement_truncate_len has its default 64-character limit.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`STATEMENT_DIGEST`](statement_digest.md), [`STATEMENT_DIGEST_TEXT`](statement_digest_text.md), [`SYS_GET_CONFIG`](sys_get_config.md).
