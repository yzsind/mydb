# `SYS_GET_CONFIG`

## Description

Reads a value from the sys schema configuration, returning a default value when it does not exist.

## Syntax

```sql
SYS_GET_CONFIG(variable, default)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `variable` | Yes | String | Name in sys.sys_config. |
| `default` | Yes | Any SQL expression | Value returned when no configuration value exists. |

## Return Value

Returns the sys.sys_config value as text when present; otherwise the supplied fallback converted to text, or NULL when appropriate. Reads configuration state rather than modifying it.

## Examples

```sql
SELECT SYS_GET_CONFIG('statement_truncate_len', '64');
```

Result: returns the configured value; returns the default value 64 when it does not exist.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_STATEMENT`](format_statement.md), [`STATEMENT_DIGEST`](statement_digest.md), [`STATEMENT_DIGEST_TEXT`](statement_digest_text.md).
