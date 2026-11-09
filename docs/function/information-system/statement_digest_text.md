# `STATEMENT_DIGEST_TEXT`

## Description

Returns the normalized digest text of SQL text.

## Syntax

```sql
STATEMENT_DIGEST_TEXT(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | SQL text to parse, normalize, or format. |

## Return Value

Returns normalized SQL digest text, replacing values with markers according to the parser's digest rules. SQL NULL returns NULL. Invalid SQL or multiple statements are rejected; the function does not execute the statement.

## Examples

```sql
SELECT STATEMENT_DIGEST_TEXT('SELECT 1');
```

Result:

```text
SELECT ?
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_STATEMENT`](format_statement.md), [`STATEMENT_DIGEST`](statement_digest.md), [`SYS_GET_CONFIG`](sys_get_config.md).
