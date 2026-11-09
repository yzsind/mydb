# `STATEMENT_DIGEST`

## Description

Returns the digest hash of normalized SQL text.

## Syntax

```sql
STATEMENT_DIGEST(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | SQL text to parse, normalize, or format. |

## Return Value

Returns a 64-character hexadecimal digest of the normalized SQL statement. SQL NULL returns NULL. The input must parse as one supported SQL statement; this function does not execute it and is not a hash of the literal source bytes.

## Examples

```sql
SELECT STATEMENT_DIGEST('SELECT 1');
```

Result:

```text
d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_STATEMENT`](format_statement.md), [`STATEMENT_DIGEST_TEXT`](statement_digest_text.md), [`SYS_GET_CONFIG`](sys_get_config.md).
