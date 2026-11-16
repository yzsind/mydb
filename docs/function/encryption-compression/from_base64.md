# `FROM_BASE64`

## Description

Decodes Base64 text.

## Syntax

```sql
FROM_BASE64(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns decoded binary bytes; NULL input or invalid Base64 returns NULL. The example uses HEX so the bytes have an unambiguous printable representation.

## Examples

```sql
SELECT HEX(FROM_BASE64('bXlkYg=='));
```

Result:

```text
6D796462
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- ASCII whitespace in wrapped Base64 is ignored. Invalid Base64 returns NULL; decoded output is binary. Use HEX or an explicit charset conversion to display it.

## Related Functions

[`TO_BASE64`](to_base64.md).
