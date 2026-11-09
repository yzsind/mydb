# `TO_BASE64`

## Description

Encodes the input bytes as Base64 text.

## Syntax

```sql
TO_BASE64(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns Base64 text; NULL input returns NULL.

## Examples

```sql
SELECT TO_BASE64('mydb');
```

Result:

```text
bXlkYg==
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- Output wraps after each 76 Base64 characters using newline. Empty input returns empty text; no charset normalization is performed before encoding bytes.

## Related Functions

[`FROM_BASE64`](from_base64.md).
