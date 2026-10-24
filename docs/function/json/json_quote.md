# `JSON_QUOTE`

## Description

Quotes and escapes a string as a JSON string literal.

## Syntax

```sql
JSON_QUOTE(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Content to quote and escape |

## Return Value

Returns text containing a quoted and escaped JSON string literal. The input is string content, not a JSON document to parse; SQL NULL returns SQL NULL.

## Examples

```sql
SELECT JSON_QUOTE('mydb');
```

Result: "mydb"

## Notes

- This function does not parse the input as JSON or accept a JSON path; even text that is not a valid JSON document can be quoted.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
