# `JSON_ARRAY`

## Description

Constructs a JSON array from the arguments.

## Syntax

```sql
JSON_ARRAY([value [, value] ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | No, repeatable | any expression | Values appended to the array in order |

## Return Value

Returns a JSON array, including JSON null for each SQL NULL argument. With no arguments it returns an empty array; JSON_ARRAY(NULL) returns [null], not SQL NULL.

## Examples

```sql
SELECT JSON_ARRAY(1, 2, 3);
```

Result: [1, 2, 3]

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_OBJECT`](json_object.md), [`JSON_QUOTE`](json_quote.md), [`JSON_UNQUOTE`](json_unquote.md).
