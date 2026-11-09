# `JSON_VALID`

## Description

Determines whether the input is a valid JSON document.

## Syntax

```sql
JSON_VALID(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns integer 1 for valid JSON, 0 for invalid JSON text, and SQL NULL for SQL NULL input. A JSON null document is valid and returns 1.

## Examples

```sql
SELECT JSON_VALID('{"a":1}');
```

Result: 1

## Notes

- This function validates a document, not a JSON path. Invalid document text returns 0 rather than a JSON parse error; argument-count and conversion errors retain their ordinary diagnostics.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_VALUE`](json_value.md), [`JSON_KEYS`](json_keys.md), [`JSON_LENGTH`](json_length.md).
