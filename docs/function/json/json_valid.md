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
| `json_doc` | Yes | JSON document or JSON path/scalar | The 1st argument; the JSON document to process |

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

This function has no direct alias in this category; see the category index for related functions.
