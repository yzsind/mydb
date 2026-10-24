# `JSON_SCHEMA_VALIDATION_REPORT`

## Description

Returns the JSON Schema validation result and failure details.

## Syntax

```sql
JSON_SCHEMA_VALIDATION_REPORT(schema, document)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `schema` | Yes | JSON document or JSON path/scalar | The 1st argument; the meaning is determined by the function syntax and description |
| `document` | Yes | JSON document or JSON path/scalar | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a JSON validation report. A valid document produces `{"valid": true}`; a failing document includes validation failure details. SQL NULL input returns NULL.

## Examples

```sql
SELECT JSON_SCHEMA_VALIDATION_REPORT('{"a":1}', '{"a":1}');
```

Result:

```text
{"valid": true}
```

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
