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
| `schema` | Yes | JSON object or matching JSON text | Draft 4 schema with validation keywords such as type/properties/required. |
| `document` | Yes | JSON document or valid JSON text | Value validated against the schema. |

## Return Value

Returns a JSON validation report. A valid document produces `{"valid": true}`; a failing document includes validation failure details. SQL NULL input returns NULL.

## Examples

```sql
SELECT JSON_SCHEMA_VALIDATION_REPORT('{"type":"integer"}', '"text"');
```

The result has `valid:false` and includes `reason`, `schema-location`, `document-location` and `schema-failed-keyword` (type). A conforming document returns `{"valid":true}`. Error wording may include details specific to the failed constraint.

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

- Schemas use Draft 4 validation, must be JSON objects, and are validated before document validation. Use type/properties/required/items and supported constraints, not an example document as a schema. Invalid schema syntax/keywords produce explicit diagnostics; remote schemas are not fetched through a general network loader.

## Related Functions

[`JSON_SCHEMA_VALID`](json_schema_valid.md).
