# `JSON_SCHEMA_VALID`

## Description

Determines whether a JSON document conforms to a given JSON Schema.

## Syntax

```sql
JSON_SCHEMA_VALID(schema, document)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `schema` | Yes | JSON object or matching JSON text | Draft 4 schema with validation keywords such as type/properties/required. |
| `document` | Yes | JSON document or valid JSON text | Value validated against the schema. |

## Return Value

Returns integer 1 if the document conforms to the supplied JSON schema, otherwise 0. SQL NULL input returns NULL; invalid JSON/schema arguments return an explicit error.

## Examples

```sql
SELECT JSON_SCHEMA_VALID('{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}', '{"a":1}'),
       JSON_SCHEMA_VALID('{"type":"integer"}', '"text"');
```

Result:

```text
1	0
```

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

- Schemas use Draft 4 validation, must be JSON objects, and are validated before document validation. Use type/properties/required/items and supported constraints, not an example document as a schema. Invalid schema syntax/keywords produce explicit diagnostics; remote schemas are not fetched through a general network loader.

## Related Functions

[`JSON_SCHEMA_VALIDATION_REPORT`](json_schema_validation_report.md).
