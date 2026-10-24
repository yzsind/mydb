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
| `schema` | Yes | JSON document or JSON path/scalar | The 1st argument; the meaning is determined by the function syntax and description |
| `document` | Yes | JSON document or JSON path/scalar | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns integer 1 if the document conforms to the supplied JSON schema, otherwise 0. SQL NULL input returns NULL; invalid JSON/schema arguments return an explicit error.

## Examples

```sql
SELECT JSON_SCHEMA_VALID('{"a":1}', '{"a":1}');
```

Result:

```text
1
```

## Notes

- JSON paths must use the MySQL JSON path syntax currently supported by mydb; an invalid document or path returns a clear error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
