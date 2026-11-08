# `JSON_UNQUOTE`

## Description

Removes JSON string quoting and processes escape sequences.

## Syntax

```sql
JSON_UNQUOTE(json_doc)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document or valid JSON text | The json document to process |

## Return Value

Returns text after decoding a JSON string value and its escapes. Non-string JSON values retain their textual representation; ordinary unquoted text is preserved. SQL NULL returns NULL. Invalid quoted JSON string text raises an error.

## Examples

```sql
SELECT JSON_UNQUOTE('"mydb"');
```

Result: mydb

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

## Related Functions

[`JSON_ARRAY`](json_array.md), [`JSON_OBJECT`](json_object.md), [`JSON_QUOTE`](json_quote.md).
