# `JSON_VALUE`

## Description

Extracts and converts a scalar value from a JSON path.

## Syntax

```sql
JSON_VALUE(json_doc, path [RETURNING type] [on_empty] [on_error])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `json_doc` | Yes | JSON document | The source document |
| `path` | Yes | constant JSON path | Must locate a single scalar |
| `RETURNING type` | No | supported return types | Defaults to CHAR(512) |
| `ON EMPTY` | No | NULL, ERROR, or DEFAULT | Handling when the path does not match |
| `ON ERROR` | No | NULL, ERROR, or DEFAULT | Handling when the conversion fails |

## Return Value

Returns the selected scalar converted to RETURNING type, default CHAR(512). No match uses ON EMPTY (default NULL); non-scalar/conversion failures use ON ERROR (default NULL). SQL NULL document returns NULL. Invalid document/path and SQL errors are not all caught by ON ERROR.

## Examples

```sql
SELECT JSON_VALUE('{"a":1}', '$.a' RETURNING UNSIGNED);
```

Result: 1

## Notes

- See [JSON values and paths](README.md#json-values-and-paths) for conversion, path support and invalid-input diagnostics.

- `NULL ON EMPTY`, `ERROR ON EMPTY`, or `DEFAULT literal ON EMPTY` select missing-path handling. The corresponding ON ERROR forms control scalar-conversion failures. Place ON EMPTY before ON ERROR.
- RETURNING accepts the supported JSON_VALUE scalar targets (CHAR, SIGNED/UNSIGNED, DECIMAL, FLOAT/DOUBLE, DATE, TIME, DATETIME, YEAR and JSON). Specify sizes/precision when needed; unsupported targets and nonconstant paths are rejected.

## Related Functions

[`JSON_EXTRACT`](json_extract.md), [`JSON_KEYS`](json_keys.md), [`JSON_LENGTH`](json_length.md), [`JSON_DEPTH`](json_depth.md).
