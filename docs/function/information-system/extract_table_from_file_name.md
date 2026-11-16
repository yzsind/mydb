# `EXTRACT_TABLE_FROM_FILE_NAME`

## Description

Extracts the table name from a server-internal file name.

## Syntax

```sql
EXTRACT_TABLE_FROM_FILE_NAME(file_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `file_name` | Yes | String | Filesystem-style path, not a numeric argument or a JSON path. |

## Return Value

Returns the final filename without its extension as text. SQL NULL returns NULL. Both slash styles are recognized; this helper parses text and does not verify that a server table/file exists.

## Examples

```sql
SELECT EXTRACT_TABLE_FROM_FILE_NAME('/var/lib/mysql/test/t.ibd');
```

Result: t

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_PATH`](format_path.md), [`EXTRACT_SCHEMA_FROM_FILE_NAME`](extract_schema_from_file_name.md).
