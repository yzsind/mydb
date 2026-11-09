# `FORMAT_PATH`

## Description

Shortens and formats Performance Schema file paths.

## Syntax

```sql
FORMAT_PATH(path)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `path` | Yes | String | Server filesystem path to format; not a JSON path. |

## Return Value

Returns text with a leading server-process HOME directory replaced by ~, or the original path when that prefix does not match. SQL NULL returns NULL. It does not implement all MySQL Performance Schema path-variable substitutions.

## Examples

```sql
SELECT FORMAT_PATH('/var/lib/mysql/test/t.ibd');
```

Result:

```text
/var/lib/mysql/test/t.ibd
```

This output assumes the path is outside the server process HOME. A matching HOME prefix is shortened to `~`.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`EXTRACT_SCHEMA_FROM_FILE_NAME`](extract_schema_from_file_name.md), [`EXTRACT_TABLE_FROM_FILE_NAME`](extract_table_from_file_name.md).
