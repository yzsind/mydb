# `LOAD_FILE`

## Description

Reads a server-side file under privilege and `secure_file_priv` constraints.

## Syntax

```sql
LOAD_FILE(file_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `file_name` | Yes | String | Server filesystem path; see FILE privilege, secure_file_priv and packet limits. |

## Return Value

Returns server-side file bytes, or SQL NULL for SQL NULL input, missing FILE privilege, unreadable/nonregular files, denied paths, or ordinary size/access failure. Conversion, allocation and cancellation errors can still raise SQL errors; this is not a client-file read.

## Examples

```sql
SELECT LOAD_FILE('/allowed/path/example.txt');
```

Result: returns the file bytes when the privilege, path, and size restrictions are satisfied; otherwise returns NULL or a clear error.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Requires the file privilege and is subject to `secure_file_priv`, server-accessible paths, and file size limits.

- Requires global FILE. Relative paths resolve under the server data directory; canonicalized paths are checked against secure_file_priv. NULL policy disables file reads, empty policy removes the directory restriction, and a directory policy restricts the canonical path. The target must be a regular file and fit max_allowed_packet/governed memory.

## Related Functions

[`SLEEP`](sleep.md), [`BENCHMARK`](benchmark.md), [`MYSQL_QUERY_ATTRIBUTE_STRING`](mysql_query_attribute_string.md).
