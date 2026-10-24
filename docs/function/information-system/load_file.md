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
| `file_name` | Yes | String | The 1st argument; the path of a file accessible to the server |

## Return Value

Reads a server-side file under privilege and `secure_file_priv` constraints. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT LOAD_FILE('/allowed/path/example.txt');
```

Result: returns the file bytes when the privilege, path, and size restrictions are satisfied; otherwise returns NULL or a clear error.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires the file privilege and is subject to `secure_file_priv`, server-accessible paths, and file size limits.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
