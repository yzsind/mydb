# `MYSQL_QUERY_ATTRIBUTE_STRING`

## Description

Reads a named attribute from the current protocol command.

## Syntax

```sql
MYSQL_QUERY_ATTRIBUTE_STRING(attribute_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `attribute_name` | Yes | String | Attribute name in the current protocol command. |

## Return Value

Returns the named query attribute converted to text, or SQL NULL when the attribute is absent or its name is NULL. Attributes belong to one command; a previous command does not supply attributes to the next SELECT.

## Examples

```sql
SELECT MYSQL_QUERY_ATTRIBUTE_STRING('example');
```

Result:

```text
NULL
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- The client must negotiate/query the protocol attribute capability and attach attributes to this command. Binding an ordinary SQL placeholder or setting @user_variable does not create a query attribute. An absent attribute returns NULL.

## Related Functions

[`SLEEP`](sleep.md), [`BENCHMARK`](benchmark.md), [`LOAD_FILE`](load_file.md).
