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
| `attribute_name` | Yes | String | The 1st argument; the query attribute name |

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

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
