# `ELT`

## Description

Returns the string from the argument list at a 1-based index.

## Syntax

```sql
ELT(index, str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `index` | Yes | Integer | One-based index into the following strings. |
| `str1` | Yes | String | First string. |
| `str2 ...` | No, repeatable | String | Additional strings, in argument order. |

## Return Value

Returns the string at the one-based index. A NULL or out-of-range index returns NULL; selecting a NULL argument returns NULL.

## Examples

```sql
SELECT ELT(2,'first','second');
```

Result:

```text
second
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`GREATEST`](greatest.md), [`LEAST`](least.md), [`FIELD`](field.md), [`INTERVAL`](interval.md).
