# `LIST_ADD`

## Description

Appends an element to a comma-separated list if it is not already present.

## Syntax

```sql
LIST_ADD(list, item)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `list` | Yes | String | Comma-separated list; empty entries are discarded and surrounding spaces are trimmed. |
| `item` | Yes | String | Element to add or remove; comparison ignores ASCII case. |

## Return Value

Returns normalized comma-separated text with item appended only if it is not already present, comparing without ASCII case. Empty entries are discarded and entry whitespace is trimmed. Any SQL NULL argument raises 1138/02200.

## Examples

```sql
SELECT LIST_ADD('a,b', 'c');
```

Result: a,b,c

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`EXPORT_SET`](export_set.md), [`MAKE_SET`](make_set.md), [`LIST_DROP`](list_drop.md), [`FORMAT`](format.md).
