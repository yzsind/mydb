# `GTID_SUBTRACT`

## Description

Subtracts one GTID set from another.

## Syntax

```sql
GTID_SUBTRACT(set, set_to_remove)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `set` | Yes | String | GTID set in UUID:interval notation; empty text is the empty set. |
| `set_to_remove` | Yes | String | GTID intervals to subtract from the first set. |

## Return Value

Returns normalized GTID set text after subtracting set_to_remove from set. Intervals are merged/split as needed, UUIDs normalized, and an empty result is empty text. SQL NULL returns NULL; malformed sets raise a GTID-format error.

## Examples

```sql
SELECT LENGTH(GTID_SUBTRACT('',''));
```

Result:

```text
0
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[Category reference](README.md).
