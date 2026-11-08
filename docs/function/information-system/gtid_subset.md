# `GTID_SUBSET`

## Description

Determines whether one GTID set is a subset of another.

## Syntax

```sql
GTID_SUBSET(subset, set)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `subset` | Yes | String | GTID set to test. |
| `set` | Yes | String | GTID set in UUID:interval notation; empty text is the empty set. |

## Return Value

Returns integer 1 if every interval in subset is contained in set, otherwise 0. SQL NULL returns NULL. Empty text is an empty set; malformed UUIDs/intervals raise GTID-format errors. This is set arithmetic and does not inspect replication progress.

## Examples

```sql
SELECT GTID_SUBSET('', '');
```

Result: 1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[Category reference](README.md).
