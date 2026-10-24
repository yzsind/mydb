# `ST_BUFFER_STRATEGY`

## Description

Constructs a strategy byte string for buffer operations.

## Syntax

```sql
ST_BUFFER_STRATEGY(strategy [, parameter])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `strategy` | Yes | Strategy name | The buffer join or end-point strategy |
| `parameter` | Depends on strategy | Positive finite number | Required for end_round, join_round, point_circle, or join_miter; omitted for end_flat |

## Return Value

Returns a 12-byte binary token (`strategy_id:u32 LE + parameter:f64 LE`), or SQL NULL for SQL NULL input. Invalid strategy/argument combinations return 1210.

## Examples

```sql
SELECT HEX(ST_BUFFER_STRATEGY('point_circle', 32));
```

Result: Returns the hexadecimal text of the buffer strategy binary value.

## Notes

- `end_flat` accepts exactly one argument; `end_round`, `join_round`, `point_circle`, and `join_miter` accept exactly two. No form accepts both a circle parameter and a miter limit. Token construction does not imply that ST_BUFFER is implemented; ST_BUFFER remains unsupported.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
