# `ST_BUFFER_STRATEGY`

## Description

Constructs a MySQL-compatible buffer strategy token, kept for tooling compatibility. `ST_BUFFER` itself is unsupported, so the token has no consumer in this release.

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

Result:

```text
050000000000000000004040
```

## Notes

- `end_flat` accepts exactly one argument; `end_round`, `join_round`, `point_circle`, and `join_miter` accept exactly two. No form accepts both a circle parameter and a miter limit. The token validates strategy syntax only; buffer computation remains unsupported.

## Related Functions

[Category reference](README.md).
