# `COLLATE`

## Description

Explicitly assigns a collation to an expression.

## Syntax

```sql
expr COLLATE collation_name
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Character expression | The expression to which the collation is assigned |
| `collation_name` | Yes | Collation name | A collation compatible with the expression's charset |

## Return Value

Returns the expression with an explicit collation and coercibility 0, preserving SQL NULL. The collation must belong to the expression character set; incompatible assignments raise 1253. This is expression syntax, not a two-argument SQL function.

Binary strings, hex literals, BIT values and NULL have a binary source charset;
applying a utf8mb4 collation directly does not decode them. Use explicit
`CONVERT(value USING utf8mb4)` before a utf8mb4 COLLATE when conversion is intended.
For example, `CONVERT(X'61' USING utf8mb4) COLLATE utf8mb4_bin` is valid, while
`X'61' COLLATE utf8mb4_bin` returns 1253. Numeric values use the connection
character domain when collated. Prepared execution checks parameter domains
on each execution; a prior text value does not authorize a later binary value.

## Examples

```sql
SELECT 'A' COLLATE utf8mb4_bin = 'a';
```

Result: 0

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`CAST`](cast.md), [`CONVERT_USING`](convert_using.md).
