# Type Conversion and Charset Functions

| Function | Description |
| --- | --- |
| [`CAST`](cast.md) | Converts an expression to the specified SQL data type. |
| [`CHARSET`](charset.md) | Returns the name of the charset used by a string. |
| [`COERCIBILITY`](coercibility.md) | Returns the collation coercibility value of a string expression. |
| [`COLLATE`](collate.md) | Explicitly assigns a collation to an expression. |
| [`COLLATION`](collation.md) | Returns the name of the collation used by a string. |
| [`CONVERT_USING`](convert_using.md) | Converts a string using the specified charset. |
| [`WEIGHT_STRING`](weight_string.md) | Returns the comparison weight string of a string under its collation. |


## Collation precedence and conversion

A character set defines byte encoding; a collation defines comparison and sorting
inside one character set. Use `SHOW CHARACTER SET` and `SHOW COLLATION` to inspect
the names available in this build. `SET NAMES` controls the connection defaults.
An explicit COLLATE must match the expression charset.

| COERCIBILITY | Typical source |
| ---: | --- |
| 0 | Explicit COLLATE |
| 1 | Collation combination |
| 2 | Column or stored-program variable |
| 3 | System string such as USER() |
| 4 | String literal |
| 5 | Numeric or temporal expression |
| 6 | NULL |

Lower coercibility wins where the collations can be reconciled; equal-precedence
incompatible collations produce an error. Binary bytes and text are different
input domains. Converting bytes with USING interprets their encoding, while
assigning COLLATE changes comparison semantics, not arbitrary source bytes.

`CAST(expr AS type)` and `CONVERT(expr,type)` share type conversion.
The index name CONVERT_USING links to the SQL spelling `CONVERT(expr USING charset)`.
[CAST](cast.md#supported-targets) lists supported targets; [WEIGHT_STRING](weight_string.md)
explains comparison bytes. See [Data types](../../data-types.md) for stored text types.
