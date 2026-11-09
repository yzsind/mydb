# Numeric and Bit Functions

| Function | Description |
| --- | --- |
| [`ABS`](abs.md) | Returns the absolute value of a number. |
| [`ACOS`](acos.md) | Returns the arc cosine. |
| [`ASIN`](asin.md) | Returns the arc sine. |
| [`ATAN`](atan.md) | Returns the arc tangent; also supports two-argument quadrant computation. |
| [`ATAN2`](atan2.md) | Returns the quadrant-aware arc tangent from two coordinate arguments. |
| [`BIN`](bin.md) | Returns a binary string representation of an integer. |
| [`BIT_COUNT`](bit_count.md) | Returns the number of set bits in an integer's binary representation. |
| [`CEIL`](ceil.md) | Returns the smallest integer not less than the argument. |
| [`CEILING`](ceiling.md) | Returns the smallest integer not less than the argument; an alias of `CEIL`. |
| [`CONV`](conv.md) | Converts integer text between bases from 2 to 36. |
| [`COS`](cos.md) | Returns the cosine. |
| [`COT`](cot.md) | Returns the cotangent. |
| [`CRC32`](crc32.md) | Returns the CRC-32 checksum of the input byte string. |
| [`DEGREES`](degrees.md) | Converts radians to degrees. |
| [`DIV`](div.md) | Performs integer division. |
| [`EXP`](exp.md) | Returns e raised to the specified power. |
| [`FLOOR`](floor.md) | Returns the largest integer not greater than the argument. |
| [`LN`](ln.md) | Returns the natural logarithm. |
| [`LOG`](log.md) | Returns the natural logarithm, or the logarithm to a specified base. |
| [`LOG10`](log10.md) | Returns the base-10 logarithm. |
| [`LOG2`](log2.md) | Returns the base-2 logarithm. |
| [`MOD`](mod.md) | Returns the remainder of a division. |
| [`OCT`](oct.md) | Returns an octal string representation of an integer. |
| [`PI`](pi.md) | Returns the value of π. |
| [`POW`](pow.md) | Returns the base raised to the specified power. |
| [`POWER`](power.md) | Returns the base raised to the specified power; an alias of `POW`. |
| [`RADIANS`](radians.md) | Converts degrees to radians. |
| [`RAND`](rand.md) | Returns a pseudorandom number in the range `[0, 1)`, with an optional seed. |
| [`ROUND`](round.md) | Rounds a number to the specified number of decimal places. |
| [`SIGN`](sign.md) | Returns the sign of a number. |
| [`SIN`](sin.md) | Returns the sine. |
| [`SQRT`](sqrt.md) | Returns the square root of a non-negative number. |
| [`TAN`](tan.md) | Returns the tangent. |
| [`TRUNCATE`](truncate.md) | Truncates a number to the specified number of decimal places. |


## Domains and diagnostics

Numeric arguments can be integer, DECIMAL, floating-point, or convertible text.
A SQL NULL operand returns SQL NULL, except that `RAND(NULL)` uses seed zero.
Integer/DECIMAL arithmetic retains an exact domain where supported; transcendental
functions return DOUBLE. Decimal spelling matters: `2.5` is exact, while `2.5E0`
is approximate. Conversion warnings are visible through `SHOW WARNINGS`.

| Operation | Exceptional input | SELECT behavior |
| --- | --- | --- |
| LN, LOG, LOG2, LOG10 | Nonpositive value, or LOG base not positive or equal to 1 | NULL and warning 3020 |
| SQRT | Negative operand | NULL |
| ASIN, ACOS | Outside [-1,1] | NULL |
| EXP, POW/POWER, COT | Result outside the finite DOUBLE range | Error 1690/22003 |
| DIV, MOD | Zero divisor | NULL; division diagnostics depend on SQL mode and statement context |
| ABS | Smallest signed 64-bit integer | Numeric overflow error |

Strict data-changing statements can promote conversion/domain warnings to errors.
Changing sql_mode does not turn every overflow into NULL. Check the final statement
status and `SHOW WARNINGS`, rather than assuming all mathematical errors are alike.

Angles consumed by SIN/COS/TAN/COT are radians. ASIN/ACOS/ATAN/ATAN2 return radians;
DEGREES and RADIANS perform explicit unit conversion. RAND is a pseudorandom SQL
sequence; use [RANDOM_BYTES](../encryption-compression/random_bytes.md) for random
key material. Bitwise operators are documented in [Operators and expressions](../../operators-and-expressions.md).
