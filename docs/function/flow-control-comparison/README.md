# Flow Control and Comparison Functions

| Function | Description |
| --- | --- |
| [`COALESCE`](coalesce.md) | Returns the first non-`NULL` value in the argument list. |
| [`ELT`](elt.md) | Returns the string from the argument list at a 1-based index. |
| [`FIELD`](field.md) | Returns the position of the first argument within the following argument list. |
| [`GREATEST`](greatest.md) | Returns the greatest value in the argument list. |
| [`IF`](if.md) | Returns one of two candidate values depending on a condition. |
| [`IFNULL`](ifnull.md) | Returns a fallback value when the first argument is `NULL`. |
| [`INTERVAL`](interval.md) | Returns the interval index of the first argument within an ordered list of boundaries. |
| [`ISNULL`](isnull.md) | Determines whether the argument is `NULL`. |
| [`LEAST`](least.md) | Returns the least value in the argument list. |
| [`NAME_CONST`](name_const.md) | Returns a validated literal value; use an explicit AS alias for the result column name. |
| [`NULLIF`](nullif.md) | Returns `NULL` when the two arguments are equal, otherwise returns the first argument. |


## Comparison and evaluation

IF converts its condition to SQL truth: zero and NULL are false. IF, IFNULL,
and COALESCE evaluate only the reached result expressions. Result-column type
resolution still considers the candidate domains; returning one branch does
not imply that its result metadata is simply the type of that literal.

GREATEST/LEAST use the common comparison domain and resolved collation. Any NULL
candidate yields NULL. FIELD returns 0 for NULL/no match; ELT returns NULL for an
invalid or out-of-range one-based index. INTERVAL requires ascending boundaries
and returns the number of boundaries not greater than its first argument; a NULL
first argument returns -1. Mixed numeric/text operands can cause conversion warnings.

[Type conversion and collation](../conversion-charset/README.md) explains the
conversion rules. These scalar functions are distinct from SQL CASE syntax;
see [Operators and expressions](../../operators-and-expressions.md).
