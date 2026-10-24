# Aggregate and Window Functions

| Function | Description |
| --- | --- |
| [`ANY_VALUE`](any_value.md) | Returns its argument unchanged; the current implementation does not exempt ungrouped columns from `ONLY_FULL_GROUP_BY`. |
| [`AVG`](avg.md) | Returns the average of non-`NULL` values; also usable as a window aggregate function. |
| [`BIT_AND`](bit_and.md) | Performs a bitwise AND aggregation over non-`NULL` values in the group. |
| [`BIT_OR`](bit_or.md) | Performs a bitwise OR aggregation over non-`NULL` values in the group. |
| [`BIT_XOR`](bit_xor.md) | Performs a bitwise XOR aggregation over non-`NULL` values in the group. |
| [`COUNT`](count.md) | Returns the number of rows or non-`NULL` values in the group; also usable as a window aggregate function. |
| [`CUME_DIST`](cume_dist.md) | Returns the cumulative distribution of the current row within its window partition. |
| [`DENSE_RANK`](dense_rank.md) | Returns a gap-free peer ranking within the window partition. |
| [`FIRST_VALUE`](first_value.md) | Returns the first value in the current window frame. |
| [`GROUP_CONCAT`](group_concat.md) | Concatenates non-`NULL` values in the group with an optional ordering and separator. |
| [`GROUPING`](grouping.md) | Indicates whether the expression in a result row was produced by `ROLLUP`. |
| [`JSON_ARRAYAGG`](json_arrayagg.md) | Aggregates values in the group into a JSON array; also usable as a window aggregate function. |
| [`JSON_OBJECTAGG`](json_objectagg.md) | Aggregates key-value pairs in the group into a JSON object; also usable as a window aggregate function. |
| [`LAG`](lag.md) | Returns the value at a specified offset before the current row within its window partition. |
| [`LAST_VALUE`](last_value.md) | Returns the last value in the current window frame. |
| [`LEAD`](lead.md) | Returns the value at a specified offset after the current row within its window partition. |
| [`MAX`](max.md) | Returns the maximum of non-`NULL` values in the group; also usable as a window aggregate function. |
| [`MIN`](min.md) | Returns the minimum of non-`NULL` values in the group; also usable as a window aggregate function. |
| [`NTH_VALUE`](nth_value.md) | Returns the Nth value in the current window frame. |
| [`NTILE`](ntile.md) | Distributes the rows of a window partition as evenly as possible into a given number of buckets. |
| [`PERCENT_RANK`](percent_rank.md) | Returns the percentage rank of the current row within its window partition. |
| [`RANK`](rank.md) | Returns a peer ranking with gaps within the window partition. |
| [`ROW_NUMBER`](row_number.md) | Returns the sequential number of a row within its window partition. |
| [`ST_COLLECT`](st_collect.md) | Aggregates geometries in the group into a geometry collection. |
| [`STD`](std.md) | Returns the population standard deviation, an alias of `STDDEV_POP`; also usable as a window aggregate function. |
| [`STDDEV`](stddev.md) | Returns the population standard deviation, an alias of `STDDEV_POP`; also usable as a window aggregate function. |
| [`STDDEV_POP`](stddev_pop.md) | Returns the population standard deviation; also usable as a window aggregate function. |
| [`STDDEV_SAMP`](stddev_samp.md) | Returns the sample standard deviation; also usable as a window aggregate function. |
| [`SUM`](sum.md) | Returns the sum of non-`NULL` values in the group; also usable as a window aggregate function. |
| [`VAR_POP`](var_pop.md) | Returns the population variance; also usable as a window aggregate function. |
| [`VAR_SAMP`](var_samp.md) | Returns the sample variance; also usable as a window aggregate function. |
| [`VARIANCE`](variance.md) | Returns the population variance, an alias of `VAR_POP`; also usable as a window aggregate function. |
