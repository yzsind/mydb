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


## Window partitions and frames

```sql
function(arguments) OVER (
  [PARTITION BY expressions]
  [ORDER BY expressions]
  [ROWS | RANGE | GROUPS frame_bounds]
)
```

PARTITION BY resets the function for each partition. Window ORDER BY defines
positions/peers and does not order final output: use an outer ORDER BY too.
Without window ordering, the default frame is the whole partition. With ordering,
the default is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, including the
current row's peers. SUM/AVG/COUNT and frame-sensitive FIRST/LAST/NTH_VALUE use the
frame; ranking functions and LAG/LEAD use partition positions.

ROWS counts row offsets; RANGE uses ordered value distances (one appropriate
numeric/temporal ORDER BY key for offset ranges); GROUPS counts peer groups as a
mydb extension. Bounds include UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW and
nonnegative PRECEDING/FOLLOWING offsets. Reversed/invalid bounds and incompatible
RANGE offset domains are errors. Temporal RANGE offsets use compatible INTERVAL
units. Supported EXCLUDE forms omit the current row, peers or ties; these extensions
should not be assumed portable to MySQL 8.0.

RESPECT NULLS is the default. IGNORE NULLS is not implemented and returns an error.
DISTINCT inside a window aggregate is rejected with an unsupported-context error.
For distinct window input, deduplicate in a pure inner query before applying OVER. GROUP_CONCAT and ST_COLLECT
have no window form. ANY_VALUE is a scalar identity, not a grouping exemption.

For the partition's final value, specify a full frame explicitly:

```sql
SELECT v, LAST_VALUE(v) OVER (
  ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS partition_last
FROM (SELECT 10 v UNION ALL SELECT 20) t ORDER BY v;
```

Both rows have partition_last=20; omitting the frame returns 10 then 20 here.
Ranking ties require a separate unique ordering key when ROW_NUMBER must be stable.


## Grouped input and result domains

COUNT(*) counts rows; other ordinary numeric aggregates skip SQL NULL. COUNT
returns 0 for empty input, while SUM/AVG/MIN/MAX and statistics return NULL except
population statistics with one non-NULL value return 0. Sample statistics require
two non-NULL values. BIT_AND uses all-ones numeric neutrality; BIT_OR/BIT_XOR use 0.
JSON_ARRAYAGG includes JSON null for SQL NULL values; empty input is SQL NULL.
JSON_OBJECTAGG rejects NULL keys and keeps the last encountered duplicate key.

Grouped COUNT/SUM/AVG/MIN/MAX and the documented GROUP_CONCAT/ST_COLLECT forms accept
DISTINCT. COUNT(DISTINCT a,b) excludes a row if any key component is NULL. Collation
and type conversion affect grouping/equality. JSON_ARRAYAGG has no in-function
ORDER BY clause; use its window ordering/frame when a stable ordered array is needed.

Integer/DECIMAL SUM uses exact decimal accumulation; AVG adds div_precision_increment
to its result scale. Floating input uses approximate arithmetic; statistics return
DOUBLE. Results remain subject to overflow and governed-memory limits. See
[GROUP_CONCAT](group_concat.md) for its independent byte limit and truncation warning.
