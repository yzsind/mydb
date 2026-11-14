# JSON_TABLE

JSON_TABLE is a table function that expands a JSON value into typed SQL rows.
It is implicitly lateral: its input may refer to preceding FROM sources. An alias
is required. It is separate from scalar JSON functions and is a mydb-supported
MySQL compatibility surface.

## Syntax

```sql
JSON_TABLE(document, row_path COLUMNS (
  name FOR ORDINALITY,
  name type PATH column_path [empty_action] [error_action],
  name type EXISTS PATH column_path,
  NESTED [PATH] nested_path COLUMNS (...)
)) AS alias
```

The EMPTY/ERROR spelling for a value column is:
`[NULL ON EMPTY | ERROR ON EMPTY | DEFAULT json_literal ON EMPTY]`
followed by `[NULL ON ERROR | ERROR ON ERROR | DEFAULT json_literal ON ERROR]`.
Both default to NULL. Defaults are JSON literals inside SQL strings, for example
`DEFAULT '7'` or `DEFAULT '"unknown"'`. Invalid documents and paths are errors;
ON ERROR is not a catch-all for malformed SQL or paths. Value conversion follows
the declared SQL type and SQL mode; conversion diagnostics can still be emitted.

## Complete example

```sql
SELECT j.ord, j.id, j.has_tags, j.tag, j.missing
FROM JSON_TABLE(
  '[{"id":1,"tags":["x","y"]},{"id":2}]',
  '$[*]' COLUMNS (
    ord FOR ORDINALITY,
    id INT PATH '$.id',
    has_tags INT EXISTS PATH '$.tags',
    missing INT PATH '$.missing' DEFAULT '7' ON EMPTY,
    NESTED PATH '$.tags[*]' COLUMNS (tag VARCHAR(10) PATH '$')
  )
) AS j ORDER BY j.ord, j.tag;
```

Rows are `(1,1,1,'x',7)`, `(1,1,1,'y',7)`, `(2,2,0,NULL,7)`.
FOR ORDINALITY starts at 1 for each applicable path expansion. A missing nested
path produces a NULL-complemented row. Sibling NESTED paths produce a union of
rows, with the other sibling's columns NULL; they do not form a Cartesian product.
An empty top-level array with `$[*]` produces no rows. A JSON null scalar maps to
SQL NULL; a missing path uses ON EMPTY instead.

```sql
SELECT j.a, j.b
FROM JSON_TABLE('{"a":[1,2],"b":[3]}', '$' COLUMNS (
  NESTED PATH '$.a[*]' COLUMNS(a INT PATH '$'),
  NESTED PATH '$.b[*]' COLUMNS(b INT PATH '$')
)) AS j;
-- Rows: (1,NULL), (2,NULL), (NULL,3); order is illustrative.

SELECT j.v
FROM JSON_TABLE('[{"x":"bad"},{}]', '$[*]' COLUMNS (
  v INT PATH '$.x' DEFAULT '-2' ON EMPTY DEFAULT '-1' ON ERROR
)) AS j;
-- -1, -2 in input order; add ORDER BY for an ordered result contract.
```

## Lateral input and boundaries

```sql
SELECT d.id, j.v
FROM (SELECT 1 AS id, '[10,20]' AS payload) AS d,
JSON_TABLE(d.payload, '$[*]' COLUMNS(v INT PATH '$')) AS j
ORDER BY d.id, j.v;
```

Only preceding sources are available to the lateral input. Do not infer arbitrary
LATERAL join shapes or recursive references from this capability. Duplicate column
names, invalid paths/types and unsupported column options fail explicitly. JSON
paths and scalar conversion follow the [JSON reference](function/json/README.md).
JSON_TABLE rows participate in ordinary joins, filtering, aggregation and memory
admission; it is not a persistent table and cannot be a DML target.
