# mydb Data Types

This document describes the data types currently available for table definitions in mydb,
their declaration boundaries, write-time conversion, and comparison and index semantics.
For overall differences from MySQL 8.0.46, see
[`mysql80-compatibility.md`](mysql80-compatibility.md). mydb is still under
development toward its first product release, and persistent formats and edge-case
compatibility behavior may continue to change.

## 1. Type Overview

| Type family | Supported declarations | Primary uses |
| --- | --- | --- |
| Integer | `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`/`INTEGER`, `BIGINT`, `BOOL`/`BOOLEAN` | counters, identifiers, statuses, and exact integer arithmetic |
| Fixed-point and floating-point | `DECIMAL`/`NUMERIC`/`DEC`, `FLOAT`, `DOUBLE`, `REAL` | money and exact decimals; measurements and approximate computation |
| String | `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | text governed by character set and collation |
| Binary string | `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | raw bytes, digests, compressed or encoded payloads |
| Date and time | `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR` | dates, durations/instants, wall-clock time, and UTC instants |
| Domain types | `ENUM`, `SET`, `BIT` | single-choice values, multi-choice sets, and fixed bit fields |
| Semi-structured | `JSON` | binary JSON documents, path queries, and multi-valued indexes |
| Spatial | `GEOMETRY` plus seven concrete subtypes | SRID/WKB geometries, spatial functions, and spatial indexes |
| Vector | `VECTOR[(n)]` | dense `f32` vectors, exact KNN, and ANN retrieval |

`NULL` is not a standalone column type. Every type can store SQL `NULL` except for columns
declared `NOT NULL`; SQL `NULL` and the `null` inside a JSON document are two distinct values.

## 2. Common Column Attributes

Data types can be combined with the following column attributes:

- `NULL` / `NOT NULL` controls whether SQL `NULL` is accepted; primary key columns are
  normalized to non-nullable.
- `DEFAULT <literal>` or the MySQL 8.0.13+ form `DEFAULT (<expr>)` provides a default value.
  `BLOB`, `TEXT`, `GEOMETRY`, and `JSON` do not accept a non-`NULL` plain literal default, but
  a valid parenthesized default expression may be used.
- Integer, `DECIMAL`, and floating-point types support `UNSIGNED` and `ZEROFILL`; `ZEROFILL`
  implies `UNSIGNED`, and both legacy attributes produce deprecation diagnostics.
  `AUTO_INCREMENT` applies to integer columns. The legacy display width is retained only for
  syntax and client compatibility and does not change the storage range.
- Character types support `CHARACTER SET`/`CHARSET`, `COLLATE`, and the column-level `BINARY`
  attribute. Unspecified values are inherited from the table/database defaults.
- `TIMESTAMP` and `DATETIME` support `DEFAULT CURRENT_TIMESTAMP[(fsp)]` and
  `ON UPDATE CURRENT_TIMESTAMP[(fsp)]` with the fsp matching the column's fractional seconds
  precision exactly.
- Any type can participate in `CHECK` and generated columns, but the expression itself must
  still satisfy the determinism, type, and function restrictions.

In strict mode, out-of-range values, invalid formats, and significant truncation usually
return the corresponding MySQL errno directly; non-strict mode may downgrade some problems to
warnings/notes and store saturated, zero, or truncated values. See
[Section 12](#12-type-conversion-and-sql_mode).

## 3. Integer Types

| Declaration | Signed range | `UNSIGNED` range | Storage width |
| --- | ---: | ---: | ---: |
| `TINYINT` | -128 to 127 | 0 to 255 | 1 byte |
| `SMALLINT` | -32,768 to 32,767 | 0 to 65,535 | 2 bytes |
| `MEDIUMINT` | -8,388,608 to 8,388,607 | 0 to 16,777,215 | 3 bytes |
| `INT` / `INTEGER` | -2,147,483,648 to 2,147,483,647 | 0 to 4,294,967,295 | 4 bytes |
| `BIGINT` | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 | 0 to 18,446,744,073,709,551,615 | 8 bytes |

Notes:

- `BOOL` and `BOOLEAN` are aliases for signed `TINYINT(1)`, not a standalone boolean type that
  can only store `0`/`1`; the `TRUE` and `FALSE` literals evaluate to `1` and `0` respectively.
- `SERIAL` expands to `BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE` in `CREATE TABLE`.
  `ALTER TABLE ... ADD ... SERIAL` is currently rejected explicitly, to avoid silently keeping
  the type while dropping its implied attributes.
- Legacy integer display widths such as `TINYINT(M)` parse and validate, but `M` changes
  neither the range, nor arithmetic, nor the physical width.
- On integer writes, fractional values are rounded following the MySQL numeric conversion
  rules. Out-of-range values raise an error in strict mode; when downgrading is permitted they
  are clamped to the target type boundary with a warning.
- The unsigned flag enters expression types and plan-cache semantics. Do not use `UNSIGNED`
  as a substitute for business-layer constraints; add a `CHECK` when a narrower business range
  is required.

## 4. Fixed-Point and Approximate Numbers

### 4.1 `DECIMAL(P,S)`

`DECIMAL`, `NUMERIC`, and `DEC` are the same exact fixed-point type. `P` is the total number
of significant digits and `S` is the number of decimal places, constrained by
`1 <= P <= 65`, `0 <= S <= 30`, and `S <= P`. A bare `DECIMAL` and `DECIMAL(0,0)` are both
normalized to `DECIMAL(10,0)`.

The largest representable absolute value is less than `10^(P-S)`, and exactly `S` decimal
places are retained. Values with excess fractional digits are scaled and rounded following
the MySQL rules; integer-digit overflow raises an error or saturates depending on
strict/non-strict mode. Exact conversion and scientific notation input never detour through
`f64` first.

mydb selects the internal coefficient representation by precision: `P <= 9`, `P <= 18`, and
`P <= 38` can enter the fixed-width `i32`/`i64`/`i128` paths respectively, while `P=39..65`
uses a wide integer. Different physical paths carry the same SQL semantics; `1.0` and `1.00`
compare equal and share the same canonical hash.

Monetary amounts, balances, tax rates, and measurements that must aggregate reproducibly
should use `DECIMAL` rather than `FLOAT`/`DOUBLE`.

### 4.2 `FLOAT`, `DOUBLE`, and `REAL`

- `FLOAT` uses the 4-byte IEEE-754 single precision; `DOUBLE` uses the 8-byte double precision.
- The `p` in `FLOAT(p)` is the SQL bit precision: `p <= 24` selects `FLOAT`, `25 <= p <= 53`
  selects `DOUBLE`, and the catalog echo omits the original `(p)`.
- The legacy `FLOAT(M,D)` and `DOUBLE(M,D)` still parse and echo back; `M <= 255`, `D <= 30`,
  and `D <= M`. The declared decimal-place and range constraints are applied on write.
- `REAL` is equivalent to `DOUBLE` by default; with `REAL_AS_FLOAT` enabled it is equivalent
  to `FLOAT`.
- `NaN`, positive/negative infinity, and ordinary rounding error are unsuitable as exact
  business data. Approximate numbers follow floating-point semantics in equality, ordering,
  and aggregation.

## 5. Character and Binary Strings

### 5.1 `CHAR` and `VARCHAR`

| Type | Default length | Length unit | Write and read semantics |
| --- | ---: | --- | --- |
| `CHAR[(n)]` | 1 | characters | `n <= 255`; trailing pad spaces are stripped on storage, and reads are not padded by default |
| `VARCHAR[(n)]` | 255 | characters | preserves the actual characters without padding; the maximum encoded byte count is subject to the 65,535-byte domain limit |

Character lengths are checked by Unicode character count; physical and index widths also
depend on the character set's maximum bytes per character. When an overlong value is truncated
and the removed part consists only of pad spaces, the truncated value is kept following the
MySQL rules; when non-blank content is lost, strict mode returns 1406/22001, while non-strict
mode stores the truncated value and produces a warning.

Comparison, sorting, `LIKE`, deduplication, and index encoding for `CHAR`, `VARCHAR`, `TEXT`,
`ENUM`, and `SET` use the column collation. Case sensitivity, accent sensitivity, and
trailing-space behavior are decided by the PAD SPACE/NO PAD rules of the specific collation.
For binary-sensitive text, choose a `_bin` collation or the column-level `BINARY` attribute;
for raw bytes, use a true binary type.

### 5.2 `BINARY` and `VARBINARY`

| Type | Default length | Length unit | Write and read semantics |
| --- | ---: | --- | --- |
| `BINARY[(n)]` | 1 | bytes | `n <= 255`; padded on the right with `0x00` when shorter, truncated/error when longer |
| `VARBINARY[(n)]` | 255 | bytes | preserves the original length without padding |

Binary types perform no character set transcoding, and comparisons are byte-wise. Hashes,
binary UUID forms, compressed data, and payloads not guaranteed to be UTF-8 should use
`BINARY`/`VARBINARY`/`BLOB`.

### 5.3 `TEXT` and `BLOB`

| Text type | Binary type | Maximum bytes |
| --- | --- | ---: |
| `TINYTEXT` | `TINYBLOB` | 255 |
| `TEXT` | `BLOB` | 65,535 |
| `MEDIUMTEXT` | `MEDIUMBLOB` | 16,777,215 |
| `LONGTEXT` | `LONGBLOB` | 4,294,967,295 |

`TEXT` limits are counted in encoded bytes, and truncation is guaranteed to stop at character
boundaries; `BLOB` is counted purely in bytes. `TEXT(M)`/`BLOB(M)` selects the smallest LOB
tier that can hold `M` bytes. `LONG` is a legacy alias for `MEDIUMTEXT`, and `LONG BYTE` is a
legacy alias for `MEDIUMBLOB`.

Large values can be externalized by the storage layer; see
[`table-and-btree-storage-format.md`](table-and-btree-storage-format.md) for the heap/LOB
formats. `TEXT`/`BLOB` columns in BTREE indexes usually require a prefix length, and the sum
of the declared maximum bytes of ordinary column BTREE key parts must not exceed 3,072 bytes.

Under non-strict DDL, a `VARCHAR` whose encoding exceeds 65,535 bytes or an overly wide
`VARBINARY` can be automatically promoted to the corresponding `TEXT`/`BLOB` tier with a
note; strict mode requires an explicit switch to a LOB. An overly wide declaration carrying a
non-null literal default does not bypass the LOB default-value rule through automatic
promotion.

## 6. Date and Time

| Type | Semantics | Fractional seconds | Key boundaries |
| --- | --- | --- | --- |
| `DATE` | calendar date | not applicable | supports the zero date and `sql_mode`-controlled invalid dates |
| `TIME[(fsp)]` | signed duration/time of day | 0–6 digits | `-838:59:59` to `838:59:59` |
| `DATETIME[(fsp)]` | wall-clock time without time zone | 0–6 digits | calendar years up to 9999; does not vary with the session time zone |
| `TIMESTAMP[(fsp)]` | UTC instant | 0–6 digits | writes/reads pass through the session `time_zone`; mydb supports values after 2038 |
| `YEAR` / `YEAR(4)` | year | not applicable | `0000` or 1901–2155 |

When `fsp` is omitted it defaults to 0; values outside `0..=6` produce an explicit error at
parse time. Writing fractional seconds beyond the declared precision uses half-away rounding
by default; enabling `TIME_TRUNCATE_FRACTIONAL` switches to truncation without a warning.

The core difference between `TIMESTAMP` and `DATETIME` is the time zone:

```sql
SET time_zone = '+08:00';
CREATE TABLE events (
    happened_at TIMESTAMP(6),
    wall_clock  DATETIME(6)
);
```

`happened_at` is converted from `+08:00` wall-clock time to UTC on write and displays a
different wall-clock time when read under another session time zone; `wall_clock` always
retains the original date-time fields. mydb intentionally allows `TIMESTAMP` to store values
beyond 2038, but replication toward MySQL is still limited by the MySQL `TIMESTAMP2` 32-bit
second range, and out-of-range values return an explicit error.

Two-digit input to `YEAR` maps `1..69` to 2001..2069 and `70..99` to 1970..1999. Invalid
times, zero dates, zero month/day, and dates with only valid components are governed by
`STRICT_*`, `NO_ZERO_DATE`, `NO_ZERO_IN_DATE`, and `ALLOW_INVALID_DATES`. Do not rely on
permissive modes to store invalid business dates.

`explicit_defaults_for_timestamp` is enabled by default; when disabled, the first `TIMESTAMP`
column of a newly created table can receive the MySQL legacy implicit default and
`ON UPDATE` behavior.

## 7. `ENUM`, `SET`, and `BIT`

### 7.1 `ENUM`

```sql
status ENUM('pending', 'paid', 'cancelled') NOT NULL
```

`ENUM` allows at most 65,535 unique members, each at most 255 characters, and the member list
cannot be empty. Members receive 1-based ordinals in declaration order; numeric `1..N` writes
the corresponding member, and ordinal 0 is the special empty-string sentinel used for invalid
input. Duplicate member detection and lookup follow the column collation, and ordinary
trailing spaces in declared values are stripped first.

`ENUM` fits cases where the member set is small, stable, and declaration order carries
business meaning. For states that change frequently, require foreign-key metadata, or need
independent attributes, use a reference table instead of `ENUM`.

### 7.2 `SET`

```sql
permissions SET('read', 'write', 'admin')
```

`SET` allows at most 64 unique members, each at most 255 characters, and member text cannot
contain commas. The physical value is a 64-bit mask aligned to declaration order; numeric
input selects members bitwise, text input is a comma-separated member list, and reads always
output in declaration order. Unknown members raise an error in strict mode and are dropped
with a warning where downgrading is permitted.

`SET` suits closed and very small bit sets. When arbitrary tags, member attributes, or
high-selectivity queries are needed, an association table is usually clearer.

### 7.3 `BIT(M)`

`BIT` is equivalent to `BIT(1)`; valid widths are `1..=64`; `BIT(0)` returns 3013, and
`BIT(65)` and wider declarations return 1439. Values can come from non-negative integers,
`b'...'`, or hexadecimal literals and are treated as unsigned integers in numeric expression
contexts. The protocol transmits column values as big-endian binary strings of `ceil(M/8)`
bytes.

## 8. `JSON`

`JSON` columns internally store compact, self-describing binary JSONB rather than the original
input text. Input text must be a valid JSON document; invalid documents return MySQL 3141. SQL
numerics and date-times convert to typed JSON scalars, and SQL `NULL` still means the column
is empty rather than JSON `null`.

The internal JSONB preserves typed leaves such as integers, unsigned integers, floating-point
numbers, exact fixed-point numbers, and date-times; they are rendered as canonical JSON text
only at the protocol and text-function boundaries. The internal format is not the InnoDB
binary JSON, but the row-based binlog boundary converts to the MySQL format.

A plain BTREE index cannot be created directly on a whole `JSON` column. Choose one of the
following instead:

- extract and convert stable paths with generated columns, then index the generated columns;
- use a multi-valued index of the `CAST(json_path AS <scalar> ARRAY)` form;
- explicitly apply `JSON_VALUE`/`JSON_EXTRACT` plus a conversion to the target type for
  scalars reused across queries.

Compatibility boundaries for JSON paths, modification functions, aggregation, `JSON_TABLE`,
and MVI are covered in [`mysql80-compatibility.md`](mysql80-compatibility.md#data-types) and
the function documentation index [`function/README.md`](function/README.md).

## 9. Spatial Types

The following declarations are supported:

- `GEOMETRY`
- `POINT`
- `LINESTRING`
- `POLYGON`
- `MULTIPOINT`
- `MULTILINESTRING`
- `MULTIPOLYGON`
- `GEOMETRYCOLLECTION` (the `GEOMCOLLECTION` alias is also accepted)

Values are stored as `SRID(4-byte little-endian) + WKB`. `GEOMETRY` accepts any supported
subtype; concrete subtype columns accept only the matching WKB type. Column definitions may
carry an `SRID n` constraint; a mismatched SRID or geometry subtype on write raises an
explicit error. Text or plain BLOB is never implicitly treated as geometry on column write;
applications should use constructors such as `ST_GeomFromText` and `ST_GeomFromWKB`.

Spatial indexes require a single, non-unique, `NOT NULL` geometry column and do not support
expression, prefix, or descending key parts. mydb uses a persistent MBR B-tree plus a
rebuildable in-memory R-tree and rechecks the exact predicate against the row after candidate
hits. See [`function/spatial/README.md`](function/spatial/README.md) for the GIS function
list; some geographic computations, projection transforms, and polygon set operations may
still return explicit unsupported errors.

## 10. `VECTOR`

`VECTOR(n)` is a mydb extension and is not part of MySQL 8.0.46 Community Server. Each element
is a finite 32-bit IEEE-754 float, the dimension range is `1..=16383`, and `NaN` or infinities
are rejected. A fixed-dimension declaration forces every written value to have exactly `n`
elements; a bare `VECTOR` accepts any in-range dimension but cannot gain the full benefits of
fixed-dimension indexing and storage planning.

The value dimension limit is separate from the inline row-size limit.
Fixed-dimension columns store all `4 × n` element bytes inline; the full encoded
row, including other columns and metadata, must fit the heap row budget.
Oversized declarations fail with `ER_TOO_BIG_ROWSIZE`; even `VECTOR(4096)`
exceeds that budget. A dimensionless `VECTOR` can externalize large values.

```sql
CREATE TABLE documents (
    id        BIGINT PRIMARY KEY,
    embedding VECTOR(3) NOT NULL,
    INDEX idx_embedding (embedding) USING HNSW
);

INSERT INTO documents VALUES
    (1, STRING_TO_VECTOR('[0.1, -0.2, 0.3]')); -- the dimension must match the column declaration
```

Vectors are contiguous little-endian `f32` arrays on disk and inside the protocol, with the
text form `[v1,v2,...]`. Applications should construct values with
`STRING_TO_VECTOR`/`TO_VECTOR` and render explicitly with `VECTOR_TO_STRING`/`FROM_VECTOR`.
See [`function/vector/README.md`](function/vector/README.md) for the function list.

Retrieval options include:

- exact brute-force KNN such as `ORDER BY L2_DISTANCE(embedding, query) LIMIT k`;
- `USING HNSW` indexes on single-column, non-unique, `NOT NULL` vectors;
- `IVF_SQ8` indexes with the corresponding `nlist`/`nprobe` configuration;
- metrics such as `COSINE`, `L2`, and `DOT`, plus the `NO_VECTOR_INDEX` hint.

The derived navigator is disabled by default to avoid the discovered high-dimensional recall
degradation and the startup overhead caused by post-write invalidation. Only under the
experimental setting `MYDB_IVF_NAVIGATOR=on` does the paged backend attempt, for `IVF_SQ8`
indexes with at least 100,000 rows and a dimension of at least 256, to build an M=8 derived
navigation graph governed by the unified memory governor. It uses
`max(candidate count, 2*nprobe+16)` as the navigation effort and returns candidates with exact
f32 ordering, avoiding per-query streaming of the full high-dimensional SQ8 codes of every
probed list; `nprobe=nlist` still forces the original inverted-file full probe, preserving the
exact escape semantics. When the derived cache has no budget or the build fails, it falls back
to the ordinary IVF list scan; successful DML invalidates it, and DDL/`OPTIMIZE TABLE` or a
cold load can rebuild it. `information_schema.mydb_vector_indexes.BACKEND` shows
`ivf_paged_nav` when the navigator is active.

Both vectors of a single distance computation must have the same dimension. Vectors have no
natural total order suitable for business sorting; queries should order by the distance
function rather than rely on the internal byte order of the raw column.

## 11. Indexes and Type Selection

| Data shape | Recommended type | Indexing advice |
| --- | --- | --- |
| Monotonic primary keys, counters | an integer of sufficient size; `UNSIGNED` when needed | BTREE primary/unique key |
| Money, exchange rates, exact ratios | `DECIMAL(P,S)` | plain BTREE; choose the smallest reasonable `P` for the business range |
| Scientific measurements, model scores | `FLOAT`/`DOUBLE` | use exact equality predicates with care |
| Short text | `VARCHAR(n)`; `CHAR(n)` only for truly fixed width | full BTREE; watch the 3,072-byte limit after character set |
| Raw short bytes | `VARBINARY(n)`; `BINARY(n)` only for truly fixed width | byte-wise BTREE |
| Large text/large bytes | the matching `TEXT`/`BLOB` tier | BTREE prefix, FULLTEXT, or no plain index |
| A single closed status | `ENUM` or a reference table | small sets can use BTREE directly |
| Bit sets | `SET`/`BIT` | for high-frequency member filtering consider generated columns/a normalized association table |
| Documents | `JSON` | generated columns, functional indexes, or MVI; do not index JSONB directly |
| Coordinates and shapes | a concrete geometry subtype + `SRID` | single-column SPATIAL index |
| embedding | fixed-dimension `VECTOR(n) NOT NULL` | exact scan for small data, HNSW/IVF_SQ8 for large data |

The type, character set, collation, unsigned attribute, and declared precision of an index key
are all SQL semantics. Changing these attributes affects comparison order, uniqueness, or
index encoding and cannot be treated as a pure metadata change.

## 12. Type Conversion and `sql_mode`

Column writes, `CAST`, prepared statement parameters, `LOAD DATA`, default backfill, and
ALTER COPY share the same core conversion rules. The main behaviors:

- Numeric strings are converted first under the MySQL numeric-prefix/exact fixed-point rules;
  out-of-range integers and fixed-point values usually return 1264, and invalid values or
  significant truncation usually returns 1366/1406.
- Strict mode (`STRICT_TRANS_TABLES`/`STRICT_ALL_TABLES`) turns degradable data problems into
  statement errors; an explicitly non-strict mode can clamp or truncate and record warnings.
  The built-in session/global default is
  `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO`.
  `SET SESSION sql_mode=''` enables non-strict empty mode; new connections
  inheriting an explicitly empty GLOBAL mode also run in non-strict mode.
- Statements such as `INSERT IGNORE` and `UPDATE IGNORE` can downgrade some row-level errors
  but do not bypass structural errors, memory errors, or unsupported type shapes.
- `NO_UNSIGNED_SUBTRACTION` affects the result type of unsigned subtraction.
- `REAL_AS_FLOAT` affects the declared type of `REAL`.
- `TIME_TRUNCATE_FRACTIONAL`, `NO_ZERO_DATE`, `NO_ZERO_IN_DATE`, and `ALLOW_INVALID_DATES`
  affect temporal writes.
- Character conversion is governed by the connection charset and the target column
  charset/collation; binary strings are not transcoded as text.

Use the following statements to inspect the actual declarations and diagnostics:

```sql
SHOW CREATE TABLE table_name;
SHOW FULL COLUMNS FROM table_name;

SELECT DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH,
       CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
       DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, SRS_ID
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'table_name';

SHOW WARNINGS;
```

`DATA_TYPE` gives the canonical type family, while `COLUMN_TYPE` retains declaration details
such as precision/scale, `UNSIGNED`, `ZEROFILL`, ENUM/SET members, and vector dimensions.
Clients also receive the corresponding wire type, length, decimals, collation, and flags
through the MySQL column definition packet.

## 13. Complete Example

```sql
CREATE TABLE products (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sku         VARBINARY(32) NOT NULL,
    name        VARCHAR(200) CHARACTER SET utf8mb4
                COLLATE utf8mb4_0900_ai_ci NOT NULL,
    price       DECIMAL(18,2) NOT NULL,
    weight_kg   DOUBLE,
    status      ENUM('draft', 'active', 'retired') NOT NULL DEFAULT 'draft',
    attributes  JSON,
    location    POINT SRID 4326 NOT NULL,
    embedding   VECTOR(384) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_products_sku (sku),
    KEY idx_products_name (name),
    SPATIAL KEY sp_products_location (location),
    INDEX vi_products_embedding (embedding) USING HNSW
);
```

A production schema should further validate, against real queries, the index byte counts
after character set, precision/range, default expressions, spatial reference systems, vector
dimensions, and import behavior under strict `sql_mode`.
