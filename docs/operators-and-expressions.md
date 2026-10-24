# SQL Operators and Expressions

This document describes the SQL expressions, operators, evaluation rules, precedence, and
common limitations currently executable in mydb. See
[`function/README.md`](function/README.md) for the function index and
[`data-types.md`](data-types.md) for type declarations and conversion boundaries. mydb targets
MySQL 8.0.46 as its primary compatibility goal but is still under development toward its
first product release; unsupported forms return explicit errors and are never silently
ignored.

## 1. Where Expressions Can Appear

Expressions compute values or evaluate conditions; common locations include:

- `SELECT` projections, `WHERE`, `JOIN ... ON`, `HAVING`, `QUALIFY`, and `ORDER BY`;
- `GROUP BY`, aggregate function arguments, window function arguments, and
  `PARTITION BY`/window `ORDER BY`;
- `INSERT ... VALUES`, `UPDATE ... SET`, `RETURNING`, and `ON DUPLICATE KEY UPDATE`;
- column defaults, generated columns, `CHECK`, functional indexes, and partitioning
  expressions;
- assignments or conditions in `SET`, `DO`, stored routines, triggers, and events.

Not every location permits arbitrary expressions. Aggregates, window functions, subqueries,
non-deterministic functions, variables, and stored functions face extra restrictions in
defaults, generated columns, `CHECK`, functional indexes, or partitioning expressions; the
restriction is checked by the DDL or statement that uses the expression.

A simplified grammar follows:

```ebnf
<expr> ::= <literal> | <column-reference> | <variable> | '?'
         | <unary-expression> | <binary-expression> | <predicate>
         | <row-constructor> | <case-expression> | <function-call>
         | <cast-expression> | <subquery-expression>

<unary-expression>  ::= ( '+' | '-' | '~' | '!' | 'NOT' | 'BINARY' ) <expr>
<binary-expression> ::= <expr> <binary-operator> <expr>
<row-constructor>   ::= '(' <expr> ',' <expr> { ',' <expr> } ')'
                      | 'ROW' '(' <expr> ',' <expr> { ',' <expr> } ')'
```

Parentheses override the default precedence and are the most recommended style inside complex
conditions.

## 2. Basic Expressions

### 2.1 Literals

| Category | Syntax examples | Notes |
| --- | --- | --- |
| Null | `NULL` | the SQL null value; distinct from `null` inside a JSON document |
| Boolean | `TRUE`, `FALSE` | evaluate to integers `1` and `0` respectively |
| Integer | `0`, `-42`, `18446744073709551615` | signed, unsigned, or exact `DECIMAL` representation selected by range |
| Exact decimal | `12.30`, `.5` | decimal literals without an exponent retain decimal precision |
| Approximate | `1e-5`, `2.5E8` | scientific notation enters approximate-number semantics |
| String | `'text'`, `N'text'` | escaping is affected by `NO_BACKSLASH_ESCAPES` |
| Hexadecimal | `0xCAFE`, `X'CAFE'` | a binary string by default; numeric/bit contexts may interpret it as a big-endian unsigned integer |
| Bit string | `0b1010`, `b'1010'` | a binary string by default; numeric/bit contexts may convert it to an unsigned integer |
| Temporal | `DATE '2026-08-30'`, `TIME '12:30:00'`, `DATETIME '...'`, `TIMESTAMP '...'` | the four supported typed literals; other `TYPE 'value'` forms raise 1064 |
| Time interval | `INTERVAL 2 DAY`, `INTERVAL '1:30' HOUR_MINUTE` | a unit is mandatory; usually combined with date arithmetic or time functions |

Strings may carry a character set introducer, for example `_utf8mb4'text'` or
`_binary'abc'`. Double quotes produce a string by default; with `ANSI_QUOTES` enabled,
`"name"` instead references an identifier. See [`data-types.md`](data-types.md) for full type
behavior.

### 2.2 Columns, Identifiers, and Wildcards

- `column_name` references a column visible in the current scope.
- `table_name.column_name` or `alias.column_name` disambiguates across multiple tables.
- Backquotes can quote keywords or special characters, for example `` `order` ``.
- `*` and `table.*` are valid only in projections or function positions that allow
  wildcards; they are not ordinary scalar values.
- An unresolvable column returns 1054; an ambiguous column returns 1052 instead of
  arbitrarily choosing one source.

The query block determines name scope. A correlated subquery may reference outer columns; an
inner column with the same name takes precedence, so qualifying correlated references with an
explicit table alias is recommended.

### 2.3 Variables, Parameters, and Default Values

| Syntax | Meaning |
| --- | --- |
| `@name` | reads a session user variable |
| `@name := expr` | assigns inside an expression and yields the assigned value; right-associative, lowest precedence, produces deprecation warning 1287 |
| `@@name` | reads an implicitly session-scoped system variable |
| `@@SESSION.name` / `@@LOCAL.name` | reads a session system variable |
| `@@GLOBAL.name` | reads a global system variable |
| `?` | a prepared statement parameter; placeholders keep their lexical source order through CTE, window, RETURNING and other AST rewrites, and are bound at execution |
| `DEFAULT` | uses the target column's default value in an assignment context |
| `DEFAULT(column)` | yields the result of the named column's default expression |

Parameters cannot substitute for identifiers, keywords, or SQL structure. For example,
`SELECT * FROM ?` is invalid; generate the SQL after validating object names on the client
side. User variable assignment inside expressions preserves the MySQL-compatible surface, but
new code should prefer `SET @name = expr` or `SELECT expr INTO @name`.

## 3. Arithmetic Operators

| Operator | Meaning | Key semantics |
| --- | --- | --- |
| `+expr` | unary plus | accepts and preserves the positive semantics of the operand |
| `-expr` | unary minus | `NULL` propagates; overflow returns a range error |
| `a + b` | addition | integers, exact decimals, and approximate numbers compute in the common numeric type |
| `a - b` | subtraction | `NO_UNSIGNED_SUBTRACTION` affects the unsigned result type |
| `a * b` | multiplication | integer overflow does not silently become floating point |
| `a / b` | division | exact operands usually produce a `DECIMAL` with fractional digits; precision is affected by `div_precision_increment` |
| `a DIV b` | integer division | computed under the MySQL numeric rules first, then truncated to an integer quotient |
| `a % b` / `a MOD b` | modulo | the two spellings are equivalent |

Ordinary arithmetic yields `NULL` when any operand is SQL `NULL`. With a zero divisor, `/`,
`DIV`, `%`, and `MOD` return `NULL`; when `sql_mode` contains `ERROR_FOR_DIVISION_BY_ZERO`,
warning 1365 is recorded and may be promoted to an error in strict DML contexts. Result types
and overflow boundaries for integers, unsigned integers, `DECIMAL`, and floating point are
covered in [`data-types.md`](data-types.md#12-type-conversion-and-sql_mode).

Date-times with `INTERVAL` use dedicated rules:

```sql
SELECT DATE '2026-08-30' + INTERVAL 2 DAY;
SELECT TIMESTAMP '2026-08-30 12:00:00' - INTERVAL 90 MINUTE;
SELECT INTERVAL 1 DAY + DATE '2026-08-30';
```

`date + interval`, `date - interval`, and `interval + date` evaluate under
`DATE_ADD`/`DATE_SUB` semantics respectively; `interval - date` is invalid. A bare `INTERVAL`
is not an ordinary displayable value; detached from the date functions or arithmetic contexts
that support it, it returns an explicit error.

## 4. Bit Operators

| Operator | Meaning |
| --- | --- |
| `~a` | bitwise NOT |
| `a ^ b` | bitwise XOR; do not confuse with the logical keyword `XOR` |
| `a & b` | bitwise AND |
| `a \| b` | bitwise OR |
| `a << b` | left shift |
| `a >> b` | right shift |

Numeric operands usually convert to 64-bit unsigned integers with a `BIGINT UNSIGNED` result;
`NULL` propagates. With a shift count of 64 or greater the numeric result is `0`. Binary
strings can also take part in bit operations: `&`, `|`, and `^` require the same byte length
on both sides and compute byte by byte; binary string shifts keep the original byte width.
Hexadecimal and bit literals retain unsigned semantics in bit contexts.

One important difference between mydb and common C-style languages is that `^` has higher
precedence than `*`, `/`, `DIV`, `%`, and `MOD`:

```sql
SELECT 1 ^ 2 * 3;     -- (1 ^ 2) * 3 = 9
SELECT 1 ^ (2 * 3);   -- 7
```

## 5. Comparison Operators and Row Comparisons

| Operator | Meaning |
| --- | --- |
| `=` | equal |
| `<=>` | NULL-safe equality |
| `<>` / `!=` | not equal |
| `<` / `<=` | less than / less than or equal |
| `>` / `>=` | greater than / greater than or equal |

An ordinary comparison returns `NULL` as soon as either side is `NULL`. `<=>` never returns
`NULL`: it yields `1` when both sides are `NULL`, `0` when exactly one side is `NULL`, and
otherwise compares under the ordinary equality rules.

```sql
SELECT NULL = NULL, NULL <=> NULL, 1 <=> NULL;
-- NULL, 1, 0
```

Comparisons perform numeric, temporal, string, binary-string, or JSON comparison depending on
the operand types. String comparison follows the collation's case, accent, and PAD SPACE
rules; an explicit `COLLATE` has higher coercibility than columns, expressions, and literals.
Do not rely on hard-to-read implicit conversions; use an explicit `CAST` for cross-type
comparisons.

Row constructors support equality, inequality, and lexicographic comparison:

```sql
SELECT ROW(1, 2) = (1, 2);
SELECT (1, 9) < (2, 0);
SELECT (a, b) IN ((1, 2), (3, 4)) FROM t;
```

Both sides must have the same column count, otherwise 1241 is returned. `=` is equivalent to
per-column equality combined with a three-valued `AND`, and `<>` to per-column inequality
combined with a three-valued `OR`; ordering comparisons proceed left to right, and the result
is `NULL` when a `NULL` appears at the position that decides the ordering. Row `<=>` performs
NULL-safe equality per column, so it ultimately returns only `0` or `1`.

## 6. Predicates

A predicate yields `1`, `0`, or SQL `NULL`. In boolean contexts such as `WHERE`,
`JOIN ... ON`, `HAVING`, `QUALIFY`, and `CHECK`, the enclosing statement interprets that
result; for example, `WHERE` keeps only rows for which the result is true, discarding both
false and `NULL`.

### 6.1 `IS` Tests

```sql
expr IS NULL
expr IS NOT NULL
expr IS TRUE
expr IS NOT TRUE
expr IS FALSE
expr IS NOT FALSE
expr IS UNKNOWN
expr IS NOT UNKNOWN
```

`IS NULL`/`IS NOT NULL` is the correct way to test for null. `IS UNKNOWN` is equivalent to
`IS NULL`; `IS TRUE` and `IS FALSE` first convert the operand per the logical context, and
they themselves return only `0` or `1`.

### 6.2 `BETWEEN`

```sql
expr [NOT] BETWEEN low AND high
```

`BETWEEN` is inclusive and means `expr >= low AND expr <= high`, using the SQL three-valued
`AND`. A `NULL` therefore does not always produce `NULL`: `10 BETWEEN NULL AND 5` is `0`,
because the upper-bound comparison is already definitely false; `NULL` is returned only when
the final truth cannot be determined. `NOT BETWEEN` negates the three-valued result, and
`NULL` stays `NULL`.

### 6.3 `IN`

```sql
expr [NOT] IN (expr1, expr2, ...)
expr [NOT] IN (subquery)
```

`IN` is true when an equal item is found. When there is no match but the list or subquery
result contains `NULL`, the result is `NULL` rather than false; this is the most common
`NOT IN` trap:

```sql
SELECT 2 IN (1, NULL);      -- NULL
SELECT 2 NOT IN (1, NULL);  -- NULL
```

If the business requires ignoring nulls, add an explicit `WHERE key IS NOT NULL` to the
subquery, or rewrite it as `NOT EXISTS` with the intended null semantics.

### 6.4 `LIKE` and Regular Expressions

```sql
expr [NOT] LIKE pattern [ESCAPE escape-character]
expr [NOT] REGEXP pattern
expr [NOT] RLIKE pattern
```

In `LIKE`, `%` matches a string of any length and `_` matches one character. Without an
explicit `ESCAPE`, the default pattern uses the backslash; with `NO_BACKSLASH_ESCAPES`
enabled there is no default escape character. An explicit `ESCAPE ''` also disables escaping,
and any other `ESCAPE` value must parse to zero or one character. Text matching obeys the
collation; when either side is a binary string, the match is byte-exact. The result is `NULL`
when either operand is `NULL`.

`REGEXP` and `RLIKE` are synonyms that use the regex semantics of `REGEXP_LIKE`; `NOT REGEXP`
and `NOT RLIKE` negate the result. MySQL 8.0 does not provide `ILIKE`, and mydb returns an
explicit syntax error; for case-insensitive matching, choose a `_ci` collation, an explicit
`COLLATE`, or `LOWER`/`UPPER`.

### 6.5 `SOUNDS LIKE` and `MEMBER OF`

```sql
left SOUNDS LIKE right
value MEMBER OF (json_array)
```

`SOUNDS LIKE` is equivalent to `SOUNDEX(left) = SOUNDEX(right)`. `MEMBER OF` decides
membership by JSON scalar type and never treats the JSON number `1` and the JSON string
`"1"` as the same value; a JSON scalar on the right is treated as a single-element array,
and invalid JSON returns the corresponding error.

### 6.6 Quantified Comparisons

```sql
expr comparison-op ANY  (subquery)
expr comparison-op SOME (subquery)
expr comparison-op ALL  (subquery)
```

`ANY` and `SOME` are synonymous and become true as soon as one row compares true; `ALL`
requires every row to be true. Over an empty result set `ANY`/`SOME` is false and `ALL` is
true; `NULL` is returned when no decisive result exists but a `NULL` is encountered. The
right side must currently be a subquery, and each row must return exactly one comparable
value.

## 7. Logical Operators and Three-Valued Logic

| Operator | Meaning | Notes |
| --- | --- | --- |
| `NOT expr` | logical negation | lower precedence than comparison predicates by default; affected by `HIGH_NOT_PRECEDENCE` |
| `!expr` | high-precedence logical negation | same value semantics as `NOT`; produces deprecation warning 1287 |
| `a AND b` / `a && b` | logical AND | `&&` produces deprecation warning 1287 |
| `a XOR b` | logical exclusive OR | returns `NULL` when either side is `NULL` |
| `a OR b` | logical OR | returns `0`, `1`, or `NULL` |
| `a \|\| b` | logical OR by default | produces warning 1287 by default; becomes string concatenation under `PIPES_AS_CONCAT` |

Non-boolean values entering a logical context convert by numeric truth: zero is false and
non-zero is true; strings use the numeric prefix, so `'0'`, `'0.0'`, and `'abc'` are false
while `'1abc'` and `'-3'` are true. SQL `NULL` is unknown.

The three-valued truth table:

| `a` | `b` | `a AND b` | `a OR b` | `a XOR b` |
| --- | --- | --- | --- | --- |
| TRUE | TRUE | TRUE | TRUE | FALSE |
| TRUE | FALSE | FALSE | TRUE | TRUE |
| TRUE | NULL | NULL | TRUE | NULL |
| FALSE | FALSE | FALSE | FALSE | FALSE |
| FALSE | NULL | FALSE | NULL | NULL |
| NULL | NULL | NULL | NULL | NULL |

`AND`/`OR` short-circuit once the left side already decides the result: `0 AND expr` does not
need the right side evaluated, and neither does `1 OR expr`. Do not rely on unspecified
ordinary subexpression evaluation order to produce side effects.

With `PIPES_AS_CONCAT` enabled, `||` is fixed at parse time as string concatenation: the
result is `NULL` when either side is `NULL`; otherwise both sides are converted to strings
and concatenated. This decision is written into the AST of prepared statements and persistent
expressions; switching `sql_mode` after preparation or creation does not reinterpret the
original expression. The `ANSI` mode implies `PIPES_AS_CONCAT`.

## 8. JSON Operators

| Operator | Equivalent form | Result |
| --- | --- | --- |
| `doc -> path` | `JSON_EXTRACT(doc, path)` | JSON value |
| `doc ->> path` | `JSON_UNQUOTE(JSON_EXTRACT(doc, path))` | unquoted scalar text or canonical JSON text |

The JSON arrows bind tighter than comparisons, so `doc -> '$.id' = 10` extracts the path
first and then compares. The result is `NULL` when the path or document is SQL `NULL`; when
the left side is not valid JSON, the current implementation returns SQL `NULL`. Path syntax,
wildcard restrictions, and multi-valued indexing are covered in the JSON function
documentation and [`data-types.md`](data-types.md#8-json).

## 9. Conditional, Conversion, and Character Set Expressions

### 9.1 `CASE`

```sql
CASE value
  WHEN compare_value THEN result
  [ WHEN compare_value THEN result ... ]
  [ ELSE fallback ]
END

CASE
  WHEN condition THEN result
  [ WHEN condition THEN result ... ]
  [ ELSE fallback ]
END
```

A simple `CASE` compares `value` against each `WHEN` value in turn; a searched `CASE` checks
each condition in turn. Only the result of the first matching branch is evaluated, and an
unmatched `CASE` without `ELSE` returns `NULL`. The result type is derived at binding time by
combining all `THEN` and `ELSE` branches and does not depend on which branch actually runs.

### 9.2 `CAST`, `CONVERT`, `BINARY`, and `COLLATE`

```sql
CAST(expr AS type)
CONVERT(expr, type)
CONVERT(expr USING charset)
BINARY expr
expr COLLATE collation_name
timestamp_expr AT TIME ZONE zone_expr
```

`CAST`/`CONVERT` performs explicit type or character set conversion; supported target types
and widths are listed in [`data-types.md`](data-types.md). Unary `BINARY expr` is equivalent
to `CAST(expr AS BINARY)` but produces deprecation warning 1287; new SQL should use `CAST`
directly. `COLLATE` does not change character content, but it changes the collation used for
comparison, sorting, `LIKE`, deduplication, and grouping, and it has the highest
coercibility.

`AT TIME ZONE` converts a timestamp by the given offset or named time zone; an invalid or
unparseable time zone returns an explicit error. For the session `time_zone` and the
difference between `TIMESTAMP` and `DATETIME`, see
[`data-types.md`](data-types.md#6-date-and-time).

## 10. Function, Aggregate, and Window Expressions

An ordinary function call is `name(arg1, arg2, ...)`. Built-in scalar functions, aggregate
functions, and window functions share one expression tree but are handled by different
execution stages; see [`function/README.md`](function/README.md) for the complete list.

```sql
SELECT ABS(-5), LOWER(name), JSON_EXTRACT(doc, '$.id') FROM t;
SELECT department, SUM(amount) FROM sales GROUP BY department;
SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY amount DESC) FROM sales;
```

Keep in mind:

- Aggregate functions summarize a group of rows; mixing plain columns with aggregates must
  satisfy grouping rules such as `ONLY_FULL_GROUP_BY`.
- `DISTINCT` is effective only when the function itself supports it, for example
  `COUNT(DISTINCT key)`.
- Window functions must carry `OVER` or reference a named window; they are computed after
  grouping and before final ordering.
- Window frames support `ROWS`, `RANGE`, `GROUPS`, and the corresponding bound/exclusion
  clauses, but individual functions may still restrict frames, argument constancy, or the
  number of ordering keys.
- Unregistered functions, or recognized but not yet implemented ones, return explicit errors
  of the 1305 or 1235 family.

Full-text search with `MATCH (column, ...) AGAINST (query [mode])` is also an expression and
can appear in projections or predicates. The default/`IN NATURAL LANGUAGE MODE` and
`IN BOOLEAN MODE` are currently supported; `WITH QUERY EXPANSION` explicitly reports
unsupported.

## 11. Subquery Expressions

| Form | Result and constraints |
| --- | --- |
| `(SELECT ...)` | scalar subquery: zero rows return `NULL`, one row and one column returns that value, multiple rows or columns raise an error |
| `EXISTS (SELECT ...)` | whether at least one row exists; returns only `0` or `1` |
| `NOT EXISTS (SELECT ...)` | logical negation of `EXISTS` |
| `expr [NOT] IN (SELECT ...)` | set membership, following three-valued `NULL` logic |
| `expr op ANY/SOME/ALL (SELECT ...)` | quantified comparison |

A subquery can be uncorrelated or reference outer query columns. The optimizer decorrelates
correlated `EXISTS`, `IN`, and similar forms into semi/anti joins when safe; when
decorrelation is impossible, correct correlated execution semantics are preserved. The
optimization changes only how the query runs, never the results over empty sets, duplicates,
or `NULL`.

## 12. Operator Precedence and Associativity

The table below lists the current parse precedence from highest to lowest. Binary operators
at the same level are usually left-associative; `:=` is right-associative. `.` denotes
qualified names/member access; function calls and explicit parentheses bind tighter than the
operators in the table.

| Precedence | Operator or construct | Notes |
| ---: | --- | --- |
| 1 (highest) | `.`, `->`, `->>`, `INTERVAL`, function calls, `(...)` | name qualification, JSON extraction, interval construction, calls, and explicit grouping |
| 2 | `COLLATE`, `AT TIME ZONE` | collation/time-zone suffixes |
| 3 | `!`, unary `-`, unary `+`, `~`, `BINARY` | high-precedence prefixes |
| 4 | `\|\|` | `PIPES_AS_CONCAT` mode only; otherwise at the logical `OR` level |
| 5 | `^` | bitwise XOR |
| 6 | `*`, `/`, `DIV`, `%`, `MOD` | multiplication, division, remainder |
| 7 | `+`, `-` | addition/subtraction and date-interval arithmetic |
| 8 | `<<`, `>>` | shifts |
| 9 | `&` | bitwise AND |
| 10 | `\|` | bitwise OR |
| 11 | `=`, `<=>`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IS`, `LIKE`, `REGEXP`, `RLIKE`, `MEMBER OF`, `SOUNDS LIKE` | comparison level |
| 12 | `BETWEEN`, `IN` | predicate level; `NOT BETWEEN`/`NOT IN` parse as a unit |
| 13 | `NOT` | default mode; raised to the high-precedence prefix level with `HIGH_NOT_PRECEDENCE` |
| 14 | `AND`, `&&` | logical AND |
| 15 | `XOR` | logical exclusive OR |
| 16 | `OR`, `\|\|` | logical OR in the default mode |
| 17 (lowest) | `:=` | user variable assignment, right-associative |

`HIGH_NOT_PRECEDENCE` gives the keyword `NOT` the same high precedence as `!`. The following
can therefore change meaning with the session mode; parenthesizing is recommended:

```sql
SELECT NOT 1 BETWEEN 0 AND 2;
SELECT (NOT 1) BETWEEN 0 AND 2;
SELECT NOT (1 BETWEEN 0 AND 2);
```

`PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE`, and `ANSI` (which implies these behaviors) all
enter the parse semantics and the plan-cache key. Prepared statements fix the interpretation
at prepare time and do not change when the mode is switched before execution.

## 13. NULL, Short-Circuit Evaluation, Type Inference, and Diagnostics

### 13.1 NULL Propagation

Ordinary arithmetic, bit operations, comparisons, `LIKE`, and regex operations usually return
`NULL` when any required operand is `NULL`. The main exceptions:

- `<=>`, `IS NULL`, `IS TRUE`, and other explicit truth/null tests;
- `AND`, `OR`, `BETWEEN`, where another part can first decide the final truth;
- `IN`/`NOT IN`, which can ignore the other `NULL`s once a decisive match is found;
- `EXISTS`, which cares only about the presence of rows, not whether projected values are
  `NULL`.

### 13.2 Evaluation and Short-Circuiting

- `AND` short-circuits when the left side is false, and `OR` when the left side is true.
- `CASE` evaluates only the selected result branch.
- Business side effects should not be built on ordinary function arguments, projection lists,
  or expression orderings without explicit guarantees.
- Constant folding, compiled expressions, row-wise execution, and vectorized execution must
  preserve the same SQL results and warning semantics; forms that cannot be compiled fall
  back to the legacy evaluator rather than skipping computation.

### 13.3 Types and Warnings

Expressions derive MySQL column definition metadata at binding time, including type, length,
decimals, unsigned, charset/collation, and nullability. When a result is empty, a prepared
parameter has not yet produced a concrete value, or a constant has been folded, the type is
likewise not inferred back from incidental runtime values.

Implicit string-to-number conversion, truncation, division by zero, and deprecated operators
can produce warnings. After execution, use:

```sql
SHOW WARNINGS;
SHOW COUNT(*) WARNINGS;
```

Strict `sql_mode`, the DML context, and `IGNORE` affect whether certain data problems are
warnings or errors; syntax errors, structural errors, column-count mismatches, and explicitly
unsupported expressions are never swallowed by `IGNORE`.

## 14. Comprehensive Example

```sql
CREATE TABLE orders (
    id         BIGINT PRIMARY KEY,
    customer   VARCHAR(100) COLLATE utf8mb4_0900_ai_ci,
    amount     DECIMAL(18,2),
    attributes JSON,
    created_at TIMESTAMP(6)
);

SELECT id,
       CASE
         WHEN amount IS NULL THEN 'unknown'
         WHEN amount >= 1000 THEN 'large'
         ELSE 'normal'
       END AS size_class,
       attributes ->> '$.channel' AS channel,
       created_at + INTERVAL 7 DAY AS expires_at
FROM orders
WHERE customer LIKE 'wang%' COLLATE utf8mb4_0900_ai_ci
  AND amount BETWEEN 100.00 AND 5000.00
  AND attributes -> '$.status' = JSON_QUOTE('paid')
ORDER BY amount DESC;
```

When mixing logical, comparison, JSON, and date arithmetic, parenthesize according to business
intent and make cross-type boundaries explicit with `CAST` and `COLLATE`. This reduces the
ambiguity introduced by `sql_mode` and collations, and also helps functional indexes, range
scans, and predicate pushdown recognize stable optimizable expressions.

## 15. Related Documentation

- [Data Types](data-types.md)
- [SQL Function Index](function/README.md)
- [SQL Syntax Notation and Common Conventions](statements/syntax-conventions.md)
- [SELECT](statements/query/select.md)
- [MySQL 8.0 Compatibility Status](mysql80-compatibility.md)
- [Executor Maintenance Constraints](executor.md)
