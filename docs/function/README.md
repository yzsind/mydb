# SQL Function Index

This directory organizes the user-visible SQL functions currently provided by mydb by their purpose. The authoritative sources are the scalar evaluator and its shared math registry, aggregate and window executors, and GIS and vector registries. Synonymous functions are listed separately, while internal execution markers such as `__...` are not part of the user interface and are not included. Recognized names that always return an unsupported error are excluded. MySQL-compatible functions, mydb extensions and partial compatibility helpers are distinguished in the category references; listing a name does not imply every MySQL overload is implemented.

The `README.md` in each category directory lists one function per line and links to the detailed documentation for that function. Some functions can also be used as window functions; each is listed only once, in the category closest to its primary use.

| Category | Contents |
| --- | --- |
| [Aggregate and Window Functions](aggregate-window/README.md) | Grouped aggregation, statistical aggregation, window ranking, window value selection |
| [Flow Control and Comparison Functions](flow-control-comparison/README.md) | Conditional branching, NULL handling, sets and comparison |
| [Numeric and Bit Functions](numeric-bit/README.md) | Mathematics, random numbers, base conversion and bitwise operations |
| [String Functions](string/README.md) | String construction, searching, formatting, and regular expressions |
| [Date and Time Functions](date-time/README.md) | Date and time construction, extraction, arithmetic, and formatting |
| [Type Conversion and Charset Functions](conversion-charset/README.md) | Type conversion, charsets, collations, and weight strings |
| [JSON Functions](json/README.md) | JSON construction, querying, modification, validation, and storage information |
| [Encryption, Hashing, and Compression Functions](encryption-compression/README.md) | Hashing, AES, Base64, random bytes, and compression |
| [Information and System Functions](information-system/README.md) | Session information, server information, diagnostics, and sys schema helper functions |
| [Network, UUID, and Sequence Functions](network-uuid-sequence/README.md) | IP addresses, UUIDs, and sequence operations |
| [Data Masking and Keyring Functions](masking-keyring/README.md) | Data masking, password strength, and keyring operations |
| [Locking Functions](locking/README.md) | User-level named locks |
| [XML Functions](xml/README.md) | XML fragments, XPath extraction and replacement |
| [Spatial Functions](spatial/README.md) | GIS construction, conversion, properties, measurement, and spatial predicates |
| [HTTP Functions](http/README.md) | Bounded HTTP/HTTPS requests, authentication, binary payloads and response metadata |
| [AI Functions](ai/README.md) | LLM text generation, structured responses, and native embedding calls |
| [Vector Functions](vector/README.md) | Vector conversion, distance, inner product, normalization, and slicing |

Example result blocks show the text-protocol values for the stated inputs.
Floating-point results may differ in their final rendered digits across
platforms. Time, connection identity, active roles and mutable setup tables
have their varying fields and preconditions called out explicitly. Without an
outer ORDER BY, displayed row order is illustrative. SQL NULL, JSON null and
the text `NULL` are distinct; function pages identify the relevant case.

`MATCH (...) AGAINST (...)` is full-text search syntax rather than a regular function call; see [Full-text search](../fulltext-search.md). `generate_series` is a table function; see [`../generate-series.md`](../generate-series.md) separately.

## Reproducing examples

Unless a page states a different prerequisite, use a disposable database and this
session baseline before comparing static outputs:

```sql
USE test;
SET SESSION sql_mode='';
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET SESSION time_zone='+00:00';
SET SESSION lc_time_names='en_US';
SET SESSION default_week_format=0;
```

`test` must exist. Empty SQL mode makes conversion diagnostics reproducible; it is
an example baseline, not a production configuration recommendation. Check warnings
with SHOW WARNINGS immediately after the statement. Strict writes can reject values
that a SELECT reports as NULL plus a warning. Category references state the rules.

Static Result blocks use text-protocol values without column headers. Empty text
is not SQL NULL. JSON null and the text NULL are distinct again. Use HEX for binary
results and VECTOR_TO_STRING for native vectors. Floating output can differ in its
last digits and unordered row/aggregate input order is illustrative unless the
example specifies ordering. Time, identity, random and external-service results
use conditional examples instead of fixed invented values.

Sequence/keyring/lock examples create state and must be run on an isolated instance
with fresh names and the documented privileges. HTTP examples require the
[local egress policy and test service](http/README.md#local-example-setup); AI examples
require configured models and AI_USE. External operations are not undone by SQL
rollback. No function-page example grants production credentials or isolation beyond
its explicit current contract.
