# SQL Function Index

This directory organizes the user-visible SQL functions currently provided by mydb by their purpose. The authoritative sources are the scalar evaluator and its shared math registry, aggregate and window executors, and GIS and vector registries. Synonymous functions are listed separately, while internal execution markers such as `__...` are not part of the user interface and are not included. Recognized names that always return an unsupported error are excluded.

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
| [Spatial Functions](spatial/README.md) | GIS construction, conversion, properties, measurement, and spatial predicates |
| [AI Functions](ai/README.md) | LLM text generation, structured responses, and native embedding calls |
| [Vector Functions](vector/README.md) | Vector conversion, distance, inner product, normalization, and slicing |

Example result blocks show the text-protocol values for the stated inputs.
Floating-point results may differ in their final rendered digits across
platforms. Time, connection identity, active roles and mutable setup tables
have their varying fields and preconditions called out explicitly. Without an
outer ORDER BY, displayed row order is illustrative. SQL NULL, JSON null and
the text `NULL` are distinct; function pages identify the relevant case.

`MATCH (...) AGAINST (...)` is full-text search syntax rather than a regular function call; see the full-text index documentation and tests. `generate_series` is a table function; see [`../generate-series.md`](../generate-series.md) separately.
