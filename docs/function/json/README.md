# JSON Functions

| Function | Description |
| --- | --- |
| [`JSON_ARRAY`](json_array.md) | Constructs a JSON array from the arguments. |
| [`JSON_ARRAY_APPEND`](json_array_append.md) | Appends values to the end of the JSON array at the specified path. |
| [`JSON_ARRAY_INSERT`](json_array_insert.md) | Inserts values at the specified array path position. |
| [`JSON_CONTAINS`](json_contains.md) | Determines whether a JSON document contains a candidate value, optionally restricted to a path. |
| [`JSON_CONTAINS_PATH`](json_contains_path.md) | Determines whether a JSON document contains any or all of the specified paths. |
| [`JSON_DEPTH`](json_depth.md) | Returns the maximum nesting depth of a JSON document. |
| [`JSON_EXTRACT`](json_extract.md) | Returns the values matched by one or more JSON paths. |
| [`JSON_INSERT`](json_insert.md) | Inserts values into a JSON document only when the path does not exist. |
| [`JSON_KEYS`](json_keys.md) | Returns an array of member names from a JSON object. |
| [`JSON_LENGTH`](json_length.md) | Returns the length of a JSON document or of the value at the specified path. |
| [`JSON_MERGE`](json_merge.md) | Merges JSON documents; a deprecated alias of `JSON_MERGE_PRESERVE`. |
| [`JSON_MERGE_PATCH`](json_merge_patch.md) | Merges JSON documents following RFC 7396 semantics. |
| [`JSON_MERGE_PRESERVE`](json_merge_preserve.md) | Merges JSON documents while preserving conflicting values. |
| [`JSON_OBJECT`](json_object.md) | Constructs a JSON object from key-value arguments. |
| [`JSON_OVERLAPS`](json_overlaps.md) | Determines whether two JSON documents share any key-value pair or array element. |
| [`JSON_PRETTY`](json_pretty.md) | Returns JSON text in a readable, indented format. |
| [`JSON_QUOTE`](json_quote.md) | Quotes and escapes a string as a JSON string literal. |
| [`JSON_REMOVE`](json_remove.md) | Removes the specified paths from a JSON document. |
| [`JSON_REPLACE`](json_replace.md) | Replaces only paths that already exist in a JSON document. |
| [`JSON_SCHEMA_VALID`](json_schema_valid.md) | Determines whether a JSON document conforms to a given JSON Schema. |
| [`JSON_SCHEMA_VALIDATION_REPORT`](json_schema_validation_report.md) | Returns the JSON Schema validation result and failure details. |
| [`JSON_SEARCH`](json_search.md) | Searches a JSON document for a string and returns the matching paths. |
| [`JSON_SET`](json_set.md) | Sets JSON path values, creating the path when it does not exist. |
| [`JSON_STORAGE_FREE`](json_storage_free.md) | Returns 0 for valid non-NULL JSON; mydb does not track MySQL partial-update free space. |
| [`JSON_STORAGE_SIZE`](json_storage_size.md) | Returns the MySQL binary-JSON encoded byte count, separate from internal JSONB storage. |
| [`JSON_TYPE`](json_type.md) | Returns the type name of a JSON value. |
| [`JSON_UNQUOTE`](json_unquote.md) | Removes JSON string quoting and processes escape sequences. |
| [`JSON_VALID`](json_valid.md) | Determines whether the input is a valid JSON document. |
| [`JSON_VALUE`](json_value.md) | Extracts and converts a scalar value from a JSON path. |


## JSON values and paths

Native JSON and valid JSON text are accepted by document arguments. SQL NULL is
not the same as JSON null or the text NULL. Constructors turn SQL NULL values into
JSON null; a NULL document or NULL path generally returns SQL NULL. JSON_VALID is
a validator and returns 0 for invalid document text; ordinary document operations
report invalid JSON. It does not treat an arbitrary non-string SQL scalar as JSON
text. Typed SQL numeric/temporal values in constructors keep their JSON scalar types.

| Path form | Example |
| --- | --- |
| Root | $ |
| Object member | $.name or $."member with spaces" |
| Array element | $[0] (zero-based), $[last], $[last-1] |
| Member/element wildcard | $.* or $[*] |
| Recursive descent | $**.name |
| Array range | $[1 to 3] |

Extraction can return multiple matches and wraps them as an array. Mutation paths
must identify an eligible single location and reject wildcard/range forms. Function
restrictions are independent of whether a path is syntactically valid: CONTAINS,
KEYS and LENGTH do not accept every multi-match extraction path. JSON_VALUE requires
a constant scalar-selection path. A missing path is distinct from an invalid path.

Multiple mutation path/value pairs are applied left to right; an earlier array
insertion/removal can change later positions. Inserting into a missing parent does
not create an entire object chain. SQL string values are JSON strings unless they
are parsed as JSON explicitly. JSON text parsing and result construction enforce
nesting and governed-memory limits; oversized/deep input raises a diagnostic.

| Operation | Missing path / special value |
| --- | --- |
| JSON_EXTRACT | SQL NULL when nothing matches |
| JSON_INSERT | Add an eligible missing member; preserve existing values |
| JSON_REPLACE | Ignore missing paths |
| JSON_SET | Replace existing or add eligible missing member/array position |
| JSON_REMOVE | Ignore a missing member; root removal is invalid |
| JSON_ARRAY_APPEND | Wrap selected scalar when necessary; ignore unmatched paths |
| JSON_ARRAY_INSERT | Insert before an array position; out-of-range positions append |
| JSON_MERGE_PATCH | JSON null in an object patch deletes that key; arrays replace |
| JSON_MERGE_PRESERVE | Preserve conflicting values by array wrapping/concatenation |

JSON_STORAGE_SIZE reports MySQL binary-JSON encoding size, not mydb heap/JSONB
allocation. JSON_STORAGE_FREE is a compatibility value of zero for valid input;
it does not measure reusable storage. JSON schemas use the Draft 4 subset supported
by the validator; see the two schema-function pages for success/failure forms.

JSON_TABLE is a table function, not a scalar function: see [JSON_TABLE](../../json-table.md) for row paths, typed columns, lateral input and nested expansion.
