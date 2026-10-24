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
