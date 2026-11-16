use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/json.json"
));

function_fixture_test!(json_array, "JSON_ARRAY", FIXTURE);
function_fixture_test!(json_array_append, "JSON_ARRAY_APPEND", FIXTURE);
function_fixture_test!(json_array_insert, "JSON_ARRAY_INSERT", FIXTURE);
function_fixture_test!(json_contains, "JSON_CONTAINS", FIXTURE);
function_fixture_test!(json_contains_path, "JSON_CONTAINS_PATH", FIXTURE);
function_fixture_test!(json_depth, "JSON_DEPTH", FIXTURE);
function_fixture_test!(json_extract, "JSON_EXTRACT", FIXTURE);
function_fixture_test!(json_insert, "JSON_INSERT", FIXTURE);
function_fixture_test!(json_keys, "JSON_KEYS", FIXTURE);
function_fixture_test!(json_length, "JSON_LENGTH", FIXTURE);
function_fixture_test!(json_merge, "JSON_MERGE", FIXTURE);
function_fixture_test!(json_merge_patch, "JSON_MERGE_PATCH", FIXTURE);
function_fixture_test!(json_merge_preserve, "JSON_MERGE_PRESERVE", FIXTURE);
function_fixture_test!(json_object, "JSON_OBJECT", FIXTURE);
function_fixture_test!(json_overlaps, "JSON_OVERLAPS", FIXTURE);
function_fixture_test!(json_pretty, "JSON_PRETTY", FIXTURE);
function_fixture_test!(json_quote, "JSON_QUOTE", FIXTURE);
function_fixture_test!(json_remove, "JSON_REMOVE", FIXTURE);
function_fixture_test!(json_replace, "JSON_REPLACE", FIXTURE);
function_fixture_test!(json_schema_valid, "JSON_SCHEMA_VALID", FIXTURE);
function_fixture_test!(
    json_schema_validation_report,
    "JSON_SCHEMA_VALIDATION_REPORT",
    FIXTURE
);
function_fixture_test!(json_search, "JSON_SEARCH", FIXTURE);
function_fixture_test!(json_set, "JSON_SET", FIXTURE);
function_fixture_test!(json_storage_free, "JSON_STORAGE_FREE", FIXTURE);
function_fixture_test!(json_storage_size, "JSON_STORAGE_SIZE", FIXTURE);
function_fixture_test!(json_type, "JSON_TYPE", FIXTURE);
function_fixture_test!(json_unquote, "JSON_UNQUOTE", FIXTURE);
function_fixture_test!(json_valid, "JSON_VALID", FIXTURE);
function_fixture_test!(json_value, "JSON_VALUE", FIXTURE);
