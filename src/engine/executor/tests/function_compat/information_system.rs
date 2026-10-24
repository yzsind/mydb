use super::*;

const FIXTURE: &str = include_str!("fixtures/information-system.json");

function_fixture_test!(benchmark, "BENCHMARK", FIXTURE);
function_fixture_test!(connection_id, "CONNECTION_ID", FIXTURE);
function_fixture_test!(current_role, "CURRENT_ROLE", FIXTURE);
function_fixture_test!(current_user, "CURRENT_USER", FIXTURE);
function_fixture_test!(database, "DATABASE", FIXTURE);
function_fixture_test!(
    extract_schema_from_file_name,
    "EXTRACT_SCHEMA_FROM_FILE_NAME",
    FIXTURE
);
function_fixture_test!(
    extract_table_from_file_name,
    "EXTRACT_TABLE_FROM_FILE_NAME",
    FIXTURE
);
function_fixture_test!(format_bytes, "FORMAT_BYTES", FIXTURE);
function_fixture_test!(format_path, "FORMAT_PATH", FIXTURE);
function_fixture_test!(format_pico_time, "FORMAT_PICO_TIME", FIXTURE);
function_fixture_test!(format_statement, "FORMAT_STATEMENT", FIXTURE);
function_fixture_test!(format_time, "FORMAT_TIME", FIXTURE);
function_fixture_test!(found_rows, "FOUND_ROWS", FIXTURE);
function_fixture_test!(gtid_subset, "GTID_SUBSET", FIXTURE);
function_fixture_test!(gtid_subtract, "GTID_SUBTRACT", FIXTURE);
function_fixture_test!(icu_version, "ICU_VERSION", FIXTURE);
function_fixture_test!(last_insert_id, "LAST_INSERT_ID", FIXTURE);
function_fixture_test!(load_file, "LOAD_FILE", FIXTURE);
function_fixture_test!(
    mysql_query_attribute_string,
    "MYSQL_QUERY_ATTRIBUTE_STRING",
    FIXTURE
);
function_fixture_test!(ps_current_thread_id, "PS_CURRENT_THREAD_ID", FIXTURE);
function_fixture_test!(ps_is_account_enabled, "PS_IS_ACCOUNT_ENABLED", FIXTURE);
function_fixture_test!(ps_is_consumer_enabled, "PS_IS_CONSUMER_ENABLED", FIXTURE);
function_fixture_test!(
    ps_is_instrument_default_enabled,
    "PS_IS_INSTRUMENT_DEFAULT_ENABLED",
    FIXTURE
);
function_fixture_test!(
    ps_is_instrument_default_timed,
    "PS_IS_INSTRUMENT_DEFAULT_TIMED",
    FIXTURE
);
function_fixture_test!(
    ps_is_thread_instrumented,
    "PS_IS_THREAD_INSTRUMENTED",
    FIXTURE
);
function_fixture_test!(ps_thread_account, "PS_THREAD_ACCOUNT", FIXTURE);
function_fixture_test!(ps_thread_id, "PS_THREAD_ID", FIXTURE);
function_fixture_test!(ps_thread_stack, "PS_THREAD_STACK", FIXTURE);
function_fixture_test!(ps_thread_trx_info, "PS_THREAD_TRX_INFO", FIXTURE);
function_fixture_test!(row_count, "ROW_COUNT", FIXTURE);
function_fixture_test!(schema, "SCHEMA", FIXTURE);
function_fixture_test!(session_user, "SESSION_USER", FIXTURE);
function_fixture_test!(sleep, "SLEEP", FIXTURE);
function_fixture_test!(statement_digest, "STATEMENT_DIGEST", FIXTURE);
function_fixture_test!(statement_digest_text, "STATEMENT_DIGEST_TEXT", FIXTURE);
function_fixture_test!(system_user, "SYSTEM_USER", FIXTURE);
function_fixture_test!(sys_get_config, "SYS_GET_CONFIG", FIXTURE);
function_fixture_test!(user, "USER", FIXTURE);
function_fixture_test!(version, "VERSION", FIXTURE);
function_fixture_test!(version_major, "VERSION_MAJOR", FIXTURE);
function_fixture_test!(version_minor, "VERSION_MINOR", FIXTURE);
function_fixture_test!(version_patch, "VERSION_PATCH", FIXTURE);
