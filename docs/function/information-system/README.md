# Information and System Functions

| Function | Description |
| --- | --- |
| [`BENCHMARK`](benchmark.md) | Repeatedly evaluates an expression a specified number of times and returns 0; used for microbenchmarks. |
| [`CONNECTION_ID`](connection_id.md) | Returns the thread ID of the current connection. |
| [`CURRENT_ROLE`](current_role.md) | Returns the roles activated in the current session. |
| [`CURRENT_USER`](current_user.md) | Returns the authenticated account used for privilege checking. |
| [`DATABASE`](database.md) | Returns the current default database. |
| [`EXTRACT_SCHEMA_FROM_FILE_NAME`](extract_schema_from_file_name.md) | Extracts the schema name from a server-internal file name. |
| [`EXTRACT_TABLE_FROM_FILE_NAME`](extract_table_from_file_name.md) | Extracts the table name from a server-internal file name. |
| [`FORMAT_BYTES`](format_bytes.md) | Formats a byte count into human-readable text. |
| [`FORMAT_PATH`](format_path.md) | Shortens and formats Performance Schema file paths. |
| [`FORMAT_PICO_TIME`](format_pico_time.md) | Formats a picosecond duration into human-readable text. |
| [`FORMAT_STATEMENT`](format_statement.md) | Truncates and formats an SQL statement into text suitable for diagnostic display. |
| [`FORMAT_TIME`](format_time.md) | Formats a picosecond duration into human-readable text; a compatibility function for `FORMAT_PICO_TIME`. |
| [`FOUND_ROWS`](found_rows.md) | Returns the preceding SELECT row count tracked by the session. |
| [`GTID_SUBSET`](gtid_subset.md) | Determines whether one GTID set is a subset of another. |
| [`GTID_SUBTRACT`](gtid_subtract.md) | Subtracts one GTID set from another. |
| [`ICU_VERSION`](icu_version.md) | Returns the pinned ICU compatibility version string. |
| [`LAST_INSERT_ID`](last_insert_id.md) | Returns the most recently generated auto-increment value for the current connection; can also set that session value. |
| [`LOAD_FILE`](load_file.md) | Reads a server-side file under privilege and `secure_file_priv` constraints. |
| [`MYSQL_QUERY_ATTRIBUTE_STRING`](mysql_query_attribute_string.md) | Reads a named attribute from the current protocol command. |
| [`PS_CURRENT_THREAD_ID`](ps_current_thread_id.md) | Returns the current connection identity used by the compatibility helper. |
| [`PS_IS_ACCOUNT_ENABLED`](ps_is_account_enabled.md) | Returns the current account-instrumentation compatibility response. |
| [`PS_IS_CONSUMER_ENABLED`](ps_is_consumer_enabled.md) | Reports whether a named Performance Schema consumer is enabled. |
| [`PS_IS_INSTRUMENT_DEFAULT_ENABLED`](ps_is_instrument_default_enabled.md) | Reports the enabled flag exposed for a named Performance Schema instrument. |
| [`PS_IS_INSTRUMENT_DEFAULT_TIMED`](ps_is_instrument_default_timed.md) | Reports the timing flag exposed for a named Performance Schema instrument. |
| [`PS_IS_THREAD_INSTRUMENTED`](ps_is_thread_instrumented.md) | Returns the current thread-instrumentation compatibility response. |
| [`PS_THREAD_ACCOUNT`](ps_thread_account.md) | Returns the current authorization account through the compatibility helper. |
| [`PS_THREAD_ID`](ps_thread_id.md) | Returns a supplied connection identity through the compatibility helper. |
| [`PS_THREAD_STACK`](ps_thread_stack.md) | Provides the current compatibility entry point for thread-stack diagnostics. |
| [`PS_THREAD_TRX_INFO`](ps_thread_trx_info.md) | Provides the current compatibility entry point for thread-transaction diagnostics. |
| [`ROW_COUNT`](row_count.md) | Returns the affected-row count recorded for the preceding statement. |
| [`SCHEMA`](schema.md) | Returns the current default database; an alias of `DATABASE`. |
| [`SESSION_USER`](session_user.md) | Returns the account that established the current connection; an alias of `USER`. |
| [`SLEEP`](sleep.md) | Pauses execution for the specified number of seconds. |
| [`STATEMENT_DIGEST`](statement_digest.md) | Returns the digest hash of normalized SQL text. |
| [`STATEMENT_DIGEST_TEXT`](statement_digest_text.md) | Returns the normalized digest text of SQL text. |
| [`SYSTEM_USER`](system_user.md) | Returns the account that established the current connection; an alias of `USER`. |
| [`SYS_GET_CONFIG`](sys_get_config.md) | Reads a value from the sys schema configuration, returning a default value when it does not exist. |
| [`USER`](user.md) | Returns the `user@host` that established the current connection. |
| [`VERSION`](version.md) | Returns the server compatibility version string. |
| [`VERSION_MAJOR`](version_major.md) | Returns the major version number of the version string. |
| [`VERSION_MINOR`](version_minor.md) | Returns the minor version number of the version string. |
| [`VERSION_PATCH`](version_patch.md) | Returns the patch version number of the version string. |
