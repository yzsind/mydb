//! UUID column type: a mydb extension, not a MySQL column type.
//!
//! Covers insert/retrieval, invalid values, canonical equality for primary and
//! unique keys, catalog echo, and a restart round trip.

use super::support::query_rows as rows;
use super::*;

const CANONICAL: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let result = rows(engine, sql);
    assert_eq!(result.len(), 1, "expected one row for `{sql}`");
    assert_eq!(result[0].len(), 1, "expected one column for `{sql}`");
    result[0][0].to_display_string()
}

fn assert_uuid_error(engine: &StorageQueryEngine, sql: &str) {
    let err = engine.execute(sql).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("incorrect uuid value"),
        "expected a uuid assignment error for `{sql}`, got {message}"
    );
}

#[test]
fn uuid_column_accepts_spellings_and_rejects_invalid_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE sessions (
                id UUID PRIMARY KEY,
                label VARCHAR(32) NOT NULL
            )",
        )
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO sessions VALUES ('{CANONICAL}', 'lower'), \
             (UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db'), 'bin')"
        ))
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT id FROM sessions WHERE label = 'lower'"),
        CANONICAL
    );
    assert_eq!(
        cell(&engine, "SELECT id FROM sessions WHERE label = 'bin'"),
        "6ccd780c-baba-1026-9564-5b8c656024db"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT id = BIN_TO_UUID(UUID_TO_BIN(id)) FROM sessions WHERE label = 'bin'"
        ),
        "1"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT HEX(id) = HEX(UUID_TO_BIN(id)) FROM sessions WHERE label = 'lower'"
        ),
        "1"
    );

    for spelling in [
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        "a0eebc999c0b4ef8bb6d6bb9bd380a11",
        "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
    ] {
        assert_eq!(
            cell(
                &engine,
                &format!("SELECT label FROM sessions WHERE id = '{spelling}'")
            ),
            "lower",
            "{spelling}"
        );
    }

    let duplicate = engine.execute(&format!(
        "INSERT INTO sessions VALUES ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'dup')"
    ));
    let message = duplicate.unwrap_err().to_string();
    assert!(
        message.contains("Duplicate entry") && message.contains(CANONICAL),
        "primary key should collapse spellings and report the canonical value, got {message}"
    );
    assert_uuid_error(&engine, "INSERT INTO sessions VALUES ('not-a-uuid', 'bad')");
    assert_uuid_error(
        &engine,
        "INSERT INTO sessions VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 ', 'pad')",
    );

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    let relaxed = engine.execute_with_session(
        &mut session,
        "INSERT INTO sessions VALUES ('still-not-a-uuid', 'loose')",
    );
    let message = relaxed.unwrap_err().to_string();
    assert!(
        message.contains("incorrect uuid value"),
        "non-strict mode still rejects invalid uuid values, got {message}"
    );
}

#[test]
fn uuid_unique_key_show_create_and_information_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE accounts (
                n INT PRIMARY KEY,
                user_id UUID NOT NULL,
                UNIQUE KEY user_id (user_id)
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO accounts VALUES (1, 'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE')")
        .unwrap();
    let duplicate =
        engine.execute("INSERT INTO accounts VALUES (2, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')");
    let message = duplicate.unwrap_err().to_string();
    assert!(
        message.contains("Duplicate entry")
            && message.contains("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        "unique key must use the canonical identity, got {message}"
    );
    assert_eq!(
        cell(&engine, "SELECT user_id FROM accounts WHERE n = 1"),
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    );

    let create = rows(&engine, "SHOW CREATE TABLE accounts");
    let create_sql = create
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| value.to_display_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        create_sql.to_ascii_lowercase().contains("`user_id` uuid"),
        "SHOW CREATE TABLE should name the uuid type, got {create_sql}"
    );
    let types = rows(
        &engine,
        "SELECT DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS \
         WHERE TABLE_NAME = 'accounts' AND COLUMN_NAME = 'user_id'",
    );
    assert_eq!(types.len(), 1, "{types:?}");
    assert_eq!(types[0][0].to_display_string(), "uuid");
    assert_eq!(types[0][1].to_display_string(), "uuid");

    let cast = cell(
        &engine,
        "SELECT CAST('AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE' AS UUID)",
    );
    assert_eq!(cast, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
}

#[test]
fn uuid_column_round_trips_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE persisted (id UUID PRIMARY KEY, note VARCHAR(8))")
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO persisted VALUES ('{CANONICAL}', 'kept')"
            ))
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        cell(&engine, "SELECT id FROM persisted WHERE note = 'kept'"),
        CANONICAL
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT note FROM persisted WHERE id = 'A0EEBC999C0B4EF8BB6D6BB9BD380A11'"
        ),
        "kept"
    );
}

#[test]
fn existing_uuid_functions_keep_their_text_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            "SELECT BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db'))"
        ),
        "6ccd780c-baba-1026-9564-5b8c656024db"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db')"
        ),
        "1"
    );
    assert_eq!(cell(&engine, "SELECT IS_UUID('nope')"), "0");
}
