//! System-metadata data-source coverage (system-metadata-improvement-plan
//! Phase 1.4 / 1.5).
//!
//! Covers the pieces whose data source now exists but was previously an
//! honest-empty view:
//! - `mysql.time_zone*` — populated from the host zoneinfo (TZif) database
//!   via `engine::tz::zone_catalog()` (N4). Skips gracefully on hosts with no
//!   zoneinfo (same honest-empty as an un-loaded MySQL).
//! - `performance_schema.setup_consumers` — factory defaults realigned to
//!   MySQL 8.0 so `statements_digest` / `events_statements_current|history`
//!   collect without an explicit `UPDATE` (§4), and the digest view is
//!   non-empty on first query.

use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set for `{sql}`, got {other:?}"),
    }
}

fn cell_string(v: &Value) -> String {
    v.to_display_string()
}

#[test]
fn engines_expose_innodb_compatibility_alias_consistently() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let show_rows = rows(&engine, "/* ApplicationName=NineData */ SHOW ENGINES");
    let information_schema_rows = rows(
        &engine,
        "SELECT ENGINE, SUPPORT, COMMENT, TRANSACTIONS, XA, SAVEPOINTS \
         FROM information_schema.ENGINES",
    );
    assert_eq!(
        show_rows, information_schema_rows,
        "SHOW ENGINES and information_schema.ENGINES must use the same engine catalog"
    );
    assert_eq!(show_rows.len(), 2);

    let innodb = show_rows
        .iter()
        .find(|row| cell_string(&row[0]).eq_ignore_ascii_case("InnoDB"))
        .expect("InnoDB compatibility alias must be advertised");
    assert_eq!(cell_string(&innodb[1]), "YES");
    assert_eq!(
        cell_string(&innodb[2]),
        "Supports transactions, row-level locking, and foreign keys"
    );
    assert_eq!(cell_string(&innodb[3]), "YES");
    assert_eq!(cell_string(&innodb[4]), "YES");
    assert_eq!(cell_string(&innodb[5]), "YES");
}

// ---------------------------------------------------------------------------
// mysql.time_zone* (Phase 1.4 / N4)
// ---------------------------------------------------------------------------

#[test]
fn time_zone_name_matches_host_zoneinfo_catalog() {
    let catalog = crate::engine::tz::zone_catalog();
    if catalog.zones.is_empty() {
        // Host has no zoneinfo database — honest empty, nothing to verify.
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let name_rows = rows(
        &engine,
        "SELECT Name, Time_zone_id FROM mysql.time_zone_name",
    );
    assert_eq!(
        name_rows.len(),
        catalog.zones.len(),
        "time_zone_name row count must equal the host zone catalog size"
    );
    // Row (id) for time_zone must be one-to-one with the name table.
    let zone_rows = rows(&engine, "SELECT Time_zone_id FROM mysql.time_zone");
    assert_eq!(zone_rows.len(), catalog.zones.len());

    // A well-known zone must resolve, and its id must be consistent across the
    // two tables.
    let expected: std::collections::HashMap<String, i64> = catalog
        .zones
        .iter()
        .map(|z| (z.name.clone(), z.id as i64))
        .collect();
    let mut seen_utc = false;
    for row in &name_rows {
        let name = cell_string(&row[0]);
        let id = match &row[1] {
            Value::UInt(v) => *v as i64,
            Value::Int(v) => *v,
            other => panic!("Time_zone_id not integral: {other:?}"),
        };
        assert_eq!(
            expected.get(&name),
            Some(&id),
            "time_zone_name id for {name} must match the catalog"
        );
        if name == "UTC" {
            seen_utc = true;
        }
    }
    assert!(
        seen_utc,
        "host zoneinfo is expected to expose a UTC zone entry"
    );
}

#[test]
fn time_zone_transition_type_has_utc_offset_zero() {
    let catalog = crate::engine::tz::zone_catalog();
    let Some(utc) = catalog.zones.iter().find(|z| z.name == "UTC") else {
        return; // no zoneinfo / no UTC entry on this host
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let type_rows = rows(
        &engine,
        &format!(
            "SELECT Transition_type_id, Offset, Is_DST, Abbreviation \
             FROM mysql.time_zone_transition_type WHERE Time_zone_id = {}",
            utc.id
        ),
    );
    assert!(
        !type_rows.is_empty(),
        "UTC must have at least one local-time type"
    );
    // Every UTC local-time type is a zero-offset, non-DST type.
    for row in &type_rows {
        let offset = match &row[1] {
            Value::Int(v) => *v,
            Value::UInt(v) => *v as i64,
            other => panic!("Offset not integral: {other:?}"),
        };
        assert_eq!(offset, 0, "UTC offset must be 0");
        let is_dst = match &row[2] {
            Value::UInt(v) => *v,
            Value::Int(v) => *v as u64,
            other => panic!("Is_DST not integral: {other:?}"),
        };
        assert_eq!(is_dst, 0, "UTC is never DST");
    }
}

#[test]
fn time_zone_transition_type_matches_parsed_dst_zone() {
    // Europe/Berlin exercises the ttinfo decode (CET/CEST offsets + DST flag +
    // abbreviations) end-to-end through the SQL surface.
    let catalog = crate::engine::tz::zone_catalog();
    let Some(berlin) = catalog.zones.iter().find(|z| z.name == "Europe/Berlin") else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let type_rows = rows(
        &engine,
        &format!(
            "SELECT Offset, Is_DST, Abbreviation \
             FROM mysql.time_zone_transition_type WHERE Time_zone_id = {}",
            berlin.id
        ),
    );
    // Standard CET (+3600, non-DST) and summer CEST (+7200, DST) must both be
    // present with their abbreviations.
    let has_cet = type_rows.iter().any(|r| {
        matches!(&r[0], Value::Int(3600))
            && matches!(&r[1], Value::UInt(0) | Value::Int(0))
            && cell_string(&r[2]) == "CET"
    });
    let has_cest = type_rows.iter().any(|r| {
        matches!(&r[0], Value::Int(7200))
            && matches!(&r[1], Value::UInt(1) | Value::Int(1))
            && cell_string(&r[2]) == "CEST"
    });
    assert!(
        has_cet,
        "Europe/Berlin must have a CET (+01:00) type: {type_rows:?}"
    );
    assert!(
        has_cest,
        "Europe/Berlin must have a CEST (+02:00, DST) type: {type_rows:?}"
    );

    // Transition rows for this zone must reference only valid type ids.
    let type_count = type_rows.len() as i64;
    let trans_rows = rows(
        &engine,
        &format!(
            "SELECT Transition_time, Transition_type_id \
             FROM mysql.time_zone_transition WHERE Time_zone_id = {}",
            berlin.id
        ),
    );
    assert!(!trans_rows.is_empty(), "Berlin has DST transitions");
    for row in &trans_rows {
        let tid = match &row[1] {
            Value::UInt(v) => *v as i64,
            Value::Int(v) => *v,
            other => panic!("Transition_type_id not integral: {other:?}"),
        };
        assert!(
            tid >= 0 && tid < type_count,
            "transition references type id {tid} out of range 0..{type_count}"
        );
    }
}

#[test]
fn time_zone_leap_second_is_empty() {
    // mydb reads leap-free TZif variants (Use_leap_seconds = 'N'), so the leap
    // table is honestly empty rather than stubbed.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let leap = rows(
        &engine,
        "SELECT Transition_time, Correction FROM mysql.time_zone_leap_second",
    );
    assert!(leap.is_empty(), "leap-second table must be empty");

    let catalog = crate::engine::tz::zone_catalog();
    if !catalog.zones.is_empty() {
        let use_leap = rows(&engine, "SELECT Use_leap_seconds FROM mysql.time_zone");
        assert!(
            use_leap.iter().all(|r| cell_string(&r[0]) == "N"),
            "every zone must report Use_leap_seconds = 'N'"
        );
    }
}

// ---------------------------------------------------------------------------
// performance_schema.setup_consumers defaults (§4)
// ---------------------------------------------------------------------------

fn consumer_enabled(engine: &StorageQueryEngine, name: &str) -> String {
    let rows = rows(
        engine,
        &format!("SELECT ENABLED FROM performance_schema.setup_consumers WHERE NAME = '{name}'"),
    );
    assert_eq!(rows.len(), 1, "consumer {name} must exist exactly once");
    cell_string(&rows[0][0])
}

#[test]
fn setup_consumers_defaults_align_with_mysql_80() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // MySQL 8.0 factory defaults.
    assert_eq!(consumer_enabled(&engine, "statements_digest"), "YES");
    assert_eq!(
        consumer_enabled(&engine, "events_statements_current"),
        "YES"
    );
    assert_eq!(
        consumer_enabled(&engine, "events_statements_history"),
        "YES"
    );
    assert_eq!(consumer_enabled(&engine, "global_instrumentation"), "YES");
    assert_eq!(consumer_enabled(&engine, "thread_instrumentation"), "YES");
    // events_transactions_current is produced live from the active-transaction
    // snapshot (M6), so it defaults ON, matching MySQL 8.0.
    assert_eq!(
        consumer_enabled(&engine, "events_transactions_current"),
        "YES"
    );
    // History-long remains off; per-thread transaction history is enabled by
    // the 8.0.46 factory default.
    assert_eq!(
        consumer_enabled(&engine, "events_statements_history_long"),
        "NO"
    );
    assert_eq!(
        consumer_enabled(&engine, "events_transactions_history"),
        "YES"
    );
    assert_eq!(consumer_enabled(&engine, "events_waits_current"), "NO");

    // Fast-path mirrors must agree with the rows.
    assert!(crate::engine::perf_setup::statements_digest_enabled());
}

#[test]
fn statements_digest_collects_without_explicit_enable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE digest_probe (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO digest_probe VALUES (1, 10), (2, 20)")
        .unwrap();
    // A read that is easy to find back in the digest table.
    let _ = rows(&engine, "SELECT v FROM digest_probe WHERE id = 1");

    let digest = rows(
        &engine,
        "SELECT DIGEST_TEXT, COUNT_STAR FROM \
         performance_schema.events_statements_summary_by_digest",
    );
    assert!(
        !digest.is_empty(),
        "statements_digest is ON by default, so the digest view must be non-empty \
         after running statements"
    );
}

// ---------------------------------------------------------------------------
// information_schema.innodb_* catalog/tablespace/buffer views (Phase 2.3–2.7)
// ---------------------------------------------------------------------------

fn i64_of(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        other => panic!("expected integral, got {other:?}"),
    }
}

fn setup_innodb_schema(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE parent (id INT PRIMARY KEY, code VARCHAR(16), UNIQUE KEY uk_code (code))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (\
               id INT PRIMARY KEY, \
               parent_id INT, \
               qty INT, \
               total INT AS (qty * 2) VIRTUAL, \
               KEY idx_parent (parent_id), \
               CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE\
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1,'a'),(2,'b')")
        .unwrap();
    engine
        .execute("INSERT INTO child (id, parent_id, qty) VALUES (10,1,5),(11,2,7)")
        .unwrap();
    engine.execute("ANALYZE TABLE parent").unwrap();
    engine.execute("ANALYZE TABLE child").unwrap();
}

#[test]
fn innodb_tables_and_tablespaces_map_user_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    // innodb_tables: one row per user table, NAME = db/table, SPACE = TABLE_ID.
    let tbl = rows(
        &engine,
        "SELECT TABLE_ID, NAME, N_COLS, SPACE, ROW_FORMAT, SPACE_TYPE \
         FROM information_schema.innodb_tables WHERE NAME = 'test/child'",
    );
    assert_eq!(tbl.len(), 1, "child must appear once in innodb_tables");
    let row = &tbl[0];
    assert_eq!(i64_of(&row[0]), i64_of(&row[3]), "SPACE id equals TABLE_ID");
    assert_eq!(cell_string(&row[1]), "test/child");
    assert_eq!(i64_of(&row[2]), 4, "child has 4 visible columns");
    assert_eq!(cell_string(&row[4]), "Dynamic");
    assert_eq!(cell_string(&row[5]), "Single");

    // INNODB_TABLESPACES uses the numeric physical SPACE id, while
    // INNODB_DATAFILES.SPACE is the same numeric ID encoded as varbinary.
    let ts = rows(
        &engine,
        "SELECT SPACE, NAME, PAGE_SIZE, FILE_SIZE FROM information_schema.innodb_tablespaces \
         WHERE NAME = 'test/child'",
    );
    assert_eq!(ts.len(), 1);
    assert_eq!(i64_of(&ts[0][2]), 16384, "mydb page size is 16 KiB");

    let df = rows(
        &engine,
        &format!(
            "SELECT SPACE, PATH FROM information_schema.innodb_datafiles WHERE SPACE = '{}'",
            cell_string(&ts[0][0])
        ),
    );
    assert_eq!(df.len(), 1);
    assert_eq!(cell_string(&df[0][0]), cell_string(&ts[0][0]));
    let datafile = cell_string(&df[0][1]).replace('\\', "/");
    let storage_id = i64_of(&ts[0][0]);
    assert!(
        datafile.contains("/objects/tables/")
            && datafile.ends_with(&format!("/{storage_id}/1.heap")),
        "datafile path must use the stable storage id: {datafile}"
    );
}

#[test]
fn innodb_columns_reflect_catalog_positions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    let cols = rows(
        &engine,
        "SELECT c.NAME, c.POS FROM information_schema.innodb_columns c \
         JOIN information_schema.innodb_tables t ON t.TABLE_ID = c.TABLE_ID \
         WHERE t.NAME = 'test/child' ORDER BY c.POS",
    );
    let names: Vec<String> = cols.iter().map(|r| cell_string(&r[0])).collect();
    assert_eq!(names, vec!["id", "parent_id", "qty", "total"]);
    // POS is 0-based and contiguous.
    for (expect, row) in cols.iter().enumerate() {
        assert_eq!(i64_of(&row[1]), expect as i64);
    }
}

#[test]
fn innodb_indexes_and_fields_join_consistently() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    // Every field references an index that exists, and the primary key index
    // carries the clustered TYPE bit.
    let idx = rows(
        &engine,
        "SELECT i.NAME, i.TYPE, i.N_FIELDS, i.INDEX_ID \
         FROM information_schema.innodb_indexes i \
         JOIN information_schema.innodb_tables t ON t.TABLE_ID = i.TABLE_ID \
         WHERE t.NAME = 'test/child' ORDER BY i.NAME",
    );
    assert!(!idx.is_empty(), "child has indexes");
    let primary = idx
        .iter()
        .find(|r| cell_string(&r[0]).eq_ignore_ascii_case("PRIMARY"))
        .expect("PRIMARY index present");
    assert_eq!(i64_of(&primary[1]) & 1, 1, "PRIMARY carries clustered bit");

    // innodb_fields join: field rows for the PRIMARY index name its key column.
    let pk_id = i64_of(&primary[3]);
    let fields = rows(
        &engine,
        &format!(
            "SELECT NAME, POS FROM information_schema.innodb_fields WHERE INDEX_ID = {pk_id} ORDER BY POS"
        ),
    );
    assert_eq!(fields.len(), 1);
    assert_eq!(cell_string(&fields[0][0]), "id");
}

#[test]
fn innodb_foreign_and_virtual_reflect_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    let fk = rows(
        &engine,
        "SELECT ID, FOR_NAME, REF_NAME, N_COLS, TYPE FROM information_schema.innodb_foreign \
         WHERE FOR_NAME = 'test/child'",
    );
    assert_eq!(fk.len(), 1, "one FK on child");
    assert_eq!(cell_string(&fk[0][2]), "test/parent");
    assert_eq!(i64_of(&fk[0][3]), 1);
    assert_eq!(i64_of(&fk[0][4]) & 1, 1, "ON DELETE CASCADE sets bit 1");

    let fk_id = cell_string(&fk[0][0]);
    let fkc = rows(
        &engine,
        &format!(
            "SELECT FOR_COL_NAME, REF_COL_NAME FROM information_schema.innodb_foreign_cols \
             WHERE ID = '{fk_id}'"
        ),
    );
    assert_eq!(fkc.len(), 1);
    assert_eq!(cell_string(&fkc[0][0]), "parent_id");
    assert_eq!(cell_string(&fkc[0][1]), "id");

    // innodb_virtual: the VIRTUAL column `total` maps to its base column `qty`.
    let virt = rows(
        &engine,
        "SELECT v.POS, v.BASE_POS FROM information_schema.innodb_virtual v \
         JOIN information_schema.innodb_tables t ON t.TABLE_ID = v.TABLE_ID \
         WHERE t.NAME = 'test/child'",
    );
    assert!(!virt.is_empty(), "child has a virtual column");
    // total is POS 3; qty is POS 2.
    assert!(virt
        .iter()
        .any(|r| i64_of(&r[0]) == 3 && i64_of(&r[1]) == 2));
}

#[test]
fn innodb_tablestats_reports_row_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    let stats = rows(
        &engine,
        "SELECT NUM_ROWS FROM information_schema.innodb_tablestats WHERE NAME = 'test/parent'",
    );
    assert_eq!(stats.len(), 1);
    assert_eq!(i64_of(&stats[0][0]), 2, "parent has 2 rows after ANALYZE");
}

#[test]
fn innodb_buffer_pool_stats_and_metrics_are_live() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    let bp = rows(
        &engine,
        "SELECT POOL_ID, POOL_SIZE, DATABASE_PAGES FROM information_schema.innodb_buffer_pool_stats",
    );
    assert_eq!(bp.len(), 1, "single buffer pool → one row");
    assert!(i64_of(&bp[0][1]) > 0, "POOL_SIZE must be positive");

    let metrics = rows(
        &engine,
        "SELECT NAME, COUNT, STATUS FROM information_schema.innodb_metrics WHERE NAME='buffer_pool_reads'",
    );
    assert_eq!(metrics.len(), 1);
    assert_eq!(cell_string(&metrics[0][2]), "enabled");
}

#[test]
fn innodb_ft_default_stopword_lists_builtin_words() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sw = rows(
        &engine,
        "SELECT value FROM information_schema.innodb_ft_default_stopword",
    );
    assert!(sw.len() >= 30, "the built-in stopword list is non-trivial");
    assert!(
        sw.iter().any(|r| cell_string(&r[0]) == "the"),
        "'the' is a default stopword"
    );
}

#[test]
fn files_view_maps_tables_to_tablespaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    let files = rows(
        &engine,
        "SELECT FILE_TYPE, TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS FROM information_schema.files \
         WHERE TABLE_NAME = 'parent'",
    );
    assert_eq!(files.len(), 1);
    assert_eq!(cell_string(&files[0][0]), "TABLESPACE");
    assert_eq!(cell_string(&files[0][1]), "test");
    assert_eq!(i64_of(&files[0][3]), 2);
}

#[test]
fn populated_innodb_producers_never_return_null_for_not_null_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_innodb_schema(&engine);

    for table_name in [
        "INNODB_BUFFER_PAGE",
        "INNODB_BUFFER_PAGE_LRU",
        "INNODB_BUFFER_POOL_STATS",
        "INNODB_CACHED_INDEXES",
        "INNODB_COLUMNS",
        "INNODB_DATAFILES",
        "INNODB_FIELDS",
        "INNODB_FOREIGN",
        "INNODB_FOREIGN_COLS",
        "INNODB_INDEXES",
        "INNODB_METRICS",
        "INNODB_TABLES",
        "INNODB_TABLESPACES",
        "INNODB_TABLESPACES_BRIEF",
        "INNODB_TABLESTATS",
        "INNODB_VIRTUAL",
        "FILES",
    ] {
        let def =
            crate::engine::information_schema::find_system_table("information_schema", table_name)
                .unwrap();
        let result = rows(
            &engine,
            &format!("SELECT * FROM information_schema.{table_name}"),
        );
        for (row_index, row) in result.iter().enumerate() {
            assert_eq!(row.len(), def.columns.len(), "{table_name} row width");
            for (column_index, (value, column)) in row.iter().zip(def.columns).enumerate() {
                assert!(
                    column.nullable || !matches!(value, Value::Null),
                    "{table_name}.{} returned NULL at row {row_index}, column {column_index}",
                    column.name
                );
            }
        }
    }
}

#[test]
fn every_information_schema_registration_obeys_row_contract() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let definitions = crate::engine::information_schema::system_table_definitions()
        .filter(|definition| definition.schema == "information_schema")
        .collect::<Vec<_>>();
    assert_eq!(definitions.len(), 103);

    for definition in definitions {
        let result = rows(
            &engine,
            &format!("SELECT * FROM information_schema.`{}`", definition.name),
        );
        for (row_index, row) in result.iter().enumerate() {
            assert_eq!(
                row.len(),
                definition.columns.len(),
                "information_schema.{} row {row_index} width",
                definition.name
            );
            for (column_index, (value, column)) in row.iter().zip(definition.columns).enumerate() {
                assert!(
                    column.nullable || !matches!(value, Value::Null),
                    "information_schema.{}.{} returned NULL at row {row_index}, column {column_index}",
                    definition.name,
                    column.name
                );
            }
        }
    }
}

#[test]
fn columns_metadata_uses_column_charset_srid_and_leading_index_rules() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE metadata_probe (\
               a INT, b INT, latin VARCHAR(10) CHARACTER SET latin1,\
               geom GEOMETRY NOT NULL SRID 4326, KEY ab (a,b))",
        )
        .unwrap();
    let metadata = rows(
        &engine,
        "SELECT COLUMN_NAME, CHARACTER_OCTET_LENGTH, SRS_ID, COLUMN_KEY \
         FROM information_schema.columns \
         WHERE TABLE_SCHEMA='test' AND TABLE_NAME='metadata_probe' \
         ORDER BY ORDINAL_POSITION",
    );
    assert_eq!(metadata[0][3], Value::String("MUL".into()));
    assert_eq!(metadata[1][3], Value::String(String::new()));
    assert_eq!(metadata[2][1], Value::Int(10));
    assert_eq!(metadata[3][2], Value::Int(4326));
}

#[test]
fn temporal_history_no_index_row_keeps_columns_aligned() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_plain (v INT)")
        .unwrap();
    let result = rows(
        &engine,
        "SELECT INDEX_NAME, SYSTEM_TIME_EPOCH_US, LAST_COMPACTION_UNIX_US \
         FROM information_schema.mydb_temporal_history \
         WHERE TABLE_SCHEMA='test' AND TABLE_NAME='temporal_plain'",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Value::Null);
    assert!(matches!(result[0][1], Value::Int(_)));
    assert!(matches!(result[0][2], Value::Null | Value::Int(_)));
}

#[test]
fn buffer_temp_and_fulltext_producers_report_real_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE temp_probe (id INT PRIMARY KEY, v VARCHAR(8))",
        )
        .unwrap();
    let temp = match engine
        .execute_with_session(
            &mut session,
            "SELECT NAME, N_COLS FROM information_schema.INNODB_TEMP_TABLE_INFO",
        )
        .unwrap()
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    };
    assert!(temp
        .iter()
        .any(|row| { row[0] == Value::String("temp_probe".into()) && row[1] == Value::Int(2) }));

    engine
        .execute(
            "CREATE TABLE ft_probe (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft_body(body))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO ft_probe VALUES (1,'database database tuning'),(2,'database systems')",
        )
        .unwrap();
    engine
        .execute("SET GLOBAL innodb_ft_aux_table='test/ft_probe'")
        .unwrap();
    assert!(!rows(
        &engine,
        "SELECT `KEY`, `VALUE` FROM information_schema.INNODB_FT_CONFIG",
    )
    .is_empty());
    let postings = rows(
        &engine,
        "SELECT WORD, DOC_COUNT, DOC_ID, POSITION \
         FROM information_schema.INNODB_FT_INDEX_TABLE WHERE WORD='database'",
    );
    assert!(!postings.is_empty());
    engine.execute("SET GLOBAL innodb_ft_aux_table=''").unwrap();

    let buffer_pages = rows(
        &engine,
        "SELECT SPACE, PAGE_NUMBER, PAGE_TYPE FROM information_schema.INNODB_BUFFER_PAGE",
    );
    assert!(!buffer_pages.is_empty());
    let cached_indexes = rows(
        &engine,
        "SELECT SPACE_ID, INDEX_ID, N_CACHED_PAGES \
         FROM information_schema.INNODB_CACHED_INDEXES",
    );
    let resident_index_pages = buffer_pages
        .iter()
        .filter(|row| row[2] == Value::String("INDEX".into()))
        .count() as i64;
    let reported_index_pages = cached_indexes
        .iter()
        .map(|row| i64_of(&row[2]))
        .sum::<i64>();
    assert_eq!(reported_index_pages, resident_index_pages);
}

#[test]
fn partitioned_innodb_views_share_physical_leaf_identity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE physical_p (id INT PRIMARY KEY, v INT, KEY v_idx(v)) \
             PARTITION BY HASH(id) PARTITIONS 2",
        )
        .unwrap();
    engine
        .execute("INSERT INTO physical_p VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();

    let tables = rows(
        &engine,
        "SELECT TABLE_ID, NAME, SPACE FROM information_schema.INNODB_TABLES \
         WHERE NAME LIKE 'test/physical_p#p#%' ORDER BY NAME",
    );
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().all(|row| i64_of(&row[0]) == i64_of(&row[2])));

    let joined = rows(
        &engine,
        "SELECT t.NAME, ts.NAME, d.SPACE, t.SPACE \
         FROM information_schema.INNODB_TABLES t \
         JOIN information_schema.INNODB_TABLESPACES ts ON ts.SPACE=t.SPACE \
         JOIN information_schema.INNODB_DATAFILES d ON d.SPACE=t.SPACE \
         WHERE t.NAME LIKE 'test/physical_p#p#%' ORDER BY t.NAME",
    );
    assert_eq!(joined.len(), 2);
    assert!(joined
        .iter()
        .all(|row| row[0] == row[1] && row[2].to_display_string() == row[3].to_display_string()));

    let indexes = rows(
        &engine,
        "SELECT t.NAME, COUNT(*) \
         FROM information_schema.INNODB_TABLES t \
         JOIN information_schema.INNODB_INDEXES i ON i.TABLE_ID=t.TABLE_ID \
         WHERE t.NAME LIKE 'test/physical_p#p#%' GROUP BY t.NAME ORDER BY t.NAME",
    );
    assert_eq!(indexes.len(), 2);
    assert!(indexes.iter().all(|row| i64_of(&row[1]) == 2));

    let files = rows(
        &engine,
        "SELECT FILE_ID, TABLESPACE_NAME FROM information_schema.FILES \
         WHERE TABLE_NAME='physical_p' ORDER BY TABLESPACE_NAME",
    );
    assert_eq!(files.len(), 2);
    assert!(files.iter().all(|row| cell_string(&row[1]).contains("#p#")));
}

#[test]
fn audit_index_metadata_and_grant_validation() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE audit_idx (a INT, b INT, c INT, UNIQUE KEY ab(a,b), UNIQUE KEY uc(c))",
        )
        .unwrap();
    let keys = rows(&engine, "SELECT COLUMN_KEY FROM information_schema.columns WHERE table_schema='test' AND table_name='audit_idx' ORDER BY ordinal_position");
    assert_eq!(
        keys,
        vec![
            vec![Value::String("MUL".into())],
            vec![Value::String("".into())],
            vec![Value::String("UNI".into())]
        ]
    );
    let show = rows(&engine, "SHOW COLUMNS FROM audit_idx");
    assert_eq!(
        show.iter().map(|r| r[3].clone()).collect::<Vec<_>>(),
        keys.iter().map(|r| r[0].clone()).collect::<Vec<_>>()
    );
    engine.execute("CREATE USER audit_reader").unwrap();
    assert!(engine
        .execute("GRANT SELECT (*) ON test.audit_idx TO audit_reader")
        .is_err());
    assert!(engine
        .execute("GRANT SELECT () ON test.audit_idx TO audit_reader")
        .is_err());
    assert_eq!(
        engine
            .execute("GRANT SELECT (missing) ON test.audit_idx TO audit_reader")
            .unwrap_err()
            .code(),
        1054
    );
    engine
        .execute("GRANT SELECT (A) ON test.audit_idx TO audit_reader")
        .unwrap();
    engine
        .execute("GRANT SELECT (a) ON test.audit_idx TO audit_reader")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM mysql.columns_priv WHERE User='audit_reader'"
        )[0][0],
        Value::Int(1)
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT Column_priv FROM mysql.tables_priv WHERE User='audit_reader'"
        )[0][0],
        Value::String("Select".into())
    );
}

#[test]
fn audit_histogram_scope_values_drop_and_auto_analyze() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE audit_hist (id INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_hist VALUES (1,10),(2,20),(3,NULL),(4,10)")
        .unwrap();
    engine.execute("ANALYZE TABLE audit_hist").unwrap();
    let query = "SELECT COLUMN_NAME,HISTOGRAM FROM information_schema.column_statistics WHERE SCHEMA_NAME='test' AND TABLE_NAME='audit_hist'";
    assert!(rows(&engine, query).is_empty());
    engine
        .execute("ANALYZE TABLE audit_hist UPDATE HISTOGRAM ON V WITH 5 BUCKETS")
        .unwrap();
    let result = rows(&engine, query);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0].to_display_string(), "v");
    let histogram: serde_json::Value =
        serde_json::from_str(&result[0][1].to_display_string()).unwrap();
    assert_eq!(
        histogram["buckets"],
        serde_json::json!([[10, 0.5], [20, 0.75]])
    );
    assert_eq!(histogram["null-values"], 0.25);
    assert_eq!(histogram["number-of-buckets-specified"], 5);
    assert_eq!(histogram["data-type"], "int");
    assert!(histogram["last-updated"].is_string());
    engine.execute("ANALYZE TABLE audit_hist").unwrap();
    assert_eq!(rows(&engine, query), result);
    engine
        .execute("ANALYZE TABLE audit_hist DROP HISTOGRAM ON v")
        .unwrap();
    assert!(rows(&engine, query).is_empty());
}

#[test]
fn audit_system_metadata_visibility_and_time_zone() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE USER audit_nop").unwrap();
    let mut reader = SessionContext::default();
    reader.user = "audit_nop".into();
    reader.login_user = "audit_nop".into();
    reader.authenticated_as = crate::engine::auth::AccountKey::new("audit_nop", "%");
    for sql in [
        "SELECT * FROM information_schema.views WHERE TABLE_SCHEMA='sys'",
        "SELECT * FROM information_schema.triggers WHERE TRIGGER_SCHEMA='sys'",
    ] {
        let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut reader, sql).unwrap()
        else {
            panic!()
        };
        assert!(rs.rows.is_empty(), "{sql}");
    }
    assert_eq!(
        engine
            .execute_with_session(&mut reader, "SELECT * FROM information_schema.innodb_trx")
            .unwrap_err()
            .code(),
        1227
    );
    engine.execute("CREATE TABLE audit_time (id INT)").unwrap();
    let mut root = SessionContext::default();
    let mut times = Vec::new();
    for zone in ["+00:00", "+08:00"] {
        engine
            .execute_with_session(&mut root, &format!("SET time_zone='{zone}'"))
            .unwrap();
        let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut root, "SELECT CREATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME='audit_time'").unwrap() else { panic!() };
        times.push(
            chrono::NaiveDateTime::parse_from_str(
                &rs.rows[0][0].to_display_string(),
                "%Y-%m-%d %H:%M:%S",
            )
            .unwrap(),
        );
    }
    assert_eq!((times[1] - times[0]).num_hours(), 8);
}

#[test]
fn audit_numeric_binary_hash_join_matches_scalar_comparison() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_n (n INT)").unwrap();
    engine
        .execute("CREATE TABLE audit_s (s VARBINARY(20))")
        .unwrap();
    engine
        .execute("INSERT INTO audit_n VALUES (1),(2),(NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_s VALUES ('1'),('01'),('2.0'),('3'),(NULL)")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM audit_n n JOIN audit_s s ON n.n=s.s"
        )[0][0],
        Value::Int(3)
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM audit_s s LEFT JOIN audit_n n ON s.s=n.n"
        )[0][0],
        Value::Int(5)
    );
}

#[test]
fn audit_histogram_empty_null_and_sampled_tables() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_sample (v INT)").unwrap();
    let query = "SELECT HISTOGRAM FROM information_schema.column_statistics WHERE TABLE_NAME='audit_sample' AND COLUMN_NAME='v'";
    let histogram = || -> serde_json::Value {
        serde_json::from_str(&rows(&engine, query)[0][0].to_display_string()).unwrap()
    };
    engine
        .execute("ANALYZE TABLE audit_sample UPDATE HISTOGRAM ON v WITH 3 BUCKETS")
        .unwrap();
    assert_eq!(histogram()["buckets"], serde_json::json!([]));
    engine
        .execute("INSERT INTO audit_sample VALUES (NULL),(NULL)")
        .unwrap();
    engine
        .execute("ANALYZE TABLE audit_sample UPDATE HISTOGRAM ON v WITH 3 BUCKETS")
        .unwrap();
    assert_eq!(histogram()["null-values"], 1.0);
    let values = (0..10000)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO audit_sample VALUES {values}"))
        .unwrap();
    engine
        .execute("ANALYZE TABLE audit_sample UPDATE HISTOGRAM ON v WITH 3 BUCKETS")
        .unwrap();
    let hist = histogram();
    assert_eq!(hist["buckets"].as_array().unwrap().len(), 3);
    assert!((hist["sampling-rate"].as_f64().unwrap() - 8192.0 / 10002.0).abs() < 1e-9);
    assert_eq!(hist["histogram-type"], "equi-height");
    engine
        .execute("ALTER TABLE audit_sample ADD COLUMN other INT")
        .unwrap();
    engine
        .execute("ALTER TABLE audit_sample DROP COLUMN v")
        .unwrap();
    assert!(rows(&engine, query).is_empty());
}

#[test]
fn audit_histogram_large_object_survives_reopen_without_catalog_trimming() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine
        .execute("CREATE TABLE audit_bighist (id INT, v LONGTEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_bighist VALUES (1, REPEAT('x',600000))")
        .unwrap();
    engine
        .execute("ANALYZE TABLE audit_bighist UPDATE HISTOGRAM ON id")
        .unwrap();
    engine
        .execute("ANALYZE TABLE audit_bighist UPDATE HISTOGRAM ON v")
        .unwrap();
    let query="SELECT COLUMN_NAME FROM information_schema.column_statistics WHERE TABLE_NAME='audit_bighist' ORDER BY COLUMN_NAME";
    let expected = vec![
        vec![Value::String("id".into())],
        vec![Value::String("v".into())],
    ];
    assert_eq!(rows(&engine, query), expected);
    drop(engine);
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(rows(&engine, query), expected);
    let histogram = &engine
        .database()
        .table("test.audit_bighist")
        .unwrap()
        .stats
        .explicit_histograms["v"];
    assert_eq!(histogram.buckets.len(), 1);
    assert_eq!(histogram.buckets[0].upper.to_display_string().len(), 600000);
}
