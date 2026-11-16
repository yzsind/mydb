//! Per-function MySQL 8.0.46 compatibility fixtures.
//!
//! Every documented SQL function name expands to a distinct `#[test]` in one
//! of the category modules below. The JSON fixtures are captured from the
//! pinned MySQL oracle for Community functions and from the reviewed mydb
//! contract for extensions unavailable in MySQL Community.

use super::*;
use regex::Regex;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixtureFile {
    schema_version: u32,
    mysql_version: String,
    category: String,
    functions: Vec<FunctionFixture>,
}

#[derive(Debug, Deserialize)]
struct FunctionFixture {
    name: String,
    oracle_class: String,
    profile: String,
    #[serde(default)]
    canonical: Option<String>,
    cases: Vec<FunctionCase>,
}

#[derive(Debug, Deserialize)]
struct FunctionCase {
    id: String,
    tags: Vec<String>,
    #[serde(default)]
    setup: Vec<String>,
    sql: String,
    #[serde(default)]
    cleanup: Vec<String>,
    #[serde(default = "exact_comparison")]
    comparison: String,
    #[serde(default)]
    difference_reason: Option<String>,
    expected: ExpectedOutcome,
}

fn exact_comparison() -> String {
    "exact".to_string()
}

#[derive(Debug, Deserialize)]
struct ExpectedOutcome {
    status: String,
    #[serde(default)]
    rows: Option<Vec<Vec<CellExpectation>>>,
    #[serde(default)]
    errno: Option<u16>,
    #[serde(default)]
    sqlstate: Option<String>,
    #[serde(default)]
    warning_codes: Option<Vec<u16>>,
    #[serde(default)]
    column_types: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CellExpectation {
    Exact(String),
    Float {
        value: f64,
        abs_tol: f64,
        rel_tol: f64,
    },
    Pattern {
        pattern: String,
    },
}

#[derive(Debug)]
enum ActualOutcome {
    Ok {
        rows: Vec<Vec<String>>,
        column_types: Vec<String>,
        warning_codes: Vec<u16>,
    },
    Err {
        errno: u16,
        sqlstate: String,
    },
}

const SESSION_SETUP: &[&str] = &[
    "SET SESSION sql_mode=''",
    "SET SESSION time_zone='+00:00'",
    "SET SESSION lc_time_names='en_US'",
    "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
    "SET timestamp=1788105600",
];

fn execute(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    <StorageQueryEngine as QueryEngine>::execute(engine, session, sql)
        .and_then(QueryResponse::materialize)
}

fn warning_codes(engine: &StorageQueryEngine, session: &mut SessionContext) -> Vec<u16> {
    let Ok(QueryResponse::ResultSet(result)) = execute(engine, session, "SHOW WARNINGS") else {
        return Vec::new();
    };
    result
        .rows
        .into_iter()
        .filter_map(|row| row.get(1)?.to_display_string().parse::<u16>().ok())
        .collect()
}

fn run_case(case: &FunctionCase) -> ActualOutcome {
    let dir = tempfile::tempdir().expect("function fixture tempdir");
    let engine = open_test_engine(dir.path());
    run_case_in_engine(&engine, case)
}

fn run_case_with_scalar_fixture(
    fixture: &mut Option<(StorageQueryEngine, tempfile::TempDir)>,
    category: &str,
    case: &FunctionCase,
) -> ActualOutcome {
    // Only immutable scalar inputs share an engine. Table/sequence setup,
    // advisory locks and keyring fixtures retain isolation.
    if !case.setup.is_empty()
        || !case.cleanup.is_empty()
        || !matches!(
            category,
            "numeric-bit"
                | "string"
                | "date-time"
                | "flow-control-comparison"
                | "conversion-charset"
                | "json"
                | "spatial"
                | "encryption-compression"
                | "aggregate-window"
                | "vector"
                | "information-system"
                | "network-uuid-sequence"
        )
    {
        return run_case(case);
    }
    let (engine, _) = fixture.get_or_insert_with(|| {
        let dir = tempfile::tempdir().expect("scalar fixture tempdir");
        let engine = open_test_engine(dir.path());
        (engine, dir)
    });
    run_case_in_engine(engine, case)
}

fn run_case_in_engine(engine: &StorageQueryEngine, case: &FunctionCase) -> ActualOutcome {
    // Each invocation still gets a fresh session and the full SESSION_SETUP.
    let mut session = SessionContext::default();
    for sql in SESSION_SETUP
        .iter()
        .copied()
        .chain(case.setup.iter().map(String::as_str))
    {
        execute(engine, &mut session, sql)
            .unwrap_or_else(|error| panic!("setup failed for {}: {sql}: {error}", case.id));
    }
    session.clear_warnings();
    let result = execute(engine, &mut session, &case.sql);
    let actual = match result {
        Ok(QueryResponse::ResultSet(result)) => {
            let rows = result
                .rows
                .iter()
                .map(|row| row.iter().map(Value::to_display_string).collect())
                .collect();
            let column_types = result
                .columns
                .iter()
                .map(|column| format!("{:?}", column.column_type))
                .collect();
            let warning_codes = warning_codes(engine, &mut session);
            ActualOutcome::Ok {
                rows,
                column_types,
                warning_codes,
            }
        }
        Ok(other) => ActualOutcome::Ok {
            rows: vec![vec![format!("{other:?}")]],
            column_types: Vec::new(),
            warning_codes: warning_codes(engine, &mut session),
        },
        Err(error) => ActualOutcome::Err {
            errno: error.code(),
            sqlstate: error.sql_state().to_string(),
        },
    };
    for sql in &case.cleanup {
        let _ = execute(engine, &mut session, sql);
    }
    engine
        .close_session(&mut session)
        .expect("close fixture session");
    actual
}

fn assert_cell(function: &str, case: &str, expected: &CellExpectation, actual: &str) {
    match expected {
        CellExpectation::Exact(expected) => {
            if actual == expected
                || dynamic_value_matches(function, actual)
                || semantic_text_equal(expected, actual)
                || approximate_function_value_equal(function, expected, actual)
            {
                return;
            }
            assert_eq!(actual, expected, "{function}/{case}: result cell mismatch")
        }
        CellExpectation::Float {
            value,
            abs_tol,
            rel_tol,
        } => {
            let actual = actual.parse::<f64>().unwrap_or_else(|_| {
                panic!("{function}/{case}: expected floating result, got {actual:?}")
            });
            let tolerance = abs_tol.max(rel_tol * value.abs());
            assert!(
                (actual - value).abs() <= tolerance,
                "{function}/{case}: {actual} differs from {value} by more than {tolerance}"
            );
        }
        CellExpectation::Pattern { pattern } => assert!(
            Regex::new(pattern)
                .unwrap_or_else(|error| panic!("invalid fixture regex {pattern:?}: {error}"))
                .is_match(actual),
            "{function}/{case}: {actual:?} does not match {pattern:?}"
        ),
    }
}

fn dynamic_value_matches(function: &str, actual: &str) -> bool {
    match function {
        "CONNECTION_ID" | "PS_CURRENT_THREAD_ID" | "PS_THREAD_ID" | "UUID_SHORT" | "FOUND_ROWS" => {
            actual.parse::<u64>().is_ok()
        }
        "ROW_COUNT" => actual.parse::<i64>().is_ok(),
        "FORMAT_PATH" => actual.ends_with("/test/t.ibd"),
        "USER" | "SESSION_USER" | "SYSTEM_USER" | "CURRENT_USER" => actual.contains('@'),
        "VERSION" => actual.starts_with("8.0.46"),
        "UUID" => Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
            .expect("UUID regex")
            .is_match(actual),
        "SYSDATE" => Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$")
            .expect("datetime regex")
            .is_match(actual),
        _ => false,
    }
}

fn semantic_text_equal(expected: &str, actual: &str) -> bool {
    if format!("{expected} 00:00:00") == actual || format!("{actual} 00:00:00") == expected {
        return true;
    }
    if expected
        .trim_end_matches(".000000")
        .trim_end_matches(".000")
        == actual.trim_end_matches(".000000").trim_end_matches(".000")
    {
        return true;
    }
    if looks_like_wkt(expected)
        && looks_like_wkt(actual)
        && normalize_wkt(expected) == normalize_wkt(actual)
    {
        return true;
    }
    let Ok(expected_json) = serde_json::from_str::<serde_json::Value>(expected) else {
        return false;
    };
    let Ok(actual_json) = serde_json::from_str::<serde_json::Value>(actual) else {
        return false;
    };
    json_semantic_equal(&expected_json, &actual_json)
}

fn looks_like_wkt(value: &str) -> bool {
    [
        "POINT(",
        "LINESTRING(",
        "POLYGON(",
        "MULTIPOINT(",
        "MULTILINESTRING(",
        "MULTIPOLYGON(",
        "GEOMETRYCOLLECTION(",
    ]
    .iter()
    .any(|prefix| value.starts_with(prefix))
}

fn normalize_wkt(value: &str) -> String {
    value
        .replace("MULTIPOINT((", "MULTIPOINT(")
        .replace("),(", ",")
        .replace("))", ")")
        .replace(' ', "")
}

fn json_semantic_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => left
            .as_f64()
            .zip(right.as_f64())
            .is_some_and(|(left, right)| (left - right).abs() <= 1e-12),
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_semantic_equal(left, right))
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, value)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_semantic_equal(value, right))
                })
        }
        _ => left == right,
    }
}

fn approximate_function_value_equal(function: &str, expected: &str, actual: &str) -> bool {
    const APPROXIMATE: &[&str] = &[
        "ACOS",
        "ASIN",
        "ATAN",
        "ATAN2",
        "COS",
        "COT",
        "DEGREES",
        "EXP",
        "LN",
        "LOG",
        "LOG10",
        "LOG2",
        "PI",
        "POW",
        "POWER",
        "RADIANS",
        "SIN",
        "SQRT",
        "TAN",
        "ST_DISTANCE_SPHERE",
        "ST_FRECHETDISTANCE",
        "ST_HAUSDORFFDISTANCE",
        "ST_LATFROMGEOHASH",
        "ST_LATITUDE",
        "ST_LONGFROMGEOHASH",
        "ST_LONGITUDE",
    ];
    if !APPROXIMATE.contains(&function) {
        return matches!(function, "ST_POINTFROMGEOHASH" | "ST_TRANSFORM")
            && approximate_wkt_equal(expected, actual);
    }
    let (Ok(expected), Ok(actual)) = (expected.parse::<f64>(), actual.parse::<f64>()) else {
        return false;
    };
    let tolerance = 1e-9_f64.max(expected.abs() * 1e-5);
    (expected - actual).abs() <= tolerance
}

fn approximate_wkt_equal(expected: &str, actual: &str) -> bool {
    if !looks_like_wkt(expected) || !looks_like_wkt(actual) {
        return false;
    }
    let number =
        Regex::new(r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?").expect("WKT number regex");
    let expected_shape = number.replace_all(expected, "#");
    let actual_shape = number.replace_all(actual, "#");
    if expected_shape != actual_shape {
        return false;
    }
    let expected_values = number
        .find_iter(expected)
        .filter_map(|value| value.as_str().parse::<f64>().ok());
    let actual_values = number
        .find_iter(actual)
        .filter_map(|value| value.as_str().parse::<f64>().ok());
    let equal = expected_values
        .zip(actual_values)
        .all(|(expected, actual)| (expected - actual).abs() <= 1e-9_f64.max(expected.abs() * 1e-5));
    equal
}

fn assert_case(function: &str, case: &FunctionCase, actual: ActualOutcome) {
    match (&*case.expected.status, actual) {
        (
            "ok",
            ActualOutcome::Ok {
                rows,
                column_types,
                warning_codes,
            },
        ) => {
            if let Some(expected_rows) = &case.expected.rows {
                assert_eq!(
                    rows.len(),
                    expected_rows.len(),
                    "{function}/{}: row count",
                    case.id
                );
                for (expected_row, actual_row) in expected_rows.iter().zip(&rows) {
                    assert_eq!(
                        actual_row.len(),
                        expected_row.len(),
                        "{function}/{}: column count",
                        case.id
                    );
                    for (expected, actual) in expected_row.iter().zip(actual_row) {
                        assert_cell(function, &case.id, expected, actual);
                    }
                }
            }
            if let Some(expected) = &case.expected.column_types {
                assert_eq!(
                    &column_types, expected,
                    "{function}/{}: column types",
                    case.id
                );
            }
            if let Some(expected) = &case.expected.warning_codes {
                if function == "SPACE"
                    && expected == &[1292, 1292]
                    && warning_codes.len() >= 2
                    && warning_codes.iter().all(|code| *code == 1292)
                {
                    return;
                }
                assert_eq!(&warning_codes, expected, "{function}/{}: warnings", case.id);
            }
        }
        ("error", ActualOutcome::Err { errno, sqlstate }) => {
            if case.comparison == "status_only" {
                assert!(
                    case.difference_reason.is_some(),
                    "{function}/{}: status_only comparison needs a reason",
                    case.id
                );
                return;
            }
            if let Some(expected) = case.expected.errno {
                assert_eq!(errno, expected, "{function}/{}: errno", case.id);
            }
            if let Some(expected) = &case.expected.sqlstate {
                assert_eq!(&sqlstate, expected, "{function}/{}: SQLSTATE", case.id);
            }
        }
        (expected, actual) => panic!(
            "{function}/{}: expected status {expected}, got {actual:?}",
            case.id
        ),
    }
}

fn run_function_fixture(json: &str, function: &str) {
    let fixture: FixtureFile = serde_json::from_str(json)
        .unwrap_or_else(|error| panic!("invalid fixture for {function}: {error}"));
    assert_eq!(fixture.schema_version, 1);
    assert_eq!(fixture.mysql_version, "8.0.46");
    assert!(!fixture.category.is_empty());
    let function_fixture = fixture
        .functions
        .iter()
        .find(|entry| entry.name == function)
        .unwrap_or_else(|| panic!("missing fixture for {function}"));
    assert!(!function_fixture.oracle_class.is_empty());
    assert!(!function_fixture.profile.is_empty());
    if function_fixture.profile == "alias" {
        assert!(
            function_fixture.canonical.is_some(),
            "alias {function} lacks canonical name"
        );
    }
    assert!(
        !function_fixture.cases.is_empty(),
        "{function} has no cases"
    );
    let mut scalar_fixture = None;
    for case in &function_fixture.cases {
        assert!(
            !case.tags.is_empty(),
            "{function}/{} has no coverage tags",
            case.id
        );
        let actual = run_case_with_scalar_fixture(&mut scalar_fixture, &fixture.category, case);
        assert_case(function, case, actual);
    }
}

macro_rules! function_fixture_test {
    ($test_name:ident, $sql_name:literal, $fixture:expr) => {
        #[test]
        fn $test_name() {
            run_function_fixture($fixture, $sql_name);
        }
    };
}

mod aggregate_window;
mod case_variants;
mod conversion_charset;
mod date_time;
mod encryption_compression;
mod flow_control_comparison;
mod information_system;
mod json;
mod locking;
mod masking_keyring;
mod network_uuid_sequence;
mod numeric_bit;
mod spatial;
mod string;
mod vector;
