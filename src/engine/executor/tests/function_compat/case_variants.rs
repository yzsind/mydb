use super::*;

fn check_category(json: &str) {
    let fixture: FixtureFile = serde_json::from_str(json).unwrap();
    let mut scalar_fixture = None;
    for function in fixture.functions {
        let mut case = function
            .cases
            .into_iter()
            .next()
            .expect("function fixture has a case");
        // These fixture functions ordinarily run in separate nextest
        // processes. This category loop must release advisory locks before
        // opening the next independent session/engine.
        if fixture.category == "locking" {
            case.cleanup.push("SELECT RELEASE_ALL_LOCKS()".into());
        }
        let sql = case.sql.clone();
        let id = case.id.clone();
        for mixed in [false, true] {
            case.sql =
                super::super::function_name_case::case_variant(&sql, Some(&function.name), mixed);
            case.id = format!("{id}/{}", if mixed { "mixed-case" } else { "lower-case" });
            let actual =
                run_case_with_scalar_fixture(&mut scalar_fixture, &fixture.category, &case);
            assert_case(&function.name, &case, actual);
        }
    }
}

macro_rules! category {
    ($test:ident, $file:literal) => {
        #[test]
        fn $test() {
            check_category(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/function-compat/",
                $file
            )));
        }
    };
}

category!(function_case_numeric_bit, "numeric-bit.json");
category!(function_case_string, "string.json");
category!(function_case_date_time, "date-time.json");
category!(function_case_conversion_charset, "conversion-charset.json");
category!(function_case_flow_control, "flow-control-comparison.json");
category!(function_case_json, "json.json");
category!(function_case_spatial, "spatial.json");
category!(function_case_vector, "vector.json");
category!(function_case_aggregate_window, "aggregate-window.json");
category!(function_case_encryption, "encryption-compression.json");
category!(function_case_information_system, "information-system.json");
category!(
    function_case_network_uuid_sequence,
    "network-uuid-sequence.json"
);
category!(function_case_locking, "locking.json");
category!(function_case_masking_keyring, "masking-keyring.json");
