use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/conversion-charset.json"
));

function_fixture_test!(cast, "CAST", FIXTURE);
function_fixture_test!(charset, "CHARSET", FIXTURE);
function_fixture_test!(coercibility, "COERCIBILITY", FIXTURE);
function_fixture_test!(collate, "COLLATE", FIXTURE);
function_fixture_test!(collation, "COLLATION", FIXTURE);
function_fixture_test!(convert_using, "CONVERT_USING", FIXTURE);
function_fixture_test!(weight_string, "WEIGHT_STRING", FIXTURE);
