use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/numeric-bit.json"
));

function_fixture_test!(abs, "ABS", FIXTURE);
function_fixture_test!(acos, "ACOS", FIXTURE);
function_fixture_test!(asin, "ASIN", FIXTURE);
function_fixture_test!(atan, "ATAN", FIXTURE);
function_fixture_test!(atan2, "ATAN2", FIXTURE);
function_fixture_test!(bin, "BIN", FIXTURE);
function_fixture_test!(bit_count, "BIT_COUNT", FIXTURE);
function_fixture_test!(ceil, "CEIL", FIXTURE);
function_fixture_test!(ceiling, "CEILING", FIXTURE);
function_fixture_test!(conv, "CONV", FIXTURE);
function_fixture_test!(cos, "COS", FIXTURE);
function_fixture_test!(cot, "COT", FIXTURE);
function_fixture_test!(crc32, "CRC32", FIXTURE);
function_fixture_test!(degrees, "DEGREES", FIXTURE);
function_fixture_test!(div, "DIV", FIXTURE);
function_fixture_test!(exp, "EXP", FIXTURE);
function_fixture_test!(floor, "FLOOR", FIXTURE);
function_fixture_test!(ln, "LN", FIXTURE);
function_fixture_test!(log, "LOG", FIXTURE);
function_fixture_test!(log10, "LOG10", FIXTURE);
function_fixture_test!(log2, "LOG2", FIXTURE);
function_fixture_test!(mod_fn, "MOD", FIXTURE);
function_fixture_test!(oct, "OCT", FIXTURE);
function_fixture_test!(pi, "PI", FIXTURE);
function_fixture_test!(pow, "POW", FIXTURE);
function_fixture_test!(power, "POWER", FIXTURE);
function_fixture_test!(radians, "RADIANS", FIXTURE);
function_fixture_test!(rand, "RAND", FIXTURE);
function_fixture_test!(round, "ROUND", FIXTURE);
function_fixture_test!(sign, "SIGN", FIXTURE);
function_fixture_test!(sin, "SIN", FIXTURE);
function_fixture_test!(sqrt, "SQRT", FIXTURE);
function_fixture_test!(tan, "TAN", FIXTURE);
function_fixture_test!(truncate, "TRUNCATE", FIXTURE);
