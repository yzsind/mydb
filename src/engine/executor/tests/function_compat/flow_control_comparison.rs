use super::*;

const FIXTURE: &str = include_str!("fixtures/flow-control-comparison.json");

function_fixture_test!(coalesce, "COALESCE", FIXTURE);
function_fixture_test!(elt, "ELT", FIXTURE);
function_fixture_test!(field, "FIELD", FIXTURE);
function_fixture_test!(greatest, "GREATEST", FIXTURE);
function_fixture_test!(if_fn, "IF", FIXTURE);
function_fixture_test!(ifnull, "IFNULL", FIXTURE);
function_fixture_test!(interval, "INTERVAL", FIXTURE);
function_fixture_test!(isnull, "ISNULL", FIXTURE);
function_fixture_test!(least, "LEAST", FIXTURE);
function_fixture_test!(name_const, "NAME_CONST", FIXTURE);
function_fixture_test!(nullif, "NULLIF", FIXTURE);
