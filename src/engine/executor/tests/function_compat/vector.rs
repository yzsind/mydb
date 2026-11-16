use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/vector.json"
));

function_fixture_test!(cosine_distance, "COSINE_DISTANCE", FIXTURE);
function_fixture_test!(distance, "DISTANCE", FIXTURE);
function_fixture_test!(dot_product, "DOT_PRODUCT", FIXTURE);
function_fixture_test!(from_vector, "FROM_VECTOR", FIXTURE);
function_fixture_test!(inner_product, "INNER_PRODUCT", FIXTURE);
function_fixture_test!(l2_distance, "L2_DISTANCE", FIXTURE);
function_fixture_test!(l2_normalize, "L2_NORMALIZE", FIXTURE);
function_fixture_test!(
    max_inner_product_distance,
    "MAX_INNER_PRODUCT_DISTANCE",
    FIXTURE
);
function_fixture_test!(string_to_vector, "STRING_TO_VECTOR", FIXTURE);
function_fixture_test!(subvector, "SUBVECTOR", FIXTURE);
function_fixture_test!(to_vector, "TO_VECTOR", FIXTURE);
function_fixture_test!(vector_dim, "VECTOR_DIM", FIXTURE);
function_fixture_test!(vector_norm, "VECTOR_NORM", FIXTURE);
function_fixture_test!(vector_to_string, "VECTOR_TO_STRING", FIXTURE);
function_fixture_test!(vec_distance, "VEC_DISTANCE", FIXTURE);
