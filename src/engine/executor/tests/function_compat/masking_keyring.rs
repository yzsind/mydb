use super::*;

const FIXTURE: &str = include_str!("fixtures/masking-keyring.json");

function_fixture_test!(keyring_key_fetch, "KEYRING_KEY_FETCH", FIXTURE);
function_fixture_test!(keyring_key_generate, "KEYRING_KEY_GENERATE", FIXTURE);
function_fixture_test!(keyring_key_length, "KEYRING_KEY_LENGTH", FIXTURE);
function_fixture_test!(keyring_key_remove, "KEYRING_KEY_REMOVE", FIXTURE);
function_fixture_test!(keyring_key_store, "KEYRING_KEY_STORE", FIXTURE);
function_fixture_test!(keyring_key_type, "KEYRING_KEY_TYPE", FIXTURE);
function_fixture_test!(mask_inner, "MASK_INNER", FIXTURE);
function_fixture_test!(mask_outer, "MASK_OUTER", FIXTURE);
function_fixture_test!(mask_pan, "MASK_PAN", FIXTURE);
function_fixture_test!(mask_pan_relaxed, "MASK_PAN_RELAXED", FIXTURE);
function_fixture_test!(mask_ssn, "MASK_SSN", FIXTURE);
function_fixture_test!(
    validate_password_strength,
    "VALIDATE_PASSWORD_STRENGTH",
    FIXTURE
);
