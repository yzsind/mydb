use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/encryption-compression.json"
));

function_fixture_test!(aes_decrypt, "AES_DECRYPT", FIXTURE);
function_fixture_test!(aes_encrypt, "AES_ENCRYPT", FIXTURE);
function_fixture_test!(compress, "COMPRESS", FIXTURE);
function_fixture_test!(from_base64, "FROM_BASE64", FIXTURE);
function_fixture_test!(md5, "MD5", FIXTURE);
function_fixture_test!(random_bytes, "RANDOM_BYTES", FIXTURE);
function_fixture_test!(sha, "SHA", FIXTURE);
function_fixture_test!(sha1, "SHA1", FIXTURE);
function_fixture_test!(sha2, "SHA2", FIXTURE);
function_fixture_test!(to_base64, "TO_BASE64", FIXTURE);
function_fixture_test!(uncompress, "UNCOMPRESS", FIXTURE);
function_fixture_test!(uncompressed_length, "UNCOMPRESSED_LENGTH", FIXTURE);
