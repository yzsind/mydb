use super::*;

const FIXTURE: &str = include_str!("fixtures/network-uuid-sequence.json");

function_fixture_test!(bin_to_uuid, "BIN_TO_UUID", FIXTURE);
function_fixture_test!(currval, "CURRVAL", FIXTURE);
function_fixture_test!(inet6_aton, "INET6_ATON", FIXTURE);
function_fixture_test!(inet6_ntoa, "INET6_NTOA", FIXTURE);
function_fixture_test!(inet_aton, "INET_ATON", FIXTURE);
function_fixture_test!(inet_ntoa, "INET_NTOA", FIXTURE);
function_fixture_test!(is_ipv4, "IS_IPV4", FIXTURE);
function_fixture_test!(is_ipv4_compat, "IS_IPV4_COMPAT", FIXTURE);
function_fixture_test!(is_ipv4_mapped, "IS_IPV4_MAPPED", FIXTURE);
function_fixture_test!(is_ipv6, "IS_IPV6", FIXTURE);
function_fixture_test!(is_uuid, "IS_UUID", FIXTURE);
function_fixture_test!(lastval, "LASTVAL", FIXTURE);
function_fixture_test!(nextval, "NEXTVAL", FIXTURE);
function_fixture_test!(setval, "SETVAL", FIXTURE);
function_fixture_test!(uuid, "UUID", FIXTURE);
function_fixture_test!(uuid_short, "UUID_SHORT", FIXTURE);
function_fixture_test!(uuid_to_bin, "UUID_TO_BIN", FIXTURE);
