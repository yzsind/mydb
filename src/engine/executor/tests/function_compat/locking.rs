use super::*;

const FIXTURE: &str = include_str!("fixtures/locking.json");

function_fixture_test!(get_lock, "GET_LOCK", FIXTURE);
function_fixture_test!(is_free_lock, "IS_FREE_LOCK", FIXTURE);
function_fixture_test!(is_used_lock, "IS_USED_LOCK", FIXTURE);
function_fixture_test!(release_all_locks, "RELEASE_ALL_LOCKS", FIXTURE);
function_fixture_test!(release_lock, "RELEASE_LOCK", FIXTURE);
