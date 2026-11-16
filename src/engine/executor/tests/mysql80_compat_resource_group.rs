//! RESOURCE GROUP is outside the product surface and must fail explicitly.
use super::*;

#[test]
fn resource_group_statements_reject_without_a_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SET RESOURCE GROUP SYS_default",
        "SET RESOURCE GROUP missing_group",
        "CREATE RESOURCE GROUP rg TYPE = USER VCPU = 0-3 THREAD_PRIORITY = 5 ENABLE",
        "ALTER RESOURCE GROUP rg VCPU = 0",
        "DROP RESOURCE GROUP rg",
        "SELECT * FROM information_schema.RESOURCE_GROUPS",
    ] {
        let err = <StorageQueryEngine as QueryEngine>::execute(&engine, &mut session, sql)
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        assert_eq!(err.code(), 1235, "{sql}: {err:?}");
    }
}
