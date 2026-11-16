//! Verify that catalog-derived column attributes (NOT_NULL / PRI_KEY /
//! UNSIGNED / ENUM_FLAG / SET_FLAG / AUTO_INCREMENT / declared length)
//! reach the wire `Column.source` on a `SELECT *` round-trip.
//!
//! Path: `CREATE TABLE` → `TableSchema::wire_column_source` →
//! `ColMeta.source` → `empty_result_columns` → `ResultSet.columns[i].source`.

use super::*;

fn columns_of(engine: &StorageQueryEngine, sql: &str) -> Vec<crate::types::Column> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.columns,
        other => panic!("expected ResultSet, got {other:?}"),
    }
}

#[test]
fn select_star_carries_not_null_and_pri_key_flags() {
    use crate::mysql::protocol::{NOT_NULL_FLAG, PRI_KEY_FLAG};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, opt INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    let cols = columns_of(&engine, "SELECT * FROM t");
    let by_name: std::collections::HashMap<&str, u16> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.source.flags))
        .collect();
    let id_flags = *by_name.get("id").expect("id column missing");
    assert!(
        id_flags & PRI_KEY_FLAG != 0,
        "PRI_KEY_FLAG missing on id (flags={:#x})",
        id_flags
    );
    assert!(
        id_flags & NOT_NULL_FLAG != 0,
        "NOT_NULL_FLAG missing on id (flags={:#x})",
        id_flags
    );
    let name_flags = *by_name.get("name").expect("name column missing");
    assert!(
        name_flags & NOT_NULL_FLAG != 0,
        "NOT_NULL_FLAG missing on name"
    );
    assert!(
        name_flags & PRI_KEY_FLAG == 0,
        "name must not be marked PRI_KEY"
    );
    let opt_flags = *by_name.get("opt").expect("opt column missing");
    assert!(
        opt_flags & NOT_NULL_FLAG == 0,
        "nullable opt must not carry NOT_NULL_FLAG"
    );
}

#[test]
fn select_star_carries_unsigned_and_auto_increment() {
    use crate::mysql::protocol::{AUTO_INCREMENT_FLAG, UNSIGNED_FLAG};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    let cols = columns_of(&engine, "SELECT * FROM t");
    assert_eq!(cols.len(), 1);
    let f = cols[0].source.flags;
    assert!(f & UNSIGNED_FLAG != 0, "UNSIGNED_FLAG missing (f={:#x})", f);
    assert!(
        f & AUTO_INCREMENT_FLAG != 0,
        "AUTO_INCREMENT_FLAG missing (f={:#x})",
        f
    );
}

#[test]
fn select_star_carries_enum_and_set_flag() {
    use crate::mysql::protocol::{ENUM_FLAG, SET_FLAG};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
              priority ENUM('low','medium','high') NOT NULL DEFAULT 'low', \
              perms SET('r','w','x') NOT NULL DEFAULT 'r')",
        )
        .unwrap();
    let cols = columns_of(&engine, "SELECT * FROM t");
    let by_name: std::collections::HashMap<&str, u16> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.source.flags))
        .collect();
    let p = *by_name.get("priority").unwrap();
    assert!(p & ENUM_FLAG != 0, "ENUM_FLAG missing on enum (f={:#x})", p);
    let s = *by_name.get("perms").unwrap();
    assert!(s & SET_FLAG != 0, "SET_FLAG missing on set (f={:#x})", s);
}

#[test]
fn select_star_carries_declared_string_length_in_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a VARCHAR(50), b CHAR(8), c VARCHAR(255))")
        .unwrap();
    let cols = columns_of(&engine, "SELECT * FROM t");
    let by_name: std::collections::HashMap<&str, Option<u32>> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.source.length))
        .collect();
    assert_eq!(by_name.get("a").copied().flatten(), Some(200));
    assert_eq!(by_name.get("b").copied().flatten(), Some(32));
    assert_eq!(by_name.get("c").copied().flatten(), Some(1020));
}

#[test]
fn select_star_carries_org_table_and_org_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    let cols = columns_of(&engine, "SELECT id AS pk, name FROM customers");
    let by_name: std::collections::HashMap<&str, (&str, &str)> = cols
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                (c.source.org_table.as_str(), c.source.org_name.as_str()),
            )
        })
        .collect();
    // `pk` is an alias for `id`; org_name should still be the real catalog
    // identifier `id`. JDBC's `getColumnName` reads org_name; `getColumnLabel`
    // reads the alias.
    assert_eq!(
        by_name.get("pk").copied(),
        Some(("customers", "id")),
        "aliased column must keep org_table/org_name pointing at the real catalog row"
    );
    assert_eq!(by_name.get("name").copied(), Some(("customers", "name")));
}
