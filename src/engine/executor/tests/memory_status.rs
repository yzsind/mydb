//! P0/D1: `SHOW ENGINE MYDB STATUS` surfaces a single roll-up of the
//! engine's independently-accounted memory pools (`tracked_memory_bytes`)
//! plus the OS peak resident set (`resident_set_peak_bytes`), so the gap
//! between "what the engine tracks" and "what the OS reports" is visible
//! in one place. Also covers the B2 undo memory observability rows.

use super::*;
use std::collections::HashMap;

fn status_map(engine: &StorageQueryEngine) -> HashMap<String, String> {
    let QueryResponse::ResultSet(result) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    // Rows are [Type="mydb", Name, Status(value)].
    result
        .rows
        .iter()
        .filter_map(|row| {
            let name = match row.get(1)? {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            };
            let value = match row.get(2)? {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            };
            Some((name, value))
        })
        .collect()
}

#[test]
fn show_engine_surfaces_memory_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Touch the heap + grow undo so the tracked pools are non-trivial.
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("UPDATE t SET v = v + 1").unwrap();
    let _ = engine.execute("SELECT * FROM t").unwrap();

    let m = status_map(&engine);
    for metric in [
        "buffer_pool_resident_bytes",
        "tracked_memory_bytes",
        "resident_set_peak_bytes",
        "undo_mem_soft_limit_bytes",
        "undo_mem_pressure",
    ] {
        assert!(
            m.contains_key(metric),
            "SHOW ENGINE MYDB STATUS missing {metric}; keys: {:?}",
            m.keys().collect::<Vec<_>>()
        );
    }

    let buf: u128 = m["buffer_pool_resident_bytes"].parse().unwrap();
    let tracked: u128 = m["tracked_memory_bytes"].parse().unwrap();
    let rss: u128 = m["resident_set_peak_bytes"].parse().unwrap();

    // tracked_memory_bytes includes the buffer-pool resident bytes.
    assert!(
        tracked >= buf,
        "tracked ({tracked}) must include buffer resident ({buf})"
    );
    // getrusage must report a non-zero peak RSS, and for this tiny dataset
    // the OS resident set dwarfs the engine's tracked floor — the visible
    // gap is exactly the untracked memory the roll-up documents.
    assert!(rss > 0, "getrusage peak RSS should be non-zero");
    assert!(
        rss > tracked,
        "OS RSS ({rss}) should exceed the tracked floor ({tracked}) for a tiny dataset"
    );

    // Soft cap defaults to disabled (0) with no pressure observed yet.
    assert_eq!(m["undo_mem_soft_limit_bytes"], "0");
    assert_eq!(m["undo_mem_pressure"], "0");
}
