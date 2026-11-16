//! Source-position contracts exercised independently of repository presentation.
use super::*;
use crate::engine::StorageQueryEngine;

#[test]
fn sealing_preserves_source_heap_lob_and_undo_allocation() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(64), payload LONGBLOB, INDEX iv(v))")
        .unwrap();
    source
        .execute("INSERT INTO t VALUES (1,'old',REPEAT('a',20000)),(2,'delete',REPEAT('b',20000)),(3,'stay',NULL)")
        .unwrap();
    source
        .execute("UPDATE t SET v='updated', payload=REPEAT('c',24000) WHERE id=1")
        .unwrap();
    source.execute("DELETE FROM t WHERE id=2").unwrap();
    let image = root.path().join("base");
    let mut source_at_end = BTreeMap::new();
    let manifest = create_backup(
        source.db_handle(),
        &BackupRequest {
            repository: image.clone(),
        },
        &random_uuid(),
        &AtomicBool::new(false),
        |phase, _, _| {
            if phase == "COPY_FINAL_FILES" && source_at_end.is_empty() {
                // Capture E while this operation still excludes maintenance.
                // The live source may legitimately vacuum after capture ends.
                let _maintenance = source
                    .db_handle()
                    .backup_maintenance_capture_guard()
                    .unwrap();
                for file in collect_snapshot_files(source.db_handle().data_dir(), false).unwrap() {
                    if file.relative.ends_with(".heap")
                        || file.relative.ends_with(".lob")
                        || file.relative.starts_with("system/undo/")
                    {
                        source_at_end.insert(file.relative, fs::read(file.path).unwrap());
                    }
                }
            }
        },
    )
    .unwrap();
    assert!(!source_at_end.is_empty());
    let mut differences = Vec::new();
    for file in &manifest.files {
        if file.path.ends_with(".heap")
            || file.path.ends_with(".lob")
            || file.path.starts_with("system/undo/")
        {
            let source_bytes = source_at_end.get(&file.path).unwrap();
            let sealed_bytes = fs::read(image.join(&file.path)).unwrap();
            if *source_bytes != sealed_bytes {
                differences.push(file.path.clone());
            }
        }
    }
    source.shutdown().unwrap();
    drop(source);
    fs::remove_dir_all(root.path().join("source")).unwrap();
    let snapshot = StorageQueryEngine::open(&image).unwrap();
    assert_eq!(
        snapshot.db_handle().open_mode(),
        crate::storage::DatabaseOpenMode::ReadOnlyImage
    );
    let QueryResponse::ResultSet(rows) = snapshot
        .execute("SELECT id,LENGTH(payload) FROM t FORCE INDEX(iv) WHERE v='updated'")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1), Value::Int(24000)]]);
    snapshot.shutdown().unwrap();
    drop(snapshot);
    native::verify(&image).unwrap();
    assert!(
        differences.is_empty(),
        "sealing changed source-owned allocation or authority: {differences:?}"
    );
}

#[test]
fn prepared_tree_continues_source_updates_and_index_splits() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
    source
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(100), INDEX iv(v))")
        .unwrap();
    source
        .execute("INSERT INTO t VALUES (1,'old'),(2,'delete'),(3,'stay')")
        .unwrap();
    let image = root.path().join("base");
    let base = create_backup(
        source.db_handle(),
        &BackupRequest {
            repository: image.clone(),
        },
        &random_uuid(),
        &AtomicBool::new(false),
        |_, _, _| {},
    )
    .unwrap();
    source
        .execute("UPDATE t SET v='changed' WHERE id=1")
        .unwrap();
    source.execute("DELETE FROM t WHERE id=2").unwrap();
    for start in (4..1004).step_by(100) {
        let values = (start..start + 100)
            .map(|id| format!("({id},'value-{id:08}-abcdefghijklmnopqrstuvwxyz')"))
            .collect::<Vec<_>>()
            .join(",");
        source
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
    }
    let prefix = snapshot_wal_files(source.db_handle()).unwrap();
    let clone = root.path().join("clone");
    native::restore_image(
        &image,
        &clone,
        prefix.last_lsn,
        prefix.captured_unix_us,
        false,
        |log| {
            for file in &prefix.files {
                crate::wal::visit_raw_prefix(
                    &file.file.open().unwrap(),
                    file.len,
                    |first, last, raw| {
                        if last > base.end_lsn {
                            assert!(first > base.end_lsn);
                            let frame = crate::wal::decode_frame_for_replication(raw)?;
                            log.append_replication_frame(raw, &frame)?;
                        }
                        Ok(())
                    },
                )?;
            }
            Ok(())
        },
    )
    .unwrap();
    let restored = StorageQueryEngine::open(&clone).unwrap();
    let query = |sql| {
        let QueryResponse::ResultSet(result) = restored.execute(sql).unwrap() else {
            panic!("expected rows")
        };
        result.rows
    };
    assert_eq!(query("SELECT COUNT(*) FROM t")[0][0], Value::Int(1002));
    assert_eq!(
        query("SELECT id FROM t FORCE INDEX(iv) WHERE v='changed'")[0][0],
        Value::Int(1)
    );
    assert_eq!(
        query(
            "SELECT id FROM t FORCE INDEX(iv) WHERE v='value-00001003-abcdefghijklmnopqrstuvwxyz'"
        )[0][0],
        Value::Int(1003)
    );
    restored.shutdown().unwrap();
    native::verify(&image).unwrap();
}

#[test]
fn sealed_xa_query_and_source_decisions_are_independent() {
    for commit in [true, false] {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let source = StorageQueryEngine::open(root.path().join("source")).unwrap();
        source
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(32), INDEX iv(v))")
            .unwrap();
        source
            .execute("INSERT INTO t VALUES(1,'committed')")
            .unwrap();
        let mut xa = crate::engine::SessionContext::default();
        for sql in [
            "XA START 'sealed-xa'",
            "UPDATE t SET v='pending' WHERE id=1",
            "INSERT INTO t VALUES(2,'new')",
            "XA END 'sealed-xa'",
            "XA PREPARE 'sealed-xa'",
        ] {
            source.execute_with_session(&mut xa, sql).unwrap();
        }
        let base = root.path().join("base");
        let manifest = create_backup(
            source.db_handle(),
            &BackupRequest {
                repository: base.clone(),
            },
            &random_uuid(),
            &AtomicBool::new(false),
            |_, _, _| {},
        )
        .unwrap();
        let image = StorageQueryEngine::open(&base).unwrap();
        let QueryResponse::ResultSet(rows) = image
            .execute("SELECT id,v FROM t FORCE INDEX(iv) WHERE v='committed'")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            rows.rows,
            vec![vec![Value::Int(1), Value::String("committed".into())]]
        );
        assert!(native::promote(&base).is_err());
        image.shutdown().unwrap();
        drop(image);
        source
            .execute(if commit {
                "XA COMMIT 'sealed-xa'"
            } else {
                "XA ROLLBACK 'sealed-xa'"
            })
            .unwrap();
        let prefix = snapshot_wal_files(source.db_handle()).unwrap();
        let end = prefix.last_lsn;
        source.shutdown().unwrap();
        drop(source);
        let target = root.path().join("target");
        native::restore_image(&base, &target, end, prefix.captured_unix_us, false, |log| {
            for file in &prefix.files {
                crate::wal::visit_raw_prefix(
                    &file.file.open().unwrap(),
                    file.len,
                    |start, end, bytes| {
                        if end > manifest.end_lsn {
                            assert!(start > manifest.end_lsn);
                            let frame = crate::wal::decode_frame_for_replication(bytes)?;
                            log.append_replication_frame(bytes, &frame)?;
                        }
                        Ok(())
                    },
                )?;
            }
            Ok(())
        })
        .unwrap();
        let restored = StorageQueryEngine::open(&target).unwrap();
        let QueryResponse::ResultSet(rows) =
            restored.execute("SELECT v FROM t WHERE id=1").unwrap()
        else {
            panic!()
        };
        assert_eq!(
            rows.rows[0][0],
            Value::String(if commit { "pending" } else { "committed" }.into())
        );
        restored.shutdown().unwrap();
        drop(restored);
        native::promote(&target).unwrap();
        let writable = StorageQueryEngine::open(&target).unwrap();
        writable
            .execute("INSERT INTO t VALUES(3,'independent')")
            .unwrap();
        writable.shutdown().unwrap();
        native::verify(&base).unwrap();
    }
}
