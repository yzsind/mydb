//! Safety regressions migrated to the unified repository implementation.
use super::*;
fn root() -> tempfile::TempDir {
    tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap()
}

#[test]
fn sync_batch_error_releases_bounded_producers_and_retry_succeeds() {
    let root = root();
    let mut paths = Vec::new();
    for id in 0..32 {
        let path = root.path().join(format!("file-{id}"));
        fs::write(&path, [id as u8; 16]).unwrap();
        paths.push(path);
    }
    paths.insert(1, root.path().join("missing"));
    let work = paths.clone();
    let (send, done) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        send.send(sync_paths_parallel(&work)).unwrap();
    });
    assert!(done.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
    worker.join().unwrap();
    paths.remove(1);
    sync_paths_parallel(&paths).unwrap();
    for (id, path) in paths.iter().enumerate() {
        assert_eq!(fs::read(path).unwrap(), [id as u8; 16]);
    }
}

fn backup_engine() -> (tempfile::TempDir, crate::engine::StorageQueryEngine) {
    let root = root();
    let engine = crate::engine::StorageQueryEngine::open(root.path().join("source")).unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(80))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,'base')").unwrap();
    (root, engine)
}
#[test]
fn audit_r01_writer_that_passed_sql_gate_cannot_cross_capture_fence() {
    let (_root, engine) = backup_engine();
    let db = engine.db_handle().clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let statement = crate::parser::parse_statement("INSERT INTO t VALUES(2,'late')").unwrap();
        wait_for_write_gate(&db, None, &statement).unwrap();
        let lazy = db.begin_txn().unwrap();
        ready_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        assert!(db.begin_dml_txn().is_err());
        assert!(db
            .insert_row(
                lazy,
                "test.t",
                vec![Value::Int(2), Value::String("late".into())]
            )
            .is_err());
        db.rollback_txn(lazy.txn_id).unwrap();
        assert!(ddl_admission(
            &db,
            &crate::parser::parse_statement("CREATE TABLE late(id INT)").unwrap()
        )
        .is_err());
        assert!(Admission::enter(&db).is_err());
    });
    ready_rx.recv().unwrap();
    let freeze = engine.db_handle().capture.freeze();
    assert!(!engine.db_handle().has_active_archive_backup_conflicts());
    resume_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(
        engine.db_handle().capture.admissions.load(Ordering::SeqCst),
        0
    );
    drop(freeze);
    engine.execute("INSERT INTO t VALUES(2,'after')").unwrap();
    engine.shutdown().unwrap();
}
#[test]
fn audit_r04_identical_torn_page_reads_are_rejected() {
    let (root, engine) = backup_engine();
    engine.db_handle().checkpoint().unwrap();
    let files = collect_snapshot_files(engine.db_handle().data_dir(), false).unwrap();
    let source = files
        .iter()
        .find(|file| {
            file.relative.ends_with(".heap") && file.len >= crate::storage::PAGE_SIZE as u64
        })
        .unwrap();
    let mut page = vec![0; crate::storage::PAGE_SIZE];
    read_exact_at(&source.file.open().unwrap(), &mut page, 0).unwrap();
    assert!(crate::storage::backup_page_valid(&page));
    page[128] ^= 0x40;
    let path = root.path().join("torn.heap");
    fs::write(&path, &page).unwrap();
    let input = File::open(&path).unwrap();
    let mut a = vec![0; page.len()];
    let mut b = vec![0; page.len()];
    read_exact_at(&input, &mut a, 0).unwrap();
    read_exact_at(&input, &mut b, 0).unwrap();
    assert_eq!(a, b, "the old double-read predicate accepts this image");
    assert!(read_stable_chunk(&input, 0, &mut a, &mut b, &path).is_err());
    assert_eq!(fs::read(&path).unwrap(), page);
    engine.shutdown().unwrap();
}

#[test]
fn bulk_stable_reads_retry_only_changed_pages() {
    let page_bytes = crate::storage::PAGE_SIZE;
    let mut output = vec![0; page_bytes * 4];
    let mut compare = vec![0; output.len()];
    let mut reads = Vec::new();
    read_stable_chunk_with(
        123,
        &mut output,
        &mut compare,
        Path::new("undo.bin"),
        |buf, offset| {
            reads.push((offset, buf.len()));
            buf.fill(1);
            if reads.len() == 2 {
                buf[page_bytes..page_bytes * 2].fill(2);
            } else if reads.len() == 3 {
                assert_eq!(offset, 123 + page_bytes as u64);
                buf.fill(2);
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        reads,
        [
            (123, page_bytes * 4),
            (123, page_bytes * 4),
            (123 + page_bytes as u64, page_bytes)
        ]
    );
    assert!(output[..page_bytes].iter().all(|b| *b == 1));
    assert!(output[page_bytes..page_bytes * 2].iter().all(|b| *b == 2));
    assert!(output[page_bytes * 2..].iter().all(|b| *b == 1));
}

#[test]
fn stable_chunk_uses_two_bulk_reads_and_rejects_torn_tail_page() {
    let mut output = vec![0; CHUNK_BYTES];
    let mut compare = vec![0; CHUNK_BYTES];
    let mut reads = 0;
    read_stable_chunk_with(
        0,
        &mut output,
        &mut compare,
        Path::new("table.heap"),
        |buf, offset| {
            assert_eq!((offset, buf.len()), (0, CHUNK_BYTES));
            reads += 1;
            buf.fill(0);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(reads, 2);
    assert!(read_stable_chunk_with(
        0,
        &mut output,
        &mut compare,
        Path::new("table.heap"),
        |buf, _| {
            buf.fill(0);
            *buf.last_mut().unwrap() = 1;
            Ok(())
        }
    )
    .is_err());
}

#[test]
fn chunk_pool_error_unblocks_queued_results() {
    let tasks = (0..128)
        .map(|chunk_index| SourceChunkTask {
            file_index: 0,
            chunk_index,
            offset: 0,
            len: 1,
        })
        .collect::<Vec<_>>();
    let result = process_chunks_parallel(
        &tasks,
        0,
        |task, _, _| {
            if task.chunk_index == 0 {
                Err(backup_err("injected chunk failure"))
            } else {
                Ok(())
            }
        },
        |_, ()| {},
    );
    assert!(result.is_err());
}
#[test]
fn audit_bounded_codec_and_path_inventory_fail_closed() {
    let clock_root = root();
    assert!(crate::wal::persist_source_clock(clock_root.path(), u64::MAX).is_err());
    assert!(!clock_root.path().join("system/commit-clock.v1").exists());
    let payload = vec![1; 1024];
    let mut blob = encode_blob(&payload, None).unwrap();
    blob[21..29].copy_from_slice(&((MAX_METADATA_BYTES as u64) + 1).to_le_bytes());
    assert!(decode_blob(&blob, None).is_err());
    let mut blob = encode_blob(&payload, None).unwrap();
    assert_eq!(blob[8] & 1, 1);
    blob[69..73].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_blob(&blob, None).is_err());
    for path in [
        "schemas/../bad",
        "schemas//bad",
        "schemas/./bad",
        "objects/C:/bad",
        "objects/x\\y",
    ] {
        assert!(validate_restore_path(path).is_err());
    }
    let f = BackupFile {
        path: "schemas/a.heap".into(),
        len: 0,
        class: FileClass::Authoritative,
        chunks: vec![],
    };
    assert!(validate_files(&[f.clone(), f]).is_err());
}
#[test]
fn audit_source_identity_changes_when_timeline_forks() {
    let root = root();
    let original = load_or_create_source_id(root.path()).unwrap();
    assert_eq!(load_or_create_source_id(root.path()).unwrap(), original);
    let mut role = crate::repl::role::RoleState::new_standalone();
    crate::repl::role::save(root.path(), &role).unwrap();
    assert_eq!(load_or_create_source_id(root.path()).unwrap(), original);
    role.promote_to_master_at(10);
    crate::repl::role::save(root.path(), &role).unwrap();
    let promoted = load_or_create_source_id(root.path()).unwrap();
    assert_ne!(promoted, original);
    assert_eq!(load_or_create_source_id(root.path()).unwrap(), promoted);
    role.promote_to_master_at(20);
    crate::repl::role::save(root.path(), &role).unwrap();
    assert_ne!(load_or_create_source_id(root.path()).unwrap(), promoted);
}
#[test]
fn admitted_sql_writer_can_finish_while_capture_drains() {
    for xa in [false, true] {
        let (_root, engine) = backup_engine();
        std::thread::scope(|scope| {
            let (ready_tx, ready_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let writer = &engine;
            let worker = scope.spawn(move || {
                let mut session = crate::engine::SessionContext::default();
                writer
                    .execute_with_session(
                        &mut session,
                        if xa {
                            "XA START 'gate-drain'"
                        } else {
                            "START TRANSACTION"
                        },
                    )
                    .unwrap();
                writer
                    .execute_with_session(&mut session, "UPDATE t SET v='first' WHERE id=1")
                    .unwrap();
                ready_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                let result = (|| {
                    writer.execute_with_session(
                        &mut session,
                        "UPDATE t SET v='finished' WHERE id=1",
                    )?;
                    if xa {
                        writer.execute_with_session(&mut session, "XA END 'gate-drain'")?;
                        writer.execute_with_session(&mut session, "XA PREPARE 'gate-drain'")?;
                    } else {
                        writer.execute_with_session(&mut session, "COMMIT")?;
                    }
                    Ok::<_, QueryError>(())
                })();
                done_tx
                    .send(
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|e| e.message().to_string()),
                    )
                    .unwrap();
                release_rx.recv().unwrap();
                if result.is_err() {
                    if xa {
                        let _ = writer.execute_with_session(&mut session, "XA END 'gate-drain'");
                        let _ =
                            writer.execute_with_session(&mut session, "XA ROLLBACK 'gate-drain'");
                    } else {
                        writer
                            .execute_with_session(&mut session, "ROLLBACK")
                            .unwrap();
                    }
                }
            });
            ready_rx.recv().unwrap();
            assert!(engine.db_handle().has_active_archive_backup_conflicts());
            let freeze = engine.db_handle().capture.freeze();
            resume_tx.send(()).unwrap();
            let outcome = done_rx.recv_timeout(Duration::from_secs(5));
            drop(freeze);
            release_tx.send(()).unwrap();
            worker.join().unwrap();
            assert_eq!(
                outcome.unwrap(),
                Ok(()),
                "capture must drain an admitted writer (xa={xa})"
            );
            assert!(!engine.db_handle().has_active_archive_backup_conflicts());
        });
        if xa {
            assert_eq!(engine.db_handle().xa_status_counters().0, 1);
            engine.execute("XA ROLLBACK 'gate-drain'").unwrap();
        }
        let QueryResponse::ResultSet(rows) = engine.execute("SELECT v FROM t WHERE id=1").unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(
            rows.rows[0][0],
            Value::String(if xa { "base" } else { "finished" }.into())
        );
        engine.shutdown().unwrap();
    }
}
