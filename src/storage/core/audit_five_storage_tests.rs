#[test]
fn audit_five_lob_old_generation_cannot_free_copied_chain() {
    for durability in [DurabilityMode::Memory, DurabilityMode::Relaxed] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let db = m4_open_db(dir.path(), durability);
        let id = m3_lob_table(&db);
        let old = db
            .with_overflow_mut(id, |lob| {
                lob.allocate_chain_compressible(b"old physical generation", false)
            })
            .unwrap();
        db.create_table(
            "shadow",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col(
                    "data",
                    DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let shadow_id = db.table("test.shadow").unwrap().id;
        let new = db
            .with_overflow_mut(shadow_id, |lob| {
                lob.allocate_chain_compressible(b"new physical generation", false)
            })
            .unwrap();
        assert_eq!(old.first_page_id, new.first_page_id);
        assert_ne!(
            old.generation, new.generation,
            "COPY allocation identities must not alias old undo"
        );
        db.with_overflow_mut(shadow_id, |lob| {
            lob.free_chain(old)?;
            assert_eq!(lob.free_count(), 0);
            assert_eq!(lob.read_chain(new)?, b"new physical generation");
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn audit_five_lob_shrink_requires_durable_wal() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Relaxed);
    let id = m3_lob_table(&db);
    db.buffer_pool.shutdown_flusher();
    db.with_overflow_mut(id, |lob| {
        let live = lob.allocate_chain_compressible(b"live", false)?;
        let tail = lob.allocate_chain_compressible(&vec![7; OVFL_PAGE_PAYLOAD * 5], false)?;
        lob.free_chain(tail)?;
        assert_eq!(lob.shrink_to_fit(1)?, 5);
        let meta = lob.read_raw_page(0)?;
        assert!(lob.log.lock().durable_lsn() >= crate::wal::page_lsn(&meta));
        assert_eq!(lob.read_chain(live)?, b"live");
        Ok(())
    })
    .unwrap();
}

#[test]
fn audit_five_lob_extent_meta_failure_preserves_free_chain() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Relaxed);
    let id = m3_lob_table(&db);
    db.with_overflow_mut(id, |lob| {
        let extent = lob.allocate_chain_compressible(&vec![8; OVFL_PAGE_PAYLOAD * 5], false)?;
        let prefix = lob.allocate_chain_compressible(b"prefix", false)?;
        lob.free_chain(extent)?;
        lob.free_chain(prefix)?;
        let head = lob.free_head;
        FAIL_LOB_META_ONCE.with(|flag| flag.set(true));
        assert!(lob.reserve_chain_pages(5).is_err());
        assert_eq!(lob.free_head, head);
        assert_eq!(lob.free_count, 6);
        assert_eq!(
            lob.count_free_list()?,
            6,
            "failed reservation must not unlink durable free pages"
        );
        let value = vec![9; OVFL_PAGE_PAYLOAD * 5];
        let reused = lob.allocate_chain_compressible(&value, false)?;
        assert_eq!(reused.first_page_id, extent.first_page_id);
        assert_eq!(lob.read_chain(reused)?, value);
        assert_eq!(lob.count_free_list()?, 1);
        Ok(())
    })
    .unwrap();
    db.flush_dirty_data().unwrap();
}
