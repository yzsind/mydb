use super::*;

fn fixture(entries: Vec<IndexEntry>) -> (tempfile::TempDir, IndexFile) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new_with_options(
        1024,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        8,
    );
    let index = IndexFile::open_with_durability(
        dir.path().join("read-ahead.idx"),
        log,
        DurabilityMode::Strict,
        pool,
    )
    .unwrap();
    index.bulk_load_sorted(1, entries).unwrap();
    (dir, index)
}

fn entry(token: &str, id: usize) -> IndexEntry {
    let mut payload = vec![0u8; 320];
    payload[..8].copy_from_slice(&(id as u64).to_be_bytes());
    IndexEntry {
        key: IndexKey::from(vec![KeyPart::String(token.into()), KeyPart::Bytes(payload)]),
        row_id: RowId {
            page_id: (id / 100) as u32,
            slot_id: (id % 100) as u16,
        },
    }
}

#[test]
fn read_ahead_fulltext_stops_before_unrelated_cached_tokens() {
    let entries = (0..600)
        .map(|id| entry(&format!("token-{id:04}"), id))
        .collect();
    let (_dir, index) = fixture(entries);
    let token = "token-0100";
    let leaf_id = index
        .find_leaf_for_key_lower_bound(&IndexKey::from(vec![KeyPart::String(token.into())]))
        .unwrap();
    let page = index.read_page(leaf_id).unwrap();
    let IndexPage::Leaf(leaf) = page.as_ref() else {
        panic!("leaf required")
    };
    assert!(
        matches!(leaf.entries.last().unwrap().key.0.first(),Some(KeyPart::String(last)) if last.as_str()>token)
    );
    let next = leaf.next_leaf.unwrap();
    drop(
        index
            .buffer_pool
            .pin_page_with_intent(index.page_key(next), AccessIntent::Scan)
            .unwrap(),
    );
    let frame = index
        .buffer_pool
        .lookup_frame(&index.page_key(next))
        .unwrap()
        .unwrap();
    frame.usage_count.store(0, Ordering::Relaxed);
    let mut rows = Vec::new();
    index
        .fulltext_scan_token(token, |id, payload| {
            assert_eq!(payload.len(), 320);
            rows.push(id);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rows,
        vec![RowId {
            page_id: 1,
            slot_id: 0
        }]
    );
    assert_eq!(
        frame.usage_count.load(Ordering::Relaxed),
        0,
        "an out-of-range sibling must not be promoted by read-ahead"
    );
    rows.clear();
    index
        .fulltext_scan_prefix(token, Some("token-0101"), |_, id, _| {
            rows.push(id);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rows,
        vec![RowId {
            page_id: 1,
            slot_id: 0
        }]
    );
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 0);
    assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
}

#[test]
fn read_ahead_fulltext_spans_leaves_and_reopens_cold() {
    let tokens = [
        ("alpha", 500),
        ("alphabet", 75),
        ("beta", 75),
        ("é", 50),
        ("éb", 50),
        ("ê", 50),
    ];
    let mut entries = Vec::new();
    for (token, count) in tokens {
        for _ in 0..count {
            let id = entries.len();
            entries.push(entry(token, id));
        }
    }
    let (dir, index) = fixture(entries);
    index.flush().unwrap();
    drop(index);
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new_with_options(
        32,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        8,
    );
    let index = IndexFile::open_with_durability(
        dir.path().join("read-ahead.idx"),
        log,
        DurabilityMode::Strict,
        pool,
    )
    .unwrap();
    for _ in 0..2 {
        let mut exact = Vec::new();
        index
            .fulltext_scan_token("alpha", |id, _| {
                exact.push(id);
                Ok(())
            })
            .unwrap();
        assert_eq!(exact.len(), 500);
        let mut prefix = Vec::new();
        index
            .fulltext_scan_prefix("alph", Some("alpi"), |_, id, _| {
                prefix.push(id);
                Ok(())
            })
            .unwrap();
        assert_eq!(prefix.len(), 575);
        let mut unicode = Vec::new();
        index
            .fulltext_scan_prefix("é", Some("ê"), |_, id, _| {
                unicode.push(id);
                Ok(())
            })
            .unwrap();
        assert_eq!(unicode.len(), 100);
        assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
    }
}

#[test]
fn read_ahead_cache_probe_preserves_recency_and_releases_pins() {
    let (_dir, index) = fixture(vec![entry("alpha", 0)]);
    let pool = &index.buffer_pool;
    let key = index.page_key(index.root_page.load(Ordering::Acquire));
    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::Scan)
            .unwrap(),
    );
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    frame.usage_count.store(0, Ordering::Relaxed);
    let before = pool.buffer_stats().hits;
    pool.with_cached_prefetch_bytes(&key, |bytes| {
        assert!(!bytes.is_empty());
        Ok(())
    })
    .unwrap();
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 0);
    assert_eq!(pool.buffer_stats().hits, before);
    let failed = pool.with_cached_prefetch_bytes(&key, |_| {
        Err::<(), _>(error(crate::error::catalog::ER_GET_ERRNO, "probe error"))
    });
    assert!(failed.is_err());
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = pool.with_cached_prefetch_bytes(&key, |_| -> Result<(), QueryError> {
            panic!("probe unwind")
        });
    }));
    assert!(unwound.is_err());
    assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
    pool.with_cached_bytes(&key, |_| Ok(())).unwrap();
    assert!(frame.usage_count.load(Ordering::Relaxed) > 0);
}
