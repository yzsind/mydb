use super::*;
use crate::engine::StorageQueryEngine;
use crate::storage::{DurabilityMode, StorageConfig};

fn sorted(mut entries: Vec<(IndexKey, RowId)>) -> Vec<(IndexKey, RowId)> {
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    entries
}

#[test]
fn repair_merge_directions_preserve_snapshot_keys_across_chunk_boundaries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            ..StorageConfig::from_env()
        },
    )
    .unwrap();
    engine.execute("CREATE TABLE repair_merge (id INT PRIMARY KEY, k VARCHAR(64), n BIGINT, INDEX idx(k,n))").unwrap();
    let values = (0..96)
        .map(|i| {
            format!(
                "({i},{},{})",
                if i % 13 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'key-{}-abcdefghijklmnop'", i % 9)
                },
                i % 3
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO repair_merge VALUES {values}"))
        .unwrap();
    let db = engine.db_for_tests();
    let schema = db.snapshot_table_schema("test.repair_merge").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name == "idx")
        .unwrap();
    let mut source = db
        .range_index_entries_cursor(&schema, index, None, None)
        .unwrap();
    let physical = source.next_chunk(1000).unwrap();
    assert_eq!(physical.len(), 96);
    for repairs in [
        Vec::new(),
        sorted(vec![
            (physical[60].0.clone(), physical[0].1),
            (physical[5].0.clone(), physical[95].1),
            (physical[30].0.clone(), physical[40].1),
        ]),
        sorted(
            physical
                .iter()
                .rev()
                .map(|(key, id)| (key.clone(), *id))
                .collect(),
        ),
    ] {
        let ids: HashSet<_> = repairs.iter().map(|(_, id)| *id).collect();
        let expected = sorted(
            physical
                .iter()
                .filter(|(_, id)| !ids.contains(id))
                .cloned()
                .chain(repairs.iter().cloned())
                .collect(),
        );
        for chunk in [1, 2, 7, 31, 128] {
            let mut forward = RepairAwareForward::with_static_repairs(
                db.range_index_entries_cursor(&schema, index, None, None)
                    .unwrap(),
                repairs.clone(),
            );
            let mut reverse = RepairAwareReverse::with_static_repairs(
                db.range_index_entries_reverse_cursor(&schema, index, None, None)
                    .unwrap(),
                repairs.clone(),
            );
            assert!(forward.next_chunk(0).unwrap().is_empty());
            assert!(reverse.next_chunk(0).unwrap().is_empty());
            let mut asc = Vec::new();
            let mut desc = Vec::new();
            while !forward.is_finished() {
                let batch = forward.next_chunk(chunk).unwrap();
                assert!(batch.len() <= chunk);
                assert!(!batch.is_empty() || forward.is_finished());
                asc.extend(batch);
            }
            while !reverse.is_finished() {
                let batch = reverse.next_chunk(chunk).unwrap();
                assert!(batch.len() <= chunk);
                assert!(!batch.is_empty() || reverse.is_finished());
                desc.extend(batch);
            }
            assert_eq!(asc, expected, "chunk={chunk}");
            assert_eq!(
                desc,
                expected.iter().rev().cloned().collect::<Vec<_>>(),
                "chunk={chunk}"
            );
            assert!(forward.next_chunk(chunk).unwrap().is_empty());
            assert!(reverse.next_chunk(chunk).unwrap().is_empty());
        }
    }
}

struct MemoryCursor<const REVERSE: bool>(std::vec::IntoIter<(IndexKey, RowId)>);
impl<const REVERSE: bool> OrderedIndexCursor for MemoryCursor<REVERSE> {
    const REVERSE: bool = REVERSE;
    fn next_chunk(&mut self, rows: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        Ok(self.0.by_ref().take(rows).collect())
    }
    fn is_finished(&self) -> bool {
        self.0.len() == 0
    }
    fn resume_position(&self) -> Option<(IndexKey, RowId)> {
        None
    }
    fn rewind(&mut self, _: Option<(IndexKey, RowId)>) {
        unreachable!("a fixed directory never moves the rollback epoch")
    }
}

#[test]
fn repair_merge_moves_wide_keys_without_copying_their_allocations() {
    fn check<const REVERSE: bool>() {
        let entry = |tag: u8, slot_id| {
            (
                IndexKey::from(
                    (0..6)
                        .map(|_| KeyPart::Bytes(vec![tag; 128]))
                        .collect::<Vec<_>>(),
                ),
                RowId {
                    page_id: 1,
                    slot_id,
                },
            )
        };
        let mut physical = vec![entry(1, 1), entry(3, 2)];
        let repair = entry(2, 3);
        let allocations: HashMap<_, _> = physical
            .iter()
            .chain(std::iter::once(&repair))
            .map(|(key, id)| {
                let KeyPart::Bytes(bytes) = &key.0[0] else {
                    unreachable!()
                };
                (*id, (key.0.as_ptr() as usize, bytes.as_ptr() as usize))
            })
            .collect();
        if REVERSE {
            physical.reverse();
        }
        let mut cursor = RepairAwareCursor::with_static_repairs(
            MemoryCursor::<REVERSE>(physical.into_iter()),
            vec![repair],
        );
        let output = cursor.next_chunk(4).unwrap();
        assert_eq!(
            output.iter().map(|(_, id)| id.slot_id).collect::<Vec<_>>(),
            if REVERSE {
                vec![2, 3, 1]
            } else {
                vec![1, 3, 2]
            }
        );
        for (key, id) in output {
            let KeyPart::Bytes(bytes) = &key.0[0] else {
                unreachable!()
            };
            assert_eq!(
                (key.0.as_ptr() as usize, bytes.as_ptr() as usize),
                allocations[&id]
            );
        }
        assert!(cursor.is_finished());
    }
    check::<false>();
    check::<true>();
}

#[test]
fn repair_merge_propagates_source_errors_before_emitting_a_repair() {
    struct FailedCursor;
    impl OrderedIndexCursor for FailedCursor {
        const REVERSE: bool = false;
        fn next_chunk(&mut self, _: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
            Err(crate::error::catalog::ER_QUERY_INTERRUPTED.error("source cancelled"))
        }
        fn is_finished(&self) -> bool {
            false
        }
        fn resume_position(&self) -> Option<(IndexKey, RowId)> {
            None
        }
        fn rewind(&mut self, _: Option<(IndexKey, RowId)>) {}
    }
    let mut cursor = RepairAwareCursor::with_static_repairs(
        FailedCursor,
        vec![(
            IndexKey::from(vec![KeyPart::Int(1)]),
            RowId {
                page_id: 1,
                slot_id: 0,
            },
        )],
    );
    assert!(cursor.next_chunk(0).unwrap().is_empty());
    assert!(cursor
        .next_chunk(1)
        .unwrap_err()
        .is(crate::error::catalog::ER_QUERY_INTERRUPTED));
    assert!(!cursor.is_finished());
}
