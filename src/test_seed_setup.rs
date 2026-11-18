//! Library-only nextest fixture setup using private test storage helpers.

#[path = "bin/test_seed.rs"]
mod seed;

pub(crate) use seed::{compact_memory_seed, empty_heap_templates_dir};

#[test]
#[ignore = "fixture generator invoked explicitly by the nextest setup script"]
fn prepare_nextest_seed() {
    seed::prepare_seed(|engine| engine.db_handle().empty_dictionary_seed_indexes())
        .expect("prepare nextest database fixtures");
}
