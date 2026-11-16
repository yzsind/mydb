//! Build one clean data-directory seed for a cargo-nextest run.
//!
//! nextest launches every Rust test in a separate process. Bootstrapping the
//! data dictionary in every process is both redundant and highly contended,
//! so the setup script runs this binary once and exposes the resulting clean
//! directory through `MYDB_TEST_SEED_DIR`.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use mydb::engine::StorageQueryEngine;

type EmptyDictionaryIndexes = Vec<(u64, Vec<PathBuf>)>;

#[allow(dead_code)] // Also included as a module by the library test harness.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prepare_seed(|_| Ok(Vec::new()))
}

// The library test harness supplies its private dictionary inspection hook.
// Standalone tool targets link the normal library, even with cfg(test).
pub(crate) fn prepare_seed<F>(empty_dictionary_indexes: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&StorageQueryEngine) -> Result<EmptyDictionaryIndexes, mydb::types::QueryError>,
{
    let nextest_env = env::var_os("NEXTEST_ENV").ok_or("NEXTEST_ENV is not set")?;
    let run_id = env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| "manual".to_string());
    let root = seed_store_root()?;
    let seed_dir = root.join(run_id).join("seed");

    if let Some(run_dir) = seed_dir.parent() {
        let _ = fs::remove_dir_all(run_dir);
    }
    fs::create_dir_all(&seed_dir)?;

    // Keep seed creation deterministic and cheap regardless of developer
    // shell settings. The on-disk image is a normal clean strict-mode data
    // directory and can subsequently be opened by any test StorageConfig.
    for (key, value) in [
        ("MYDB_DURABILITY", "strict"),
        ("MYDB_BUFFER_POOL_PAGES", "512"),
        ("MYDB_BUFFER_POOL_IO_THREADS", "1"),
        ("MYDB_BUFFER_POOL_PREFETCH_PAGES", "0"),
        ("MYDB_GROUP_COMMIT_US", "0"),
        ("MYDB_CHECKPOINT_INTERVAL_MS", "60000"),
        ("MYDB_UNDO_RETENTION_MS", "60000"),
    ] {
        env::set_var(key, value);
    }

    // The setup test must bootstrap real DD heaps rather than cloning a
    // fixture inherited from an outer test invocation.
    env::set_var("MYDB_TEST_DISABLE_SEED", "1");
    let engine = StorageQueryEngine::open(&seed_dir).map_err(query_error)?;
    engine.shutdown().map_err(query_error)?;
    drop(engine);

    // SQL-only memory fixtures also reuse the decoded, immutable bootstrap
    // catalog as a normal reopen sees it (including canonical grant order).
    // Their private DD heaps still back every subsequent mutation.
    let engine = StorageQueryEngine::open(&seed_dir).map_err(query_error)?;
    let memory_catalog = seed_dir.parent().unwrap().join("memory-catalog.json");
    fs::write(
        &memory_catalog,
        serde_json::to_vec(&engine.db_handle().catalog())?,
    )?;
    let empty_indexes = empty_dictionary_indexes(&engine).map_err(query_error)?;
    engine.shutdown().map_err(query_error)?;
    drop(engine);

    // Logs and the advisory-lock inode are runtime artifacts. Recreated on
    // open, they do not belong in each test's private clone.
    let _ = fs::remove_dir_all(seed_dir.join("system/logs"));
    let _ = fs::remove_file(seed_dir.join("system/mydb.lock"));

    // Keep all heap allocation metadata and populated dictionary indexes.
    // Empty indexes are initialized on first use in private memory. The
    // ordinary index-handle cache serializes root creation and publication.
    let memory_seed_dir = seed_dir.parent().unwrap().join("memory-seed");
    copy_seed_tree(&seed_dir, &memory_seed_dir)?;
    if !empty_indexes.is_empty() {
        compact_memory_seed(&memory_seed_dir, &empty_indexes)?;
    }
    // Memory fixtures reconstruct undo and temporal state from WAL and do
    // not read the persistent undo segments or temporal ledgers. Avoid
    // cloning and unlinking these unused files in every isolated fixture.
    // The durable seed above keeps them for recovery/backup tests.
    fs::remove_dir_all(memory_seed_dir.join("system/undo"))?;
    fs::remove_file(memory_seed_dir.join("system/commit-ledger.v1"))?;
    fs::remove_file(memory_seed_dir.join("system/history-index.v1"))?;
    // Empty dictionary heaps contain no seed state. The normal heap opener
    // creates them on first access; cloning them now only adds filesystem
    // work, including for dictionary families the test never reads.
    for entry in fs::read_dir(memory_seed_dir.join("system/catalog"))? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() && metadata.len() == 0 {
            fs::remove_file(entry.path())?;
        }
    }
    let memory_seed_dir = fs::canonicalize(memory_seed_dir)?;
    append_nextest_env(
        Path::new(&nextest_env),
        "MYDB_TEST_MEMORY_SEED_DIR",
        &memory_seed_dir,
    )?;
    append_nextest_env(
        Path::new(&nextest_env),
        "MYDB_TEST_MEMORY_CATALOG",
        &fs::canonicalize(memory_catalog)?,
    )?;

    // tempfile uses TMPDIR on Unix. Keep all per-test databases and scratch
    // files under the repository's designated temporary-data root.
    let tmp_dir = seed_dir.parent().unwrap().join("tmp");
    fs::create_dir_all(&tmp_dir)?;
    append_nextest_env(
        Path::new(&nextest_env),
        "TMPDIR",
        &fs::canonicalize(&tmp_dir)?,
    )?;
    append_nextest_env(
        Path::new(&nextest_env),
        "MYDB_TEST_TMP_DIR",
        &fs::canonicalize(&tmp_dir)?,
    )?;
    #[cfg(windows)]
    for key in ["TEMP", "TMP"] {
        append_nextest_env(
            Path::new(&nextest_env),
            key,
            &seed_dir.parent().unwrap().join("tmp"),
        )?;
    }

    let seed_dir = fs::canonicalize(seed_dir)?;
    append_nextest_env(Path::new(&nextest_env), "MYDB_TEST_SEED_DIR", &seed_dir)?;
    append_nextest_env(
        Path::new(&nextest_env),
        "MYDB_TEST_SEED_CATALOG_SOURCE",
        &seed_dir,
    )?;
    append_nextest_env(
        Path::new(&nextest_env),
        "MYDB_TEST_SEED_CATALOG",
        &fs::canonicalize(seed_dir.parent().unwrap().join("memory-catalog.json"))?,
    )?;
    // Functional backup cases do not measure a one-second admission SLA.
    if env::var_os("MYDB_BACKUP_FREEZE_BUDGET_MS").is_none() {
        append_nextest_value(
            Path::new(&nextest_env),
            "MYDB_BACKUP_FREEZE_BUDGET_MS",
            "30000",
        )?;
    }
    Ok(())
}

fn copy_seed_tree(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_seed_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

pub(crate) fn compact_memory_seed(
    root: &Path,
    empty_indexes: &[(u64, Vec<PathBuf>)],
) -> io::Result<()> {
    let catalog = root.join("system/catalog");
    for relative in empty_indexes.iter().flat_map(|(_, paths)| paths) {
        let path = root.join(relative);
        fs::remove_file(&path)?;
        let mut parent = path.parent();
        while let Some(directory) = parent.filter(|directory| *directory != catalog) {
            match fs::remove_dir(directory) {
                Ok(()) => parent = directory.parent(),
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::NotFound
                    ) =>
                {
                    break
                }
                Err(err) => return Err(err),
            }
        }
    }
    Ok(())
}

fn query_error(error: mydb::types::QueryError) -> io::Error {
    io::Error::other(format!("MySQL error {}: {}", error.code(), error.message()))
}

fn seed_store_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Keep generated database fixtures outside the workspace, as required by
    // the repository's test-data policy. Nextest still supplies the run UUID.
    Ok(env::var_os("MYDB_TEST_SEED_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/mydb/nextest-seeds")))
}

fn append_nextest_env(path: &Path, key: &str, value: &Path) -> io::Result<()> {
    let value = value
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 seed path"))?;
    append_nextest_value(path, key, value)
}

fn append_nextest_value(path: &Path, key: &str, value: &str) -> io::Result<()> {
    let mut file = fs::OpenOptions::new().append(true).open(path)?;
    writeln!(file, "{key}={value}")
}
