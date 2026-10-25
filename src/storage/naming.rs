use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::schema::{Catalog, DatabaseSchema, CATALOG_VERSION, DEFAULT_DATABASE};

pub fn normalize_name(name: &str) -> String {
    if name.bytes().all(|b| !b.is_ascii_uppercase()) {
        name.to_string()
    } else {
        name.to_ascii_lowercase()
    }
}

fn is_already_normalized(name: &str) -> bool {
    name.bytes().all(|b| !b.is_ascii_uppercase())
}

/// Allocation-free `normalize_name` for lookup paths: already-lowercase
/// names (the common case on hot per-statement cache probes) borrow.
pub(crate) fn normalize_name_cow(name: &str) -> std::borrow::Cow<'_, str> {
    if is_already_normalized(name) {
        std::borrow::Cow::Borrowed(name)
    } else {
        std::borrow::Cow::Owned(name.to_ascii_lowercase())
    }
}

pub(crate) fn catalog_version() -> u32 {
    CATALOG_VERSION
}

pub(crate) fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn default_database_name() -> String {
    DEFAULT_DATABASE.to_string()
}

pub fn table_key(database: &str, table: &str) -> String {
    if is_already_normalized(database) && is_already_normalized(table) {
        let mut key = String::with_capacity(database.len() + 1 + table.len());
        key.push_str(database);
        key.push('.');
        key.push_str(table);
        key
    } else {
        format!("{}.{}", normalize_name(database), normalize_name(table))
    }
}

pub fn routine_key(database: &str, kind: &str, name: &str) -> String {
    format!(
        "{}:{}:{}",
        normalize_name(database),
        kind.to_ascii_lowercase(),
        normalize_name(name)
    )
}

pub fn trigger_key(database: &str, name: &str) -> String {
    format!("{}:{}", normalize_name(database), normalize_name(name))
}

pub fn event_key(database: &str, name: &str) -> String {
    format!("{}:{}", normalize_name(database), normalize_name(name))
}

pub(crate) fn normalize_table_key(current_database: &str, name: &str) -> String {
    if let Some(dot) = name.find('.') {
        let db = &name[..dot];
        let tbl = &name[dot + 1..];
        if is_already_normalized(db) && is_already_normalized(tbl) {
            return name.to_string();
        }
        return table_key(db, tbl);
    }
    if is_already_normalized(current_database) && is_already_normalized(name) {
        let mut key = String::with_capacity(current_database.len() + 1 + name.len());
        key.push_str(current_database);
        key.push('.');
        key.push_str(name);
        return key;
    }
    let (database, table) = split_table_name(current_database, name);
    table_key(&database, &table)
}

pub fn split_table_name(current_database: &str, name: &str) -> (String, String) {
    let mut parts = name
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty());
    let first = parts.next().unwrap_or(name);
    let second = parts.next();
    match second {
        Some(table) => (normalize_name(first), normalize_name(table)),
        None => (normalize_name(current_database), normalize_name(first)),
    }
}

pub fn is_system_database(name: &str) -> bool {
    matches!(
        normalize_name(name).as_str(),
        "information_schema" | "mysql" | "performance_schema" | "sys"
    )
}

pub(crate) fn register_builtin_databases(catalog: &mut Catalog) {
    catalog
        .databases
        .entry(DEFAULT_DATABASE.to_string())
        .or_insert(DatabaseSchema {
            name: DEFAULT_DATABASE.to_string(),
            system: false,
            charset: crate::charset::DEFAULT_CHARSET.to_string(),
            collation: crate::charset::DEFAULT_COLLATION.to_string(),
            created_ms: 0,
            updated_ms: 0,
        });
    for name in ["information_schema", "mysql", "performance_schema", "sys"] {
        catalog
            .databases
            .entry(name.to_string())
            .or_insert(DatabaseSchema {
                name: name.to_string(),
                system: true,
                charset: crate::charset::DEFAULT_CHARSET.to_string(),
                collation: crate::charset::DEFAULT_COLLATION.to_string(),
                created_ms: 0,
                updated_ms: 0,
            });
    }
}

// ---------------------------------------------------------------------------
// On-disk path layout (current version — see `CATALOG_VERSION` history in
// `src/storage/schema.rs`):
//
//   <data_dir>/
//     system/
//       wal/          (WAL segments — formerly `redo/`)
//       undo/         (undo tablespace)
//       replication/  (mysql + mydb-native replication state)
//       tmp/          (spill, temp tables, temp indexes)
//         temp_tables/, temp_indexes/, sort-*/, grace-*/
//     objects/
//       tables/<storage_id>/<generation>.<heap|lob|zm>
//       indexes/<storage_id>/<generation>/<index_id>.<idx|hnsw|hvec>
//
// Every persistent table/index path is independent of schema, table and index
// names. RENAME TABLE and RENAME INDEX are therefore catalog-only operations.
// ---------------------------------------------------------------------------

/// Fold any character outside `[A-Za-z0-9_]` to `_`. Filesystem-safe
/// encoding for SQL identifiers used as on-disk file or directory name
/// components.
pub fn sanitize_fs_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

/// Root for everything that is not tied to a specific user database.
pub fn system_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("system")
}

pub fn system_wal_dir(data_dir: &Path) -> PathBuf {
    system_dir(data_dir).join("wal")
}

pub fn system_undo_dir(data_dir: &Path) -> PathBuf {
    system_dir(data_dir).join("undo")
}

pub fn system_replication_dir(data_dir: &Path) -> PathBuf {
    system_dir(data_dir).join("replication")
}

pub fn system_tmp_dir(data_dir: &Path) -> PathBuf {
    if let Some(runtime) = crate::storage::image_io::runtime_dir(data_dir) {
        return runtime.join("tmp");
    }
    system_dir(data_dir).join("tmp")
}

pub fn system_catalog_dir(data_dir: &Path) -> PathBuf {
    system_dir(data_dir).join("catalog")
}

pub fn objects_root(data_dir: &Path) -> PathBuf {
    data_dir.join("objects")
}

pub fn object_shard_dir(data_dir: &Path, storage_id: u64) -> PathBuf {
    objects_root(data_dir).join(format!("{:02x}", storage_id & 0xff))
}

/// Convert an absolute storage object path into the stable path carried by
/// WAL. Object handles do not retain the data-directory root, so locate the
/// last well-known root component instead of keeping a fixed number of tail
/// components (which would accidentally include a short temp-dir name for
/// `system/catalog/<id>.*`).
pub(crate) fn wal_relative_object_path(path: &Path) -> String {
    let segments = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let start = segments
        .iter()
        .rposition(|segment| matches!(segment.as_str(), "schemas" | "objects" | "system"))
        .unwrap_or_else(|| segments.len().saturating_sub(4));
    segments[start..].join("/")
}

fn is_dd_storage_id(storage_id: u64) -> bool {
    storage_id >= crate::storage::data_dictionary::DD_TABLE_ID_BASE
        && storage_id < crate::storage::data_dictionary::DD_TABLE_ID_BASE + 4096
}

pub fn temp_tables_dir(data_dir: &Path) -> PathBuf {
    system_tmp_dir(data_dir).join("temp_tables")
}

pub fn temp_indexes_dir(data_dir: &Path) -> PathBuf {
    system_tmp_dir(data_dir).join("temp_indexes")
}

/// Root containing one subdirectory per database.
pub fn schemas_root(data_dir: &Path) -> PathBuf {
    data_dir.join("schemas")
}

/// `<data_dir>/schemas/<db>/` — one database's on-disk home.
pub fn schema_dir(data_dir: &Path, db_name: &str) -> PathBuf {
    schemas_root(data_dir).join(sanitize_fs_name(&normalize_name(db_name)))
}

pub fn schema_tables_dir(data_dir: &Path, db_name: &str) -> PathBuf {
    schema_dir(data_dir, db_name).join("tables")
}

pub fn schema_indexes_dir(data_dir: &Path, db_name: &str) -> PathBuf {
    schema_dir(data_dir, db_name).join("indexes")
}

/// Absolute path for a table/storage-leaf sidecar. Database and table names
/// are intentionally ignored; the monotonic storage id is authoritative.
pub fn table_object_path(
    data_dir: &Path,
    _db_name: &str,
    _table_name: &str,
    table_id: u64,
    ext: &str,
) -> PathBuf {
    table_object_generation_path(data_dir, table_id, 1, ext)
}

pub fn table_object_generation_path(
    data_dir: &Path,
    table_id: u64,
    generation: u64,
    ext: &str,
) -> PathBuf {
    if is_dd_storage_id(table_id) {
        system_catalog_dir(data_dir).join(format!("{table_id}.{ext}"))
    } else {
        data_dir
            .join("objects/tables")
            .join(table_id.to_string())
            .join(format!("{generation}.{ext}"))
    }
}

/// Canonical name-independent path for one physical index generation.
pub fn index_object_path_by_id(
    data_dir: &Path,
    table_id: u64,
    generation: u64,
    index_id: u64,
    ext: &str,
) -> PathBuf {
    if is_dd_storage_id(table_id) {
        system_catalog_dir(data_dir)
            .join("indexes")
            .join(table_id.to_string())
            .join(generation.to_string())
            .join(format!("{index_id}.{ext}"))
    } else {
        data_dir
            .join("objects/indexes")
            .join(table_id.to_string())
            .join(generation.to_string())
            .join(format!("{index_id}.{ext}"))
    }
}

/// Absolute path for one of a table's index files (`.idx` for B-tree /
/// FULLTEXT / SPATIAL, `.hnsw` for vector).
pub fn index_object_path(
    data_dir: &Path,
    _db_name: &str,
    _table_name: &str,
    table_id: u64,
    index_name: &str,
    ext: &str,
) -> PathBuf {
    let file = format!("{table_id}.index_{}.{ext}", sanitize_fs_name(index_name));
    if is_dd_storage_id(table_id) {
        system_catalog_dir(data_dir).join(file)
    } else {
        object_shard_dir(data_dir, table_id).join(file)
    }
}

#[cfg(test)]
/// Relative path of a table sidecar, with `/` separators, suitable for WAL.
pub fn relative_table_object_path(
    _db_name: &str,
    _table_name: &str,
    table_id: u64,
    ext: &str,
) -> String {
    relative_table_object_generation_path(table_id, 1, ext)
}

#[cfg(test)]
pub fn relative_table_object_generation_path(table_id: u64, generation: u64, ext: &str) -> String {
    if is_dd_storage_id(table_id) {
        format!("system/catalog/{table_id}.{ext}")
    } else {
        format!("objects/tables/{table_id}/{generation}.{ext}")
    }
}

/// Classified result of parsing an on-disk object file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectFileInfo {
    pub kind: ObjectFileKind,
    pub table_id: u64,
    /// `"data"` for table sidecars or the index name for `.idx`/`.hnsw`.
    pub part: String,
    /// Extension (no leading `.`), e.g. `"heap"`, `"lob"`, `"idx"`.
    pub ext: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectFileKind {
    Table,
    Index,
}

/// Parse a unified-format object file name (without directory). Returns
/// `None` on any structural mismatch — callers fall back to legacy
/// handling or skip the file.
pub fn parse_object_file_name(file_name: &str, kind: ObjectFileKind) -> Option<ObjectFileInfo> {
    let (stem, ext) = file_name.rsplit_once('.')?;
    if let Some((id, index_name)) = stem.split_once(".index_") {
        let table_id = id.parse().ok()?;
        return (kind == ObjectFileKind::Index).then(|| ObjectFileInfo {
            kind,
            table_id,
            part: index_name.to_string(),
            ext: ext.to_string(),
        });
    }
    if let Ok(table_id) = stem.parse::<u64>() {
        return (kind == ObjectFileKind::Table).then(|| ObjectFileInfo {
            kind,
            table_id,
            part: "data".to_string(),
            ext: ext.to_string(),
        });
    }
    // Need `<name>__<id>__<part>` with three underscore-pair segments.
    // Split from the right so a `__` inside the sanitized table_name
    // does not steal the id segment.
    let (left, part) = stem.rsplit_once("__")?;
    let (_name, id_str) = left.rsplit_once("__")?;
    let table_id: u64 = id_str.parse().ok()?;
    Some(ObjectFileInfo {
        kind,
        table_id,
        part: part.to_string(),
        ext: ext.to_string(),
    })
}

/// Parse a relative WAL `path` string like
/// `schemas/<db>/(tables|indexes)/<name>__<id>__<part>.<ext>` and
/// return the classified info plus the database name. Returns `None`
/// for any other path shape (legacy / non-object files).
pub fn parse_relative_object_path(rel_path: &str) -> Option<(String, ObjectFileInfo)> {
    let parts = rel_path.split('/').collect::<Vec<_>>();
    if let ["objects", "tables", table_id, file] = parts.as_slice() {
        let table_id = table_id.parse().ok()?;
        let (generation, ext) = file.rsplit_once('.')?;
        generation.parse::<u64>().ok()?;
        return Some((
            String::new(),
            ObjectFileInfo {
                kind: ObjectFileKind::Table,
                table_id,
                part: "data".to_string(),
                ext: ext.to_string(),
            },
        ));
    }
    if let ["objects", "indexes", table_id, generation, directory, file] = parts.as_slice() {
        let table_id = table_id.parse().ok()?;
        generation.parse::<u64>().ok()?;
        let index_id = directory.strip_suffix(".vector")?;
        index_id.parse::<u64>().ok()?;
        let (physical, ext) = file.rsplit_once('.')?;
        physical.parse::<u64>().ok()?;
        if ext != "hvec" && ext != "hnsw" { return None; }
        return Some((String::new(), ObjectFileInfo { kind: ObjectFileKind::Index,
            table_id, part: index_id.to_string(), ext: ext.to_string() }));
    }
    if let ["objects", "indexes", table_id, generation, file] = parts.as_slice() {
        let table_id = table_id.parse().ok()?;
        generation.parse::<u64>().ok()?;
        let (index_id, ext) = file.rsplit_once('.')?;
        index_id.parse::<u64>().ok()?;
        return Some((
            String::new(),
            ObjectFileInfo {
                kind: ObjectFileKind::Index,
                table_id,
                part: index_id.to_string(),
                ext: ext.to_string(),
            },
        ));
    }
    if let ["system", "catalog", "indexes", table_id, generation, file] = parts.as_slice() {
        let table_id = table_id.parse().ok()?;
        generation.parse::<u64>().ok()?;
        let (index_id, ext) = file.rsplit_once('.')?;
        index_id.parse::<u64>().ok()?;
        return Some((
            "mysql".to_string(),
            ObjectFileInfo {
                kind: ObjectFileKind::Index,
                table_id,
                part: index_id.to_string(),
                ext: ext.to_string(),
            },
        ));
    }
    if parts.len() == 3 && parts[0] == "objects" {
        let file = parts[2];
        let kind = if file.contains(".index_") {
            ObjectFileKind::Index
        } else {
            ObjectFileKind::Table
        };
        return parse_object_file_name(file, kind).map(|info| (String::new(), info));
    }
    if matches!(parts.as_slice(), ["system", "catalog", _]) {
        let file = parts[2];
        let kind = if file.contains(".index_") {
            ObjectFileKind::Index
        } else {
            ObjectFileKind::Table
        };
        return parse_object_file_name(file, kind).map(|info| ("mysql".to_string(), info));
    }
    let ["schemas", db, bucket, file_name] = parts.as_slice() else {
        return None;
    };
    let kind = match bucket {
        &"tables" => ObjectFileKind::Table,
        &"indexes" => ObjectFileKind::Index,
        _ => return None,
    };
    let info = parse_object_file_name(file_name, kind)?;
    Some(((*db).to_string(), info))
}

pub(crate) fn migrate_catalog(mut catalog: Catalog) -> Catalog {
    // v13 adds the persistent MySQL-compatible auth catalog. mydb's
    // development storage format intentionally rejects older catalogs.
    //
    // v12 introduced schema-driven row format v3 (compact per-type binary
    // encoding for heap rows; varint+NULL-bitmap value section already
    // landed in v11/format v2). Pre-v12 row payloads are not in-place
    // upgradable — the catalog format itself also did not gain any new
    // fields — so we reject any other version outright. As CLAUDE.md notes,
    // mydb does not preserve compatibility with old local development
    // storage formats: start with a fresh MYDB_DATA_DIR (or rebuild the
    // data) when the version changes.
    assert!(
        catalog.version == CATALOG_VERSION,
        "catalog version {} is incompatible with mydb development storage format version {}; use a new data directory or rebuild data",
        catalog.version,
        CATALOG_VERSION
    );
    register_builtin_databases(&mut catalog);
    // Bootstrap only a brand-new authority. An explicitly dropped root (or
    // revoked root privilege) must never reappear after restart.
    if catalog.auth.epoch == 1 && catalog.auth.accounts.is_empty() {
        std::sync::Arc::make_mut(&mut catalog.auth).ensure_bootstrap_root();
    } else {
        std::sync::Arc::make_mut(&mut catalog.auth).rebuild_runtime_indexes();
    }
    for table in catalog.tables.values_mut() {
        if let Some(table) = std::sync::Arc::get_mut(table) {
            // Crash recovery: an index that finds itself in `Building`
            // state on startup means the previous process exited
            // mid-ADD-INDEX without flipping to Public. The on-disk
            // index file (if any) is partially populated and missing
            // pre-DDL rows. The simplest sound recovery is to drop
            // such entries from the catalog so the next ADD INDEX
            // starts from a clean slate; the user re-issues the DDL.
            // Index files tied to dropped entries are left for the
            // existing index-cache cleanup path on next add_index /
            // drop_index.
            table
                .indexes
                .retain(|idx| idx.state != crate::storage::schema::IndexState::Building);
            table.pending_primary_key = None;
            table.build_column_index();
        }
    }
    // Rebuild foreign-key runtime caches (child_col_idx + the reverse
    // `fk_referenced_by` index) now that every table's column maps are
    // ready. Must run after the build_column_index loop above.
    super::schema::build_fk_runtime(&mut catalog);
    catalog.version = CATALOG_VERSION;
    catalog
}
