use std::collections::BTreeMap;
use std::path::Path;

use crate::engine::auth::AuthCatalog;
use crate::storage::schema::{Catalog, CATALOG_VERSION};
use crate::types::QueryError;

use super::core::error;

/// Relative path of the obsolete single-file JSON catalog. mydb now stores
/// the catalog exclusively in the data-dictionary heap tables
/// (`tables/<id>.heap`); a data dir still carrying this file was written by
/// an older format and is rejected at startup.
pub(crate) const LEGACY_DATA_DICTIONARY_JSON: &str = "catalog/data_dictionary.json";

/// First id handed out to a user table. Dictionary tables occupy a reserved
/// high id range (see [`crate::storage::data_dictionary::DD_TABLE_ID_BASE`]),
/// so user ids start at 1 and the long-standing "first user table =
/// tables/1.heap" invariant is preserved.
pub(crate) const FIRST_USER_TABLE_ID: u64 = 1;

/// Reject data directories written by an older catalog format. mydb does not
/// preserve compatibility with old development storage; the catalog now lives
/// solely in the data-dictionary heap tables, so both the very old
/// `catalog.json` and the JSON `catalog/data_dictionary.json` are refused with
/// a clear error pointing the user at a fresh data directory.
pub(crate) fn reject_legacy_catalog_json(data_dir: &Path) -> Result<(), QueryError> {
    for legacy in ["catalog.json", LEGACY_DATA_DICTIONARY_JSON] {
        if data_dir.join(legacy).exists() {
            return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                format!(
                    "legacy JSON catalog '{legacy}' is incompatible with mydb development storage format version {CATALOG_VERSION}; the catalog now lives in the data-dictionary heap tables — use a new data directory or rebuild data"
                ),
            ));
        }
    }
    Ok(())
}

/// A minimal, unpopulated catalog at the current version.
/// [`crate::storage::naming::migrate_catalog`] turns this into the bootstrap
/// catalog (builtin schemas + `root@localhost` account) for a fresh data directory.
pub(crate) fn empty_catalog() -> Catalog {
    Catalog {
        version: CATALOG_VERSION,
        next_table_id: FIRST_USER_TABLE_ID,
        next_sequence_id: 1,
        databases: BTreeMap::new(),
        tables: BTreeMap::new(),
        views: BTreeMap::new(),
        routines: BTreeMap::new(),
        triggers: BTreeMap::new(),
        events: BTreeMap::new(),
        sequences: BTreeMap::new(),
        auth: std::sync::Arc::new(AuthCatalog::empty()),
    }
}
