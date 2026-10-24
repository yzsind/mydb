// Crash-safe RENAME TABLE physical-path journal.

const RENAME_TABLE_INTENT_DIR: &str = "ddl/rename_table";

fn rename_table_intent_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(RENAME_TABLE_INTENT_DIR)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RenameTableIntent {
    version: u32,
    table_id: u64,
    old_database: String,
    old_table: String,
    new_database: String,
    new_table: String,
    /// Relative-to-data-dir source/destination pairs.
    renames: Vec<(String, String)>,
}

impl Database {
    pub(crate) fn remove_rename_table_intent(&self, path: Option<&PathBuf>) {
        if let Some(path) = path {
            let _ = std::fs::remove_file(path);
        }
    }

    fn apply_rename_pairs(
        &self,
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<(), QueryError> {
        for (from, to) in pairs {
            let from = self.data_dir.join(from);
            let to = self.data_dir.join(to);
            if from == to {
                continue;
            }
            if !from.exists() {
                continue;
            }
            if to.exists() {
                return Err(error(
                    crate::error::catalog::ER_ERROR_ON_RENAME,
                    format!(
                        "rename recovery found both source '{}' and destination '{}'",
                        from.display(),
                        to.display()
                    ),
                ));
            }
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent).map_err(io_err)?;
            }
            std::fs::rename(from, to).map_err(io_err)?;
        }
        Ok(())
    }

    pub(crate) fn recover_rename_tables(&self) -> Result<(), QueryError> {
        let dir = rename_table_intent_dir(&self.data_dir);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_err(err)),
        };
        let mut paths = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            let bytes = std::fs::read(&path).map_err(io_err)?;
            let intent: RenameTableIntent = match serde_json::from_slice(&bytes) {
                Ok(intent) => intent,
                Err(err) => {
                    // A legacy interrupted write must not wedge startup.
                    // Retain the evidence; never execute partially decoded paths.
                    eprintln!(
                        "warning: skipping corrupt rename-table intent '{}': {err}",
                        path.display()
                    );
                    continue;
                }
            };
            if intent.version != 1 {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("unsupported rename-table intent version {}", intent.version),
                ));
            }
            let committed = self.table_by_id(intent.table_id).is_some_and(|table| {
                table.database.eq_ignore_ascii_case(&intent.new_database)
                    && table.name.eq_ignore_ascii_case(&intent.new_table)
            });
            if committed {
                self.apply_rename_pairs(intent.renames.clone())?;
            } else {
                self.apply_rename_pairs(
                    intent
                        .renames
                        .iter()
                        .rev()
                        .map(|(from, to)| (to.clone(), from.clone())),
                )?;
            }
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }
}
