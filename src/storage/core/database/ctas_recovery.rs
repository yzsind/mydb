// Crash-safe CREATE TABLE ... AS SELECT intent handling.
//
// The target schema is kept out of the durable data dictionary while the
// SELECT rows are loaded.  A durable intent carries the schema and load txn
// id, allowing startup to temporarily publish the schema for WAL undo/redo and
// then either finish the catalog commit (load committed) or remove the files
// (load rolled back).

const CTAS_INTENT_DIR: &str = "ddl/ctas";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CtasIntent {
    txn_id: u64,
    load_committed: bool,
    table: TableSchema,
    files: Vec<String>,
    vector_files: Vec<String>,
}

impl Database {
    pub(crate) fn write_ctas_intent(
        &self,
        table_name: &str,
        txn_id: u64,
    ) -> Result<Option<PathBuf>, QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let table = self.table(table_name)?;
        let drop = self.build_drop_table_intent(&table);
        let intent = CtasIntent {
            txn_id,
            load_committed: false,
            table: table.as_ref().clone(),
            files: drop.files,
            vector_files: drop.vector_files,
        };
        let dir = self.data_dir.join(CTAS_INTENT_DIR);
        std::fs::create_dir_all(&dir).map_err(io_err)?;
        let path = dir.join(format!("{}.json", table.id));
        Self::write_ctas_intent_file(&path, &intent)?;
        crate::platform::fs::sync_path(&dir).map_err(io_err)?;
        Ok(Some(path))
    }

    fn write_ctas_intent_file(path: &Path, intent: &CtasIntent) -> Result<(), QueryError> {
        let bytes = serde_json::to_vec(intent).map_err(|e| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("serialize CTAS intent: {e}"),
            )
        })?;
        let tmp = path.with_extension("json.tmp");
        crate::util::file::write_staged_file(path, &tmp, &bytes, true).map_err(io_err)
    }

    pub(crate) fn mark_ctas_intent_committed(
        &self,
        path: Option<&PathBuf>,
    ) -> Result<(), QueryError> {
        let Some(path) = path else { return Ok(()) };
        let bytes = std::fs::read(path).map_err(io_err)?;
        let mut intent: CtasIntent = serde_json::from_slice(&bytes).map_err(|e| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("read CTAS intent: {e}"),
            )
        })?;
        intent.load_committed = true;
        Self::write_ctas_intent_file(path, &intent)
    }

    pub(crate) fn remove_ctas_intent(&self, path: Option<&PathBuf>) {
        if let Some(path) = path {
            let _ = std::fs::remove_file(path);
        }
    }

    fn read_ctas_intents(&self) -> Result<Vec<(PathBuf, CtasIntent)>, QueryError> {
        let dir = self.data_dir.join(CTAS_INTENT_DIR);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_err(e)),
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(io_err)?;
            let intent = serde_json::from_slice(&bytes).map_err(|e| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("invalid CTAS intent '{}': {e}", path.display()),
                )
            })?;
            out.push((path, intent));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Publish intent schemas in memory before incomplete-transaction undo.
    /// This supplies the row codec for CTAS heap records even though the DD
    /// row was intentionally deferred until after the load commit.
    pub(crate) fn prepare_ctas_recovery(&self) -> Result<(), QueryError> {
        let intents = self.read_ctas_intents()?;
        if intents.is_empty() {
            return Ok(());
        }
        let mut catalog = self.catalog_write()?;
        for (_, mut intent) in intents {
            intent.table.build_column_index();
            let key = table_key(&intent.table.database, &intent.table.name);
            catalog
                .tables
                .entry(key)
                .or_insert_with(|| Arc::new(intent.table));
        }
        build_fk_runtime(&mut catalog);
        Ok(())
    }

    /// Resolve every prepared intent after WAL undo has classified its load
    /// transaction. Committed loads publish one incremental DD object;
    /// uncommitted loads are unpublished and their files are reclaimed.
    pub(crate) fn finish_ctas_recovery(&self) -> Result<(), QueryError> {
        let intents = self.read_ctas_intents()?;
        let mut commit_catalog = false;
        let mut completed = Vec::new();
        for (path, intent) in intents {
            let committed = intent.load_committed
                || self
                    .txns_read()?
                    .status(intent.txn_id)
                    .is_some_and(|status| status == TxnStatus::Committed);
            if committed {
                log_message(&format!(
                    "CTAS recovery: completing committed {}.{}",
                    intent.table.database, intent.table.name
                ));
                commit_catalog = true;
                self.note_catalog_table(intent.table.id);
            } else {
                log_message(&format!(
                    "CTAS recovery: rolling back incomplete {}.{}",
                    intent.table.database, intent.table.name
                ));
                let key = table_key(&intent.table.database, &intent.table.name);
                {
                    let mut catalog = self.catalog_write()?;
                    if catalog
                        .tables
                        .get(&key)
                        .is_some_and(|table| table.id == intent.table.id)
                    {
                        catalog.tables.remove(&key);
                        build_fk_runtime(&mut catalog);
                    }
                }
                let cleaned = self.purge_dropped_table_files(&DropTableIntent {
                    retired_index: None,
                    table_id: intent.table.id,
                    had_auto_increment: intent.table.auto_increment_column_index().is_some(),
                    database: intent.table.database.clone(),
                    table_name: intent.table.name.clone(),
                    files: intent.files,
                    vector_files: intent.vector_files,
                });
                if !cleaned {
                    continue;
                }
            }
            completed.push(path);
        }
        if commit_catalog {
            self.commit_catalog_txn_now()?;
        }
        // Never discard the only recoverable descriptor before the DD commit.
        for path in completed {
            std::fs::remove_file(&path).map_err(io_err)?;
            if let Some(parent) = path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
        }
        Ok(())
    }
}
