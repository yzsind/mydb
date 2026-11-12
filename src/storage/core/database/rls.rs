impl Database {
    pub(crate) fn row_security_catalog_snapshot(&self) -> Arc<crate::engine::rls::CatalogSnapshot> {
        self.catalog.row_security.snapshot()
    }
    /// Identity of the published row-security catalog; see
    /// [`Database::auth_catalog_ptr`].
    pub(crate) fn row_security_catalog_ptr(&self) -> *const crate::engine::rls::CatalogSnapshot {
        self.catalog.row_security.snapshot_ptr()
    }
    pub(crate) fn row_security_may_be_configured(&self, object_id: u64) -> bool {
        self.catalog.row_security.may_have_security(object_id)
    }

    fn row_security_header(
        row: &[Value],
    ) -> Result<(u64, u64, Arc<crate::engine::rls::RowSecurity>), QueryError> {
        let number = |index| match row.get(index) {
            Some(Value::UInt(value)) => Ok(*value),
            Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
            _ => Err(crate::engine::rls::violation(
                "Invalid policy catalog identity",
            )),
        };
        let options = match row.get(18) {
            Some(Value::Null) => crate::storage::schema::TableOptions::default(),
            Some(value) => serde_json::from_str::<crate::storage::schema::TableOptions>(
                &value_to_string(value),
            )
            .map_err(|_| crate::engine::rls::violation("Invalid policy catalog options"))?,
            None => {
                return Err(crate::engine::rls::violation(
                    "Missing policy catalog options",
                ))
            }
        };
        Ok((number(0)?, number(1)?, options.row_security))
    }

    /// A dictionary snapshot loads flags and policy rows together. This is
    /// metadata I/O, never a policy query or a business-table scan.
    pub(crate) fn load_row_security_snapshot(
        &self,
        storage_id: u64,
        version: u64,
    ) -> Result<Arc<crate::engine::rls::RowSecurity>, QueryError> {
        use crate::storage::data_dictionary::{DD_ROW_SECURITY_POLICIES, DD_TABLES};
        let snapshot = StorageReadSnapshot::new(self);
        let tables = self.catalog_rows_for_selector_at(
            DD_TABLES,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(storage_id)]),
            snapshot.txn,
        )?;
        let row = tables
            .first()
            .ok_or_else(crate::engine::rls::catalog_snapshot_conflict)?;
        let (_, _, header) = Self::row_security_header(row)?;
        if header.version != version {
            return Err(crate::engine::rls::catalog_snapshot_conflict());
        }
        let rows = self.catalog_rows_for_selector_at(
            DD_ROW_SECURITY_POLICIES,
            CatalogRowSelector::PrefixValues(vec![Value::UInt(storage_id)]),
            snapshot.txn,
        )?;
        let mut security = header.as_ref().clone();
        security.policies = rows
            .iter()
            .map(|row| {
                row.get(2)
                    .ok_or_else(|| crate::engine::rls::violation("Missing policy definition"))
                    .and_then(|value| {
                        serde_json::from_str(&value_to_string(value))
                            .map_err(|_| crate::engine::rls::violation("Invalid policy definition"))
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(security))
    }

    fn prepare_row_security_replacements(
        &self,
        replacements: &[CatalogRowReplacement],
    ) -> Result<Option<crate::engine::rls::CatalogPublication<'_>>, QueryError> {
        use crate::storage::data_dictionary::{DD_ROW_SECURITY_POLICIES, DD_TABLES};
        if !replacements
            .iter()
            .any(|replacement| replacement.dd_table_id == DD_TABLES)
        {
            return Ok(None);
        }
        let current = self.catalog.row_security.snapshot();
        // EXCHANGE moves a logical object to a new DD primary key. A
        // retirement of its old storage key must not remove the replacement,
        // regardless of the journal's mutation order.
        let incoming_objects = replacements.iter()
            .filter(|replacement| replacement.dd_table_id == DD_TABLES)
            .flat_map(|replacement| replacement.rows.iter())
            .map(|row| Self::row_security_header(row).map(|(_, object, _)| object))
            .collect::<Result<HashSet<_>, _>>()?;
        let mut changes = Vec::new();
        for replacement in replacements
            .iter()
            .filter(|replacement| replacement.dd_table_id == DD_TABLES)
        {
            if replacement.rows.is_empty() {
                if let Some(id) = replacement.selector.object_id() {
                    let object = self
                        .catalog
                        .table_key_by_id(id)
                        .and_then(|key| {
                            self.catalog
                                .table_headers
                                .read()
                                .ok()?
                                .get(&key)
                                .map(|table| table.object_id)
                        })
                        .or_else(|| current.object_for_storage(id));
                    if let Some(object) = object.filter(|object| !incoming_objects.contains(object)) {
                        changes.push((object, None));
                    }
                }
                continue;
            }
            for row in &replacement.rows {
                let (id, object, header) = Self::row_security_header(row)?;
                if current.matches(object, id, &header) {
                    continue;
                }
                let mut rows = self.catalog_rows_for_selector(
                    DD_ROW_SECURITY_POLICIES,
                    CatalogRowSelector::PrefixValues(vec![Value::UInt(id)]),
                )?;
                for policy in replacements
                    .iter()
                    .filter(|replacement| replacement.dd_table_id == DD_ROW_SECURITY_POLICIES)
                {
                    let key = match &policy.selector {
                        CatalogRowSelector::All => None,
                        CatalogRowSelector::PrimaryValues(key)
                        | CatalogRowSelector::PrefixValues(key) => Some(key),
                    };
                    if key
                        .is_some_and(|key| !key.is_empty() && key.first() != Some(&Value::UInt(id)))
                    {
                        continue;
                    }
                    if let Some(key) = key {
                        rows.retain(|row| row.get(..key.len()) != Some(key.as_slice()));
                    } else {
                        rows.clear();
                    }
                    rows.extend(
                        policy
                            .rows
                            .iter()
                            .filter(|row| row.first() == Some(&Value::UInt(id)))
                            .cloned(),
                    );
                }
                let mut security = header.as_ref().clone();
                security.policies = rows
                    .iter()
                    .map(|row| {
                        row.get(2)
                            .ok_or_else(|| {
                                crate::engine::rls::violation("Missing policy definition")
                            })
                            .and_then(|value| {
                                serde_json::from_str(&value_to_string(value)).map_err(|_| {
                                    crate::engine::rls::violation("Invalid policy definition")
                                })
                            })
                    })
                    .collect::<Result<_, _>>()?;
                changes.push((object, Some((id, Arc::new(security)))));
            }
        }
        if changes.is_empty() {
            return Ok(None);
        }
        self.catalog.row_security.prepare(changes, |id, version| {
            self.load_row_security_snapshot(id, version)
        })
    }

    fn row_security_old(
        &self,
        txn: TxnView,
        table: &TableSchema,
        command: crate::parser::rls::Command,
        values: &[Value],
    ) -> Result<bool, QueryError> {
        let allowed = crate::engine::rls::row_allowed(self, txn, table, command, false, values)?;
        if !allowed
            && (FK_CASCADE_DEPTH.with(|depth| depth.get() != 0) || crate::engine::rls::strict_old())
        {
            return Err(crate::engine::rls::violation(
                if FK_CASCADE_DEPTH.with(|depth| depth.get() != 0) {
                    "Foreign key cascade violates child row security"
                } else {
                    "Conflicting row is hidden by row security"
                },
            ));
        }
        Ok(allowed)
    }
    /// Publish a metadata-only security change through the durable catalog
    /// candidate protocol. No business heap is scanned or rewritten.
    pub(crate) fn alter_row_security(
        &self,
        expected: &TableSchema,
        security: crate::engine::rls::RowSecurity,
    ) -> Result<(), QueryError> {
        let mut candidate = expected.clone();
        candidate.row_security = Arc::new(security);
        self.publish_stats_candidate(expected, candidate)
    }
}
