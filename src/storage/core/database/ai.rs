impl Database {
    pub(crate) fn ai_catalog(&self) -> Result<Arc<crate::engine::ai::AiCatalog>, QueryError> {
        Ok(self.catalog_read()?.ai.clone())
    }

    /// Commit ciphertext and model-grant retirement in one DD transaction.
    pub(crate) fn mutate_ai_catalog<R>(
        &self,
        change: impl FnOnce(&mut crate::engine::ai::AiCatalog) -> Result<R, QueryError>,
    ) -> Result<R, QueryError> {
        static MUTATION: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _lock = MUTATION.lock().map_err(|_| {
            crate::engine::ai::error("ER_AI_INVALID_ARGUMENT", "AI catalog lock poisoned")
        })?;
        crate::engine::cancel::check_cancelled()?;
        let snapshot = self.ai_catalog()?;
        let mut candidate = (*snapshot).clone();
        let result = change(&mut candidate)?;
        crate::engine::cancel::check_cancelled()?;
        candidate.encryption_key_ids.extend(
            snapshot
                .secrets
                .values()
                .chain(candidate.secrets.values())
                .map(|s| s.key_id.clone()),
        );
        if !snapshot.active_key_id.is_empty() {
            candidate
                .encryption_key_ids
                .insert(snapshot.active_key_id.clone());
        }
        if !candidate.active_key_id.is_empty() {
            candidate
                .encryption_key_ids
                .insert(candidate.active_key_id.clone());
        }
        let encoded = serde_json::to_string(&candidate).map_err(|_| {
            crate::engine::ai::error("ER_AI_INVALID_ARGUMENT", "cannot encode AI catalog")
        })?;
        if encoded.len() > 1024 * 1024 {
            return Err(crate::engine::ai::error(
                "ER_AI_BUDGET_EXCEEDED",
                "AI catalog exceeds 1 MiB",
            ));
        }
        candidate.install_charge()?;
        let deleted: std::collections::HashSet<_> = snapshot
            .models
            .values()
            .filter(|old| !candidate.models.values().any(|new| new.id == old.id))
            .map(|m| m.id)
            .collect();
        let _auth_lock = if deleted.is_empty() {
            None
        } else {
            Some(
                crate::engine::auth::catalog_mutation_lock()
                    .lock()
                    .map_err(|_| {
                        crate::engine::ai::error(
                            "ER_AI_INVALID_ARGUMENT",
                            "auth mutation lock poisoned",
                        )
                    })?,
            )
        };
        let mut auth_candidate = None;
        let _charge = crate::engine::executor::mem_budget::force_charge_scoped(encoded.len())?;
        let mut txn = CatalogTxn::new(self);
        txn.mark_ai_catalog();
        txn.replace(
            crate::storage::data_dictionary::DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("ai_catalog".into())]),
            vec![vec![
                Value::String("ai_catalog".into()),
                Value::String(encoded),
            ]],
        );
        if !deleted.is_empty() {
            let old = self.auth_catalog();
            let mut auth = (*old).clone();
            let removed:Vec<_>=old.grants.iter().filter(|grant|matches!(&grant.privilege,crate::engine::auth::Privilege::Dynamic(name) if crate::engine::auth::ai_model_privilege_id(name).is_some_and(|id|deleted.contains(&id)))).map(crate::storage::data_dictionary::auth_grant_key).collect();
            auth.grants.begin_mutation();
            auth.grants.retain(|grant|!matches!(&grant.privilege,crate::engine::auth::Privilege::Dynamic(name) if crate::engine::auth::ai_model_privilege_id(name).is_some_and(|id|deleted.contains(&id))));
            auth.epoch = auth.epoch.checked_add(1).ok_or_else(|| {
                crate::engine::ai::error(
                    "ER_AI_BUDGET_EXCEEDED",
                    "authorization generation exhausted",
                )
            })?;
            auth.grants.reconcile_bytes(&old.grants);
            auth.refresh_runtime_indexes(Some(&old));
            auth.install_store_charge(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                    auth.clone_capacity_bytes(),
                )?,
            );
            for key in removed {
                txn.replace(
                    crate::storage::data_dictionary::DD_GLOBAL_PRIVILEGES,
                    CatalogRowSelector::PrimaryValues(vec![Value::String(key)]),
                    Vec::new(),
                );
            }
            txn.replace(
                crate::storage::data_dictionary::DD_DD_PROPERTIES,
                CatalogRowSelector::PrimaryValues(vec![Value::String("auth_epoch".into())]),
                vec![vec![
                    Value::String("auth_epoch".into()),
                    Value::String(auth.epoch.to_string()),
                ]],
            );
            auth_candidate = Some(Arc::new(auth));
        }
        txn.set_delta(Vec::new(), true);
        crate::engine::ai::catalog_faults::hit("before_dd")?;
        crate::engine::cancel::check_cancelled()?;
        txn.commit()?;
        crate::engine::ai::catalog_faults::hit("after_durable")?;
        {
            let mut catalog = self.catalog_write()?;
            catalog.ai = Arc::new(candidate);
            if let Some(auth) = auth_candidate {
                catalog.auth = auth;
            }
        }
        if !deleted.is_empty() {
            self.publish_auth_snapshot()?;
        }
        crate::engine::ai::catalog_faults::hit("after_publish")?;
        Ok(result)
    }
}
