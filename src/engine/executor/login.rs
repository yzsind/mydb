//! Account policy shared by protocol-token and plaintext authentication.
use std::collections::BTreeSet;

use crate::engine::auth::{
    self, access_denied, current_time_ms, AccountKey, AuthOutcome, AuthRequest,
    AuthorizationContext, UserAccount,
};
use crate::storage::Database;
use crate::types::QueryError;

pub(super) struct LoginAccount {
    pub account: UserAccount,
    active_roles: BTreeSet<AccountKey>,
    epoch: u64,
    is_system_user: bool,
}

impl LoginAccount {
    pub fn load(db: &Database, username: &str, host: &str) -> Result<Self, QueryError> {
        db.with_auth_catalog(|auth| {
            let account = auth
                .match_account(username, host)
                .cloned()
                .ok_or_else(|| access_denied(username, host))?;
            Ok(Self {
                active_roles: auth.login_active_roles(&account.key),
                epoch: auth.epoch,
                is_system_user: auth.account_is_system_user(&account.key),
                account,
            })
        })?
    }

    /// Call only after credential verification: account policy must not reveal
    /// whether an account is locked to a caller supplying an invalid password.
    pub fn validate_verified(
        &self,
        username: &str,
        request: &AuthRequest<'_>,
    ) -> Result<bool, QueryError> {
        let account = &self.account;
        if account.account_locked {
            return Err(crate::mysql::errors::error(
                crate::mysql::errors::ER_ACCOUNT_HAS_BEEN_LOCKED,
                format!("Access denied for user '{}': account is locked", username),
            ));
        }
        if let Some(until) = auth::active_login_lock(account, current_time_ms()) {
            return Err(auth::temporary_login_lock_error(account, until));
        }
        auth::failed_login_clear(&account.key);
        let lifetime_expired = auth::password_lifetime_expired(
            account.password_lifetime_days,
            account.password_last_changed_ms,
        );
        let expired = account.password_expired || lifetime_expired;
        if expired
            && request.client_capabilities
                & crate::mysql::protocol::CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
                == 0
        {
            return Err(crate::mysql::errors::error(
                crate::mysql::errors::ER_MUST_CHANGE_PASSWORD_LOGIN,
                "Your password has expired. To log in you must \
                 change it using a client that supports expired passwords.",
            ));
        }
        Ok(expired)
    }

    /// Credential-specific cache updates happen before charging the quota.
    /// Every successful protocol path charges a connection exactly once.
    pub fn finish(self, password_expired: bool) -> Result<AuthOutcome, QueryError> {
        let account = &self.account;
        auth::charge_new_connection(
            &account.key,
            account.max_connections,
            account.max_user_connections,
        )?;
        Ok(AuthOutcome {
            context: AuthorizationContext {
                authenticated_as: account.key.clone(),
                active_roles: self.active_roles.into(),
                epoch: self.epoch,
            },
            password_expired,
            account_locked: account.account_locked,
            resource_limits: account.resource_limits(),
            current_user_is_system: self.is_system_user,
        })
    }
}
