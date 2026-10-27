//! Keep statement metadata stable for deferred result producers.
use super::dispatch::PreparedMdlGuard;
use crate::engine::api::{QueryResponse, ResultCursor, VecResultCursor};
use crate::storage::{Database, MdlDuration};
use crate::types::{QueryError, Value};
use std::sync::Arc;

thread_local! {
    static SCOPES: std::cell::RefCell<Vec<((usize, u64), bool)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// CTE/routine dispatch can recursively execute statements on the same
/// connection. Only the outer scope owns their shared statement tickets.
pub(super) struct StatementScope {
    key: (usize, u64),
    outermost: bool,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl StatementScope {
    pub(super) fn enter(db: &Arc<Database>, connection: u64) -> Self {
        let key = (Arc::as_ptr(db) as usize, connection);
        let outermost = SCOPES.with(|stack| {
            let mut stack = stack.borrow_mut();
            let outermost = !stack.iter().any(|(owner, _)| *owner == key);
            stack.push((key, false));
            outermost
        });
        Self {
            key,
            outermost,
            _thread_bound: std::marker::PhantomData,
        }
    }
    pub(super) fn active(db: &Arc<Database>, connection: u64) -> bool {
        let key = (Arc::as_ptr(db) as usize, connection);
        SCOPES.with(|stack| stack.borrow().iter().any(|(owner, _)| *owner == key))
    }

    fn defer_statement_grants(db: &Arc<Database>, connection: u64) {
        let key = (Arc::as_ptr(db) as usize, connection);
        SCOPES.with(|stack| {
            if let Some((_, pending)) = stack
                .borrow_mut()
                .iter_mut()
                .find(|(owner, _)| *owner == key)
            {
                *pending = true;
            }
        });
    }

    fn take_deferred_grants(db: &Arc<Database>, connection: u64) -> bool {
        let key = (Arc::as_ptr(db) as usize, connection);
        SCOPES.with(|stack| {
            stack
                .borrow_mut()
                .iter_mut()
                .find(|(owner, _)| *owner == key)
                .is_some_and(|(_, pending)| std::mem::take(pending))
        })
    }

    pub(super) fn outermost(&self) -> bool {
        self.outermost
    }
}
impl Drop for StatementScope {
    fn drop(&mut self) {
        SCOPES.with(|stack| {
            let key = stack.borrow_mut().pop();
            debug_assert_eq!(key.map(|(owner, _)| owner), Some(self.key));
        });
    }
}

struct Lease {
    db: Arc<Database>,
    owner: u64,
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingMdlLease")
            .field("owner", &self.owner)
            .finish()
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.db.release_mdl_owner(self.owner);
    }
}

#[derive(Debug)]
struct Cursor {
    inner: Option<Box<dyn ResultCursor>>,
    lease: Option<Arc<Lease>>,
    warnings: crate::engine::api::SqlWarnings,
}
impl Cursor {
    fn close(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            self.warnings.append(&mut inner.take_warnings());
            // Cancel/join the producer before releasing protection of files
            // it may still be reading (including on early consumer close).
            drop(inner);
        }
        self.lease.take();
    }
}
impl Drop for Cursor {
    fn drop(&mut self) {
        self.close();
    }
}
impl ResultCursor for Cursor {
    fn external_io(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| inner.external_io())
    }
    fn external_batch_rows(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(1, |inner| inner.external_batch_rows())
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        let result = match self.inner.as_mut() {
            Some(inner) => inner.next_row(),
            None => Ok(None),
        };
        if !matches!(result, Ok(Some(_))) {
            self.close();
        }
        result
    }
    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        let mut warnings = std::mem::take(&mut self.warnings);
        if let Some(inner) = self.inner.as_mut() {
            warnings.append(&mut inner.take_warnings());
        }
        warnings
    }
}

fn has_stream(response: &QueryResponse) -> bool {
    match response {
        QueryResponse::StreamingResultSet(stream) => !stream.cursor.external_io(),
        QueryResponse::MultiResult(results) => results.iter().any(has_stream),
        _ => false,
    }
}
fn attach(response: &mut QueryResponse, lease: &Arc<Lease>) {
    match response {
        QueryResponse::StreamingResultSet(stream) if !stream.cursor.external_io() => {
            let inner = std::mem::replace(
                &mut stream.cursor,
                Box::new(VecResultCursor::new(Vec::new())),
            );
            stream.cursor = Box::new(Cursor {
                inner: Some(inner),
                lease: Some(lease.clone()),
                warnings: Default::default(),
            });
        }
        QueryResponse::MultiResult(results) => {
            for response in results {
                attach(response, lease);
            }
        }
        _ => {}
    }
}

#[inline]
pub(super) fn finish(
    db: &Arc<Database>,
    connection: u64,
    grant: PreparedMdlGuard,
    mut result: Result<QueryResponse, QueryError>,
    release_statement: bool,
) -> Result<QueryResponse, QueryError> {
    if !release_statement && matches!(grant, PreparedMdlGuard::Statement) {
        StatementScope::defer_statement_grants(db, connection);
    }
    // A wrapper such as SQL EXECUTE owns no table tickets itself, yet its
    // nested statement can acquire them. The outer boundary must release or
    // transfer those tickets too, including errors and deferred result sets.
    if release_statement
        && StatementScope::take_deferred_grants(db, connection)
        && !matches!(grant, PreparedMdlGuard::Statement)
    {
        result = finish(db, connection, PreparedMdlGuard::Statement, result, true);
    }
    let streaming = result.as_ref().is_ok_and(has_stream);
    let lease = match grant {
        PreparedMdlGuard::Fast(grant) if streaming => {
            let owner = match crate::storage::mdl::allocate_internal_owner() {
                Ok(owner) => owner,
                Err(err) => {
                    drop(result);
                    db.release_fast_mdl(grant);
                    return Err(err);
                }
            };
            if !db.retain_streaming_fast_mdl(&grant, owner, connection) {
                drop(result);
                db.release_fast_mdl(grant);
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "streaming result fast metadata lock handoff failed",
                ));
            }
            db.release_fast_mdl(grant);
            Some(Lease {
                db: db.clone(),
                owner,
            })
        }
        PreparedMdlGuard::Statement if streaming && release_statement => {
            let owner = match crate::storage::mdl::allocate_internal_owner() {
                Ok(owner) => owner,
                Err(err) => {
                    drop(result);
                    db.release_mdl_duration(connection, MdlDuration::Statement);
                    return Err(err);
                }
            };
            // A connection reset/next statement must not release the cursor's
            // tickets. Preserve the connection id only as diagnostic ownership.
            if !db.transfer_mdl_duration_to_owner(
                connection,
                MdlDuration::Statement,
                owner,
                Some(connection),
                MdlDuration::Statement,
                "sql/streaming-result",
            ) {
                drop(result);
                db.release_mdl_duration(connection, MdlDuration::Statement);
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "streaming result metadata lock handoff failed",
                ));
            }
            Some(Lease {
                db: db.clone(),
                owner,
            })
        }
        PreparedMdlGuard::None if streaming => {
            let owner = crate::storage::mdl::allocate_internal_owner()?;
            db.retain_streaming_transaction_mdl(connection, owner)
                .then(|| Lease {
                    db: db.clone(),
                    owner,
                })
        }
        PreparedMdlGuard::Fast(grant) => {
            db.release_fast_mdl(grant);
            None
        }
        PreparedMdlGuard::Statement if release_statement => {
            db.release_mdl_duration(connection, MdlDuration::Statement);
            None
        }
        _ => None,
    };
    if let (Some(lease), Ok(response)) = (lease, &mut result) {
        attach(response, &Arc::new(lease));
    }
    result
}
