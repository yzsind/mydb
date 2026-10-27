//! Statement-owned effects survive nesting and deferred cursor consumption.
use super::error;
use crate::{
    engine::{
        api::{QueryResponse, ResultCursor, SqlWarnings},
        expr::SessionExprInfo,
    },
    storage::Database,
    types::{QueryError, Value},
};
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
#[derive(Default)]
pub(crate) struct Effects {
    pub attempts: AtomicUsize,
    pub outputs: std::sync::Mutex<Vec<super::runtime::Memory>>,
}
#[derive(Clone)]
pub(crate) struct Context {
    pub db: Arc<Database>,
    pub effects: Arc<Effects>,
}
struct Frame {
    db: Arc<Database>,
    effects: Option<Arc<Effects>>,
}
thread_local! {static CURRENT:RefCell<Option<Frame>>=const {RefCell::new(None)};}
pub(crate) struct Guard {
    previous: Option<Frame>,
    installed: bool,
}
impl Drop for Guard {
    fn drop(&mut self) {
        if self.installed {
            CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
        }
    }
}
/// Ordinary SQL installs only an Arc reference; effects allocate on first use.
pub(crate) fn enter(db: &Arc<Database>) -> Guard {
    if CURRENT.with(|c| c.borrow().as_ref().is_some_and(|s| Arc::ptr_eq(&s.db, db))) {
        return Guard {
            previous: None,
            installed: false,
        };
    }
    let previous = CURRENT.with(|c| {
        c.replace(Some(Frame {
            db: db.clone(),
            effects: None,
        }))
    });
    Guard {
        previous,
        installed: true,
    }
}
/// A protocol batch has separate top-level statements. Nested stored-program
/// statements continue to inherit their caller's scope through enter().
pub(crate) fn fresh_statement() -> Guard {
    let next = CURRENT.with(|c| {
        c.borrow().as_ref().map(|f| Frame {
            db: f.db.clone(),
            effects: f.effects.as_ref().map(|_| Arc::new(Effects::default())),
        })
    });
    let Some(next) = next else {
        return Guard {
            previous: None,
            installed: false,
        };
    };
    let previous = CURRENT.with(|c| c.replace(Some(next)));
    Guard {
        previous,
        installed: true,
    }
}
pub(crate) fn current() -> Result<Context, QueryError> {
    CURRENT.with(|c| {
        let mut c = c.borrow_mut();
        let f = c.as_mut().ok_or_else(|| {
            error(
                "ER_HTTP_UNSUPPORTED_CONTEXT",
                "HTTP requires an owning SQL statement",
            )
        })?;
        Ok(Context {
            db: f.db.clone(),
            effects: f
                .effects
                .get_or_insert_with(|| Arc::new(Effects::default()))
                .clone(),
        })
    })
}
pub(crate) fn activate() {
    CURRENT.with(|c| {
        if let Some(f) = c.borrow_mut().as_mut() {
            f.effects
                .get_or_insert_with(|| Arc::new(Effects::default()));
        }
    });
}
pub(crate) fn capture() -> Option<Context> {
    CURRENT.with(|c| {
        c.borrow().as_ref().and_then(|f| {
            f.effects.as_ref().map(|effects| Context {
                db: f.db.clone(),
                effects: effects.clone(),
            })
        })
    })
}
pub(crate) fn enter_context(ctx: Option<&Context>) -> Guard {
    match ctx {
        Some(ctx) => install(ctx.clone()),
        None => Guard {
            previous: None,
            installed: false,
        },
    }
}
pub(crate) fn sent() -> bool {
    CURRENT.with(|c| {
        c.borrow()
            .as_ref()
            .and_then(|c| c.effects.as_ref())
            .is_some_and(|e| e.attempts.load(Ordering::Acquire) > 0)
    })
}
fn install(ctx: Context) -> Guard {
    let previous = CURRENT.with(|c| {
        c.replace(Some(Frame {
            db: ctx.db,
            effects: Some(ctx.effects),
        }))
    });
    Guard {
        previous,
        installed: true,
    }
}
pub(crate) fn wrap_response(response: QueryResponse) -> QueryResponse {
    let QueryResponse::StreamingResultSet(mut stream) = response else {
        return response;
    };
    if let (Ok(ctx), Some(info)) = (current(), crate::engine::expr::current_session_expr_info()) {
        stream.cursor = Box::new(HttpCursor {
            inner: stream.cursor,
            ctx,
            info,
        });
    }
    QueryResponse::StreamingResultSet(stream)
}
struct HttpCursor {
    inner: Box<dyn ResultCursor>,
    ctx: Context,
    info: SessionExprInfo,
}
impl std::fmt::Debug for HttpCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpCursor([redacted])")
    }
}
impl ResultCursor for HttpCursor {
    fn external_io(&self) -> bool {
        true
    }
    fn external_http_io(&self) -> bool {
        true
    }
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        self.ctx
            .effects
            .outputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        let _guard = install(self.ctx.clone());
        let _serial = crate::planner::force_serial_scope();
        crate::engine::expr::with_session_expr_info_snapshot(self.info.clone(), || {
            self.inner.next_row()
        })
    }
    fn take_warnings(&mut self) -> SqlWarnings {
        self.inner.take_warnings()
    }
}
