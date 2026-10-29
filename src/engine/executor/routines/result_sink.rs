//! Synchronous routine output hook owned by the current protocol worker.
//! The transport supplies backpressure; interpreter-only callers retain the
//! ordinary materialized multi-result API.
use super::*;
use std::cell::RefCell;

pub(crate) type ResultSink =
    Box<dyn FnMut(&mut SessionContext, QueryResponse) -> Result<(), QueryError> + Send>;

thread_local! {
    static SINK: RefCell<Option<ResultSink>> = const { RefCell::new(None) };
}

pub(crate) fn with_sink<T>(sink: Option<ResultSink>, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<ResultSink>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SINK.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(SINK.with(|slot| slot.replace(sink)));
    work()
}

pub(super) fn emit(
    session: &mut SessionContext,
    response: QueryResponse,
) -> Result<Option<QueryResponse>, QueryError> {
    SINK.with(|slot| match slot.borrow_mut().as_mut() {
        Some(sink) => sink(session, response).map(|()| None),
        None => Ok(Some(response)),
    })
}
