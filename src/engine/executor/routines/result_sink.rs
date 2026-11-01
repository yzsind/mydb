//! Synchronous routine output hook owned by the current protocol worker.
//! The transport supplies backpressure; interpreter-only callers retain the
//! ordinary materialized multi-result API.
use super::*;
use std::cell::RefCell;

pub(crate) type ResultSink =
    Box<dyn FnMut(&mut SessionContext, QueryResponse) -> Result<(), QueryError> + Send>;

thread_local! {
    static SINK: RefCell<Option<ResultSink>> = const { RefCell::new(None) };
    static DISCARDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn is_discarding() -> bool {
    DISCARDING.with(|flag| flag.get())
}

pub(crate) fn discard(response: QueryResponse) -> Result<(), QueryError> {
    match response {
        QueryResponse::StreamingResultSet(mut result) => {
            while result.cursor.next_row()?.is_some() {}
        }
        QueryResponse::MultiResult(results) => {
            for result in results {
                discard(result)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn with_discard_sink<T>(work: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            DISCARDING.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(DISCARDING.with(|flag| flag.replace(true)));
    with_sink(Some(Box::new(|_, response| discard(response))), work)
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
