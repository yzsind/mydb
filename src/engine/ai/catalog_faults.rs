//! Test-only catalog failure/crash boundaries; production calls compile away.
use crate::types::QueryError;

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn hit(_stage: &str) -> Result<(), QueryError> {
    Ok(())
}

#[cfg(test)]
thread_local! {static FAILURE:std::cell::Cell<&'static str>=const{std::cell::Cell::new("")};}
#[cfg(test)]
pub(crate) fn with_failure<T>(stage: &'static str, action: impl FnOnce() -> T) -> T {
    struct Reset(&'static str);
    impl Drop for Reset {
        fn drop(&mut self) {
            FAILURE.with(|value| value.set(self.0));
        }
    }
    let _reset = Reset(FAILURE.with(|value| value.replace(stage)));
    action()
}
#[cfg(test)]
pub(crate) fn hit(stage: &str) -> Result<(), QueryError> {
    if std::env::var("MYDB_AI_TEST_CRASH_STAGE").ok().as_deref() == Some(stage) {
        std::process::exit(74);
    }
    if FAILURE.with(|value| value.get() == stage) {
        assert!(
            matches!(stage, "before_dd" | "after_dd_rows"),
            "post-commit failures are crash-only"
        );
        return Err(crate::error::catalog::ER_GET_ERRNO
            .error(format!("injected AI catalog failure: {stage}")));
    }
    Ok(())
}
