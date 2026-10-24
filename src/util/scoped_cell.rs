//! Synchronous, unwind-safe overrides for copyable thread-local state.
use std::cell::Cell;
use std::thread::LocalKey;

/// Restore the previous value on normal return, error return and unwinding.
/// Nested calls restore their enclosing synchronous scope.
#[inline]
pub(crate) fn with_value<T: Copy + 'static, R>(
    key: &'static LocalKey<Cell<T>>,
    value: T,
    f: impl FnOnce() -> R,
) -> R {
    struct Restore<T: Copy + 'static> {
        key: &'static LocalKey<Cell<T>>,
        previous: T,
    }
    impl<T: Copy> Drop for Restore<T> {
        fn drop(&mut self) {
            self.key.with(|cell| cell.set(self.previous));
        }
    }
    let _restore = Restore {
        key,
        previous: key.with(|cell| cell.replace(value)),
    };
    f()
}

#[cfg(test)]
mod tests {
    use super::*;
    thread_local! { static VALUE: Cell<Option<u64>> = const { Cell::new(None) }; }

    #[test]
    fn nested_overrides_restore_on_success_error_and_unwind() {
        assert_eq!(VALUE.get(), None);
        let answer = with_value(&VALUE, Some(7), || {
            let error: Result<(), &str> = with_value(&VALUE, None, || {
                assert_eq!(VALUE.get(), None);
                Err("expected")
            });
            assert_eq!(error, Err("expected"));
            assert_eq!(VALUE.get(), Some(7));
            assert!(
                std::panic::catch_unwind(|| with_value(&VALUE, Some(9), || panic!("unwind")))
                    .is_err()
            );
            assert_eq!(VALUE.get(), Some(7));
            42
        });
        assert_eq!(answer, 42);
        assert_eq!(VALUE.get(), None);
    }

    #[test]
    fn overrides_do_not_cross_thread_boundaries() {
        with_value(&VALUE, Some(7), || {
            std::thread::spawn(|| {
                assert_eq!(VALUE.get(), None);
                with_value(&VALUE, Some(9), || assert_eq!(VALUE.get(), Some(9)));
                assert_eq!(VALUE.get(), None);
            })
            .join()
            .unwrap();
            assert_eq!(VALUE.get(), Some(7));
        });
        assert_eq!(VALUE.get(), None);
    }
}
