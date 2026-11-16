use super::*;

fn ids() -> Vec<u32> {
    current_session_registry()
        .unwrap()
        .snapshot()
        .iter()
        .map(|row| row.id)
        .collect()
}

#[test]
fn shared_registry_scope_preserves_live_updates_and_nested_identity() {
    let a = Arc::new(SessionRegistry::new());
    let b = SessionRegistry::new();
    let _one = a.register(11, "a".into(), "localhost".into(), "test".into());
    let _two = b.register(22, "b".into(), "localhost".into(), "test".into());
    assert!(current_session_registry().is_none());
    with_shared_session_registry(&a, || {
        assert_eq!(ids(), vec![11]);
        with_session_registry(b.clone(), || {
            assert_eq!(ids(), vec![22]);
            with_shared_session_registry(&a, || assert_eq!(ids(), vec![11]));
            assert_eq!(ids(), vec![22]);
        });
        assert_eq!(ids(), vec![11]);
        a.unregister(11);
        with_shared_session_registry(&a, || assert!(ids().is_empty()));
    });
    assert!(current_session_registry().is_none());
}

#[test]
fn registry_scope_restores_after_nested_unwind_and_keeps_owned_snapshot_live() {
    let registry = Arc::new(SessionRegistry::new());
    let _handle = registry.register(7, "user".into(), "localhost".into(), "test".into());
    let captured = with_shared_session_registry(&registry, || {
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_session_registry(SessionRegistry::new(), || {
                assert!(ids().is_empty());
                panic!("registry scope unwind");
            });
        }));
        assert!(error.is_err());
        assert_eq!(ids(), vec![7]);
        current_session_registry().unwrap()
    });
    assert!(current_session_registry().is_none());
    drop(registry);
    std::thread::spawn(move || {
        with_session_registry(captured, || assert_eq!(ids(), vec![7]));
        assert!(current_session_registry().is_none());
    })
    .join()
    .unwrap();
}
