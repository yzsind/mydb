use super::*;
use std::os::unix::fs::{symlink, PermissionsExt};

fn state() -> PersistedChannel {
    PersistedChannel {
        config: ReplicationChannelConfig {
            channel: "auth_audit".into(),
            source_host: "127.0.0.1".into(),
            source_password: "local-test-secret".into(),
            ..Default::default()
        },
        status: ReplicationStatus::default(),
    }
}

#[test]
fn auth_security_change_source_validation_rejects_public_or_linked_credentials() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let state = state();
    persist_channel(dir.path(), &state).unwrap();
    let path = channel_path(dir.path(), &state.config.channel);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let target = dir.path().join("target.json");
    fs::rename(&path, &target).unwrap();
    symlink(&target, &path).unwrap();
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
    assert_eq!(fs::read(&target).unwrap(), fs::read(&path).unwrap());
}

#[test]
fn auth_security_load_rejects_linked_or_public_channel_directory() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine,
        channels: Mutex::new(HashMap::new()),
        retired_gtids: Mutex::new(None),
    };
    let state = state();
    persist_channel(dir.path(), &state).unwrap();
    let channels = channel_path(dir.path(), &state.config.channel)
        .parent().unwrap().to_path_buf();
    let target = dir.path().join("redirected-channels");
    fs::rename(&channels, &target).unwrap();
    symlink(&target, &channels).unwrap();
    assert!(manager.load_channel(&state.config.channel).is_err());
    fs::remove_file(&channels).unwrap();
    fs::rename(target, &channels).unwrap();
    fs::set_permissions(&channels, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(manager.load_channel(&state.config.channel).is_err());
}

#[test]
fn auth_security_directory_rejection_does_not_create_through_symlink() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let outside = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    fs::create_dir_all(crate::storage::naming::system_dir(dir.path())).unwrap();
    symlink(outside.path(), replication_dir(dir.path())).unwrap();
    assert!(persist_channel(dir.path(), &state()).is_err());
    assert!(!outside.path().join("channels").exists());
}
