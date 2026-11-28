use super::*;
use std::os::unix::fs::{symlink, PermissionsExt};

#[test]
fn wal_fault_native_bootstrap_metadata_is_private_before_engine_open() {
    for first in ["role", "progress", "resync", "slots"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        match first {
            "role" => {
                crate::repl::role::save(dir.path(), &crate::repl::role::RoleState::new_standalone())
                    .unwrap()
            }
            "progress" => {
                crate::repl::replica::progress::save_progress(dir.path(), &Default::default())
                    .unwrap()
            }
            "resync" => {
                crate::repl::replica::progress::write_resync_marker(dir.path(), "test").unwrap()
            }
            "slots" => {
                crate::repl::master::SlotRegistry::open(dir.path()).unwrap();
            }
            _ => unreachable!(),
        }
        let path = replication_dir(dir.path());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700,
            "{first}"
        );
        ChannelDirectory::replication(dir.path()).unwrap();
        for entry in fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                assert_eq!(
                    entry.metadata().unwrap().permissions().mode() & 0o777,
                    0o600,
                    "{first}"
                );
            }
        }
    }
}

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
        .parent()
        .unwrap()
        .to_path_buf();
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

#[test]
fn auth_security_all_metadata_parents_reject_symlinks_without_side_effects() {
    for relative in [
        "system",
        "system/replication",
        "system/replication/channels",
    ] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let outside = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let state = state();
        persist_channel(dir.path(), &state).unwrap();
        let path = dir.path().join(relative);
        let saved = dir.path().join("saved");
        fs::rename(&path, &saved).unwrap();
        symlink(outside.path(), &path).unwrap();
        let mode = fs::metadata(outside.path()).unwrap().permissions().mode();
        assert!(persist_channel(dir.path(), &state).is_err(), "{relative}");
        assert!(
            validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err(),
            "{relative}"
        );
        assert!(
            read_private_channel(dir.path(), std::ffi::OsStr::new("auth_audit.json")).is_err(),
            "{relative}"
        );
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
        assert_eq!(
            fs::metadata(outside.path()).unwrap().permissions().mode(),
            mode
        );
    }
}

#[test]
fn auth_security_parent_permissions_are_checked_before_read_and_update() {
    for (relative, mode) in [
        ("", 0o777),
        ("system", 0o777),
        ("system/replication", 0o755),
        ("system/replication/channels", 0o755),
    ] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let state = state();
        persist_channel(dir.path(), &state).unwrap();
        let path = channel_path(dir.path(), &state.config.channel);
        let bytes = fs::read(&path).unwrap();
        let parent = dir.path().join(relative);
        fs::set_permissions(&parent, fs::Permissions::from_mode(mode)).unwrap();
        assert!(
            validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err(),
            "{relative}"
        );
        assert!(
            read_private_channel(dir.path(), path.file_name().unwrap()).is_err(),
            "{relative}"
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
        validate_existing_channel_metadata(dir.path(), &state.config.channel).unwrap();
    }
}

#[test]
fn auth_security_rejects_hardlinks_special_and_oversized_credential_files() {
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let state = state();
    persist_channel(dir.path(), &state).unwrap();
    let path = channel_path(dir.path(), &state.config.channel);
    let target = dir.path().join("target.json");
    fs::hard_link(&path, &target).unwrap();
    let bytes = fs::read(&target).unwrap();
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
    assert!(persist_channel(dir.path(), &state).is_err());
    assert_eq!(fs::read(&target).unwrap(), bytes);
    fs::remove_file(&target).unwrap();
    fs::remove_file(&path).unwrap();
    let fifo = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: fifo is a valid NUL-terminated path in this test's private directory.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    // O_NONBLOCK must reject a FIFO without waiting for a writer.
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
    assert!(persist_channel(dir.path(), &state).is_err());
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
    assert!(persist_channel(dir.path(), &state).is_err());
    fs::remove_dir(&path).unwrap();
    let file = File::create(&path).unwrap();
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    assert!(validate_existing_channel_metadata(dir.path(), &state.config.channel).is_err());
}

#[test]
fn auth_security_open_directory_remains_anchored_after_path_replacement() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let outside = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let mut state = state();
    persist_channel(dir.path(), &state).unwrap();
    let directory = ChannelDirectory::open(dir.path(), false).unwrap();
    let channels = replication_dir(dir.path()).join("channels");
    let saved = dir.path().join("saved-channels");
    fs::rename(&channels, &saved).unwrap();
    symlink(outside.path(), &channels).unwrap();
    let name = std::ffi::OsStr::new("auth_audit.json");
    assert_eq!(
        directory.read(name).unwrap(),
        fs::read(saved.join(name)).unwrap()
    );
    state.config.source_password = "updated-secret".into();
    directory.write(name, &state).unwrap();
    let decoded: PersistedChannel = crate::storage::metadata::decode(
        crate::storage::metadata::CHANNEL,
        crate::storage::format_versions::MYSQL_CHANNEL_VERSION,
        &fs::read(saved.join(name)).unwrap(),
    )
    .unwrap();
    assert_eq!(decoded.config.source_password, "updated-secret");
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    assert_eq!(fs::read_dir(saved).unwrap().count(), 1);
}

#[test]
fn auth_security_concurrent_atomic_writers_preserve_private_valid_metadata() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    persist_channel(dir.path(), &state()).unwrap();
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let dir = dir.path();
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for iteration in 0..8 {
                    let mut state = state();
                    state.config.source_password = format!("worker-{worker}-{iteration}");
                    persist_channel(dir, &state).unwrap();
                    validate_existing_channel_metadata(dir, &state.config.channel).unwrap();
                }
            });
        }
    });
    let path = channel_path(dir.path(), "auth_audit");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
}

#[test]
fn auth_security_trusted_root_alias_and_owned_directory_hardening_are_supported() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let root = dir.path().join("data");
    fs::create_dir(&root).unwrap();
    let alias = dir.path().join("configured-root");
    symlink(&root, &alias).unwrap();
    let mut state = state();
    state.config.channel = "多通道-2".into();
    persist_channel(&alias, &state).unwrap();
    for path in [
        replication_dir(&root),
        replication_dir(&root).join("channels"),
    ] {
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    persist_channel(&alias, &state).unwrap();
    validate_existing_channel_metadata(&alias, &state.config.channel).unwrap();
    for path in [
        replication_dir(&root),
        replication_dir(&root).join("channels"),
    ] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    let directory = ChannelDirectory::open(&alias, false).unwrap();
    for invalid in ["../outside", "/outside", "./x", "x/", "x/."] {
        assert!(directory.read(std::ffi::OsStr::new(invalid)).is_err());
    }
}
