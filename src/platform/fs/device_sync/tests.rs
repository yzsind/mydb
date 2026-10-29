use super::*;
use std::io::Write;
use std::process::{Child, Command, Stdio};

fn fixture() -> (tempfile::TempDir, Arc<Coordinator>, File) {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let group = Arc::new(
        Coordinator::open(&root.path().join("control"))
            .unwrap()
            .unwrap(),
    );
    let mut file = File::create(root.path().join("data")).unwrap();
    file.write_all(b"durable contents").unwrap();
    (root, group, file)
}

#[test]
fn successful_barrier_acknowledges_only_its_sampled_prefix() {
    let (_root, group, file) = fixture();
    let first = group.request(&file).unwrap().unwrap();
    let later = std::cell::Cell::new(0);
    group
        .finish_with(first, || {
            later.set(group.request(&file)?.unwrap());
            file.sync_all()
        })
        .unwrap();
    assert_eq!(group.state().completed.load(Ordering::Acquire), first);
    assert!(later.get() > first);
    group.finish_with(later.get(), || file.sync_all()).unwrap();
    assert_eq!(group.state().flushes.load(Ordering::Acquire), 2);
}

#[test]
fn failed_barrier_cannot_acknowledge_or_strand_requests() {
    let (_root, group, file) = fixture();
    let first = group.request(&file).unwrap().unwrap();
    assert!(group
        .finish_with(first, || Err(io::Error::other("injected barrier failure")))
        .is_err());
    assert_eq!(group.state().completed.load(Ordering::Acquire), 0);
    assert_eq!(group.state().leader.load(Ordering::Acquire), 0);
    let second = group.request(&file).unwrap().unwrap();
    group.finish_with(second, || file.sync_all()).unwrap();
    assert_eq!(group.state().completed.load(Ordering::Acquire), second);
    assert_eq!(group.state().flushes.load(Ordering::Acquire), 1);
}

#[test]
fn request_registration_requires_a_successful_file_drain() {
    let (_root, group, _) = fixture();
    let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let socket_fd: std::os::fd::OwnedFd = socket.into();
    let unsupported = File::from(socket_fd);
    assert!(group.request(&unsupported).is_err());
    assert_eq!(group.state().requested.load(Ordering::Acquire), 0);
    assert_eq!(
        sync(&unsupported).unwrap_err().raw_os_error(),
        unsupported.sync_all().unwrap_err().raw_os_error()
    );
}

#[test]
fn a_file_write_error_cannot_be_consumed_by_a_successful_device_retry() {
    let (_root, group, file) = fixture();
    DRAIN_ERROR.with(|error| error.set(Some(libc::EIO)));
    assert_eq!(
        group.sync(&file).unwrap_err().raw_os_error(),
        Some(libc::EIO)
    );
    assert_eq!(group.state().requested.load(Ordering::Acquire), 0);
    assert_eq!(group.state().completed.load(Ordering::Acquire), 0);
    assert_eq!(group.state().flushes.load(Ordering::Acquire), 0);
    group.sync(&file).unwrap();
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "worker did not create {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn worker(root: &Path, mode: &str) -> Worker {
    Worker(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "platform::fs::device_sync::tests::device_sync_worker",
                "--nocapture",
            ])
            .env("MYDB_TEST_DEVICE_SYNC_WORKER", root)
            .env("MYDB_TEST_DEVICE_SYNC_MODE", mode)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn device_sync_worker() {
    let Some(root) = std::env::var_os("MYDB_TEST_DEVICE_SYNC_WORKER") else {
        return;
    };
    let root = PathBuf::from(root);
    let group = Coordinator::open(&root.join("control")).unwrap().unwrap();
    let mut file = File::create(root.join("child-data")).unwrap();
    file.write_all(b"child durable contents").unwrap();
    let ticket = group.request(&file).unwrap().unwrap();
    let mode = std::env::var("MYDB_TEST_DEVICE_SYNC_MODE").unwrap();
    if mode == "cohort" {
        fs::write(root.join("ready"), []).unwrap();
        wait_for(&root.join("release"));
        group.finish_with(ticket, || file.sync_all()).unwrap();
        return;
    }
    group
        .finish_with(ticket, || {
            if mode == "death-after-flush" {
                file.sync_all()?;
            }
            fs::write(root.join("ready"), [])?;
            if mode.starts_with("death") {
                std::process::exit(86);
            }
            // The parent verifies WIFSTOPPED, completes a newer request, then
            // resumes us while another leader owns the shared state.
            unsafe {
                libc::raise(libc::SIGSTOP);
            }
            file.sync_all()
        })
        .unwrap();
}

#[test]
fn one_device_barrier_covers_ready_requests_in_another_process() {
    let (root, group, file) = fixture();
    let first = group.request(&file).unwrap().unwrap();
    let mut child = worker(root.path(), "cohort");
    wait_for(&root.path().join("ready"));
    assert_eq!(group.state().requested.load(Ordering::Acquire), first + 1);
    group.finish_with(first, || file.sync_all()).unwrap();
    fs::write(root.path().join("release"), []).unwrap();
    assert!(child.0.wait().unwrap().success());
    assert_eq!(group.state().completed.load(Ordering::Acquire), first + 1);
    assert_eq!(group.state().flushes.load(Ordering::Acquire), 1);
}

#[test]
fn dead_leader_before_or_after_flush_never_supplies_a_completion_proof() {
    for mode in ["death", "death-after-flush"] {
        let (root, group, file) = fixture();
        let mut child = worker(root.path(), mode);
        wait_for(&root.path().join("ready"));
        assert_eq!(child.0.wait().unwrap().code(), Some(86));
        assert_eq!(group.state().completed.load(Ordering::Acquire), 0);
        let next = group.request(&file).unwrap().unwrap();
        let began = Instant::now();
        group.finish_with(next, || file.sync_all()).unwrap();
        assert!(began.elapsed() < Duration::from_secs(1));
        assert_eq!(group.state().completed.load(Ordering::Acquire), next);
        assert_eq!(group.state().flushes.load(Ordering::Acquire), 1);
    }
}

#[test]
fn paused_leader_cannot_block_or_clear_a_successor_or_regress_completion() {
    let (root, group, file) = fixture();
    let mut child = worker(root.path(), "pause");
    wait_for(&root.path().join("ready"));
    let mut status = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = unsafe {
            libc::waitpid(
                child.0.id() as i32,
                &mut status,
                libc::WUNTRACED | libc::WNOHANG,
            )
        };
        if got > 0 {
            assert!(libc::WIFSTOPPED(status));
            break;
        }
        assert_eq!(got, 0);
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(2));
    }
    let second = group.request(&file).unwrap().unwrap();
    let began = Instant::now();
    group.finish_with(second, || file.sync_all()).unwrap();
    assert!(began.elapsed() < Duration::from_secs(1));
    let third = group.request(&file).unwrap().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let next_group = group.clone();
    let next_file = file.try_clone().unwrap();
    let next = std::thread::spawn(move || {
        next_group.finish_with(third, || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            next_file.sync_all()
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGCONT) }, 0);
    assert!(child.0.wait().unwrap().success());
    assert_eq!(group.state().completed.load(Ordering::Acquire), second);
    assert_eq!(group.state().leader.load(Ordering::Acquire), third);
    release_tx.send(()).unwrap();
    next.join().unwrap().unwrap();
    assert_eq!(group.state().completed.load(Ordering::Acquire), third);
}

#[test]
fn malformed_control_state_is_optional_and_new_state_has_no_old_proof() {
    let (root, group, file) = fixture();
    let ticket = group.request(&file).unwrap().unwrap();
    group.finish_with(ticket, || file.sync_all()).unwrap();
    let next = Coordinator::open(&root.path().join("different-boot-control"))
        .unwrap()
        .unwrap();
    assert_eq!(next.state().completed.load(Ordering::Acquire), 0);
    let bad = root.path().join("malformed");
    let bad_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&bad)
        .unwrap();
    bad_file.set_len(7).unwrap();
    assert!(Coordinator::open(&bad).unwrap().is_none());
    assert!(!boot_id().unwrap().is_empty());
}
