//! Share complete Darwin device-cache barriers without sharing data-file state.
//!
//! Every request is registered AFTER its own file's fsync has completed. A
//! leader samples that request prefix BEFORE F_FULLFSYNC, then acknowledges
//! only that prefix after success. The control mapping is an optional cache,
//! isolated by boot and filesystem identity; it never supplies recovery data.

use super::File;
use crate::platform::mapping::FileMapping;
use std::collections::BTreeMap;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAGIC: u64 = 0x4d59444253594e01;
const CONTROL_BYTES: u64 = 4096;

#[repr(C, align(64))]
struct Shared {
    magic: AtomicU64,
    requested: AtomicU64,
    completed: AtomicU64,
    leader: AtomicU64,
    last_sync_ns: AtomicU64,
    flushes: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Device {
    dev: u64,
    fsid: [i32; 2],
}

impl Device {
    fn of(file: &File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let mut info: libc::statfs = unsafe { std::mem::zeroed() };
        // SAFETY: info is a correctly sized output buffer and file owns fd.
        if unsafe { libc::fstatfs(file.as_raw_fd(), &mut info) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if info.f_flags & libc::MNT_LOCAL as u32 == 0 {
            return Err(io::ErrorKind::Unsupported.into());
        }
        // Restrict the shared-device proof to Darwin's built-in disk
        // filesystems. A user-space filesystem may implement a file-specific
        // remote flush despite advertising itself as local.
        let name = &info.f_fstypename;
        let apfs = name[..5]
            .iter()
            .map(|b| *b as u8)
            .eq(b"apfs\0".iter().copied());
        let hfs = name[..4]
            .iter()
            .map(|b| *b as u8)
            .eq(b"hfs\0".iter().copied());
        if !apfs && !hfs {
            return Err(io::ErrorKind::Unsupported.into());
        }
        // SAFETY: Darwin's opaque fsid_t is the two-int filesystem identity
        // defined by sys/mount.h. Include it to separate mounted filesystems.
        let fsid = unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(info.f_fsid) };
        Ok(Self {
            dev: metadata.dev(),
            fsid,
        })
    }
}

struct Coordinator {
    mapping: FileMapping,
}

impl Coordinator {
    fn open(path: &Path) -> io::Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Ok(None);
        }
        if metadata.len() == CONTROL_BYTES {
            let mapped = Self::map(&file)?;
            if mapped.valid() {
                return Ok(Some(mapped));
            }
        } else if metadata.len() != 0 {
            return Ok(None);
        }
        // Initialization is short and optional. A paused initializer must not
        // prevent anyone from using the ordinary full-sync system call.
        if file.try_lock().is_err() {
            return Ok(None);
        }
        if file.metadata()?.len() == 0 {
            file.set_len(CONTROL_BYTES)?;
        }
        if file.metadata()?.len() != CONTROL_BYTES {
            return Ok(None);
        }
        let mapped = Self::map(&file)?;
        if mapped.valid() {
            return Ok(Some(mapped));
        }
        let state = mapped.state();
        if state.magic.load(Ordering::Acquire) != 0 {
            return Ok(None);
        }
        // Zero-filled file storage is a valid AtomicU64 representation. No
        // requester accesses these fields before the release of MAGIC.
        state.requested.store(0, Ordering::Relaxed);
        state.completed.store(0, Ordering::Relaxed);
        state.leader.store(0, Ordering::Relaxed);
        state.last_sync_ns.store(5_000_000, Ordering::Relaxed);
        state.flushes.store(0, Ordering::Relaxed);
        state.magic.store(MAGIC, Ordering::Release);
        // Closing file releases the initialization lock; the mapping remains.
        Ok(Some(mapped))
    }

    fn map(file: &File) -> io::Result<Self> {
        // SAFETY: the private control file is fixed-size for its lifetime.
        // Published fields are accessed exclusively through aligned atomics.
        let mapping = unsafe { FileMapping::open(file, 0, CONTROL_BYTES as usize, true)? };
        if mapping
            .as_ptr()
            .align_offset(std::mem::align_of::<Shared>())
            != 0
        {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(Self { mapping })
    }

    fn state(&self) -> &Shared {
        // SAFETY: map validated extent/alignment; all fields admit zero bits
        // and have atomic interior mutability. The owned mapping outlives this
        // borrow. Initialization is serialized before publishing MAGIC.
        unsafe { &*self.mapping.as_ptr().cast::<Shared>() }
    }

    fn valid(&self) -> bool {
        let state = self.state();
        state.magic.load(Ordering::Acquire) == MAGIC
            && state.completed.load(Ordering::Acquire) <= state.requested.load(Ordering::Acquire)
    }

    fn request(&self, file: &File) -> io::Result<Option<u64>> {
        drain(file)?;
        Ok(self
            .state()
            .requested
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .ok()
            .map(|previous| previous + 1))
    }

    fn sync(&self, file: &File) -> io::Result<()> {
        match self.request(file)? {
            Some(ticket) => self.finish_with(ticket, || file.sync_all()),
            None => file.sync_all(),
        }
    }

    fn finish_with(&self, ticket: u64, flush: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let state = self.state();
        let began = Instant::now();
        let allowance = Duration::from_nanos(
            state
                .last_sync_ns
                .load(Ordering::Relaxed)
                .saturating_mul(4)
                .clamp(10_000_000, 100_000_000),
        );
        let mut spins = 0;
        loop {
            if state.completed.load(Ordering::Acquire) >= ticket {
                return Ok(());
            }
            let leader = state.leader.load(Ordering::Acquire);
            let expired = began.elapsed() >= allowance;
            if leader == 0 || expired {
                let owns = state
                    .leader
                    .compare_exchange(leader, ticket, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
                if owns || expired {
                    if state.completed.load(Ordering::Acquire) >= ticket {
                        if owns {
                            let _ = state.leader.compare_exchange(
                                ticket,
                                0,
                                Ordering::Release,
                                Ordering::Relaxed,
                            );
                        }
                        return Ok(());
                    }
                    // Only an already contended queue can benefit from a
                    // cohort. Give the prior leader's next publication one
                    // small fraction of a device flush to join, without
                    // delaying a lone request or a timeout takeover.
                    if owns
                        && !expired
                        && state
                            .requested
                            .load(Ordering::Acquire)
                            .saturating_sub(state.completed.load(Ordering::Acquire))
                            > 1
                        && coalescing_window_enabled()
                    {
                        let nanos = (state.last_sync_ns.load(Ordering::Relaxed) / 16).min(250_000);
                        if nanos != 0 {
                            std::thread::sleep(Duration::from_nanos(nanos));
                        }
                        if state.completed.load(Ordering::Acquire) >= ticket {
                            let _ = state.leader.compare_exchange(
                                ticket,
                                0,
                                Ordering::Release,
                                Ordering::Relaxed,
                            );
                            return Ok(());
                        }
                    }
                    // Even an abandoned/stopped leader cannot block progress.
                    // A fallback barrier proves the same prefix independently.
                    let covered = state.requested.load(Ordering::Acquire);
                    let started = Instant::now();
                    let result = flush();
                    if result.is_ok() {
                        state.completed.fetch_max(covered, Ordering::Release);
                        state.flushes.fetch_add(1, Ordering::Relaxed);
                        state.last_sync_ns.store(
                            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                            Ordering::Relaxed,
                        );
                    }
                    if owns {
                        // A resumed old leader must not clear its successor.
                        let _ = state.leader.compare_exchange(
                            ticket,
                            0,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }
                    return result;
                }
            }
            if spins < 16 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    }
}

fn coalescing_window_enabled() -> bool {
    #[cfg(test)]
    {
        static DISABLED: OnceLock<bool> = OnceLock::new();
        return !*DISABLED
            .get_or_init(|| std::env::var_os("MYDB_TEST_DISABLE_DEVICE_SYNC_WINDOW").is_some());
    }
    #[cfg(not(test))]
    true
}

fn drain(file: &File) -> io::Result<()> {
    #[cfg(test)]
    if let Some(errno) = DRAIN_ERROR.with(|error| error.take()) {
        return Err(io::Error::from_raw_os_error(errno));
    }
    loop {
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(test)]
thread_local! {
    static DRAIN_ERROR: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

fn boot_id() -> io::Result<String> {
    let mut bytes = [0u8; 128];
    let mut len = bytes.len();
    // SAFETY: the NUL-terminated key and output buffer remain live throughout.
    if unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let text = std::str::from_utf8(bytes.get(..len).ok_or(io::ErrorKind::InvalidData)?)
        .map_err(|_| io::ErrorKind::InvalidData)?
        .trim_end_matches('\0');
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(text.to_owned())
}

fn cache_path(device: Device) -> io::Result<PathBuf> {
    static ROOT: OnceLock<Option<(PathBuf, String)>> = OnceLock::new();
    let (root, boot) = ROOT
        .get_or_init(|| {
            (|| -> io::Result<_> {
                #[cfg(test)]
                let parent = PathBuf::from("/tmp/mydb");
                #[cfg(not(test))]
                let parent = std::env::temp_dir();
                let root = parent.join(format!("mydb-device-sync-v1-{}", unsafe {
                    libc::geteuid()
                }));
                DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&root)?;
                let meta = fs::symlink_metadata(&root)?;
                if !meta.is_dir()
                    || meta.uid() != unsafe { libc::geteuid() }
                    || meta.mode() & 0o022 != 0
                {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                Ok((root, boot_id()?))
            })()
            .ok()
        })
        .as_ref()
        .ok_or(io::ErrorKind::Unsupported)?;
    Ok(root.join(format!(
        "{boot}-{:x}-{:x}-{:x}.state",
        device.dev, device.fsid[0], device.fsid[1]
    )))
}

pub(super) fn sync(file: &File) -> io::Result<()> {
    #[cfg(test)]
    {
        static DISABLED: OnceLock<bool> = OnceLock::new();
        if *DISABLED
            .get_or_init(|| std::env::var_os("MYDB_TEST_DISABLE_DEVICE_SYNC_GROUP").is_some())
        {
            return file.sync_all();
        }
    }
    type Cache = BTreeMap<Device, Option<Arc<Coordinator>>>;
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let group = Device::of(file).ok().and_then(|device| {
        let mut cache = CACHE.get_or_init(Mutex::default).lock().unwrap();
        if cache.len() >= 64 && !cache.contains_key(&device) {
            return None;
        }
        cache
            .entry(device)
            .or_insert_with(|| {
                cache_path(device)
                    .ok()
                    .and_then(|path| Coordinator::open(&path).ok().flatten())
                    .map(Arc::new)
            })
            .clone()
    });
    if let Some(group) = group {
        // A failed data drain must not be hidden by retrying a device flush:
        // some filesystems consume a pending write error on the first fsync.
        return group.sync(file);
    }
    // Cache creation, filesystem support and counter overflow never weaken a
    // barrier or add a new failure mode: retain the original complete sync.
    file.sync_all()
}

pub(super) fn supports(file: &File) -> bool {
    Device::of(file).is_ok()
}

#[cfg(test)]
mod tests;
