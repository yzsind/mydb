//! Host/container memory discovery for capacity defaults.
//!
//! Explicit memory configuration always wins. These helpers are only used
//! when the operator leaves `MYDB_MEMORY_TARGET` unset.

const MIB: u64 = 1024 * 1024;
const FALLBACK_TARGET_MB: u64 = 1024;

/// Parse memory-target syntax shared by startup validation, the governor and
/// buffer-pool sizing. A bare integer is MiB; K/M/G/T suffixes are binary.
/// Targets must resolve to a positive whole number of MiB.
pub fn parse_mebibyte_or_size_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let (digits, multiplier) = match value.as_bytes().last().copied()? {
        b'K' | b'k' => (&value[..value.len() - 1], 1024u64),
        b'M' | b'm' => (&value[..value.len() - 1], MIB),
        b'G' | b'g' => (&value[..value.len() - 1], 1024u64.saturating_pow(3)),
        b'T' | b't' => (&value[..value.len() - 1], 1024u64.saturating_pow(4)),
        _ => (value, MIB),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|number| *number > 0)
        .map(|number| number.saturating_mul(multiplier))
        .filter(|bytes| *bytes > 0 && *bytes % MIB == 0)
}

/// Automatic advisory working-set target: 50% of effective host/cgroup memory.
/// Hard admission separately derives its limits from the full effective memory.
pub fn default_memory_target_mb() -> u64 {
    effective_memory_limit_bytes()
        .map(default_memory_target_mb_for_limit)
        .unwrap_or(FALLBACK_TARGET_MB)
}

fn default_memory_target_mb_for_limit(limit_bytes: u64) -> u64 {
    let limit_mb = limit_bytes / 2 / MIB;
    (limit_mb != 0)
        .then_some(limit_mb)
        .unwrap_or(FALLBACK_TARGET_MB)
}

/// Keep allocator residency proportional to touched bytes in memory-limited
/// Linux processes. Sparse per-thread allocator arenas can otherwise fault a
/// 2 MiB THP for a few live objects, outside the logical allocation governor.
/// Apply before starting any worker threads (the policy is inherited).
pub fn configure_allocator_page_policy() {
    #[cfg(all(target_os = "linux", feature = "fast-alloc"))]
    if linux_cgroup_memory_limit_bytes()
        .is_some_and(|limit| host_memory_bytes().is_none_or(|host| limit < host))
    {
        // Respect an explicit allocator tuning choice; mimalloc itself reads
        // this option before main when its first heap is initialized.
        if std::env::var_os("MIMALLOC_ALLOW_THP").is_some() {
            return;
        }
        // SAFETY: prctl changes only this process's THP policy and takes
        // scalar arguments. Explicit hugetlb mappings are unaffected.
        let result = unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) };
        if result != 0 {
            crate::util::logging::log_message(format_args!(
                "mydb: could not disable transparent huge pages for memory-limited allocator: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
}

/// Effective memory ceiling: the lower of physical RAM and a finite cgroup
/// limit when both are available.
pub fn effective_memory_limit_bytes() -> Option<u64> {
    let host = host_memory_bytes();
    #[cfg(target_os = "linux")]
    {
        return match (host, linux_cgroup_memory_limit_bytes()) {
            (Some(host), Some(cgroup)) => Some(host.min(cgroup)),
            (Some(host), None) => Some(host),
            (None, Some(cgroup)) => Some(cgroup),
            (None, None) => None,
        };
    }
    #[cfg(not(target_os = "linux"))]
    host
}

#[cfg(target_os = "linux")]
pub(crate) fn host_memory_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| {
            let (name, rest) = line.split_once(':')?;
            if name != "MemTotal" {
                return None;
            }
            rest.split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
                .map(|kib| kib.saturating_mul(1024))
        })
}

#[cfg(target_os = "linux")]
fn linux_cgroup_memory_limit_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let limit = cgroup_memory_files(&text).find_map(|(_, limit)| read_finite_limit(&limit));
    limit
}

/// Controller paths are common to capacity defaults and live diagnostics.
/// Prefer v2; a finite-limit caller may still fall back to a v1 controller.
#[cfg(any(target_os = "linux", test))]
fn cgroup_memory_files(
    text: &str,
) -> impl Iterator<Item = (std::path::PathBuf, std::path::PathBuf)> + '_ {
    let v2 = text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|relative| {
            let base =
                std::path::Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
            (base.join("memory.current"), base.join("memory.max"))
        });
    let v1 = text
        .lines()
        .find_map(|line| {
            let mut fields = line.splitn(3, ':');
            let _id = fields.next()?;
            let controllers = fields.next()?;
            let relative = fields.next()?;
            controllers
                .split(',')
                .any(|name| name == "memory")
                .then_some(relative)
        })
        .map(|relative| {
            let base = std::path::Path::new("/sys/fs/cgroup/memory")
                .join(relative.trim_start_matches('/'));
            (
                base.join("memory.usage_in_bytes"),
                base.join("memory.limit_in_bytes"),
            )
        });
    v2.into_iter().chain(v1)
}

#[cfg(target_os = "linux")]
pub(crate) fn linux_cgroup_memory() -> (Option<u64>, Option<u64>) {
    let Ok(text) = std::fs::read_to_string("/proc/self/cgroup") else {
        return (None, None);
    };
    for (used, limit) in cgroup_memory_files(&text) {
        let used = std::fs::read_to_string(used)
            .ok()
            .and_then(|raw| raw.trim().parse().ok());
        // Diagnostics retain zero as a reported value; capacity defaults use
        // read_finite_limit's positive-only policy below.
        let limit = std::fs::read_to_string(limit)
            .ok()
            .and_then(|raw| raw.trim().parse().ok());
        if used.is_some() || limit.is_some() {
            return (used, limit);
        }
    }
    (None, None)
}

#[cfg(target_os = "linux")]
fn read_finite_limit(path: &std::path::Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse::<u64>().ok().filter(|limit| *limit > 0)
}

#[cfg(target_os = "macos")]
fn host_memory_bytes() -> Option<u64> {
    // Darwin's physical RAM size is a host property, not a pressure reading.
    // Statement-boundary governor refreshes must not issue sysctl for the
    // same value on every idle -> active transition. Linux cgroup limits
    // remain dynamically discovered by their existing path.
    static HOST_BYTES: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    if let Some(bytes) = HOST_BYTES.get() {
        return Some(*bytes);
    }
    let detected = unsafe {
        let mut total = 0u64;
        let mut len = std::mem::size_of::<u64>();
        (libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut total as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        ) == 0)
            .then_some(total)
    };
    if let Some(bytes) = detected.filter(|bytes| *bytes != 0) {
        let _ = HOST_BYTES.set(bytes);
    }
    detected
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn host_memory_bytes() -> Option<u64> {
    None
}

#[cfg(windows)]
fn host_memory_bytes() -> Option<u64> {
    crate::platform::memory::host_memory().map(|(total, _)| total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_target_uses_half_effective_limit() {
        assert_eq!(default_memory_target_mb_for_limit(512 * MIB), 256);
        assert_eq!(default_memory_target_mb_for_limit(2 * 1024 * MIB), 1024);
        assert_eq!(default_memory_target_mb_for_limit(64 * 1024 * MIB), 32_768);
        assert_eq!(default_memory_target_mb_for_limit(513 * MIB), 256);
    }

    #[test]
    fn explicit_target_parser_matches_startup_size_syntax() {
        assert_eq!(parse_mebibyte_or_size_bytes("200"), Some(200 * MIB));
        assert_eq!(parse_mebibyte_or_size_bytes("200M"), Some(200 * MIB));
        assert_eq!(parse_mebibyte_or_size_bytes("1G"), Some(1024 * MIB));
        assert_eq!(parse_mebibyte_or_size_bytes("1K"), None);
        assert_eq!(parse_mebibyte_or_size_bytes("bad"), None);
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;
    #[test]
    fn v2_precedence_and_v1_memory_controller_are_shared() {
        let paths: Vec<_> =
            cgroup_memory_files("5:cpu:/ignored\n2:memory,cpu:/legacy\n0::/unified\n").collect();
        assert_eq!(
            paths,
            [
                (
                    "/sys/fs/cgroup/unified/memory.current".into(),
                    "/sys/fs/cgroup/unified/memory.max".into()
                ),
                (
                    "/sys/fs/cgroup/memory/legacy/memory.usage_in_bytes".into(),
                    "/sys/fs/cgroup/memory/legacy/memory.limit_in_bytes".into()
                ),
            ]
        );
        assert_eq!(cgroup_memory_files("5:cpu:/ignored").count(), 0);
    }
}
