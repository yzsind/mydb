# Hardware and OS Requirements

This page describes the operating systems, CPU, memory, storage, and build
toolchain that mydb expects, and how the engine derives resource defaults from
the host or container it runs in. Configuration knobs mentioned here are
documented in detail in [Startup Configuration](configuration.md); this page
only summarizes what the platform must provide.

mydb is a single-process, multi-threaded server. It does not require huge pages,
external services, or kernel modules. Native OS libraries and the linked C/C++
runtime must be available; the current Windows MSVC binaries dynamically link
the Visual C++ runtime and Universal CRT.

## Operating systems

The table below describes source-build support and platform verification. The
unpublished 1.0.0 first-distribution candidate is scoped to macOS arm64 archives,
Linux x86_64/glibc 2.17 archives, and the locally verified `linux/amd64` image.
Other source-build targets and historical Windows verification do not imply
artifacts are included in that candidate. See
[platforms and distribution](../README.md#platforms-and-distribution).

| Platform | Status | Notes |
| --- | --- | --- |
| Linux x86_64 | Supported | The candidate archive targets glibc 2.17 or newer; see below for kernel and filesystem notes |
| Linux aarch64 | Supported | Source builds and Dockerfile support (`linux/arm64`); outside this candidate |
| macOS (Apple Silicon and Intel) | Supported | Development and regression platform; CI runs a compile and release smoke pass on macOS |
| Windows 11 x64 | Native foreground operation verified | Local NTFS, ordinary account; startup, TLS, SQL, process crash recovery, replication tests, and backup/restore verified |
| Windows 10 x64 / Windows Server 2016 and later x64 | Expected compatible; not individually verified | Within the toolchain baseline; require compatible runtimes and local NTFS; this is a static compatibility assessment, not native acceptance |
| Docker (`linux/amd64`, `linux/arm64`) | Supported | Local builds use `debian:bookworm-slim`; only amd64 is in this candidate; see [Docker](docker.md) |

### Linux

The x86_64 candidate archive targets glibc 2.17, providing a libc baseline for
distributions from RHEL/CentOS 7 era onward, as well as current Ubuntu, Debian,
and SUSE releases. Building from source works on any distribution with a
supported Rust toolchain.

Kernel requirements depend on the feature used:

- **Normal server operation** has no special kernel version requirement beyond
  what the C library needs.
- **Native backup publication** uses `renameat2` with `RENAME_NOREPLACE` for
  atomic, non-overwriting publication. This needs Linux 3.15 or a distribution
  backport, and a filesystem that implements the flag. Systems without the
  support return an explicit publication error; mydb never silently falls back
  to an overwriting rename. See
  [Backup and Recovery](backup-and-recovery.md).
- **Direct I/O for buffer pool data files** is optional and off by default
  (`MYDB_DATA_FILE_DIRECT_IO=1`). It applies only to heap, index, and LOB data
  files — never to WAL, undo, or binlog — and falls back to buffered I/O on
  filesystems that reject `O_DIRECT`. Alignment is handled by the engine.

mydb reads cgroup v2 (`memory.max`) and v1 (`memory.limit_in_bytes`) memory
limits, so capacity defaults inside containers and systemd units follow the
configured limit rather than host RAM. CPU parallelism defaults follow the
process's available CPUs (affinity-aware).

### macOS

Both Apple Silicon and Intel Macs are supported for development and evaluation.
The macOS counterpart of the Direct I/O switch is `MYDB_DATA_FILE_NOCACHE`
(off by default), which drops the page cache hint on data files without
changing durability semantics.

### Windows

The current target is `x86_64-pc-windows-msvc`, built with Rust 1.95.0.
The [Rust MSVC target baseline](https://doc.rust-lang.org/rustc/platform-support/windows-msvc.html)
is Windows 10 or later for clients and Windows Server 2016 or later for servers.
This requirement applies to generated programs, not only to the build machine.

| Windows version | Current assessment |
| --- | --- |
| XP / Vista / 7; Server 2003 / 2008 / 2008 R2 | Current binary is incompatible; it directly imports APIs absent from these systems |
| 8 / 8.1; Server 2012 / 2012 R2 | Outside the supported toolchain baseline; not supported by this build contract |
| 10 x64 | Expected compatible, pending native acceptance |
| 11 x64 | Native foreground operation verified on the tested Windows 11 host |
| Server 2016 / 2019 / 2022 / 2025 x64 | Expected compatible, pending native acceptance on each version |

An import-table audit of the tested executable found direct imports of
`WaitOnAddress`, `WakeByAddressAll`, `WakeByAddressSingle`, and
`GetSystemTimePreciseAsFileTime`. For example,
[WaitOnAddress](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitonaddress)
requires Windows 8 / Server 2012 or later, providing a concrete loader-level
blocker on Windows 7 and older. The PE subsystem version alone does not establish
the minimum runnable OS version.

The executable also imports `VCRUNTIME140.dll` and Universal CRT API sets.
Install a compatible x64
[Visual C++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
when these are absent. Changing Cargo's `rust-version` does not remove these
binary dependencies or add legacy Windows compatibility.

The verified scope is foreground operation on local NTFS under an ordinary
account. Windows Service registration/SCM lifecycle is not implemented. Windows
ARM64, 32-bit Windows, Nano Server, Windows containers, ReFS, SMB, and whole-machine
power-loss behavior have not been accepted.

## CPU

- **Architectures**: x86_64 and aarch64. Release binaries target the baseline
  ISA of each architecture and detect faster vector instructions (for example
  AVX2 and BMI2 on x86) at runtime, so older CPUs run correctly on scalar
  fallback paths.
- **Core count**: there is no minimum, and the engine is parallel — query
  execution workers, the blocking pool, and background maintenance all scale
  with the available CPU count by default. More cores benefit analytical
  queries, index builds, and concurrent OLTP load.
- **Per-worker stacks** default to 8 MiB (`MYDB_BLOCKING_STACK_MB`), which is
  included when sizing memory-constrained containers.

## Memory

mydb sizes itself from the **effective memory** of the machine or container:
physical RAM, or the cgroup memory limit when one is lower.

- **Default working-set target** (`MYDB_MEMORY_TARGET`): 50% of effective
  memory; when no memory can be detected, 1 GiB is assumed. The value accepts a
  bare number in MiB or `K`/`M`/`G`/`T` (binary) suffixes.
- **Buffer pool**: when not set explicitly, it is derived smoothly between 20%
  and 60% of the working-set target (for example, a 1000 MiB target yields a
  488 MiB pool), reserving headroom for catalog metadata, protocol buffers,
  undo, scratch space, and thread stacks during growth.
- **Unified memory governor**: buffer pages, executor memory, SQL caches, undo,
  and vector indexes use controlled reservations. The target sizes caches and
  spill watermarks; ordinary governed SQL admission has a separate hard boundary
  of 80% of effective memory, with 80–90% reserved for rollback/compensation.
  Optional caches can be skipped or evicted, and spillable operators spill at
  their watermarks; unreclaimable admission failure returns 1037 (HY001).
- **Crash recovery** maps validated WAL segments read-only in 64 MiB windows,
  with a residency ceiling of one third of the working-set target and at most
  2 GiB, so recovery memory scales with the target instead of the full WAL
  size.

`MYDB_MEMORY_TARGET` is an advisory working-set goal, not a hard process limit:
transient allocations and the allocator itself can exceed it. For hard
enforcement, set a cgroup (or container `--memory`) limit — mydb reads that
limit and derives its defaults from it. Inside memory-limited Linux containers
with the default `fast-alloc` allocator, mydb also disables transparent huge
pages for its own process to keep allocator residency proportional.

Practical guidance: small evaluation instances run with on the order of 1 GiB
of effective memory. Production sizing should start from the working set you
want cached (data and indexes actually touched) and set `MYDB_MEMORY_TARGET`
explicitly, leaving the rest of effective memory to the operating system page
cache and the process itself.

## Storage

- **Page size**: standard heap, LOB, index, vector and Zone Map pages use
  16 KiB, fixed at build time; a data directory is not relocatable across
  engines built with a different page size. WAL is a segmented stream of
  variable-length frames (48-byte headers plus payloads), with a default
  256 MiB segment rotation threshold. See [WAL layout](storage-format.md#121-wal-segment-and-frame-layout).
- **I/O mode**: buffered I/O by default, so the operating system page cache
  warms index lookups and heap fetches — leave RAM for the page cache when
  sizing memory. Optional Direct I/O on Linux bypasses it for data files (see
  above).
- **Durability**: the `strict` durability level guarantees that an
  acknowledged commit survives power loss. This requires storage that actually
  completes `fsync` — local SSD/NVMe, or virtual disks with write-through or
  flushed caches. Storage that acknowledges but delays writes (some virtual
  disk write-back modes, unsynchronized network filesystems) silently weakens
  crash durability regardless of the mydb setting. `relaxed` and `memory`
  levels deliberately trade durability for throughput.
- **Space planning**: reserve room for data and index files, WAL segments
  (reclaimed at checkpoints), undo and history retention, binlog (when
  enabled), spill files for large sorts/hash joins, and temporary space for
  online DDL and backup staging. Online DDL copy and backup staging can
  transiently need space comparable to the tables involved.
- **Filesystem operations**: backup publication requires atomic
  non-overwriting rename support (see the Linux notes above); checkpoint WAL
  reclamation replaces files rather than truncating in place.
- **File descriptors**: the server requests an fd soft limit of 65535 at
  startup (`open_files_limit`) and keeps up to 1024 data-file handles long term
  (`buffer_pool_file_cache_max`). Ensure the OS hard limit allows at least the
  configured value; the server never lowers an already higher inherited limit.

## Network

The SQL service listens on all IPv4 interfaces (`0.0.0.0`) on port `8738` by
default; restrict it with `--host` for local-only deployments. TLS is
advertised by default using an ephemeral self-signed certificate when no
certificate is configured. Replication links (both MySQL-source and native
physical replication) need reachable TCP connectivity between the nodes; there
is no UDP or multicast requirement.

## Build toolchain (building from source)

- **Rust 1.95 or newer**; the repository pins 1.95.0 in `rust-toolchain.toml`.
- A native C toolchain and Perl. RSA authentication builds vendored OpenSSL;
  SQL and native-replication TLS retain the rustls backend. The RSA backend
  requires OpenSSL 3.2 or newer with implicit PKCS#1 rejection and rejects
  incompatible providers at startup.
- A MySQL client (such as `mysql` from your distribution) for connecting.
- CI is configured for pull requests and pushes to `main`: Ubuntu 24.04
  runs the full test matrix and macOS 15 runs compile and release smoke checks.
  Tool binaries additionally build with
  `--features tools`.

## Quick sizing reference

| Scenario | Starting point |
| --- | --- |
| Functional evaluation, CI, unit-scale tests | Any supported OS, ~1 GiB effective memory, a few GiB of free disk |
| Development laptop (typical 16–32 GiB machine) | Defaults need no changes: target lands at 50% of RAM; cap with a cgroup/container limit if desired |
| OLTP production | Working-set-sized memory target (set `MYDB_MEMORY_TARGET` explicitly), local SSD/NVMe with honest `fsync`, headroom for WAL/undo/binlog/spill |
| Analytical / HTAP | Scale memory with the data being scanned and joined; more cores and I/O bandwidth directly help parallel execution and index builds |
