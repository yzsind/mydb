# Local binlog compression maintenance

This directory contains the verified crates.io mysql_common 0.37.3 release.
The upstream archive checksum and repository are recorded in `UPSTREAM.json`.
Both upstream licenses are retained. `Cargo.toml.orig` is the unmodified
upstream manifest for provenance; Cargo uses the patched `Cargo.toml`.

The local patch upgrades the optional binlog zstd dependency from 0.13 to
0.14.3. This removes the vulnerable zstd/zstd-safe/zstd-sys dependency chain
reported by RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403, and
RUSTSEC-2026-0410. Protocol and binlog parsing source is unchanged.

Both the root package and vendored mysql client resolve to this directory.
Upstream updates must review and reapply this dependency patch until the
maintained upstream release uses the fixed compression dependencies.
