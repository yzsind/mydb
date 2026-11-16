# First-release persistent-format corpus

These binary files are embedded in the Rust test executables and compared with
the current encoders. They are test-only data and are not read by the database
at runtime. Archived test binaries retain the expected bytes without requiring
source-tree access at runtime.

The corpus covers root admission, classic heap/index/LOB pages, all ten B-tree
leaf and internal key layouts, spatial/vector/Zone Map pages, row values and
codec descriptors, WAL and undo segment/frame envelopes, vector state and
checked metadata. The existing decoder, DDL, crash-recovery, backup and
replication regression suites exercise the associated semantic contracts and
error paths.

Page ownership is fixed to sixteen `0xa5` bytes and the page LSN to 17 where
applicable, with explicit physical page numbers. Segment/control samples use
that fixed storage UUID; no timestamps, host paths, native padding or randomized
hash order belong in these fixtures. Pages retain the standard 16 KiB size and
IEEE CRC32. Scalars use little-endian encoding; byte order and codec IDs are
part of the contract. `SHA256SUMS` records the fixture checksums.

Run the following on each supported architecture to check the frozen bytes
and verify architecture-independent encoding:

```bash
cargo nextest run -E 'test(release_format) | test(zone_map_page_header_golden_and_standard_lsn)'
```

To propose a deliberate format revision, run those tests with
`MYDB_FORMAT_GOLDEN_OUTPUT=/tmp/mydb/format-candidates`, inspect the byte
changes alongside the canonical version constants and format documentation,
then explicitly replace the approved fixture files. Ordinary tests never
rewrite the references. A mismatch is not a reason to regenerate them without
a format review.

`scripts/fuzz_storage_formats.sh` seeds the persistence-decoder fuzzer from
these fixtures and keeps generated data under `/tmp/mydb`.
