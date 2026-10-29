# First-release persistent-format corpus

These bytes are embedded by the `release_format` tests and compared with the
current encoders. Page samples use a fixed owner (`a5` repeated 16 times), LSN
17 and explicit physical page numbers. Segment/control samples use that fixed
storage UUID; no host paths, clocks, native padding or randomized hash order
belong in these fixtures. Scalars use little-endian encoding.

The corpus covers root admission, classic heap/index/LOB pages, all ten B-tree
leaf and internal key layouts, spatial/vector/Zone Map pages, row values and
codec descriptors, WAL and undo segment/frame envelopes and checked metadata.
The existing decoder, DDL, crash-recovery, backup and replication regression
suites exercise the associated semantic contracts and error paths.

Run `cargo nextest run -E 'test(release_format)'` to check the frozen bytes.
Intentional format revisions first generate candidates with
`MYDB_FORMAT_GOLDEN_OUTPUT=/tmp/mydb/format-candidates` using the same filter;
review the protocol/version changes and candidate bytes before replacing this
corpus. Normal tests never regenerate their own expected results.

`scripts/fuzz_storage_formats.sh` seeds the persistence-decoder fuzzer from
these fixtures and keeps generated data under `/tmp/mydb`. Run the same tests
on each supported architecture to check the architecture-independent bytes.
