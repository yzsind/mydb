# Changing MyDB Versions

Public previews do not promise backward physical-format compatibility. Pin a
release archive/checksum or an image digest; `latest` can change and is unsuitable
as the only identity of a recoverable deployment. Documentation in a checkout
describes that source revision. Use the docs from the matching Git tag when
operating a release; the [format registry](storage-format-versions.md) on main
must not be assumed to describe an older downloaded binary.

## Choose a path

| Change | Supported approach |
| --- | --- |
| Compatible physical formats | Stop cleanly, preserve a full backup and keys, then reopen with the compatible binary and validate |
| Incompatible physical formats | Start a fresh directory and reload logical schema/data whose SQL and types the destination supports |
| Physical backup restore | Use a binary compatible with that backup's formats; this is not a format-conversion tool |
| Native replica replacement | Peers must match physical formats; reseeding is not cross-format migration |

## Rehearsal and cutover

1. Record SELECT VERSION(), executable or image checksum, configuration, session
   SQL modes, persisted globals, keys and the source format registry.
2. Rehearse a verified physical backup restore with the original compatible
   binary. Keep the old binary and protected keys available.
3. Export logical definitions and data while the source is quiescent. Use the
   client/driver to preserve unsigned integers, DECIMAL, bytes, temporal values,
   JSON, geometry and native vectors. Table-by-table SELECT/INSERT is a supported
   building block; there is no universal cross-version logical export command
   that promises every native feature. Inspect SHOW CREATE results and recreate
   users/grants, routines, events and policies deliberately.
4. Load into an independent fresh directory with the new binary. Reconcile
   counts, primary-key ranges and deterministic content digests, then run
   representative SQL, transactions, indexes and permission checks. Never reuse
   an incompatible source directory or edit format markers.
5. For final cutover, stop application writes, repeat or finish the consistent
   transfer, validate and switch the application endpoint. Enable scheduled events
   on only the intended writer. Keep the original instance stopped and preserved.

If acceptance fails before new writes, switch back to the unchanged old instance.
Once the new instance accepts writes, returning to the old copy loses those new
writes unless an independently validated reconciliation path exists. Native WAL
and physical backup cannot supply a cross-format rollback bridge. Do not promise
zero-downtime upgrade or automatic downgrade.

See [MySQL migration](mysql-migration.md), [backup](backup-and-recovery.md) and
[storage recovery](storage-format.md).
