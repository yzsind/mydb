#!/usr/bin/env bash
# Compatibility wrapper for the retired mysqlbinlog-based PITR runbook.
# Native physical backup/WAL recovery is implemented by `mydb-backup`.

set -euo pipefail

cat >&2 <<'EOF'
scripts/pitr_restore.sh has been retired.

Use the crash-safe native restore tool instead:

  mydb-backup restore --repository /path/to/repository \
    --target-time '2026-08-27T12:34:56+08:00' \
    --data-dir /path/to/new-data-dir

or select an exact WAL position with --target-lsn. See
docs/backup-and-recovery.md. The old script deleted the destination and
replayed external MySQL binlogs; it is intentionally no longer executed.
EOF

exit 2
