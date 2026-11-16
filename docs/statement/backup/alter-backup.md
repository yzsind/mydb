# `ALTER MYDB BACKUP`

## Syntax

```ebnf
<alter-backup> ::= 'ALTER MYDB BACKUP TO' <string-literal>
                   'SET' <backup-option> { ',' <backup-option> }
<backup-option> ::= 'WAL_ARCHIVE' '=' ( 'DISABLED' | 'MANUAL' | 'CONTINUOUS' )
                    | 'MAX_PENDING_WAL_BYTES' '=' <unsigned-integer>
                    | 'DISCARD_PENDING' '=' ( 'TRUE' | 'FALSE' )
```

## Behavior

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=MANUAL, MAX_PENDING_WAL_BYTES=17179869184;
```

WAL_ARCHIVE accepts DISABLED, MANUAL and CONTINUOUS. Configuration can initialize
an empty repository but does not create a baseline. Enabled policies enter
WAITING_BASE until `BACKUP DATABASE` publishes a protected baseline. MANUAL keeps
unarchived WAL on the source; CONTINUOUS also transfers it automatically.
Switching between these enabled policies preserves the stream and retention.

DISABLED normally creates a drain job: a fixed final target is durably archived
and closed before retention is released. Failure preserves protection. To
explicitly abandon the pending tail, including an unavailable repository:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=DISABLED, DISCARD_PENDING=TRUE;
```

Re-enabling requires a new baseline and stream. The default pending WAL budget is
16 GiB. Exhaustion blocks new business writes while management, reads, archiving
and rollback remain available. No automatic policy downgrade occurs. Duplicate,
unknown or inconsistent options fail. Requires backup administration privileges.

See [Backup and Recovery](../../backup-and-recovery.md).

## Repository and Results

Repository is an absolute server-side path, optionally prefixed with `file://`; a relative path is rejected. It is not a client upload or remote object-store URI. Requires global `BACKUP_ADMIN`. An ALTER that schedules a drain returns the [job row schema](show-backup-jobs.md#result-columns); a synchronous configuration change returns OK.

## Option and Policy Transitions

| Option | Value and default |
| --- | --- |
| WAL_ARCHIVE | DISABLED, MANUAL or CONTINUOUS; a new unconfigured repository defaults to DISABLED |
| MAX_PENDING_WAL_BYTES | Positive u64 byte budget; default 17179869184 (16 GiB) |
| DISCARD_PENDING | TRUE/FALSE, default FALSE; TRUE requires WAL_ARCHIVE=DISABLED in the same statement |

Duplicate/unknown options and inconsistent combinations fail. Omitted options retain their current values. Enabling MANUAL/CONTINUOUS without a baseline enters WAITING_BASE. Publishing a baseline establishes ACTIVE protection. MANUAL↔CONTINUOUS preserves the stream. DISABLED normally passes through DRAINING until the fixed final tail is archived and CLOSED. A drain failure keeps protection; inspect Last_Error and retry. DISCARD_PENDING explicitly abandons the tail, and re-enabling needs a new baseline/stream. No automatic downgrade occurs when the WAL budget is exhausted.
