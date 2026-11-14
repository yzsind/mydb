# `SHOW MYDB BACKUP STATUS`

## Syntax

```ebnf
<show-backup-status> ::= 'SHOW MYDB BACKUP STATUS'
```

## Behavior

```sql
SHOW MYDB BACKUP STATUS;
```

Returns Name/Value rows describing requested/effective policy, lifecycle, health,
repository/source/stream identity, latest base E/T and readiness, source/durable
LSN, time coverage, source WAL retention floor, pending bytes, budget, errors and
retry time. WAITING_BASE, ACTIVE, DRAINING, CLOSED and BROKEN make the current
protection state explicit. Unconfigured instances still return source counters;
read-only images report READ_ONLY_IMAGE.

Reading does not initiate checkpoints, archiving or full backups. Concurrently
changing counters are diagnostic rather than a transactional snapshot.

See [Backup and Recovery](../../backup-and-recovery.md).

## Fields and Units

All rows use string columns `Name` and `Value`, including numeric/boolean content. Configured bindings expose the following rows. An unconfigured source returns source counters and DISABLED/UNCONFIGURED policy values; a read-only image returns only Lifecycle=READ_ONLY_IMAGE. Requires global BACKUP_ADMIN.

| Name | Value meaning |
| --- | --- |
| `Source_LSN` | Current source WAL boundary. |
| `Backup_Process_IO` | JSON process backup I/O diagnostics. |
| `Pending_WAL_Bytes` | Protected bytes not yet durably archived. |
| `WAL_Pressure` | Boolean; new write admission is blocked when true. |
| `WAL_Floor` | Protected source WAL retention floor; empty if none. |
| `Requested_Policy` | Requested DISABLED/MANUAL/CONTINUOUS. |
| `Effective_Policy` | Currently effective policy. |
| `Lifecycle` | UNCONFIGURED, WAITING_BASE, ACTIVE, DRAINING, CLOSED, BROKEN; read-only image uses READ_ONLY_IMAGE. |
| `Health` | OK, BROKEN, WAL_PRESSURE, CLOSED_PENDING_REPOSITORY, RETRYING, ERROR, CLOSED, or WAITING_FOR_COMMAND. |
| `Repository_ID` | Bound repository identity. |
| `Source_ID` | Source identity. |
| `Timeline_ID` | Source native timeline. |
| `Stream_ID` | Current continuation stream; empty if absent. |
| `Latest_Base` | Latest sealed baseline ID; empty if absent. |
| `Latest_Base_LSN` | Latest baseline source end LSN (E). |
| `Latest_Base_Time` | Latest baseline source end time (T), Unix microseconds. |
| `QUERY_READY` | Boolean: a sealed baseline is available for direct query. |
| `CONTINUATION_READY` | Boolean: a baseline exists for WAL continuation. |
| `Recoverable_From_LSN` | Baseline end LSN. |
| `Recoverable_Through_LSN` | Maximum confirmed archive/baseline coverage boundary. |
| `Next_Retry_Unix_Ms` | Next automatic retry time, Unix milliseconds. |
| `Durable_Archived_LSN` | Confirmed archive cursor. |
| `Covered_Time` | Confirmed source coverage time, Unix microseconds. |
| `MAX_PENDING_WAL_BYTES` | Configured protected pending-byte budget. |
| `Last_Error` | Latest error text; empty if none. |
| `Close_Publication_Pending` | Boolean: local close still needs repository publication. |
| `Discarded` | Boolean: tail protection was explicitly abandoned. |
| `Consecutive_Errors` | Consecutive retry error count. |
| `Last_Error_Unix_Ms` | Latest error time, Unix milliseconds. |
| `Source_Time_Lag_Us` | Current source-time lag, microseconds. |
| `Backup_Resources` | JSON limits and resource configuration. |

QUERY_READY is not a promise that all newer source WAL is archived. For continuation, inspect the completed receipt and Recoverable_Through_LSN/Covered_Time. On WAL_PRESSURE, restore archiving or explicitly choose a supported policy transition; do not assume the server drops retained WAL automatically.
