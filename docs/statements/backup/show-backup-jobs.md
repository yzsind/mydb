# `SHOW MYDB BACKUP JOBS`

## Syntax

```ebnf
<show-backup-jobs> ::= 'SHOW MYDB BACKUP JOBS'
```

## Behavior

```sql
SHOW MYDB BACKUP JOBS;
```

Lists running and recent durable job history, ordered newest sequence first. Completed history retains up to 256 WAL jobs and, independently, 256 other jobs, so continuous WAL archiving cannot evict recent database-backup results. Running work returns immediately; completion must be observed explicitly. Read-only images expose an empty local job list. Reading does not copy data files.

A RUNNING job restored after process interruption becomes FAILED with Failure_Class=PROCESS_INTERRUPTED; committed repository artifacts are reconciled independently. Retrying is a new request and must take the current publication state into account.

Use Job_ID with [CANCEL MYDB BACKUP](cancel-backup.md).

## Result Columns

Columns are shared by BACKUP DATABASE, BACKUP WAL and ALTER results that start a drain job. Numeric diagnostic values added after Error currently use VARCHAR wire metadata; their content and units remain as listed.

| Column | Wire type | Meaning |
| --- | --- | --- |
| `Job_ID` | VARCHAR | Stable job identifier used by CANCEL. |
| `Kind` | VARCHAR | DATABASE (full baseline), WAL, or STOP (policy drain). |
| `State` | VARCHAR | RUNNING, COMPLETED, FAILED or CANCELLED. |
| `Phase` | VARCHAR | Current/last phase; QUEUED and WAITING_RESOURCES are not completion. |
| `Base_ID` | VARCHAR | Published baseline identity when assigned; otherwise NULL. |
| `Requested_LSN` | BIGINT | Fixed source LSN target, when applicable. |
| `Requested_Source_Time` | BIGINT | Requested source time, Unix microseconds. |
| `Durable_Through_LSN` | BIGINT | Durably published repository coverage boundary. |
| `Covered_Time` | BIGINT | Confirmed source time coverage, Unix microseconds. |
| `Catalog_Generation` | BIGINT | Repository publication generation confirming the receipt. |
| `Bytes` | BIGINT | Processed byte count. |
| `Error` | VARCHAR | Empty on success/no recorded failure; diagnostic text on failure. |
| `Created_Unix_Ms` | VARCHAR | Integer timestamp in Unix milliseconds. |
| `Started_Unix_Ms` | VARCHAR | Integer start timestamp; 0 before start. |
| `Completed_Unix_Ms` | VARCHAR | Integer terminal timestamp; 0 before completion. |
| `Failure_Class` | VARCHAR | Diagnostic category, e.g. PROCESS_INTERRUPTED. |
| `Phase_Times_Us` | VARCHAR | JSON object of phase elapsed microseconds. |
| `Repository_ID` | VARCHAR | Repository identity, or NULL when not yet assigned. |
| `Files_Done` | VARCHAR | Integer count of copied files. |
| `Phase_Remaining_Bytes` | VARCHAR | Integer remaining bytes for a fixed copy phase; NULL if unknown. |

## Privileges and Polling

Requires global BACKUP_ADMIN, including read-only SHOW. Read repeatedly until the requested Job_ID is COMPLETED, FAILED or CANCELLED. Do not mistake an empty Error field on a RUNNING row for success.

```sql
SHOW MYDB BACKUP JOBS;
SHOW MYDB BACKUP STATUS;
```

Publishing/QUERY_READY phases reject cancellation; see [CANCEL](cancel-backup.md). Phase is diagnostic progress, not a second terminal-state enum.
