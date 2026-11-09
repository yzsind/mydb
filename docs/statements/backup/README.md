# Backup Management Statements

| Statement | Description |
| --- | --- |
| [BACKUP DATABASE](backup-database.md) | Creates one sealed, queryable full baseline |
| [BACKUP WAL](backup-wal.md) | Archives new WAL and returns a durability receipt |
| [ALTER MYDB BACKUP](alter-backup.md) | Configures DISABLED, MANUAL or CONTINUOUS archiving |
| [SHOW MYDB BACKUP JOBS](show-backup-jobs.md) | Lists running and persisted recent jobs |
| [SHOW MYDB BACKUP STATUS](show-backup-status.md) | Shows policy, protection, coverage and errors |
| [SHOW MYDB BACKUPS](show-backups.md) | Lists published repository baselines |
| [CANCEL MYDB BACKUP](cancel-backup.md) | Cancels an eligible running job |

See [Backup and Recovery](../../backup-and-recovery.md) for direct queries,
PITR, offline export/import, retention and key configuration.

## Operational Flow

Configure MANUAL/CONTINUOUS, publish a full baseline, observe COMPLETED, then confirm WAL receipt coverage. New repositories default to DISABLED; BACKUP DATABASE alone does not enable continuous retention. All SQL, including SHOW, requires BACKUP_ADMIN. See [job columns and polling](show-backup-jobs.md#result-columns), [status fields](show-backup-status.md#fields-and-units), and [policy transitions](alter-backup.md#option-and-policy-transitions). CANCEL uses Job_ID; direct queries/recovery use Base_ID and confirmed coverage.
