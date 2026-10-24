# `SHOW MYDB BACKUP JOBS`

```sql
SHOW MYDB BACKUP JOBS;
```

Lists running and recent durable job history, bounded to 256 entries. Columns:
Job_ID, Kind, State, Phase, Base_ID, Requested_LSN, Requested_Source_Time,
Durable_Through_LSN, Covered_Time, Catalog_Generation, Bytes and Error.
Running work returns immediately; completion must be observed explicitly.
Interrupted jobs are reconciled on restart. Read-only images expose an empty
local job list. Reading this view does not start work or scan data files.

Use Job_ID with [CANCEL MYDB BACKUP](cancel-backup.md).
