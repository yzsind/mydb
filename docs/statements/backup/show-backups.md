# `SHOW MYDB BACKUPS`

```sql
SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repository';
```

Lists live, published baselines in the unified repository catalog. Columns:
Base_ID, Source_ID, Stream_ID, Source_End_LSN and Source_End_Time. Standalone
baselines have no stream. Unpublished staging files and pruned baselines are
excluded. Catalog/identity errors fail explicitly. The operation holds a shared
repository lease and modifies neither the source nor the repository.

See [Backup and Recovery](../../backup-and-recovery.md).
