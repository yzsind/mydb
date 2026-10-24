# `SHOW MYDB BACKUP STATUS`

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
