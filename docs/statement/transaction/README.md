# Transactions and Sessions

| Group | Statements |
| --- | --- |
| Transaction boundaries | [START TRANSACTION / BEGIN](start-transaction.md) · [COMMIT](commit.md) · [ROLLBACK](rollback.md) |
| Savepoints | [SAVEPOINT](savepoint.md) · [ROLLBACK TO](rollback-to-savepoint.md) · [RELEASE](release-savepoint.md) |
| Table locks | [LOCK TABLES](lock-tables.md) · [UNLOCK TABLES](unlock-tables.md) |
| Default characteristics | [SET TRANSACTION](set-transaction.md) · [SET AUTOCOMMIT](set-autocommit.md) |
| Session settings | [SET](set.md) · [RESET PERSIST](reset-persist.md) · [SET NAMES](set-names.md) · [SET CHARACTER SET](set-character-set.md) · [SET RESOURCE GROUP](set-resource-group.md) |
| XA | [START](xa-start.md) · [END](xa-end.md) · [PREPARE](xa-prepare.md) · [COMMIT](xa-commit.md) · [ROLLBACK](xa-rollback.md) · [RECOVER](xa-recover.md) |

Read [MVCC and locking](../../mvcc-and-locking.md) for the RC default, dirty RU reads, RR snapshots without gap locks, SERIALIZABLE table locks, timeout/deadlock behavior and savepoint lock retention. [Execution contracts](../execution-contracts.md#transaction-boundaries) list implicit commits. [RLS context](../security/set-rls-context.md) has explicit transaction/savepoint restoration. RESOURCE GROUP is unsupported. See [XA START](xa-start.md) for branch creation and [XA RECOVER](xa-recover.md) for recovery inspection.
