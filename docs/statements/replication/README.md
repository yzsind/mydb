# Replication and Binlog Management Statements

| Group | Statements |
| --- | --- |
| Binlog source | [STATUS](show-binary-log-status.md) · [LOGS](show-binary-logs.md) · [REPLICAS](show-replicas.md) · [FLUSH](flush-binary-logs.md) · [RESET](reset-binary-logs.md) · [EVENTS](show-binlog-events.md) · [PURGE](purge-binary-logs.md) |
| Legacy profiler compatibility | [SHOW PROFILES/PROFILE](show-profiles.md) |
| MySQL→mydb | [CHANGE SOURCE](change-replication-source.md) · [START](start-replica.md) · [STOP](stop-replica.md) · [RESET](reset-replica.md) · [STATUS](show-replica-status.md) · [RELAYLOG EVENTS](show-relaylog-events.md) |
| mydb→mydb status | [MASTER STATUS](show-mydb-master-status.md) · [REPLICA STATUS](show-mydb-replica-status.md) · [REPLICAS](show-mydb-replicas.md) |
| mydb→mydb control | [CHANGE MASTER](change-mydb-master.md) · [CHANGE SEQUENCE SYNC](change-mydb-sequence-sync.md) · [START](start-mydb-replication.md) · [STOP](stop-mydb-replication.md) · [RESET](reset-mydb-replication.md) |
| mydb role switching | [STOP WRITES](stop-master-writes.md) · [RESUME WRITES](resume-master-writes.md) · [PROMOTE](promote-to-master.md) · [DEMOTE](demote-to-replica.md) · [DROP REPLICA](drop-mydb-replica.md) |

The MySQL→mydb receiver is included in every build; mydb→mydb uses a separate protocol v3, LSN, timeline, and slot, and is not mixed with MySQL binlog commands.

## Choosing and Observing Replication

MySQL channels require an existing snapshot and ROW/FULL-compatible source history; CHANGE SOURCE does not copy previously purged data. Native replication uses physical reseeding, persisted role state and WAL retention slots. Source file positions, native LSNs and slot acknowledgments are different coordinates.

MySQL SHOW commands need REPLICATION CLIENT, controls need REPLICATION_SLAVE_ADMIN. Native administration and SHOW require SYSTEM_VARIABLES_ADMIN; FORCE_RESYNC additionally requires SHUTDOWN. A successful START means a start request succeeded, not that the replica has caught up. Use the [native replica status](show-mydb-replica-status.md) and [MySQL channel status](show-replica-status.md). Logical binlog file deletion and native WAL retention are separate operations. See [execution contracts](../execution-contracts.md) and [native deployment](../../native-replication.md).
