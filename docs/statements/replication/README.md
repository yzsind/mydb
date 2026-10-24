# Replication and Binlog Management Statements

| Group | Statements |
| --- | --- |
| Binlog source | [STATUS](show-binary-log-status.md) · [LOGS](show-binary-logs.md) · [REPLICAS](show-replicas.md) · [FLUSH](flush-binary-logs.md) · [RESET](reset-binary-logs.md) · [EVENTS](show-binlog-events.md) · [PURGE](purge-binary-logs.md) |
| Legacy profiler compatibility | [SHOW PROFILES/PROFILE](show-profiles.md) |
| MySQL→mydb | [CHANGE SOURCE](change-replication-source.md) · [START](start-replica.md) · [STOP](stop-replica.md) · [RESET](reset-replica.md) · [STATUS](show-replica-status.md) · [RELAYLOG EVENTS](show-relaylog-events.md) |
| mydb→mydb status | [MASTER STATUS](show-mydb-master-status.md) · [REPLICA STATUS](show-mydb-replica-status.md) · [REPLICAS](show-mydb-replicas.md) |
| mydb→mydb control | [CHANGE MASTER](change-mydb-master.md) · [CHANGE SEQUENCE SYNC](change-mydb-sequence-sync.md) · [START](start-mydb-replication.md) · [STOP](stop-mydb-replication.md) · [RESET](reset-mydb-replication.md) |
| mydb role switching | [STOP WRITES](stop-master-writes.md) · [RESUME WRITES](resume-master-writes.md) · [PROMOTE](promote-to-master.md) · [DEMOTE](demote-to-replica.md) · [DROP REPLICA](drop-mydb-replica.md) |

The MySQL→mydb receiver requires `--features mysql-replica`; mydb→mydb uses a separate protocol v2, LSN, timeline, and slot, and is not mixed with MySQL binlog commands.
