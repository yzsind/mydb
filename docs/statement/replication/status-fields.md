# Replication Status Fields

SQL result columns are ordered as listed. LSNs and binlog positions are unsigned-valued counters carried in integer metadata; they are not interchangeable coordinates. A status read samples concurrently changing sources. Native SHOW requires SYSTEM_VARIABLES_ADMIN and an initialized native manager; MySQL channel SHOW requires REPLICATION CLIENT. A missing manager returns the documented empty channel schema, while native manager absence is 1235.

## SHOW MYDB MASTER STATUS

| Column | Wire type | Meaning |
| --- | --- | --- |
| `Node_UUID` | VARCHAR | This node identity; physical seed copies do not make two nodes share a live identity. |
| `Cluster_UUID` | VARCHAR | Native replication cluster identity. |
| `Timeline_UUID` | VARCHAR | Current timeline identity. |
| `Role` | VARCHAR | Persisted standalone/master/replica role. |
| `Epoch` | BIGINT | Role-generation epoch, not the independent sequence-sync configuration epoch. |
| `Current_LSN` | BIGINT | Source appended WAL end. |
| `Durable_LSN` | BIGINT | Locally durable WAL watermark; on a replica, received WAL durability. |
| `Min_Available_LSN` | BIGINT | Oldest source WAL boundary still available. |
| `Active_Segment_Id` | BIGINT | Current WAL segment identifier. |
| `Segment_Count` | BIGINT | Retained segment count. |
| `WAL_Bytes` | BIGINT | Source WAL bytes. |
| `Active_Slot_Count` | BIGINT | Count of slots in this retention state. |
| `Invalidated_Slot_Count` | BIGINT | Count of slots in this retention state. |
| `Abandoned_Slot_Count` | BIGINT | Count of slots in this retention state. |
| `Min_Slot_Acked_LSN` | BIGINT | Minimum active slot retention watermark; NULL if no active slot. |
| `Replication_Mode` | VARCHAR | Configured native replication mode. |
| `Bind_Port` | BIGINT | Configured native listener port. |
| `Listener_Running` | VARCHAR | yes/no listener state. |
| `Connected_Senders` | BIGINT | Live sender connection count. |
| `Semi_Sync_Acked_Commits` | BIGINT | Cumulative commit outcome count for the named semi-sync outcome. |
| `Semi_Sync_Timeout_Commits` | BIGINT | Cumulative commit outcome count for the named semi-sync outcome. |
| `Semi_Sync_Degraded_Commits` | BIGINT | Cumulative commit outcome count for the named semi-sync outcome. |
| `Writes_Frozen` | VARCHAR | yes/no write fence/capture freeze state. |

## SHOW MYDB REPLICA STATUS

| Column | Wire type | Meaning |
| --- | --- | --- |
| `Node_UUID` | VARCHAR | This node identity; physical seed copies do not make two nodes share a live identity. |
| `Role` | VARCHAR | Persisted standalone/master/replica role. |
| `Epoch` | BIGINT | Role-generation epoch, not the independent sequence-sync configuration epoch. |
| `Master_Endpoint` | VARCHAR | Persisted upstream host:port; NULL if absent. |
| `Last_Master_UUID` | VARCHAR | Most recently observed upstream identity; NULL if none. |
| `Last_Master_Epoch` | BIGINT | Most recently observed source role epoch. |
| `Received_LSN` | BIGINT | Highest received source frame boundary. |
| `Durable_LSN` | BIGINT | Locally durable WAL watermark; on a replica, received WAL durability. |
| `Applied_LSN` | BIGINT | Highest successfully applied source boundary in memory. |
| `Durable_Applied_LSN` | BIGINT | Boundary both durably stored and applied; use for catch-up/failover. |
| `Local_Next_LSN` | BIGINT | Next local WAL allocation position, not proof of successful replay. |
| `Receive_Apply_Gap_LSN` | BIGINT | received minus durable_applied, saturating at zero. |
| `Last_Update_Ms` | BIGINT | Progress timestamp in Unix milliseconds. |
| `Receiver_Running` | VARCHAR | yes/no receiver lifecycle state. |
| `Resync_Required` | VARCHAR | Reseed marker reason, or NULL when none. |
| `Last_Status` | VARCHAR | Receiver diagnostic or NULL; can identify replay blocked by an old snapshot. |

## SHOW MYDB REPLICAS

| Column | Wire type | Meaning |
| --- | --- | --- |
| `Replica_UUID` | VARCHAR | Replica node identity registered in a slot. |
| `Connected` | VARCHAR | yes/no current sender connection state. |
| `Retention_State` | VARCHAR | `active`, `invalidated` or `abandoned` slot retention state. |
| `Replica_Acked_LSN` | BIGINT | Master-maintained slot retention watermark; can originate from a seed/resume boundary. |
| `Lag_LSN` | BIGINT | Current source LSN minus slot ack, saturating at zero; not measured seconds. |
| `Last_Seen_Ms` | BIGINT | Last slot contact timestamp, Unix milliseconds. |
| `First_Registered_Ms` | BIGINT | Initial slot registration timestamp, Unix milliseconds. |

A disconnected `active` slot can still pin WAL; `invalidated`/`abandoned` slots do not. ACK is constrained by valid source/receiver bounds and is not a direct SQL read of the replica. Do not promote using Received_LSN, Local_Next_LSN or an old RR read snapshot as a catch-up proof. Wait for Durable_Applied_LSN, then start a fresh read. Sequence synchronization configuration is observable through information_schema.MYDB_SEQUENCES.SYNC_CONFIG_EPOCH; it is not the Role Epoch column.

## SHOW REPLICA STATUS

The following table also gives legacy SHOW SLAVE STATUS headers; both forms carry the same row values. Legacy spelling emits warning 1287. All 60 columns are present even when the result is empty. Seconds_Behind_Source can be NULL when lag cannot be measured. Delay/retry durations are seconds; relay/source positions and space are byte coordinates; *_Gtid_Set is text, and *_Running/SSL flags are compatibility string values.

| Column | Wire type | Legacy header |
| --- | --- | --- |
| `Replica_IO_State` | VARCHAR | `Slave_IO_State` |
| `Source_Host` | VARCHAR | `Master_Host` |
| `Source_User` | VARCHAR | `Master_User` |
| `Source_Port` | BIGINT | `Master_Port` |
| `Connect_Retry` | BIGINT | `Connect_Retry` |
| `Source_Log_File` | VARCHAR | `Master_Log_File` |
| `Read_Source_Log_Pos` | BIGINT | `Read_Master_Log_Pos` |
| `Relay_Log_File` | VARCHAR | `Relay_Log_File` |
| `Relay_Log_Pos` | BIGINT | `Relay_Log_Pos` |
| `Relay_Source_Log_File` | VARCHAR | `Relay_Master_Log_File` |
| `Replica_IO_Running` | VARCHAR | `Slave_IO_Running` |
| `Replica_SQL_Running` | VARCHAR | `Slave_SQL_Running` |
| `Replicate_Do_DB` | VARCHAR | `Replicate_Do_DB` |
| `Replicate_Ignore_DB` | VARCHAR | `Replicate_Ignore_DB` |
| `Replicate_Do_Table` | VARCHAR | `Replicate_Do_Table` |
| `Replicate_Ignore_Table` | VARCHAR | `Replicate_Ignore_Table` |
| `Replicate_Wild_Do_Table` | VARCHAR | `Replicate_Wild_Do_Table` |
| `Replicate_Wild_Ignore_Table` | VARCHAR | `Replicate_Wild_Ignore_Table` |
| `Last_Errno` | BIGINT | `Last_Errno` |
| `Last_Error` | VARCHAR | `Last_Error` |
| `Skip_Counter` | BIGINT | `Skip_Counter` |
| `Exec_Source_Log_Pos` | BIGINT | `Exec_Master_Log_Pos` |
| `Relay_Log_Space` | BIGINT | `Relay_Log_Space` |
| `Until_Condition` | VARCHAR | `Until_Condition` |
| `Until_Log_File` | VARCHAR | `Until_Log_File` |
| `Until_Log_Pos` | BIGINT | `Until_Log_Pos` |
| `Source_SSL_Allowed` | VARCHAR | `Master_SSL_Allowed` |
| `Source_SSL_CA_File` | VARCHAR | `Master_SSL_CA_File` |
| `Source_SSL_CA_Path` | VARCHAR | `Master_SSL_CA_Path` |
| `Source_SSL_Cert` | VARCHAR | `Master_SSL_Cert` |
| `Source_SSL_Cipher` | VARCHAR | `Master_SSL_Cipher` |
| `Source_SSL_Key` | VARCHAR | `Master_SSL_Key` |
| `Seconds_Behind_Source` | BIGINT | `Seconds_Behind_Master` |
| `Source_SSL_Verify_Server_Cert` | VARCHAR | `Master_SSL_Verify_Server_Cert` |
| `Last_IO_Errno` | BIGINT | `Last_IO_Errno` |
| `Last_IO_Error` | VARCHAR | `Last_IO_Error` |
| `Last_SQL_Errno` | BIGINT | `Last_SQL_Errno` |
| `Last_SQL_Error` | VARCHAR | `Last_SQL_Error` |
| `Replicate_Ignore_Server_Ids` | VARCHAR | `Replicate_Ignore_Server_Ids` |
| `Source_Server_Id` | BIGINT | `Master_Server_Id` |
| `Source_UUID` | VARCHAR | `Master_UUID` |
| `Source_Info_File` | VARCHAR | `Master_Info_File` |
| `SQL_Delay` | BIGINT | `SQL_Delay` |
| `SQL_Remaining_Delay` | BIGINT | `SQL_Remaining_Delay` |
| `Replica_SQL_Running_State` | VARCHAR | `Slave_SQL_Running_State` |
| `Source_Retry_Count` | BIGINT | `Master_Retry_Count` |
| `Source_Bind` | VARCHAR | `Master_Bind` |
| `Last_IO_Error_Timestamp` | VARCHAR | `Last_IO_Error_Timestamp` |
| `Last_SQL_Error_Timestamp` | VARCHAR | `Last_SQL_Error_Timestamp` |
| `Source_SSL_Crl` | VARCHAR | `Master_SSL_Crl` |
| `Source_SSL_Crlpath` | VARCHAR | `Master_SSL_Crlpath` |
| `Retrieved_Gtid_Set` | VARCHAR | `Retrieved_Gtid_Set` |
| `Executed_Gtid_Set` | VARCHAR | `Executed_Gtid_Set` |
| `Auto_Position` | BIGINT | `Auto_Position` |
| `Replicate_Rewrite_DB` | VARCHAR | `Replicate_Rewrite_DB` |
| `Channel_Name` | VARCHAR | `Channel_Name` |
| `Source_TLS_Version` | VARCHAR | `Master_TLS_Version` |
| `Source_public_key_path` | VARCHAR | `Master_public_key_path` |
| `Get_Source_public_key` | BIGINT | `Get_master_public_key` |
| `Network_Namespace` | VARCHAR | `Network_Namespace` |

Read_Source_Log_Pos is receive progress; Exec_Source_Log_Pos and Executed_Gtid_Set reflect committed application progress. Last_IO_* and Last_SQL_* identify transport versus application failure. SQL_Delay/SQL_Remaining_Delay describe intentional delay, not necessarily an unhealthy receiver. An uninitialized manager yields zero rows with the canonical columns. FOR CHANNEL narrows the query; do not treat one channel's progress as a global barrier.

## Binlog and Relay Event Listings

SHOW BINARY LOG STATUS columns are File (VARCHAR), Position (BIGINT), Binlog_Do_DB (VARCHAR), Binlog_Ignore_DB (VARCHAR), Executed_Gtid_Set (VARCHAR). SHOW BINARY LOGS returns Log_name (VARCHAR), File_size (BIGINT bytes), Encrypted (VARCHAR, currently No). Disabled binlogging returns 1235.

SHOW BINLOG EVENTS and SHOW RELAYLOG EVENTS use Log_name, Pos, Event_type, Server_id, End_log_pos, Info. The binlog coordinates address binary events; relay JSON-Lines coordinates address the local framing, not a native MySQL relay binary stream. SHOW REPLICAS returns Server_id, Host, Port, Source_id and Replica_UUID; it lists currently registered logical consumers, not native WAL slots.
