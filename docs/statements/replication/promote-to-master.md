# `PROMOTE TO MASTER`

## Description

Promotes a standalone node, or a native replica that has caught up and is safely fenced, to master.

## Syntax

```ebnf
<promote> ::= 'PROMOTE TO MASTER'
              [ 'AT LSN' <unsigned-integer>
                'FENCE_TOKEN' '=' <string-literal>
                'WAIT' <seconds>
              | 'FORCE FENCED' ]
              [ 'SEQUENCE_SYNC_EPOCH' '=' <positive-integer> ]
```

## Behavior, Prerequisites, and Limitations

- A standalone node accepts only a bare PROMOTE; a replica must provide planned-switch parameters or an explicit FORCE FENCED.
- Planned mode validates that the source is frozen, the token, timeline freshness, the durable applied LSN, and pending undo; WAIT ranges over 1..3600.
- FORCE FENCED may lose data beyond durable_applied_lsn and is intended only when the old master's isolation has been externally confirmed.
- Active snapshots or pending replication transactions block the role switch.
- The epoch suffix must be the final token, written `SEQUENCE_SYNC_EPOCH=N` without spaces around `=`.
- When sequence synchronization uses `remote_apply`, append the operator-confirmed current `SEQUENCE_SYNC_EPOCH=N` after the other parameters. Missing or mismatched epochs return 50030 (`MYDB_ER_ROLE_CHANGE_BLOCKED`). The supplied epoch must be a positive integer and match the persisted configuration even if the local policy does not require remote_apply.
- A promoted replica may have no eligible remote candidate left; new strict reservation windows then fail until a replacement candidate is configured and caught up. External fencing and retained configuration epochs remain operational requirements. See [CHANGE MYDB SEQUENCE SYNC TO](change-mydb-sequence-sync.md).

## Examples

```sql
PROMOTE TO MASTER AT LSN 481516
FENCE_TOKEN='550e8400-e29b-41d4-a716-446655440000'
WAIT 60;
```

For `remote_apply` with a confirmed current epoch of 2, the suffix is required:

```sql
PROMOTE TO MASTER FORCE FENCED SEQUENCE_SYNC_EPOCH=2;
```

Replace 2 with the persisted epoch confirmed by operations, and use FORCE FENCED
only after isolating the old master externally. For planned switchover, append
the same suffix after WAIT instead.

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
