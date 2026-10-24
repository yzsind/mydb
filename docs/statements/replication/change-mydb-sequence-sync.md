# `CHANGE MYDB SEQUENCE SYNC TO`

Changes the native primary's persisted sequence reservation synchronization policy.
It is independent of MySQL binlog and best-effort native semi-sync.

## Syntax

```ebnf
<change-sequence-sync> ::= 'CHANGE MYDB SEQUENCE SYNC TO' <json-string>
```

The quoted JSON object has these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `remote_apply` | boolean | false requires local WAL durability; true also requires every configured remote candidate to durably apply each new reservation |
| `replicas` | array of UUID strings | Sorted, duplicate-free remote node UUIDs, at most 64; nonempty for remote_apply and empty for local |
| `epoch` | positive integer | Exactly the previous persisted epoch plus one; the initial epoch is 1 |

## Preconditions and Effects

- Requires `SYSTEM_VARIABLES_ADMIN` or an equivalent administrative privilege.
- Run on a native master after successful `STOP MASTER WRITES` and writer drain;
  incomplete promotion, active conflicting work or an invalid candidate list fails explicitly.
- Obtain actual remote node UUIDs from `SHOW MYDB REPLICA STATUS` or
  `SHOW MYDB REPLICAS`. The master itself cannot be a candidate.
- Before a strict configuration is committed, all selected candidates must cover
  the current durable WAL boundary; they must then also acknowledge the configuration
  commit. Do not resume writes until this succeeds. Failure keeps writes stopped;
  inspect the persisted epoch before constructing a retry, since publication may
  have completed before the final acknowledgment failed.
- Operations must retain the epoch and externally fence removed candidates from
  promotion eligibility. This command does not provide leader election or fencing.
- Memory durability and independent writable standalone instances do not support
  remote_apply. Existing confirmed sequence CACHE windows require no per-value
  network round trip; new windows and SETVAL require policy confirmation.

## Examples

For an existing primary at epoch 1, replace the illustrative UUID with an actual
remote node UUID:

```sql
STOP MASTER WRITES WAIT 30;
CHANGE MYDB SEQUENCE SYNC TO '{"remote_apply":true,"replicas":["11111111-1111-4111-8111-111111111111"],"epoch":2}';
RESUME MASTER WRITES;
```

To deliberately change an epoch-2 configuration back to local durability:

```sql
STOP MASTER WRITES WAIT 30;
CHANGE MYDB SEQUENCE SYNC TO '{"remote_apply":false,"replicas":[],"epoch":3}';
RESUME MASTER WRITES;
```

Under local policy, forced promotion of a lagging replica can reuse reservations
that were not replicated. Strict promotion requires the operator-confirmed epoch:
`PROMOTE TO MASTER FORCE FENCED SEQUENCE_SYNC_EPOCH=2` for the first configuration
above. Confirm the actual epoch and external fencing before issuing it.

## See Also

- [Sequences](../../sequences.md)
- [PROMOTE TO MASTER](promote-to-master.md)
- [Native replication](../../native-replication.md)
- [Replication statement index](README.md)
