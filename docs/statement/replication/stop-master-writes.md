# `STOP MASTER WRITES`

## Description

Freezes new writes on the native master, waits for active writers to drain, and returns the fence LSN/token.

## Syntax

```ebnf
<stop-master-writes> ::= 'STOP MASTER WRITES'
                         [ 'WAIT' <seconds> ]
```

## Behavior, Prerequisites, and Limitations

- Allowed only on a master; WAIT defaults to 30 seconds with a range of 1..3600.
- On success it checkpoints and returns Fence_LSN, Timeline_ID, and Fence_Token for use by a planned PROMOTE.
- After a timeout the master stays frozen; resolve the writers before continuing the switch, or run RESUME MASTER WRITES.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN on the native master. The returned fence fields are Fence_LSN (BIGINT), Timeline_ID and Fence_Token (strings); record all of them for a planned switch. WAIT defaults to 30 seconds and accepts 1..3600. Active writers and conflicting snapshot/apply work can delay the fence. Timeout or KILL QUERY does not automatically unfreeze; inspect the persisted fence before retrying or RESUME MASTER WRITES.

## Examples

```sql
STOP MASTER WRITES WAIT 60;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
