# `DEMOTE TO REPLICA`

## Description

Demotes a frozen native master and makes it follow a new master.

## Syntax

```ebnf
<demote> ::= 'DEMOTE TO REPLICA'
             'WITH MASTER' [ '=' ] <string-literal>
```

## Behavior, Prerequisites, and Limitations

- The current node must be a master on which STOP MASTER WRITES has been issued, and its durable LSN must not have passed the fence.
- The MASTER value must be host:port; the switch stops the master service, persists the new role, and starts the receiver.

## Privileges, Results, and Operational Details

Requires SYSTEM_VARIABLES_ADMIN on a frozen master. Endpoint accepts host:port and bracketed IPv6, for example [::1]:3316. The old timeline must not have written past the fence/fork boundary. Publication persists the role and endpoint before following the new source; divergence requires explicit reseeding rather than concatenating WAL from different histories. Check Role/Resync_Required/Last_Status after any failed transition. See [planned switchover](../../native-replication.md#planned-switchover).

## Examples

```sql
DEMOTE TO REPLICA WITH MASTER='new-primary.example:3317';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
