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

## Examples

```sql
DEMOTE TO REPLICA WITH MASTER='new-primary.example:3317';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
