# `XA END`

## Description

Ends the active phase of an XA branch.

## Syntax

```ebnf
<xa-end> ::= 'XA END' <xid> [ 'SUSPEND' [ 'FOR MIGRATE' ] ]
```

## Behavior and Limitations

- The branch must be owned by this session and be in the ACTIVE state.
- The SUSPEND form is currently unsupported and returns a clear error.

## Examples

```sql
XA END 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
