# `XA START`

## Description

Starts or associates an XA branch.

## Syntax

```ebnf
<xa-start> ::= 'XA START' <xid> [ 'JOIN' | 'RESUME' ]
<xid> ::= <gtrid> [ ',' <bqual> [ ',' <format-id> ] ]
```

## Behavior and Limitations

- mydb supports the basic XA 2PC flow but not JOIN/RESUME; these modifiers return a clear 1235.
- The XID must be within the permitted length and format, and must not conflict with an active branch.

## Examples

```sql
XA START 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
