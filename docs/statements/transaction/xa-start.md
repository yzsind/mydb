# `XA START`

## Description

Starts or associates an XA branch.

## Syntax

```ebnf
<xa-start> ::= ( 'XA START' | 'XA BEGIN' ) <xid> [ 'JOIN' | 'RESUME' ]
<xid> ::= <gtrid> [ ',' <bqual> [ ',' <format-id> ] ]
```

## Behavior and Limitations

- mydb supports the basic XA 2PC flow but not JOIN/RESUME; these modifiers return a clear 1235.
- The XID must be within the permitted length and format, and must not conflict with an active branch.

## Transaction Identifiers

gtrid is 1 through 64 bytes; bqual is 0 through 64 bytes and defaults to empty. String and X'...' binary literals identify bytes, not case-insensitive names. format-id defaults to 1; use a nonnegative integer token within signed 64-bit range. The full triple identifies a branch. An invalid identifier returns XAER_INVAL (1398); an already registered XID returns XAER_DUPID (1440).

## Complete Two-Phase Flow

```sql
XA START 'order-42';
UPDATE account SET balance=balance-10 WHERE id=1;
UPDATE account SET balance=balance+10 WHERE id=2;
XA END 'order-42';
XA PREPARE 'order-42';
XA RECOVER CONVERT XID;
XA COMMIT 'order-42';
```

The normal states are ACTIVE → ENDED → PREPARED → decision. A separate recovery connection can resolve a prepared branch. JOIN/RESUME and XA END SUSPEND are unsupported (1235). Ordinary transaction commands cannot implicitly decide the XA branch.

## Examples

```sql
XA START 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
