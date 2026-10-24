# `DELIMITER`

## Description

Compatibility for the client-side delimiter directive found in import scripts.

## Syntax

```ebnf
<delimiter> ::= 'DELIMITER' <delimiter-token>
```

## Behavior and Limitations

- The real MySQL CLI splits statements on the client and never sends DELIMITER to the server.
- The mydb server accepts the line and returns OK, but does not change the delimiter of subsequent protocol packets; the caller must still split routine bodies correctly.
- This compatibility no-op exists only for script replay and must not be relied on as server session state.

## Examples

```sql
DELIMITER //
CREATE PROCEDURE p() BEGIN SELECT 1; END//
DELIMITER ;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
