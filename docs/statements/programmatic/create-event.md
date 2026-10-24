# `CREATE EVENT`

## Description

Creates a one-time or recurring scheduled event with its SQL routine body.

## Syntax

```ebnf
<create-event> ::= 'CREATE' [ 'DEFINER' [ '=' ] <account> ]
                   'EVENT' [ 'IF NOT EXISTS' ] <qualified-name>
                   'ON SCHEDULE' <event-schedule>
                   [ 'ON COMPLETION' [ 'NOT' ] 'PRESERVE' ]
                   [ 'ENABLE' | 'DISABLE' | 'DISABLE ON SLAVE' ]
                   [ 'COMMENT' <string-literal> ]
                   'DO' <routine-statement>
<event-schedule> ::= 'AT' <expr>
                     | 'EVERY' <interval-value> <interval-unit>
                       [ 'STARTS' <expr> ] [ 'ENDS' <expr> ]
```

## Behavior and Limitations

- AT/STARTS/ENDS are evaluated as runtime constants; time zones follow session/event-definition semantics. Numeric time expressions return an explicit error — convert Unix seconds with `FROM_UNIXTIME()`; compact numeric date literals are not supported.
- The EVERY interval must be positive, with no single component exceeding 2147483647; compound units such as `YEAR_MONTH` and `DAY_SECOND` are supported.
- The routine body supports a single SQL statement as well as `BEGIN … END`, reusing stored program local variables, branches, loops, and exception handling.
- The event scheduler executes the routine body according to its state; execution privileges are checked as the definer.
- ON COMPLETION controls whether the definition is kept after a one-time event finishes; with PRESERVE the definition is kept after completion and marked DISABLED.
- An already expired one-time event is never executed by CREATE. NOT PRESERVE returns note 1588 without retaining the event; PRESERVE retains a disabled definition. CURRENT_TIMESTAMP is evaluated at the schedule's time precision.

## Examples

```sql
CREATE EVENT purge_sessions
ON SCHEDULE EVERY 1 HOUR
DO DELETE FROM session_token WHERE expires_at < NOW();
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
