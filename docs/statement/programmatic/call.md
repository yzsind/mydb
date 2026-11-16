# `CALL`

## Description

Invokes a stored procedure with IN/OUT/INOUT arguments and may return multiple result sets plus an OUT parameter result.

## Syntax

```ebnf
<call> ::= 'CALL' <qualified-name>
           [ '(' [ <expr> { ',' <expr> } ] ')' ]
```

## Behavior and Limitations

- OUT/INOUT targets can be user variables; prepared CALL also supports placeholders.
- Each SELECT inside the procedure can produce a result set; binary prepared OUT results set SERVER_PS_OUT_PARAMS.
- Argument count, modes, types, the EXECUTE privilege, and definer/invoker privileges are validated before the call.

## Context, Errors, and Lifecycle

Requires EXECUTE on the procedure; body objects are checked under saved DEFINER/INVOKER security. OUT/INOUT targets must be writable variables/parameters of the execution context. Argument conversion can fail before or during execution; a scalar expression is not an OUT destination.

A procedure can emit several SELECT result sets and a final status. Clients must consume all results, including prepared OUT-parameter results. CALL is not automatically one atomic business transaction: a procedure's successful earlier work or explicit COMMIT can remain after a later error. Use the [caller transaction example](../examples.md#explicit-transaction-around-a-procedure) when the business operation requires grouping.

## Examples

```sql
SET @new_balance = NULL;
CALL transfer(1, 2, 10.00);
CALL get_balance(1, @new_balance);
SELECT @new_balance;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
