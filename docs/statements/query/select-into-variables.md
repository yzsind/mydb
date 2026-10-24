# `SELECT ... INTO` User Variables

## Description

Assigns, by position, the single row returned by a query to session user variables; the statement returns no ordinary result set to the client.

## Syntax

```ebnf
<select-into-vars> ::= <select-prefix> 'INTO' <user-variable>
                       { ',' <user-variable> } <select-tail>
```

`INTO` may appear at the MySQL position after the select list; the query body can still contain CTEs, set operations, ordering, and expressions.

## Behavior and Limitations

- The number of targets must equal the query's column count.
- With exactly one row, columns are assigned one by one; with zero rows the variables keep their previous values; more than one row returns 1172.
- User variables belong to the current connection and disappear on disconnect. This statement cannot assign to local routine variables; for the in-routine syntax see stored-program SELECT INTO.
- The legacy position after the select list may produce deprecation warning 1287 under the MySQL compatibility rules.

## Examples

```sql
SET @old_total = 0;
SELECT COUNT(*), COALESCE(SUM(total), 0)
INTO @order_count, @order_total
FROM orders
WHERE customer_id = 42;

SELECT @order_count, @order_total;

SELECT id INTO @one_id FROM orders ORDER BY id LIMIT 1;
```

If the last query lacks `LIMIT 1` and returns multiple rows, the statement fails and reports 1172.

## Related Statements

- [SELECT](select.md)
- [Routine SELECT INTO](../programmatic/select-into.md)
