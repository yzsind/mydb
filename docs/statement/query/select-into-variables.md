# `SELECT ... INTO` User Variables

## Description

Assigns, by position, the single row returned by a query to session user variables; the statement returns no ordinary result set to the client.

## Syntax

```ebnf
<select-into-vars> ::= <select-prefix> 'INTO' <user-variable>
                       { ',' <user-variable> } <select-tail>
                     | <query-expression> 'INTO' <user-variable>
                       { ',' <user-variable> }
```

INTO may appear after the select list or at the end of the query. The query body can contain CTEs, set operations, ordering and expressions; the target must still produce at most one row.

## Behavior and Limitations

- The number of targets must equal the query's column count.
- With exactly one row, columns are assigned one by one; with zero rows the variables keep their previous values; more than one row returns 1172.
- User variables belong to the current connection and disappear on disconnect. This statement cannot assign to local routine variables; for the in-routine syntax see stored-program SELECT INTO.
- Zero rows produces warning 1329 and leaves targets unchanged. The export writer has its own legacy-position warning; this variable-assignment form does not promise that export warning.

## Privileges, Results, and Boundaries

Source SELECT privileges apply; assigning @variables needs no extra blanket administration privilege. Source cardinality is checked before assigning the returned row: 1172 for more than one row, 1222 for a nonempty row whose column count differs from target count. No-data returns OK with warning 1329. Targets are session state, not transaction data.

```sql
SET @one_id=-1;
SELECT id FROM orders WHERE id=1 INTO @one_id;
SELECT @one_id; -- 1 with the shared fixture.
SELECT id FROM orders WHERE id=-1 INTO @one_id;
SHOW WARNINGS; -- 1329; @one_id is still 1.
```

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
