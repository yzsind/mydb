-- mydb TPC-H extension Query 23: Daily Order Revenue
-- Covers joins, GROUP BY, aggregate ordering, and an ascending DATE sort key.
SELECT
    o.o_orderdate,
    o.o_orderpriority,
    COUNT(*) AS line_count,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    orders AS o
    INNER JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey
WHERE
    o.o_orderdate >= DATE '1995-01-01'
    AND o.o_orderdate < DATE '1996-01-01'
GROUP BY
    o.o_orderdate,
    o.o_orderpriority
ORDER BY
    o.o_orderdate,
    revenue DESC,
    o.o_orderpriority
LIMIT 100;
