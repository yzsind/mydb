-- mydb TPC-H extension Query 25: Zone Map DATE Equality
-- o_orderkey follows load order, making skipped zones observable; the DATE
-- equality remains a residual predicate and validates combined pruning.
SELECT /*+ ZONE_MAP(o) */
    o.o_orderdate,
    COUNT(*) AS order_count,
    SUM(o.o_totalprice) AS total_price
FROM
    orders AS o
WHERE
    o.o_orderdate = DATE '1996-08-20'
    AND o.o_orderkey BETWEEN 1 AND 6000
GROUP BY
    o.o_orderdate
ORDER BY
    o.o_orderdate;
