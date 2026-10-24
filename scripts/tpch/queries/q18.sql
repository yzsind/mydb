-- TPC-H Query 18: Large Volume Customer
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_qty
FROM
    customer
    INNER JOIN orders ON c_custkey = o_custkey
    INNER JOIN lineitem ON o_orderkey = l_orderkey
WHERE
    o_orderkey IN (
        SELECT l_orderkey
        FROM lineitem
        GROUP BY l_orderkey
        HAVING SUM(l_quantity) > 300
    )
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
