-- TPC-H Query 5: Local Supplier Volume
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer
    INNER JOIN orders ON c_custkey = o_custkey
    INNER JOIN lineitem ON l_orderkey = o_orderkey
    INNER JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey
    INNER JOIN nation ON s_nationkey = n_nationkey
    INNER JOIN region ON n_regionkey = r_regionkey
WHERE
    r_name = 'ASIA'
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    n_name
ORDER BY
    revenue DESC;
