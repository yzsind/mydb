-- TPC-H Query 10: Returned Item Reporting
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer
    INNER JOIN orders ON c_custkey = o_custkey
    INNER JOIN lineitem ON l_orderkey = o_orderkey
    INNER JOIN nation ON c_nationkey = n_nationkey
WHERE
    o_orderdate >= '1993-10-01'
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
