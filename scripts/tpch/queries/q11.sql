-- TPC-H Query 11: Important Stock Identification
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp
    INNER JOIN supplier ON ps_suppkey = s_suppkey
    INNER JOIN nation ON s_nationkey = n_nationkey
WHERE
    n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM
            partsupp
            INNER JOIN supplier ON ps_suppkey = s_suppkey
            INNER JOIN nation ON s_nationkey = n_nationkey
        WHERE
            n_name = 'GERMANY'
    )
ORDER BY
    value DESC;
