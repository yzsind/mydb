-- TPC-H Query 2: Minimum Cost Supplier
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part
    INNER JOIN partsupp ON p_partkey = ps_partkey
    INNER JOIN supplier ON s_suppkey = ps_suppkey
    INNER JOIN nation ON s_nationkey = n_nationkey
    INNER JOIN region ON n_regionkey = r_regionkey
WHERE
    p_size = 15
    AND p_type LIKE '%BRASS'
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT MIN(ps_supplycost)
        FROM
            partsupp
            INNER JOIN supplier ON s_suppkey = ps_suppkey
            INNER JOIN nation ON s_nationkey = n_nationkey
            INNER JOIN region ON n_regionkey = r_regionkey
        WHERE
            p_partkey = ps_partkey
            AND r_name = 'EUROPE'
    )
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
