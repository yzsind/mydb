-- mydb TPC-H extension Query 24: Daily Supplier Receipts
-- Covers a three-table join, GROUP BY, and a descending DATE sort key.
SELECT
    l.l_receiptdate,
    n.n_nationkey,
    COUNT(*) AS line_count,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem AS l
    INNER JOIN supplier AS s ON s.s_suppkey = l.l_suppkey
    INNER JOIN nation AS n ON n.n_nationkey = s.s_nationkey
WHERE
    l.l_receiptdate >= DATE '1996-01-01'
    AND l.l_receiptdate < DATE '1996-04-01'
GROUP BY
    l.l_receiptdate,
    n.n_nationkey
ORDER BY
    l.l_receiptdate DESC,
    revenue DESC,
    n.n_nationkey
LIMIT 100;
