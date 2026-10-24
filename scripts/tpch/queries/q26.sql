-- mydb TPC-H extension Query 26: Zone Map DATE Range
SELECT 
    l_shipinstruct,
    l_shipdate,
    MIN(l.l_shipdate) AS first_shipdate,
    MAX(l.l_shipdate) AS last_shipdate,
    COUNT(*) AS line_count,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem AS l
WHERE
    l_extendedprice=73200.15 and l_receiptdate<=date'1998-09-06'
GROUP BY
    l_shipinstruct,
    l_shipdate
ORDER BY
    first_shipdate,
    last_shipdate
LIMIT 10;
