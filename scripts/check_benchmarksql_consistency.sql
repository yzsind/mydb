-- Run against the BenchmarkSQL database after all terminals and delivery
-- workers have finished. Every violations column must be zero.
-- Example: mysql --database=tpcc < scripts/check_benchmarksql_consistency.sql
START TRANSACTION WITH CONSISTENT SNAPSHOT;

SELECT 'warehouse_district_ytd' AS check_name, COUNT(*) AS violations
FROM bmsql_warehouse w
LEFT JOIN (
    SELECT d_w_id, SUM(d_ytd) AS ytd FROM bmsql_district GROUP BY d_w_id
) d ON d.d_w_id = w.w_id
WHERE d.d_w_id IS NULL OR ABS(w.w_ytd - d.ytd) > 0.005;

SELECT 'district_next_order' AS check_name, COUNT(*) AS violations
FROM bmsql_district d
LEFT JOIN (
    SELECT o_w_id, o_d_id, MAX(o_id) AS last_id
    FROM bmsql_oorder GROUP BY o_w_id, o_d_id
) o ON o.o_w_id = d.d_w_id AND o.o_d_id = d.d_id
WHERE o.last_id IS NULL OR d.d_next_o_id <> o.last_id + 1;

SELECT 'new_order_contiguous' AS check_name, COUNT(*) AS violations
FROM (
    SELECT no_w_id, no_d_id, COUNT(*) AS n, MIN(no_o_id) AS first_id,
           MAX(no_o_id) AS last_id
    FROM bmsql_new_order GROUP BY no_w_id, no_d_id
) n WHERE n.n <> n.last_id - n.first_id + 1;

SELECT 'order_line_count' AS check_name, COUNT(*) AS violations
FROM bmsql_oorder o
LEFT JOIN (
    SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) AS n
    FROM bmsql_order_line GROUP BY ol_w_id, ol_d_id, ol_o_id
) l ON l.ol_w_id = o.o_w_id AND l.ol_d_id = o.o_d_id AND l.ol_o_id = o.o_id
WHERE l.n IS NULL OR l.n <> o.o_ol_cnt;

SELECT 'new_order_carrier' AS check_name, COUNT(*) AS violations
FROM bmsql_oorder o
LEFT JOIN bmsql_new_order n
ON n.no_w_id = o.o_w_id AND n.no_d_id = o.o_d_id AND n.no_o_id = o.o_id
WHERE (o.o_carrier_id IS NULL AND n.no_o_id IS NULL)
   OR (o.o_carrier_id IS NOT NULL AND n.no_o_id IS NOT NULL);

SELECT 'order_line_delivery' AS check_name, COUNT(*) AS violations
FROM bmsql_order_line l
LEFT JOIN bmsql_oorder o
ON o.o_w_id = l.ol_w_id AND o.o_d_id = l.ol_d_id AND o.o_id = l.ol_o_id
WHERE o.o_id IS NULL
   OR (o.o_carrier_id IS NULL AND l.ol_delivery_d IS NOT NULL)
   OR (o.o_carrier_id IS NOT NULL AND l.ol_delivery_d IS NULL);

SELECT 'district_history_ytd' AS check_name, COUNT(*) AS violations
FROM bmsql_district d
LEFT JOIN (
    SELECT h_w_id, h_d_id, SUM(h_amount) AS amount
    FROM bmsql_history GROUP BY h_w_id, h_d_id
) h ON h.h_w_id = d.d_w_id AND h.h_d_id = d.d_id
WHERE h.amount IS NULL OR ABS(d.d_ytd - h.amount) > 0.005;

SELECT 'customer_history_payment' AS check_name, COUNT(*) AS violations
FROM bmsql_customer c
LEFT JOIN (
    SELECT h_c_w_id, h_c_d_id, h_c_id, SUM(h_amount) AS amount, COUNT(*) AS n
    FROM bmsql_history GROUP BY h_c_w_id, h_c_d_id, h_c_id
) h ON h.h_c_w_id = c.c_w_id AND h.h_c_d_id = c.c_d_id AND h.h_c_id = c.c_id
WHERE h.amount IS NULL OR ABS(c.c_ytd_payment - h.amount) > 0.005
   OR c.c_payment_cnt <> h.n;

COMMIT;
