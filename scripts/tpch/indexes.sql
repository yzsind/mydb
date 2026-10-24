USE tpch;

-- Multi-action ALTER TABLE fuses every ADD INDEX on the same table
-- onto one shared heap scan. Per-table CREATE INDEX would re-scan
-- lineitem (~800 MB) seven times; this folds it into one. Statement
-- ordering inside the batch is irrelevant — Database::add_indexes
-- holds the table X lock once and runs a single fan-out scan.
ALTER TABLE supplier ADD INDEX idx_supplier_nation (s_nationkey);
ALTER TABLE customer ADD INDEX idx_customer_nation (c_nationkey);
ALTER TABLE partsupp
    ADD INDEX idx_partsupp_partkey (ps_partkey),
    ADD INDEX idx_partsupp_suppkey (ps_suppkey);
ALTER TABLE orders
    ADD INDEX idx_orders_custkey (o_custkey),
    ADD INDEX idx_orders_orderdate (o_orderdate);
ALTER TABLE lineitem
    ADD INDEX idx_lineitem_orderkey (l_orderkey),
    ADD INDEX idx_lineitem_partkey (l_partkey),
    ADD INDEX idx_lineitem_suppkey (l_suppkey),
    ADD INDEX idx_lineitem_partsuppkey (l_partkey, l_suppkey),
    ADD INDEX idx_lineitem_shipdate (l_shipdate),
    ADD INDEX idx_lineitem_commitdate (l_commitdate),
    ADD INDEX idx_lineitem_receiptdate (l_receiptdate);
