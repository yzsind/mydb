USE tpcds;

-- Auxiliary secondary indexes for the TPC-DS join/filter paths.
--
-- These are NOT part of the official TPC-DS specification (which permits only
-- the declared primary keys). They are added here so the 99 queries run with
-- realistic plans on a row store instead of degrading to full fact-table scans
-- for every star join. Each multi-action ALTER TABLE fuses all of a table's
-- ADD INDEX clauses onto a single heap scan (Database::add_indexes holds the
-- table X lock once), so the huge fact tables are scanned once, not once per
-- index.

-- Date dimension: nearly every query filters date_dim then joins *_sold_date_sk.
ALTER TABLE date_dim
    ADD INDEX idx_date_dim_date (d_date),
    ADD INDEX idx_date_dim_year (d_year),
    ADD INDEX idx_date_dim_moy (d_moy),
    ADD INDEX idx_date_dim_qoy (d_qoy),
    ADD INDEX idx_date_dim_month_seq (d_month_seq),
    ADD INDEX idx_date_dim_week_seq (d_week_seq);

-- Item dimension: category/class/brand/manufacturer rollups and joins.
ALTER TABLE item
    ADD INDEX idx_item_category (i_category),
    ADD INDEX idx_item_class (i_class),
    ADD INDEX idx_item_brand (i_brand),
    ADD INDEX idx_item_manufact (i_manufact_id),
    ADD INDEX idx_item_item_id (i_item_id);

-- Customer / address / demographics join keys.
ALTER TABLE customer
    ADD INDEX idx_customer_addr (c_current_addr_sk),
    ADD INDEX idx_customer_cdemo (c_current_cdemo_sk),
    ADD INDEX idx_customer_hdemo (c_current_hdemo_sk),
    ADD INDEX idx_customer_first_sales (c_first_sales_date_sk),
    ADD INDEX idx_customer_first_shipto (c_first_shipto_date_sk);
ALTER TABLE customer_address
    ADD INDEX idx_ca_state (ca_state),
    ADD INDEX idx_ca_city (ca_city),
    ADD INDEX idx_ca_zip (ca_zip),
    ADD INDEX idx_ca_county (ca_county),
    ADD INDEX idx_ca_country (ca_country);

-- store_sales (largest fact): date / item / customer / store / promo joins.
ALTER TABLE store_sales
    ADD INDEX idx_ss_sold_date (ss_sold_date_sk),
    ADD INDEX idx_ss_item (ss_item_sk),
    ADD INDEX idx_ss_customer (ss_customer_sk),
    ADD INDEX idx_ss_store (ss_store_sk),
    ADD INDEX idx_ss_promo (ss_promo_sk),
    ADD INDEX idx_ss_cdemo (ss_cdemo_sk),
    ADD INDEX idx_ss_hdemo (ss_hdemo_sk),
    ADD INDEX idx_ss_addr (ss_addr_sk),
    ADD INDEX idx_ss_sold_time (ss_sold_time_sk);

-- catalog_sales fact.
ALTER TABLE catalog_sales
    ADD INDEX idx_cs_sold_date (cs_sold_date_sk),
    ADD INDEX idx_cs_ship_date (cs_ship_date_sk),
    ADD INDEX idx_cs_item (cs_item_sk),
    ADD INDEX idx_cs_bill_customer (cs_bill_customer_sk),
    ADD INDEX idx_cs_bill_cdemo (cs_bill_cdemo_sk),
    ADD INDEX idx_cs_ship_customer (cs_ship_customer_sk),
    ADD INDEX idx_cs_call_center (cs_call_center_sk),
    ADD INDEX idx_cs_warehouse (cs_warehouse_sk),
    ADD INDEX idx_cs_promo (cs_promo_sk);

-- web_sales fact.
ALTER TABLE web_sales
    ADD INDEX idx_ws_sold_date (ws_sold_date_sk),
    ADD INDEX idx_ws_ship_date (ws_ship_date_sk),
    ADD INDEX idx_ws_item (ws_item_sk),
    ADD INDEX idx_ws_bill_customer (ws_bill_customer_sk),
    ADD INDEX idx_ws_bill_addr (ws_bill_addr_sk),
    ADD INDEX idx_ws_ship_customer (ws_ship_customer_sk),
    ADD INDEX idx_ws_web_site (ws_web_site_sk),
    ADD INDEX idx_ws_warehouse (ws_warehouse_sk),
    ADD INDEX idx_ws_promo (ws_promo_sk);

-- Returns facts.
ALTER TABLE store_returns
    ADD INDEX idx_sr_returned_date (sr_returned_date_sk),
    ADD INDEX idx_sr_item (sr_item_sk),
    ADD INDEX idx_sr_customer (sr_customer_sk),
    ADD INDEX idx_sr_store (sr_store_sk),
    ADD INDEX idx_sr_reason (sr_reason_sk),
    ADD INDEX idx_sr_cdemo (sr_cdemo_sk),
    ADD INDEX idx_sr_hdemo (sr_hdemo_sk),
    ADD INDEX idx_sr_addr (sr_addr_sk);
ALTER TABLE catalog_returns
    ADD INDEX idx_cr_returned_date (cr_returned_date_sk),
    ADD INDEX idx_cr_item (cr_item_sk),
    ADD INDEX idx_cr_refunded_customer (cr_refunded_customer_sk),
    ADD INDEX idx_cr_returning_customer (cr_returning_customer_sk),
    ADD INDEX idx_cr_call_center (cr_call_center_sk),
    ADD INDEX idx_cr_warehouse (cr_warehouse_sk),
    ADD INDEX idx_cr_reason (cr_reason_sk);
ALTER TABLE web_returns
    ADD INDEX idx_wr_returned_date (wr_returned_date_sk),
    ADD INDEX idx_wr_item (wr_item_sk),
    ADD INDEX idx_wr_refunded_customer (wr_refunded_customer_sk),
    ADD INDEX idx_wr_returning_customer (wr_returning_customer_sk),
    ADD INDEX idx_wr_web_page (wr_web_page_sk),
    ADD INDEX idx_wr_reason (wr_reason_sk),
    ADD INDEX idx_wr_order_number (wr_order_number);

-- inventory: PK is (date,item,warehouse); add item-leading lookup for joins.
ALTER TABLE inventory
    ADD INDEX idx_inv_item (inv_item_sk),
    ADD INDEX idx_inv_warehouse (inv_warehouse_sk);

-- Smaller dimensions used as the driving filter in many queries.
ALTER TABLE customer_demographics
    ADD INDEX idx_cd_gender_marital (cd_gender, cd_marital_status, cd_education_status);
ALTER TABLE store
    ADD INDEX idx_store_state (s_state);
ALTER TABLE call_center
    ADD INDEX idx_cc_name (cc_name);
ALTER TABLE web_site
    ADD INDEX idx_web_site_name (web_name);
ALTER TABLE warehouse
    ADD INDEX idx_warehouse_state (w_state);
