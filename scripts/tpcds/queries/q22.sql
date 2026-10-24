-- TPC-DS Query 22 (adapted for mydb / MySQL 8.0 from tpcds-kit)
select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           ,date_dim
           ,item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1176 and 1176 + 11
       group by i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category with rollup
order by qoh, i_product_name, i_brand, i_class, i_category
limit 100;
