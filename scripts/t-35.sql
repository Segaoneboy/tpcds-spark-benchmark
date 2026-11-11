-- Q1: Full Scan Aggregation
SELECT
    COUNT(*)                                                AS total_rows,
    COUNT(ss_quantity)                                      AS valid_quantity_rows,
    COUNT(ss_net_paid)                                      AS valid_net_paid_rows,
    SUM(CAST(ss_quantity AS BIGINT))                        AS sum_quantity,
    SUM(CAST(ss_wholesale_cost AS NUMERIC(18, 2)))          AS sum_wholesale_cost,
    SUM(CAST(ss_list_price AS NUMERIC(18, 2)))              AS sum_list_price,
    SUM(CAST(ss_sales_price AS NUMERIC(18, 2)))             AS sum_sales_price,
    SUM(CAST(ss_ext_sales_price AS NUMERIC(18, 2)))         AS sum_ext_sales_price,
    SUM(CAST(ss_net_paid AS NUMERIC(18, 2)))                AS sum_net_paid,
    SUM(CAST(ss_net_profit AS NUMERIC(18, 2)))              AS sum_net_profit,
    MIN(ss_sold_date_sk)                                    AS min_date_sk,
    MAX(ss_sold_date_sk)                                    AS max_date_sk,
    MIN(ss_item_sk)                                         AS min_item_sk,
    MAX(ss_item_sk)                                         AS max_item_sk
FROM store_sales;
