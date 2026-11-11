-- Q4: Сравнение агрегатов CSV и CSV.GZ (выполнить на обоих представлениях или через FULL JOIN)
WITH csv_metrics AS (
    SELECT
        COALESCE(ss_store_sk, -1)                           AS store_key,
        COUNT(*)                                            AS row_cnt,
        COUNT(ss_customer_sk)                               AS cust_cnt,
        SUM(CAST(ss_quantity AS BIGINT))                    AS sum_qty,
        SUM(CAST(ss_net_paid AS NUMERIC(18, 2)))            AS sum_paid,
        SUM(CAST(ss_net_profit AS NUMERIC(18, 2)))          AS sum_profit,
        MIN(ss_ticket_number)                               AS min_ticket,
        MAX(ss_ticket_number)                               AS max_ticket
    FROM store_sales_csv
    GROUP BY COALESCE(ss_store_sk, -1)
),
csvgz_metrics AS (
    SELECT
        COALESCE(ss_store_sk, -1)                           AS store_key,
        COUNT(*)                                            AS row_cnt,
        COUNT(ss_customer_sk)                               AS cust_cnt,
        SUM(CAST(ss_quantity AS BIGINT))                    AS sum_qty,
        SUM(CAST(ss_net_paid AS NUMERIC(18, 2)))            AS sum_paid,
        SUM(CAST(ss_net_profit AS NUMERIC(18, 2)))          AS sum_profit,
        MIN(ss_ticket_number)                               AS min_ticket,
        MAX(ss_ticket_number)                               AS max_ticket
    FROM store_sales_csvgz
    GROUP BY COALESCE(ss_store_sk, -1)
)
SELECT
    COALESCE(a.store_key, b.store_key)                      AS store_sk,
    a.row_cnt                                               AS csv_rows,
    b.row_cnt                                               AS csvgz_rows,
    (a.row_cnt - b.row_cnt)                                 AS diff_rows,
    (a.sum_paid - b.sum_paid)                               AS diff_net_paid,
    (a.sum_profit - b.sum_profit)                           AS diff_net_profit,
    CASE 
        WHEN a.row_cnt = b.row_cnt 
         AND a.cust_cnt = b.cust_cnt
         AND a.sum_qty = b.sum_qty
         AND a.sum_paid = b.sum_paid 
         AND a.sum_profit = b.sum_profit
         AND a.min_ticket = b.min_ticket
         AND a.max_ticket = b.max_ticket
        THEN 'OK' 
        ELSE 'MISMATCH' 
    END                                                     AS validation_status
FROM csv_metrics a
FULL OUTER JOIN csvgz_metrics b 
    ON a.store_key = b.store_key
ORDER BY store_sk;
