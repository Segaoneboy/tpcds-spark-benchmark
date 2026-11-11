-- Q2: Selective Read (Projection + Filter)
SELECT
    ss_sold_date_sk,
    COUNT(*)                                                AS filtered_row_count,
    SUM(CAST(ss_quantity AS BIGINT))                        AS sum_quantity,
    SUM(CAST(ss_sales_price AS NUMERIC(18, 2)))             AS sum_sales_price,
    SUM(CAST(ss_net_paid AS NUMERIC(18, 2)))                AS sum_net_paid,
    AVG(CAST(ss_sales_price AS NUMERIC(18, 4)))             AS avg_sales_price
FROM store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451910           -- Ровно 1 календарный год (2000)
  AND ss_sales_price > 50.00
  AND ss_quantity IS NOT NULL
GROUP BY ss_sold_date_sk
ORDER BY ss_sold_date_sk ASC;
