-- Q3: Join store_sales and item with Category Aggregation
SELECT
    COALESCE(i.i_category, 'UNKNOWN_CATEGORY')              AS item_category,
    COALESCE(i.i_class, 'UNKNOWN_CLASS')                    AS item_class,
    COUNT(*)                                                AS joined_row_count,
    COUNT(DISTINCT ss.ss_ticket_number)                     AS unique_tickets,
    SUM(CAST(ss.ss_quantity AS BIGINT))                     AS total_quantity,
    SUM(CAST(ss.ss_ext_sales_price AS NUMERIC(18, 2)))      AS total_ext_sales_price,
    SUM(CAST(ss.ss_net_paid AS NUMERIC(18, 2)))             AS total_net_paid,
    SUM(CAST(ss.ss_net_profit AS NUMERIC(18, 2)))           AS total_net_profit
FROM store_sales ss
INNER JOIN item i 
    ON ss.ss_item_sk = i.i_item_sk
WHERE ss.ss_sold_date_sk IS NOT NULL
GROUP BY 
    COALESCE(i.i_category, 'UNKNOWN_CATEGORY'),
    COALESCE(i.i_class, 'UNKNOWN_CLASS')
ORDER BY 
    item_category ASC, 
    item_class ASC;
