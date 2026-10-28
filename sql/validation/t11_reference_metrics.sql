-- ============================================================================
-- T11: эталонные метрики исходных таблиц (reference_metrics)
-- Результат — «длинная» таблица: table_name | metric | column_name | value
-- Все значения приводятся к тексту, чтобы суммы NUMERIC не теряли точность.
-- Эти же числа потом сверяются с Parquet/ORC/Avro/CSV (скрипт 03_validate_integrity.py).
-- ============================================================================

-- ---------- date_dim ----------
SELECT 'date_dim' AS table_name, 'count'           AS metric, '*'          AS column_name, COUNT(*)::text                 AS value FROM date_dim
UNION ALL SELECT 'date_dim', 'count_distinct', 'd_date_sk',  COUNT(DISTINCT d_date_sk)::text FROM date_dim
UNION ALL SELECT 'date_dim', 'sum',            'd_date_sk',  COALESCE(SUM(d_date_sk)::text, 'NULL')  FROM date_dim
UNION ALL SELECT 'date_dim', 'min',            'd_date_sk',  COALESCE(MIN(d_date_sk)::text, 'NULL')  FROM date_dim
UNION ALL SELECT 'date_dim', 'max',            'd_date_sk',  COALESCE(MAX(d_date_sk)::text, 'NULL')  FROM date_dim
UNION ALL SELECT 'date_dim', 'min',            'd_date',     COALESCE(MIN(d_date)::text, 'NULL')     FROM date_dim
UNION ALL SELECT 'date_dim', 'max',            'd_date',     COALESCE(MAX(d_date)::text, 'NULL')     FROM date_dim
UNION ALL SELECT 'date_dim', 'sum',            'd_year',     COALESCE(SUM(d_year)::text, 'NULL')     FROM date_dim
UNION ALL SELECT 'date_dim', 'min',            'd_year',     COALESCE(MIN(d_year)::text, 'NULL')     FROM date_dim
UNION ALL SELECT 'date_dim', 'max',            'd_year',     COALESCE(MAX(d_year)::text, 'NULL')     FROM date_dim
UNION ALL SELECT 'date_dim', 'sum',            'd_moy',      COALESCE(SUM(d_moy)::text, 'NULL')      FROM date_dim
UNION ALL SELECT 'date_dim', 'null_count',     'd_date',     COUNT(*) FILTER (WHERE d_date IS NULL)::text FROM date_dim

-- ---------- item ----------
UNION ALL SELECT 'item', 'count',           '*',               COUNT(*)::text FROM item
UNION ALL SELECT 'item', 'count_distinct',  'i_item_sk',       COUNT(DISTINCT i_item_sk)::text FROM item
UNION ALL SELECT 'item', 'count_distinct',  'i_item_id',       COUNT(DISTINCT i_item_id)::text FROM item
UNION ALL SELECT 'item', 'sum',             'i_item_sk',       COALESCE(SUM(i_item_sk)::text, 'NULL')        FROM item
UNION ALL SELECT 'item', 'min',             'i_item_sk',       COALESCE(MIN(i_item_sk)::text, 'NULL')        FROM item
UNION ALL SELECT 'item', 'max',             'i_item_sk',       COALESCE(MAX(i_item_sk)::text, 'NULL')        FROM item
UNION ALL SELECT 'item', 'sum',             'i_current_price', COALESCE(SUM(i_current_price)::text, 'NULL')  FROM item
UNION ALL SELECT 'item', 'min',             'i_current_price', COALESCE(MIN(i_current_price)::text, 'NULL')  FROM item
UNION ALL SELECT 'item', 'max',             'i_current_price', COALESCE(MAX(i_current_price)::text, 'NULL')  FROM item
UNION ALL SELECT 'item', 'sum',             'i_wholesale_cost',COALESCE(SUM(i_wholesale_cost)::text, 'NULL') FROM item
UNION ALL SELECT 'item', 'min',             'i_wholesale_cost',COALESCE(MIN(i_wholesale_cost)::text, 'NULL') FROM item
UNION ALL SELECT 'item', 'max',             'i_wholesale_cost',COALESCE(MAX(i_wholesale_cost)::text, 'NULL') FROM item
UNION ALL SELECT 'item', 'count_distinct',  'i_category',      COUNT(DISTINCT i_category)::text FROM item
UNION ALL SELECT 'item', 'null_count',      'i_current_price', COUNT(*) FILTER (WHERE i_current_price IS NULL)::text FROM item

-- ---------- store_sales ----------
UNION ALL SELECT 'store_sales', 'count',          '*',                  COUNT(*)::text FROM store_sales
UNION ALL SELECT 'store_sales', 'count_non_null', 'ss_sold_date_sk',    COUNT(ss_sold_date_sk)::text FROM store_sales
UNION ALL SELECT 'store_sales', 'null_count',     'ss_sold_date_sk',    COUNT(*) FILTER (WHERE ss_sold_date_sk IS NULL)::text FROM store_sales
UNION ALL SELECT 'store_sales', 'count_distinct', 'ss_item_sk',         COUNT(DISTINCT ss_item_sk)::text FROM store_sales
UNION ALL SELECT 'store_sales', 'count_distinct', 'ss_ticket_number',   COUNT(DISTINCT ss_ticket_number)::text FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_sold_date_sk',    COALESCE(MIN(ss_sold_date_sk)::text, 'NULL')    FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_sold_date_sk',    COALESCE(MAX(ss_sold_date_sk)::text, 'NULL')    FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_item_sk',         COALESCE(MIN(ss_item_sk)::text, 'NULL')         FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_item_sk',         COALESCE(MAX(ss_item_sk)::text, 'NULL')         FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_ticket_number',   COALESCE(MIN(ss_ticket_number)::text, 'NULL')   FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_ticket_number',   COALESCE(MAX(ss_ticket_number)::text, 'NULL')   FROM store_sales
UNION ALL SELECT 'store_sales', 'sum',            'ss_quantity',        COALESCE(SUM(ss_quantity)::text, 'NULL')        FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_quantity',        COALESCE(MIN(ss_quantity)::text, 'NULL')        FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_quantity',        COALESCE(MAX(ss_quantity)::text, 'NULL')        FROM store_sales
UNION ALL SELECT 'store_sales', 'sum',            'ss_sales_price',     COALESCE(SUM(ss_sales_price)::text, 'NULL')     FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_sales_price',     COALESCE(MIN(ss_sales_price)::text, 'NULL')     FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_sales_price',     COALESCE(MAX(ss_sales_price)::text, 'NULL')     FROM store_sales
UNION ALL SELECT 'store_sales', 'sum',            'ss_ext_sales_price', COALESCE(SUM(ss_ext_sales_price)::text, 'NULL') FROM store_sales
UNION ALL SELECT 'store_sales', 'sum',            'ss_net_paid',        COALESCE(SUM(ss_net_paid)::text, 'NULL')        FROM store_sales
UNION ALL SELECT 'store_sales', 'sum',            'ss_net_profit',      COALESCE(SUM(ss_net_profit)::text, 'NULL')      FROM store_sales
UNION ALL SELECT 'store_sales', 'min',            'ss_net_profit',      COALESCE(MIN(ss_net_profit)::text, 'NULL')      FROM store_sales
UNION ALL SELECT 'store_sales', 'max',            'ss_net_profit',      COALESCE(MAX(ss_net_profit)::text, 'NULL')      FROM store_sales
ORDER BY 1, 3, 2;
