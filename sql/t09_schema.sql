BEGIN;

DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS date_dim;

CREATE TABLE date_dim (
    d_date_sk            integer      NOT NULL,   -- суррогатный ключ дня
    d_date_id            char(16)     NOT NULL,   -- бизнес-ключ
    d_date               date,
    d_month_seq          integer,
    d_week_seq           integer,
    d_quarter_seq        integer,
    d_year               integer,
    d_dow                integer,
    d_moy                integer,
    d_dom                integer,
    d_qoy                integer,
    d_fy_year            integer,
    d_fy_quarter_seq     integer,
    d_fy_week_seq        integer,
    d_day_name           char(9),
    d_quarter_name       char(6),
    d_holiday            char(1),
    d_weekend            char(1),
    d_following_holiday  char(1),
    d_first_dom          integer,
    d_last_dom           integer,
    d_same_day_ly        integer,
    d_same_day_lq        integer,
    d_current_day        char(1),
    d_current_week       char(1),
    d_current_month      char(1),
    d_current_quarter    char(1),
    d_current_year       char(1),
    CONSTRAINT pk_date_dim PRIMARY KEY (d_date_sk)
);

CREATE TABLE item (
    i_item_sk         integer       NOT NULL,   -- суррогатный ключ товара
    i_item_id         char(16)      NOT NULL,   -- бизнес-ключ (не уникален: версии товара)
    i_rec_start_date  date,
    i_rec_end_date    date,
    i_item_desc       varchar(200),
    i_current_price   numeric(7,2),
    i_wholesale_cost  numeric(7,2),
    i_brand_id        integer,
    i_brand           char(50),
    i_class_id        integer,
    i_class           char(50),
    i_category_id     integer,
    i_category        char(50),
    i_manufact_id     integer,
    i_manufact        char(50),
    i_size            char(20),
    i_formulation     char(20),
    i_color           char(20),
    i_units           char(10),
    i_container       char(10),
    i_manager_id      integer,
    i_product_name    char(50),
    CONSTRAINT pk_item PRIMARY KEY (i_item_sk)
);

CREATE TABLE store_sales (
    ss_sold_date_sk        integer,
    ss_sold_time_sk        integer,
    ss_item_sk             integer       NOT NULL,
    ss_customer_sk         integer,
    ss_cdemo_sk            integer,
    ss_hdemo_sk            integer,
    ss_addr_sk             integer,
    ss_store_sk            integer,
    ss_promo_sk            integer,
    ss_ticket_number       integer       NOT NULL,
    ss_quantity            integer,
    ss_wholesale_cost      numeric(7,2),
    ss_list_price          numeric(7,2),
    ss_sales_price         numeric(7,2),
    ss_ext_discount_amt    numeric(7,2),
    ss_ext_sales_price     numeric(7,2),
    ss_ext_wholesale_cost  numeric(7,2),
    ss_ext_list_price      numeric(7,2),
    ss_ext_tax             numeric(7,2),
    ss_coupon_amt          numeric(7,2),
    ss_net_paid            numeric(7,2),
    ss_net_paid_inc_tax    numeric(7,2),
    ss_net_profit          numeric(7,2),
    -- одна строка продажи = пара (товар, номер чека)
    CONSTRAINT pk_store_sales PRIMARY KEY (ss_item_sk, ss_ticket_number),
    -- связи
    CONSTRAINT fk_store_sales_date
        FOREIGN KEY (ss_sold_date_sk) REFERENCES date_dim (d_date_sk),
    CONSTRAINT fk_store_sales_item
        FOREIGN KEY (ss_item_sk)      REFERENCES item (i_item_sk)
);

CREATE INDEX idx_store_sales_sold_date ON store_sales (ss_sold_date_sk);
CREATE INDEX idx_store_sales_ticket_number ON store_sales (ss_ticket_number);

CREATE UNIQUE INDEX idx_date_dim_date ON date_dim (d_date);
CREATE INDEX idx_date_dim_year_moy    ON date_dim (d_year, d_moy);

CREATE INDEX idx_item_category ON item (i_category);
CREATE INDEX idx_item_brand_id ON item (i_brand_id);
CREATE INDEX idx_item_item_id  ON item (i_item_id);

COMMIT;

ANALYZE date_dim;
ANALYZE item;
ANALYZE store_sales;
