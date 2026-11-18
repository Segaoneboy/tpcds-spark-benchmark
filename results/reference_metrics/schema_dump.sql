--
-- PostgreSQL database dump
--

\restrict U0fhZrIDs3ZckUhEUX52CqZAZlW4ndQVoc8cyHq3i0XqkBlOtW0WGvpwKrfzjcU

-- Dumped from database version 14.24 (Ubuntu 14.24-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.24 (Ubuntu 14.24-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: date_dim; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.date_dim (
    d_date_sk integer NOT NULL,
    d_date_id character(16) NOT NULL,
    d_date date,
    d_month_seq integer,
    d_week_seq integer,
    d_quarter_seq integer,
    d_year integer,
    d_dow integer,
    d_moy integer,
    d_dom integer,
    d_qoy integer,
    d_fy_year integer,
    d_fy_quarter_seq integer,
    d_fy_week_seq integer,
    d_day_name character(9),
    d_quarter_name character(6),
    d_holiday character(1),
    d_weekend character(1),
    d_following_holiday character(1),
    d_first_dom integer,
    d_last_dom integer,
    d_same_day_ly integer,
    d_same_day_lq integer,
    d_current_day character(1),
    d_current_week character(1),
    d_current_month character(1),
    d_current_quarter character(1),
    d_current_year character(1)
);


--
-- Name: item; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.item (
    i_item_sk integer NOT NULL,
    i_item_id character(16) NOT NULL,
    i_rec_start_date date,
    i_rec_end_date date,
    i_item_desc character varying(200),
    i_current_price numeric(7,2),
    i_wholesale_cost numeric(7,2),
    i_brand_id integer,
    i_brand character(50),
    i_class_id integer,
    i_class character(50),
    i_category_id integer,
    i_category character(50),
    i_manufact_id integer,
    i_manufact character(50),
    i_size character(20),
    i_formulation character(20),
    i_color character(20),
    i_units character(10),
    i_container character(10),
    i_manager_id integer,
    i_product_name character(50)
);


--
-- Name: store_sales; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.store_sales (
    ss_sold_date_sk integer,
    ss_sold_time_sk integer,
    ss_item_sk integer NOT NULL,
    ss_customer_sk integer,
    ss_cdemo_sk integer,
    ss_hdemo_sk integer,
    ss_addr_sk integer,
    ss_store_sk integer,
    ss_promo_sk integer,
    ss_ticket_number integer NOT NULL,
    ss_quantity integer,
    ss_wholesale_cost numeric(7,2),
    ss_list_price numeric(7,2),
    ss_sales_price numeric(7,2),
    ss_ext_discount_amt numeric(7,2),
    ss_ext_sales_price numeric(7,2),
    ss_ext_wholesale_cost numeric(7,2),
    ss_ext_list_price numeric(7,2),
    ss_ext_tax numeric(7,2),
    ss_coupon_amt numeric(7,2),
    ss_net_paid numeric(7,2),
    ss_net_paid_inc_tax numeric(7,2),
    ss_net_profit numeric(7,2)
);


--
-- Name: date_dim pk_date_dim; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.date_dim
    ADD CONSTRAINT pk_date_dim PRIMARY KEY (d_date_sk);


--
-- Name: item pk_item; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.item
    ADD CONSTRAINT pk_item PRIMARY KEY (i_item_sk);


--
-- Name: store_sales pk_store_sales; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.store_sales
    ADD CONSTRAINT pk_store_sales PRIMARY KEY (ss_item_sk, ss_ticket_number);


--
-- Name: idx_date_dim_date; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_date_dim_date ON public.date_dim USING btree (d_date);


--
-- Name: idx_date_dim_year_moy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_date_dim_year_moy ON public.date_dim USING btree (d_year, d_moy);


--
-- Name: idx_item_brand_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_item_brand_id ON public.item USING btree (i_brand_id);


--
-- Name: idx_item_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_item_category ON public.item USING btree (i_category);


--
-- Name: idx_item_item_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_item_item_id ON public.item USING btree (i_item_id);


--
-- Name: idx_store_sales_sold_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_store_sales_sold_date ON public.store_sales USING btree (ss_sold_date_sk);


--
-- Name: idx_store_sales_ticket_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_store_sales_ticket_number ON public.store_sales USING btree (ss_ticket_number);


--
-- Name: store_sales fk_store_sales_date; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.store_sales
    ADD CONSTRAINT fk_store_sales_date FOREIGN KEY (ss_sold_date_sk) REFERENCES public.date_dim(d_date_sk);


--
-- Name: store_sales fk_store_sales_item; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.store_sales
    ADD CONSTRAINT fk_store_sales_item FOREIGN KEY (ss_item_sk) REFERENCES public.item(i_item_sk);


--
-- PostgreSQL database dump complete
--

\unrestrict U0fhZrIDs3ZckUhEUX52CqZAZlW4ndQVoc8cyHq3i0XqkBlOtW0WGvpwKrfzjcU

