/*
  SCRIPT 00_STAGING.sql
  Objetivo: Ingestão inicial e preparação dos dados brutos (Raw Data).
  Origem: Arquivos CSV (Dataset Olist)
  Destino: Tabelas temporárias tmp_* (Staging)
  
  Lógica:
  1. Limpeza: Remove tabelas anteriores (DROP IF EXISTS).
  2. Estrutura: Cria tabelas com IDs como TEXT para suportar qualquer formato de hash.
  3. Carga: Importação em massa via comando COPY.
  4. Cria tabelas UNLOGGED no schema 'analytics' para persistirem entre sessões.
*/

-- 1. Tabela Staging: ORDERS
DROP TABLE IF EXISTS analytics.tmp_orders;
CREATE UNLOGGED TABLE analytics.tmp_orders (
    order_id TEXT,
    customer_id TEXT,
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

COPY analytics.tmp_orders 
FROM '../data/olist_orders_dataset.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- 2. Tabela Staging: ORDER ITEMS
DROP TABLE IF EXISTS analytics.tmp_items;
CREATE UNLOGGED TABLE analytics.tmp_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2)
);

COPY analytics.tmp_items 
FROM '../data/olist_order_items_dataset.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- 3. Tabela Staging: CUSTOMERS
DROP TABLE IF EXISTS analytics.tmp_customers;
CREATE UNLOGGED TABLE analytics.tmp_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

COPY analytics.tmp_customers 
FROM '../data/olist_customers_dataset.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- 4. Tabela Staging: PRODUCTS
DROP TABLE IF EXISTS analytics.tmp_products;
CREATE UNLOGGED TABLE analytics.tmp_products (
    product_id TEXT,
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

COPY analytics.tmp_products 
FROM '../data/olist_products_dataset.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- 5. Tabela Staging: SELLERS
DROP TABLE IF EXISTS analytics.tmp_sellers;
CREATE UNLOGGED TABLE analytics.tmp_sellers (
    seller_id TEXT,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);

COPY analytics.tmp_sellers 
FROM '../data/olist_sellers_dataset.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

COMMIT;