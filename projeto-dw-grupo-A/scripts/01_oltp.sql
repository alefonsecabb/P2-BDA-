/*
  SCRIPT 01_OLTP.sql
  Objetivo: Normalizar dados do Staging para o ambiente Transacional.
  
  Ações Principais:
  1. Padronização: Remove hífens ('-') de todos os IDs (UUIDs) para garantir que 'abc-123' seja igual a 'abc123'.
  2. Tipagem: Usa TEXT para IDs para ser permissivo com o tamanho.
  3. Integridade: Garante que registros filhos (pedidos/itens) só sejam criados se os pais (clientes/produtos) existirem.
  4. Limpeza: Trata nulos (COALESCE) e duplicatas (DISTINCT ON).
  5. Lê as tabelas de origem explicitamente do schema 'analytics'.
*/

BEGIN;

-- 1. TABELA DE CLIENTES (tb_customers)
DROP TABLE IF EXISTS tb_customers CASCADE;

CREATE TABLE tb_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

INSERT INTO tb_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT DISTINCT ON (REPLACE(customer_id::TEXT, '-', ''))
    REPLACE(customer_id::TEXT, '-', ''),
    REPLACE(customer_unique_id::TEXT, '-', ''),
    COALESCE(customer_zip_code_prefix, '00000'),
    COALESCE(customer_city, 'Desconhecido'),
    COALESCE(customer_state, 'XX')
FROM analytics.tmp_customers;  -- << AJUSTADO AQUI


-- 2. TABELA DE VENDEDORES (tb_sellers)
DROP TABLE IF EXISTS tb_sellers CASCADE;

CREATE TABLE tb_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);

INSERT INTO tb_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT DISTINCT ON (REPLACE(seller_id::TEXT, '-', ''))
    REPLACE(seller_id::TEXT, '-', ''),
    COALESCE(seller_zip_code_prefix, '00000'),
    COALESCE(seller_city, 'Desconhecido'),
    COALESCE(seller_state, 'XX')
FROM analytics.tmp_sellers;  -- << AJUSTADO AQUI


-- 3. TABELA DE PRODUTOS (tb_products)
DROP TABLE IF EXISTS tb_products CASCADE;

CREATE TABLE tb_products (
    product_id TEXT PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

INSERT INTO tb_products
SELECT DISTINCT ON (REPLACE(product_id::TEXT, '-', ''))
    REPLACE(product_id::TEXT, '-', ''),
    COALESCE(product_category_name, 'outros'),
    COALESCE(product_name_lenght, 0),
    COALESCE(product_description_lenght, 0),
    COALESCE(product_photos_qty, 0),
    COALESCE(product_weight_g, 0),
    COALESCE(product_length_cm, 0),
    COALESCE(product_height_cm, 0),
    COALESCE(product_width_cm, 0)
FROM analytics.tmp_products;  -- << AJUSTADO AQUI


-- 4. TABELA DE PEDIDOS (tb_orders)
DROP TABLE IF EXISTS tb_orders CASCADE;

CREATE TABLE tb_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES tb_customers(customer_id),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

INSERT INTO tb_orders
SELECT DISTINCT ON (REPLACE(t.order_id::TEXT, '-', ''))
    REPLACE(t.order_id::TEXT, '-', ''),
    REPLACE(t.customer_id::TEXT, '-', ''),
    COALESCE(t.order_status, 'indefinido'),
    t.order_purchase_timestamp,
    t.order_approved_at,
    t.order_delivered_carrier_date,
    t.order_delivered_customer_date,
    t.order_estimated_delivery_date
FROM analytics.tmp_orders t  -- << AJUSTADO AQUI
INNER JOIN tb_customers c ON REPLACE(t.customer_id::TEXT, '-', '') = c.customer_id;


-- 5. TABELA DE ITENS DE PEDIDO (tb_order_items)
DROP TABLE IF EXISTS tb_order_items CASCADE;

CREATE TABLE tb_order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT REFERENCES tb_products(product_id),
    seller_id TEXT REFERENCES tb_sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2) DEFAULT 0,
    freight_value NUMERIC(10,2) DEFAULT 0,
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES tb_orders(order_id)
);

INSERT INTO tb_order_items
SELECT DISTINCT ON (REPLACE(i.order_id::TEXT, '-', ''), i.order_item_id)
    REPLACE(i.order_id::TEXT, '-', ''),
    i.order_item_id,
    REPLACE(i.product_id::TEXT, '-', ''),
    REPLACE(i.seller_id::TEXT, '-', ''),
    i.shipping_limit_date,
    COALESCE(i.price, 0.0),
    COALESCE(i.freight_value, 0.0)
FROM analytics.tmp_items i  -- << AJUSTADO AQUI
INNER JOIN tb_orders o ON REPLACE(i.order_id::TEXT, '-', '') = o.order_id
INNER JOIN tb_products p ON REPLACE(i.product_id::TEXT, '-', '') = p.product_id
INNER JOIN tb_sellers s ON REPLACE(i.seller_id::TEXT, '-', '') = s.seller_id;

COMMIT;