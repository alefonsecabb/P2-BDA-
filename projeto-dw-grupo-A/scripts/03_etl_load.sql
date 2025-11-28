/*
  SCRIPT 03_ETL_LOAD.sql
  Objetivo: Popuar as tabelas do Data Warehouse (Schema Analytics)
  Origem: Tabelas tb_* (OLTP)
  Destino: analytics.dim_* e analytics.fact_*
  
  Lógica:
  1. Dimensão Tempo: Gera série de datas baseada nos pedidos.
  2. Dimensões (Customer, Product, Seller): Carga incremental (SCD Tipo 1/2 simplificado).
  3. Fato Vendas: Join entre OLTP e Dimensões para obter as Surrogate Keys (SKs).
*/

BEGIN;

-- 1. POPULAR DIMENSÃO TEMPO (dim_date)
INSERT INTO analytics.dim_date (full_date, year, month, day, quarter, day_of_week, week_of_year)
SELECT
    datum::DATE,
    EXTRACT(YEAR FROM datum),
    EXTRACT(MONTH FROM datum),
    EXTRACT(DAY FROM datum),
    EXTRACT(QUARTER FROM datum),
    EXTRACT(ISODOW FROM datum), -- 1=Segunda, 7=Domingo
    EXTRACT(WEEK FROM datum)
FROM generate_series(
    (SELECT MIN(order_purchase_timestamp)::DATE FROM tb_orders), -- Data mínima
    (SELECT MAX(order_purchase_timestamp)::DATE FROM tb_orders), -- Data máxima
    '1 day'::INTERVAL
) AS datum
ON CONFLICT (full_date) DO NOTHING; -- Garante que não duplique datas se rodar 2x

-- 2. POPULAR DIMENSÃO CLIENTE (dim_customer)
-- Lógica: Insere novos clientes. Define 'is_current' = TRUE.
INSERT INTO analytics.dim_customer (customer_id, effective_from, effective_to, is_current)
SELECT 
    c.customer_id,
    CURRENT_DATE, -- Data de carga
    NULL,
    TRUE
FROM tb_customers c
LEFT JOIN analytics.dim_customer d ON d.customer_id = c.customer_id
WHERE d.customer_id IS NULL; -- Apenas clientes que ainda não existem na dimensão

-- 3. POPULAR DIMENSÃO PRODUTO (dim_product)
INSERT INTO analytics.dim_product (product_id)
SELECT 
    p.product_id
FROM tb_products p
LEFT JOIN analytics.dim_product d ON d.product_id = p.product_id
WHERE d.product_id IS NULL;

-- 4. POPULAR DIMENSÃO VENDEDOR (dim_seller)
INSERT INTO analytics.dim_seller (seller_id)
SELECT 
    s.seller_id
FROM tb_sellers s
LEFT JOIN analytics.dim_seller d ON d.seller_id = s.seller_id
WHERE d.seller_id IS NULL;

-- 5. POPULAR TABELA FATO (fact_sales)
-- Lógica: Busca os dados em tb_order_items (detalhe), junta com tb_orders (cabeçalho)
-- e faz lookup nas dimensões para pegar as chaves substitutas (SKs).
INSERT INTO analytics.fact_sales (order_id, customer_sk, product_sk, seller_sk, date_sk, price, freight_value)
SELECT
    oi.order_id,
    dc.customer_sk,
    dp.product_sk,
    ds.seller_sk,
    dd.date_sk,
    oi.price,
    oi.freight_value
FROM tb_order_items oi
-- Join com Pedidos para pegar Data e Cliente
INNER JOIN tb_orders o ON oi.order_id = o.order_id
-- Join com Dimensões para obter as SKs
INNER JOIN analytics.dim_customer dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
INNER JOIN analytics.dim_product dp ON oi.product_id = dp.product_id
INNER JOIN analytics.dim_seller ds ON oi.seller_id = ds.seller_id
INNER JOIN analytics.dim_date dd ON o.order_purchase_timestamp::DATE = dd.full_date
-- Verificação para evitar duplicatas na carga (Idempotência)
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.fact_sales fs 
    WHERE fs.order_id = oi.order_id 
      AND fs.product_sk = dp.product_sk 
      AND fs.seller_sk = ds.seller_sk
);

COMMIT;