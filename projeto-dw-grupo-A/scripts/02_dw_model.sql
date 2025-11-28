/*
  SCRIPT 02_DW_MODEL.sql (Versão Segura / Não-Destrutiva)
  Objetivo: Criar a estrutura do Data Warehouse (Schema Star) APENAS SE NÃO EXISTIR.
  
  Comportamento:
  - Se a tabela já existe: Mantém os dados e a estrutura atual (NÃO FAZ NADA).
  - Se a tabela não existe: Cria a tabela do zero.
*/

-- 1. Criação do Schema (se não existir)
CREATE SCHEMA IF NOT EXISTS analytics;

-- 2. Criação das Dimensões (Com IF NOT EXISTS)

-- Dimensão Cliente
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id TEXT,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN,
    CONSTRAINT uk_dim_customer_id_dates UNIQUE (customer_id, effective_from)
);

-- Dimensão Produto
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id TEXT UNIQUE
);

-- Dimensão Vendedor
CREATE TABLE IF NOT EXISTS analytics.dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id TEXT UNIQUE
);

-- Dimensão Tempo (Data)
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    week_of_year INT
);


-- 3. Criação da Tabela Fato (Com IF NOT EXISTS)
-- A criação da fato deve ocorrer após as dimensões para garantir que as FKs funcionem na criação.

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_sk SERIAL PRIMARY KEY,
    order_id TEXT,
    
    -- Chaves Estrangeiras
    customer_sk INT NOT NULL,
    product_sk INT NOT NULL,
    seller_sk INT NOT NULL,
    date_sk INT NOT NULL,
    
    -- Métricas
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    
    -- Definição das Restrições (Foreign Keys)
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES analytics.dim_customer(customer_sk),
    CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES analytics.dim_product(product_sk),
    CONSTRAINT fk_fact_seller FOREIGN KEY (seller_sk) REFERENCES analytics.dim_seller(seller_sk),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_sk) REFERENCES analytics.dim_date(date_sk)
);