/*
  SCRIPT 99_CLEANUP.sql
  Objetivo: Limpar o ambiente, removendo as tabelas criadas pelos scripts 00 e 01.
  Escopo: 
    - Tabelas de Staging (analytics.tmp_*)
    - Tabelas OLTP (tb_*)
*/


-- 1. REMOVER TABELAS OLTP (Script 01)

-- Remove a tabela de itens (filha de todas)
DROP TABLE IF EXISTS tb_order_items CASCADE;

-- Remove tabelas principais
DROP TABLE IF EXISTS tb_orders CASCADE;
DROP TABLE IF EXISTS tb_products CASCADE;
DROP TABLE IF EXISTS tb_sellers CASCADE;
DROP TABLE IF EXISTS tb_customers CASCADE;



-- 2. REMOVER TABELAS DE STAGING (Script 00)
-- Nota: Estão no schema 'analytics' conforme o ajuste recente.
DROP TABLE IF EXISTS analytics.tmp_items CASCADE;
DROP TABLE IF EXISTS analytics.tmp_orders CASCADE;
DROP TABLE IF EXISTS analytics.tmp_products CASCADE;
DROP TABLE IF EXISTS analytics.tmp_sellers CASCADE;
DROP TABLE IF EXISTS analytics.tmp_customers CASCADE;