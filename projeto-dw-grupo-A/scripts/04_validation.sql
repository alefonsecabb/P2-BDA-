/*
  SCRIPT 04_VALIDATION.sql
  Objetivo: Validar a volumetria e a integridade dos dados no Data Warehouse.
  Comparativo: OLTP (tb_*) vs DW (analytics.*)
*/


-- 1. VALIDAÇÃO DE VOLUMETRIA (COUNTS)
-- Verifica se a quantidade de registros faz sentido entre a origem e o destino.
-- Esperado: 
--   - 'DW: Fato Vendas' deve ser igual a 'OLTP: Itens de Pedido' (se não houver filtros de exclusão).

SELECT 
    '1. OLTP: Pedidos (tb_orders)' AS tabela_origem, 
    COUNT(*) AS total_registros 
FROM tb_orders

UNION ALL

SELECT 
    '2. OLTP: Itens de Pedido (tb_order_items)', 
    COUNT(*) 
FROM tb_order_items

UNION ALL

SELECT 
    '3. DW: Fato Vendas (fact_sales)', 
    COUNT(*) 
FROM analytics.fact_sales;

-- 2. VALIDAÇÃO DE QUALIDADE DE DADOS (INTEGRIDADE REFERENCIAL)
-- Verifica se existem registros na Fato com chaves nulas (SKs).
-- Esperado: Todos os valores devem ser 0.
-- Se houver valores > 0, significa que itens de venda foram carregados sem
-- encontrar o respectivo Cliente, Produto ou Vendedor correspondente.

SELECT 
    'Registros com Customer_SK Nulo' AS validacao,
    COUNT(*) AS qtd_erros
FROM analytics.fact_sales 
WHERE customer_sk IS NULL

UNION ALL

SELECT 
    'Registros com Product_SK Nulo',
    COUNT(*)
FROM analytics.fact_sales 
WHERE product_sk IS NULL

UNION ALL

SELECT 
    'Registros com Seller_SK Nulo',
    COUNT(*)
FROM analytics.fact_sales 
WHERE seller_sk IS NULL

UNION ALL

SELECT 
    'Registros com Date_SK Nulo',
    COUNT(*)
FROM analytics.fact_sales 
WHERE date_sk IS NULL;

-- 3. CHECK DE CONSISTÊNCIA FINANCEIRA (SOMA)
-- Valida se o valor total monetário bate entre OLTP e DW.

SELECT 
    'OLTP: Soma Total Preço' as metrica,
    SUM(price) as valor_total
FROM tb_order_items

UNION ALL

SELECT 
    'DW: Soma Total Preço',
    SUM(price)
FROM analytics.fact_sales;