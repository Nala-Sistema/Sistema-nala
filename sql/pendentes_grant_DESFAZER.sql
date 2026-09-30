-- =============================================================================
-- DESFAZER sql/pendentes_grant.sql   [VENDAS PENDENTES ML]
-- =============================================================================
-- ONDE RODAR: Neon "Gestao Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- O QUE FAZ: tira do coletor_vendas tudo o que pendentes_grant.sql deu em
--   fact_vendas_pendentes. O coletor volta a pular as pendentes em silencio
--   ("permissao pendente" no ml_execucoes) e a sincronizacao do snapshot
--   continua normal (a guarda confere a permissao antes de tentar).
-- O QUE NAO FAZ: nao apaga as pendentes que ja' entraram; elas ficam na tabela
--   (e na aba Vendas Pendentes). Apagar seria decisao separada.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

REVOKE USAGE ON SEQUENCE public.fact_vendas_pendentes_id_seq FROM coletor_vendas;
REVOKE SELECT (numero_pedido, sku, loja_origem)
    ON public.fact_vendas_pendentes FROM coletor_vendas;
REVOKE INSERT ON public.fact_vendas_pendentes FROM coletor_vendas;

-- Conferencia. Esperado: tudo false.
SELECT has_table_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'INSERT') AS insert,
       has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'numero_pedido', 'SELECT') AS select_numero_pedido,
       has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'sku', 'SELECT') AS select_sku,
       has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'loja_origem', 'SELECT') AS select_loja,
       has_sequence_privilege('coletor_vendas', 'public.fact_vendas_pendentes_id_seq', 'USAGE') AS usage_seq;

COMMIT;
