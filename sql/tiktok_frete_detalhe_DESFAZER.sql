-- =============================================================================
-- DESFAZER: fact_tiktok_frete_detalhe (frente [ALARME FRETE], etapa 2)
-- =============================================================================
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner, pelo
--             SQL EDITOR do Neon, arquivo inteiro de uma vez.
--
-- O QUE FAZ: apaga a tabela e o que ela guardou (peso e reembolso do TikTok
-- dos uploads feitos desde que ela existe). Nenhuma outra tabela é tocada.
-- Depois disso o upload do TikTok só avisa que não gravou o detalhe, e a aba
-- "🚚 Penalização de frete" volta a funcionar sem as colunas do TikTok.
-- =============================================================================

BEGIN;

DROP TABLE IF EXISTS public.fact_tiktok_frete_detalhe;

SELECT to_regclass('public.fact_tiktok_frete_detalhe') AS deve_ser_nulo;

COMMIT;
