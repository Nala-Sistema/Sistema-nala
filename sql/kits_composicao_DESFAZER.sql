-- =============================================================================
-- DESFAZER: sql/kits_composicao.sql (frente [KITS], 1ª entrega)
-- =============================================================================
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner, pelo
--             SQL EDITOR do Neon, arquivo inteiro de uma vez. A CREDENCIAL É A
--             DO DONO (neondb_owner) — o editor já abre conectado como ele.
--
-- O QUE FAZ: apaga as duas tabelas de kit E O QUE ELAS GUARDARAM (composições
--   carregadas do UpSeller e pendências). Não toca em venda, custo nem margem.
--   ANTES: se a carga de kits já estiver publicada no app, tirá-la do ar
--   primeiro, ou a tela quebra por tabela ausente. A composição volta com um
--   novo upload do export do UpSeller depois de recriar as tabelas.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- Quanto vai ser perdido (olhar antes do COMMIT aparecer).
SELECT 'dim_kit_composicao' AS tabela, count(*) AS registros_apagados
  FROM public.dim_kit_composicao
UNION ALL
SELECT 'dim_kit_composicao_pendente', count(*) FROM public.dim_kit_composicao_pendente;

DROP TABLE public.dim_kit_composicao_pendente;
DROP TABLE public.dim_kit_composicao;

-- Esperado: duas colunas nulas.
SELECT to_regclass('public.dim_kit_composicao')          AS composicao_deve_ser_nulo,
       to_regclass('public.dim_kit_composicao_pendente') AS pendente_deve_ser_nulo;

COMMIT;
