-- =============================================================================
-- sinal_historico + usuário sinais_historico   [SINAIS DO DIA] v1.3 "Aparece desde"
-- =============================================================================
-- 06/10/2026 (plano aprovado pelo Thiago/Mestre). A tela Sinais do Dia ganha a
-- coluna "Aparece desde" (há quantos dias o sinal acende). Para isso cada sinal
-- do dia fica gravado aqui, uma linha por dia × sinal.
--
-- CHAVE = data + a chave do Ciente (marketplace, loja, regra, objeto): o
-- "aparece desde" casa com o sinal_ciente. regra = lista da CHECK abaixo
-- (igual a sinais_dia.REGRAS e à CHECK de sinal_ciente).
--
-- QUEM GRAVA: SÓ o job diário jobs/historico_sinais.py (GitHub Actions do repo
-- Sistema-nala, 12h de Brasília; e o backfill de 30 dias, rodado pelo Mestre),
-- com o usuário NOVO sinais_historico, criado aqui:
--   - NOLOGIN (padrão dos coletores): nasce sem poder entrar. O script do
--     Mestre faz, no mesmo passo, ALTER ROLE sinais_historico WITH LOGIN
--     PASSWORD ... e a senha vai só para o secret SINAIS_HIST_DB_URL do
--     repositório. Nenhuma senha passa por este arquivo.
--   - SELECT só nas fontes que a montagem dos sinais lê; INSERT e UPDATE só em
--     sinal_historico; nada de DELETE; nada em sinal_ciente nem em dim_produtos.
-- A tela (app) lê sinal_historico com o usuário dela (hoje o dono) e nunca grava.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se a tabela ou o usuário já existirem (nada é tocado).
--   2. CREATE TABLE + CHECKs + índice de leitura.
--   3. CREATE ROLE sinais_historico NOLOGIN + GRANTs.
--   4. Conferência: 3 CHECKs, índice, tabela vazia, usuário NOLOGIN e cada
--      privilégio esperado (inclusive os que NÃO podem existir); senão aborta.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: fora da janela dos coletores (04:00–06:30). A tabela é nova; os
--   GRANTs não travam as tabelas lidas.
-- DEPOIS: (a) o script do Mestre libera o login e define a senha (ALTER ROLE
--   sinais_historico WITH LOGIN PASSWORD ...); (b) cadastra o secret
--   SINAIS_HIST_DB_URL no repo Sistema-nala; (c) roda o backfill de 30 dias;
--   (d) dispara o workflow "Sinais do Dia - historico diario" à mão.
-- DESFAZER: sql/sinais_historico_DESFAZER.sql.
--
-- ATENÇÃO aos testes: tests/test_sinais_dia.py LÊ o CREATE TABLE e o CREATE
-- INDEX DESTE arquivo para criar a TEMP. Mudou aqui, muda lá.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF to_regclass('public.sinal_historico') IS NOT NULL THEN
        RAISE EXCEPTION 'A tabela sinal_historico já existe. Nada foi aplicado.';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sinais_historico') THEN
        RAISE EXCEPTION 'O usuário sinais_historico já existe. Nada foi aplicado.';
    END IF;
END $$;

CREATE TABLE public.sinal_historico (
    data          date         NOT NULL,
    marketplace   varchar(30)  NOT NULL,
    loja          varchar(60)  NOT NULL,
    regra         varchar(30)  NOT NULL,
    objeto        varchar(120) NOT NULL,
    medida        numeric,
    em_jogo       numeric,
    gravado_em    timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    CONSTRAINT sinal_historico_pkey PRIMARY KEY (data, marketplace, loja, regra, objeto),
    CONSTRAINT sinal_historico_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT sinal_historico_sem_vazio CHECK (btrim(loja) <> '' AND btrim(objeto) <> ''),
    CONSTRAINT sinal_historico_regra_valida CHECK (regra IN (
        'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
        'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
        'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta'))
);

CREATE INDEX ix_sinal_historico_loja_data ON public.sinal_historico (marketplace, loja, data);

COMMENT ON TABLE public.sinal_historico IS
    'Sinais do Dia v1.3: os sinais de cada dia (antes do Ciente), gravados só pelo job diário (usuário sinais_historico). Base do "Aparece desde".';

-- Usuário do job: NOLOGIN (o script do Mestre libera o login com a senha depois).
CREATE ROLE sinais_historico NOLOGIN;

GRANT USAGE ON SCHEMA public TO sinais_historico;
GRANT SELECT ON
    public.dim_lojas,
    public.fact_vendas_snapshot,
    public.dim_metas_loja,
    public.dim_estoque_anuncio,
    public.fact_estoque_diario,
    public.fact_ads_performance,
    public.fact_ads_campanha_config,
    public.fact_saude_anuncio,
    public.fact_pedidos_itens_marketplace,
    public.dim_kit_composicao,
    public.dim_sku_mapeamento,
    public.vw_visitas_dia,
    public.vw_views_shopee_30d
TO sinais_historico;
GRANT SELECT, INSERT, UPDATE ON public.sinal_historico TO sinais_historico;

DO $$
DECLARE
    n int;
    t text;
BEGIN
    SELECT count(*) INTO n FROM pg_constraint
     WHERE conrelid = 'public.sinal_historico'::regclass AND contype = 'c';
    IF n <> 3 THEN
        RAISE EXCEPTION 'Esperava 3 CHECKs em sinal_historico, achei %. Nada foi aplicado.', n;
    END IF;
    IF to_regclass('public.ix_sinal_historico_loja_data') IS NULL THEN
        RAISE EXCEPTION 'Índice ix_sinal_historico_loja_data não foi criado. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO n FROM public.sinal_historico;
    IF n <> 0 THEN
        RAISE EXCEPTION 'sinal_historico deveria nascer vazia (tem %). Nada foi aplicado.', n;
    END IF;
    IF (SELECT rolcanlogin FROM pg_roles WHERE rolname = 'sinais_historico') THEN
        RAISE EXCEPTION 'sinais_historico deveria nascer NOLOGIN. Nada foi aplicado.';
    END IF;
    FOREACH t IN ARRAY ARRAY['dim_lojas', 'fact_vendas_snapshot', 'dim_metas_loja',
        'dim_estoque_anuncio', 'fact_estoque_diario', 'fact_ads_performance',
        'fact_ads_campanha_config', 'fact_saude_anuncio', 'fact_pedidos_itens_marketplace',
        'dim_kit_composicao', 'dim_sku_mapeamento', 'vw_visitas_dia', 'vw_views_shopee_30d',
        'sinal_historico'] LOOP
        IF NOT has_table_privilege('sinais_historico', 'public.' || t, 'SELECT') THEN
            RAISE EXCEPTION 'sinais_historico sem SELECT em %. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    IF NOT has_table_privilege('sinais_historico', 'public.sinal_historico', 'INSERT')
       OR NOT has_table_privilege('sinais_historico', 'public.sinal_historico', 'UPDATE') THEN
        RAISE EXCEPTION 'sinais_historico sem INSERT/UPDATE em sinal_historico. Nada foi aplicado.';
    END IF;
    -- o que NÃO pode existir
    IF has_table_privilege('sinais_historico', 'public.sinal_historico', 'DELETE')
       OR has_table_privilege('sinais_historico', 'public.fact_vendas_snapshot', 'INSERT')
       OR has_table_privilege('sinais_historico', 'public.fact_vendas_snapshot', 'UPDATE')
       OR has_table_privilege('sinais_historico', 'public.fact_vendas_snapshot', 'DELETE')
       OR (to_regclass('public.sinal_ciente') IS NOT NULL
           AND has_table_privilege('sinais_historico', 'public.sinal_ciente', 'SELECT'))
       OR (to_regclass('public.dim_produtos') IS NOT NULL
           AND has_table_privilege('sinais_historico', 'public.dim_produtos', 'SELECT')) THEN
        RAISE EXCEPTION 'sinais_historico com privilégio a mais (herdado de PUBLIC?). Nada foi aplicado.';
    END IF;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): as CHECKs, o índice e os privilégios do usuário.
SELECT conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conrelid = 'public.sinal_historico'::regclass AND contype = 'c'
 ORDER BY conname;
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'sinal_historico';
SELECT table_name, string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privilegios
  FROM information_schema.role_table_grants
 WHERE grantee = 'sinais_historico'
 GROUP BY table_name
 ORDER BY table_name;
