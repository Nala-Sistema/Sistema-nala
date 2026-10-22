-- =============================================================================
-- DESFAZER sql/ads_log_estrategico.sql   [HISTÓRICO E AÇÕES] etapa 1
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- ANTES: volte o rodar_ciclo.py e o analista-ads.md para as cópias em
--   C:\Users\thiag\Nala-Analises\_antes_etapa1\ (senão o próximo ciclo para
--   no passo que grava no banco) e apague a variável de usuário
--   NALA_CICLO_DB_URL do Windows.
--
-- RECUSA RODAR se ads_log_estrategico tiver linha: é o histórico oficial e só
-- acrescenta. A importação pode ser refeita dos arquivos (zip + metodo\logs),
-- mas linha gravada por ciclo depois disso só existe aqui e no .md do ciclo.
-- Se for mesmo para apagar tudo, rode antes, à mão (o gatilho recusa TRUNCATE
-- até para o dono, por isso o DISABLE explícito):
--     ALTER TABLE public.ads_log_estrategico DISABLE TRIGGER ads_log_sem_truncate;
--     TRUNCATE public.ads_log_estrategico;
-- EFEITO: tabela, view, funções e o usuário ciclo_ads somem.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.ads_log_estrategico') IS NULL THEN
        RAISE EXCEPTION 'ads_log_estrategico não existe. Nada a desfazer.';
    END IF;
    SELECT count(*) INTO n FROM public.ads_log_estrategico;
    IF n > 0 THEN
        RAISE EXCEPTION
'ads_log_estrategico tem % linha(s). Se for mesmo para apagar, veja o cabeçalho deste arquivo. Nada foi desfeito.', n;
    END IF;
END $$;

DROP VIEW IF EXISTS public.v_ads_log_vigente;
DROP TABLE public.ads_log_estrategico;
DROP FUNCTION IF EXISTS public.ads_log_entrada();
DROP FUNCTION IF EXISTS public.ads_log_so_acrescenta();

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ciclo_ads') THEN
        REVOKE ALL ON ALL TABLES IN SCHEMA public FROM ciclo_ads;
        REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM ciclo_ads;
        REVOKE USAGE ON SCHEMA public FROM ciclo_ads;
        DROP ROLE ciclo_ads;
    END IF;
END $$;

COMMIT;
