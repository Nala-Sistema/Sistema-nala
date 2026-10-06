-- =============================================================================
-- DESFAZER sql/sinais_historico.sql   [SINAIS DO DIA] v1.3
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- ANTES: desligue o job (GitHub -> Actions -> "Sinais do Dia - historico
-- diario" -> Disable workflow) e apague o secret SINAIS_HIST_DB_URL; senão o
-- job falha todo dia ao tentar gravar.
--
-- RECUSA RODAR se sinal_historico tiver linha: o histórico pode ser refeito
-- pelo backfill (com a venda de hoje), mas o que foi gravado no dia guarda a
-- venda daquele dia. Se for mesmo para apagar, rode antes, à mão:
--     TRUNCATE public.sinal_historico;
-- EFEITO: a tabela e o usuário sinais_historico somem; a tela volta a mostrar
-- "—" em "Aparece desde", sem erro (ela confere se a tabela existe).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.sinal_historico') IS NULL THEN
        RAISE EXCEPTION 'sinal_historico não existe. Nada a desfazer.';
    END IF;
    SELECT count(*) INTO n FROM public.sinal_historico;
    IF n > 0 THEN
        RAISE EXCEPTION
'sinal_historico tem % linha(s). Se for mesmo para apagar, TRUNCATE à mão antes. Nada foi desfeito.', n;
    END IF;
END $$;

DROP TABLE public.sinal_historico;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sinais_historico') THEN
        REVOKE ALL ON ALL TABLES IN SCHEMA public FROM sinais_historico;
        REVOKE USAGE ON SCHEMA public FROM sinais_historico;
        DROP ROLE sinais_historico;
    END IF;
END $$;

COMMIT;
