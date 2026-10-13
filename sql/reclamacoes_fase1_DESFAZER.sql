-- =============================================================================
-- DESFAZER reclamacoes_fase1.sql  (so o que ele criou; nao toca em nada do sistema)
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\reclamacoes_fase1_DESFAZER.sql
-- ONDE: "Gestão Marketplaces" (still-shape-14526725) / branch production / neondb
-- CREDENCIAL: DONO do banco (neondb_owner).
--
-- TRAVA: se fact_reclamacoes ou fact_reputacao_loja ja tiverem dado, este
-- arquivo RECUSA rodar (nada e apagado). Para desfazer COM dado, esvaziar as
-- tabelas de proposito, a mao, depois de decidir que o dado pode ir embora.
-- Pode rodar de novo: se o papel ou as tabelas ja nao existirem, nao da erro.
-- =============================================================================
BEGIN;

DO $$
DECLARE
    n_reclam BIGINT := 0;
    n_reput  BIGINT := 0;
BEGIN
    IF to_regclass('public.fact_reclamacoes') IS NOT NULL THEN
        EXECUTE 'SELECT COUNT(*) FROM public.fact_reclamacoes' INTO n_reclam;
    END IF;
    IF to_regclass('public.fact_reputacao_loja') IS NOT NULL THEN
        EXECUTE 'SELECT COUNT(*) FROM public.fact_reputacao_loja' INTO n_reput;
    END IF;
    IF n_reclam > 0 OR n_reput > 0 THEN
        RAISE EXCEPTION
            'DESFAZER RECUSADO: fact_reclamacoes tem % linha(s) e fact_reputacao_loja tem % linha(s). Nada foi apagado.',
            n_reclam, n_reput;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coletor_reclamacoes') THEN
        IF to_regclass('public.fact_reclamacoes') IS NOT NULL THEN
            REVOKE ALL ON public.fact_reclamacoes FROM coletor_reclamacoes;
        END IF;
        IF to_regclass('public.fact_reputacao_loja') IS NOT NULL THEN
            REVOKE ALL ON public.fact_reputacao_loja FROM coletor_reclamacoes;
        END IF;
        REVOKE ALL ON public.dim_fonte_dados FROM coletor_reclamacoes;
        REVOKE ALL ON SCHEMA public FROM coletor_reclamacoes;
        DROP ROLE coletor_reclamacoes;
    END IF;
END
$$;

DROP TABLE IF EXISTS fact_reclamacoes;
DROP TABLE IF EXISTS fact_reputacao_loja;

COMMIT;
