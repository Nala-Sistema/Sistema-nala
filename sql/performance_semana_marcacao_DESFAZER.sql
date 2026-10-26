-- =============================================================================
-- DESFAZER sql/performance_semana_marcacao.sql   [PERFORMANCE SEMANAL] E1
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- Não precisa voltar o app antes: sem a tabela, a aba Semana mostra as
--   sugestões e avisa "Marcação aguardando o SQL" (não quebra).
--
-- RECUSA RODAR se a tabela tiver marcação (é a resposta das gestoras). Se for
--   mesmo para apagar, à mão antes: DELETE FROM public.perf_semana_marcacao;
-- EFEITO: a tabela, as 2 funções e os gatilhos somem; os GRANTs saem juntos.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.perf_semana_marcacao') IS NULL THEN
        RAISE EXCEPTION 'perf_semana_marcacao não existe. Nada a desfazer.';
    END IF;
    SELECT count(*) INTO n FROM public.perf_semana_marcacao;
    IF n > 0 THEN
        RAISE EXCEPTION 'perf_semana_marcacao tem % marcação(ões). Veja o cabeçalho deste arquivo. Nada foi desfeito.', n;
    END IF;
END $$;

DROP TABLE public.perf_semana_marcacao;
DROP FUNCTION public.perf_semana_marcacao_entrada();
DROP FUNCTION public.perf_semana_marcacao_sem_update();

COMMIT;

SELECT to_regclass('public.perf_semana_marcacao') AS deve_ser_nulo;
