-- =============================================================================
-- DESFAZER sql/plano_escala.sql   [PLANO ESCALA]
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- RECUSA RODAR se plano_escala ou plano_escala_decisao tiver QUALQUER linha:
-- cada plano e cada decisão é registro das gestoras (quem, quando, por quê),
-- e o DROP o apagaria sem volta. Nesse caso, exporte antes (SELECT * de cada
-- tabela -> CSV) e decida com o Thiago.
-- EFEITO: as 3 tabelas somem; a tela "Plano de escala" volta a avisar
-- "tabelas ainda não criadas", sem erro (ela confere antes de ler).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.plano_escala') IS NULL THEN
        RAISE EXCEPTION 'plano_escala não existe. Nada a desfazer.';
    END IF;
    SELECT (SELECT count(*) FROM public.plano_escala)
         + (SELECT count(*) FROM public.plano_escala_decisao) INTO n;
    IF n > 0 THEN
        RAISE EXCEPTION
'O plano de escala tem % linha(s) de plano/decisão gravadas. Exporte antes e decida com o Thiago. Nada foi desfeito.', n;
    END IF;
END $$;

DROP TABLE public.plano_escala_decisao;
DROP TABLE public.plano_escala_rodada;
DROP TABLE public.plano_escala;

COMMIT;
