-- =============================================================================
-- DESFAZER sql/sinais_ciente.sql   [SINAIS DO DIA]
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- RECUSA RODAR se sinal_ciente tiver QUALQUER linha: cada linha é o registro
-- de intenção de uma gestora (quem, quando, por quê), e o DROP o apagaria sem
-- volta. Nesse caso, exporte a tabela antes (SELECT * FROM sinal_ciente ->
-- CSV) e decida com o Thiago.
-- EFEITO: a tabela some; o app volta a funcionar como a v1 (sem "Ciente"),
-- sem erro: ele confere se a tabela existe antes de ler.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.sinal_ciente') IS NULL THEN
        RAISE EXCEPTION 'sinal_ciente não existe. Nada a desfazer.';
    END IF;
    SELECT count(*) INTO n FROM public.sinal_ciente;
    IF n > 0 THEN
        RAISE EXCEPTION
'sinal_ciente tem % linha(s) de ciente gravadas. Exporte antes e decida com o Thiago. Nada foi desfeito.', n;
    END IF;
END $$;

DROP TABLE public.sinal_ciente;

COMMIT;
