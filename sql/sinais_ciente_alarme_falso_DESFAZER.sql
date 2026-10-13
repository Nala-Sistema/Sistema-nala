-- =============================================================================
-- DESFAZER sql/sinais_ciente_alarme_falso.sql   [SINAIS AJUSTE]
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- RECUSA RODAR se existir QUALQUER ciente com motivo 'alarme_falso': a CHECK antiga
-- não aceitaria a linha e apagar a linha destruiria o registro de intenção da gestora.
-- Nesse caso, exporte (SELECT * FROM sinal_ciente WHERE motivo = 'alarme_falso' -> CSV)
-- e decida com o Thiago.
-- EFEITO: a view some e a CHECK volta aos 4 motivos de antes. Antes de rodar, o código
-- novo do app (que oferece o motivo) tem que ter saído do ar, senão a gestora o escolhe
-- e a gravação é recusada.
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
    SELECT count(*) INTO n FROM public.sinal_ciente WHERE motivo = 'alarme_falso';
    IF n > 0 THEN
        RAISE EXCEPTION
'Há % ciente(s) com motivo alarme_falso. Exporte antes e decida com o Thiago. Nada foi desfeito.', n;
    END IF;
END $$;

DROP VIEW IF EXISTS public.v_sinal_alarme_falso;

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_motivo_valido;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_motivo_valido CHECK (motivo IN (
    'falta_fornecedor', 'proposital', 'em_andamento', 'outro'));

COMMIT;
