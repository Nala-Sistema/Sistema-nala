-- =============================================================================
-- DESFAZER sql/pendentes_status_aguardando.sql   [VENDAS PENDENTES ML]
-- =============================================================================
-- ONDE RODAR: Neon "Gestao Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- RECUSA RODAR se existir qualquer linha com status 'Aguardando coleta': voltar
-- a constraint antiga falharia (ou obrigaria a apagar/alterar dado). Nesse caso
-- resolva as linhas antes (a tela fecha as que a coleta ja' trouxe; as demais
-- sao decisao de gente) e rode de novo. ATENCAO: sem a constraint nova, o botao
-- "Reprocessar SKUs" das pendentes da API volta a dar CheckViolation.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n integer;
BEGIN
    SELECT count(*) INTO n FROM public.fact_vendas_pendentes
     WHERE status = 'Aguardando coleta';
    IF n > 0 THEN
        RAISE EXCEPTION
'Existem % linha(s) com status "Aguardando coleta". Nada foi desfeito.', n;
    END IF;
END $$;

ALTER TABLE public.fact_vendas_pendentes
    DROP CONSTRAINT fact_vendas_pendentes_status_check;

ALTER TABLE public.fact_vendas_pendentes
    ADD CONSTRAINT fact_vendas_pendentes_status_check
    CHECK (status IN ('Pendente', 'Reprocessado', 'Revisado manualmente'));

-- Conferencia. Esperado: definicao com 3 status, validada = true.
SELECT pg_get_constraintdef(c.oid) AS definicao, c.convalidated AS validada
  FROM pg_constraint c
 WHERE c.conrelid = 'public.fact_vendas_pendentes'::regclass
   AND c.conname = 'fact_vendas_pendentes_status_check';

COMMIT;
