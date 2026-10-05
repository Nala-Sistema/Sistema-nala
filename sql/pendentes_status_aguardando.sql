-- =============================================================================
-- fact_vendas_pendentes: aceitar o status 'Aguardando coleta'   [VENDAS PENDENTES ML]
-- =============================================================================
-- 05/10/2026. ERRO EM PRODUCAO (main 98bca15): "Reprocessar SKUs" numa pendente
-- da API quebra com
--   CheckViolation: new row for relation "fact_vendas_pendentes" violates check
--   constraint "fact_vendas_pendentes_status_check"
-- porque o codigo (database_utils._marcar_aguardando_coleta) grava o status
-- 'Aguardando coleta' e a constraint real so' aceita
--   ('Pendente', 'Reprocessado', 'Revisado manualmente').
-- A constraint nao estava em nenhum SQL do repo; foi lida de pg_constraint em
-- producao. Efeito do erro: a transacao inteira foi revertida (nenhum
-- mapeamento, nenhuma pendente alterada, nada no snapshot).
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSACAO SO:
--   1. Guarda: confere que a definicao ATUAL da constraint e' exatamente a
--      conhecida (3 status). Qualquer outra coisa -> aborta sem tocar em nada.
--   2. DROP + ADD da constraint, mesmo nome, agora com 4 status:
--      'Pendente', 'Reprocessado', 'Revisado manualmente', 'Aguardando coleta'.
--   3. Conferencia no fim.
--
-- ONDE RODAR: Neon "Gestao Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: qualquer hora (a tabela tem ~900 linhas; o ADD valida em
--   milissegundos). O lock_timeout de 5s aborta limpo se algo estiver segurando
--   a tabela.
-- EFEITO: nenhuma linha muda. Depois disto o botao "Reprocessar SKUs" das
--   pendentes da API passa a funcionar.
-- DESFAZER: sql/pendentes_status_aguardando_DESFAZER.sql (recusa rodar se ja'
--   existir linha 'Aguardando coleta', para nao apagar dado).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    atual text;
BEGIN
    IF to_regclass('public.fact_vendas_pendentes') IS NULL THEN
        RAISE EXCEPTION 'fact_vendas_pendentes nao existe. Nada foi aplicado.';
    END IF;

    SELECT pg_get_constraintdef(c.oid) INTO atual
      FROM pg_constraint c
     WHERE c.conrelid = 'public.fact_vendas_pendentes'::regclass
       AND c.conname = 'fact_vendas_pendentes_status_check';

    IF atual IS NULL THEN
        RAISE EXCEPTION
'A constraint fact_vendas_pendentes_status_check nao existe. Nada foi aplicado.';
    END IF;

    IF atual LIKE '%Aguardando coleta%' THEN
        RAISE EXCEPTION
'A constraint ja aceita "Aguardando coleta" (%). Nada a fazer.', atual;
    END IF;

    -- Definicao conhecida, lida de pg_constraint em producao em 05/10/2026.
    IF atual <> 'CHECK (((status)::text = ANY (ARRAY[''Pendente''::text, ''Reprocessado''::text, ''Revisado manualmente''::text])))' THEN
        RAISE EXCEPTION
'A constraint atual nao e a esperada. Nada foi aplicado. Definicao encontrada: %', atual;
    END IF;
END $$;

ALTER TABLE public.fact_vendas_pendentes
    DROP CONSTRAINT fact_vendas_pendentes_status_check;

ALTER TABLE public.fact_vendas_pendentes
    ADD CONSTRAINT fact_vendas_pendentes_status_check
    CHECK (status IN ('Pendente', 'Reprocessado', 'Revisado manualmente',
                      'Aguardando coleta'));

-- -----------------------------------------------------------------------------
-- Conferencia (dentro da transacao). Esperado:
--   definicao com os 4 status; validada = true; nenhuma linha fora da lista.
-- Se divergir, ROLLBACK em vez de COMMIT.
-- -----------------------------------------------------------------------------
SELECT pg_get_constraintdef(c.oid) AS definicao,
       c.convalidated              AS validada,
       (SELECT count(*) FROM public.fact_vendas_pendentes
         WHERE status NOT IN ('Pendente', 'Reprocessado', 'Revisado manualmente',
                              'Aguardando coleta')) AS linhas_fora_da_lista
  FROM pg_constraint c
 WHERE c.conrelid = 'public.fact_vendas_pendentes'::regclass
   AND c.conname = 'fact_vendas_pendentes_status_check';

COMMIT;
