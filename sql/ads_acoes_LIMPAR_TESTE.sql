-- =============================================================================
-- LIMPAR O TESTE DA CAIXINHA (usuário admin)   [HISTÓRICO E AÇÕES] etapa 2
-- =============================================================================
-- 22/10/2026. O teste da caixinha no app de dev é feito SÓ com o usuário admin.
-- Este arquivo apaga o que o teste deixou, e nada mais:
--   - as decisões do admin em ads_acao_decisao;
--   - os cientes do admin na regra 'ads_acao' (sinal_ciente);
--   - as linhas do histórico (sinal_historico) das ações que o admin marcou, se
--     o job das 12h chegou a gravá-las.
-- As AÇÕES (ads_acao) NÃO saem: são permanentes (só acrescenta) e são o plano
-- real do C28 da ML-LPT, válido até o C29.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Rodar tudo de uma vez.
-- RESULTADO ESPERADO: a conferência no fim mostra 0 nas três contagens.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

CREATE TEMP TABLE _acoes_do_teste ON COMMIT DROP AS
SELECT 'acao:' || id_acao AS objeto FROM public.ads_acao_decisao WHERE decidido_por = 'admin';

DELETE FROM public.sinal_historico
 WHERE regra = 'ads_acao' AND objeto IN (SELECT objeto FROM _acoes_do_teste);
DELETE FROM public.sinal_ciente WHERE regra = 'ads_acao' AND criado_por = 'admin';
DELETE FROM public.ads_acao_decisao WHERE decidido_por = 'admin';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.ads_acao_decisao WHERE decidido_por = 'admin')
       OR EXISTS (SELECT 1 FROM public.sinal_ciente WHERE regra = 'ads_acao' AND criado_por = 'admin') THEN
        RAISE EXCEPTION 'Sobrou teste do admin. Nada foi apagado.';
    END IF;
END $$;

COMMIT;

SELECT (SELECT count(*) FROM public.ads_acao_decisao WHERE decidido_por = 'admin') AS decisoes_admin,
       (SELECT count(*) FROM public.sinal_ciente WHERE regra = 'ads_acao' AND criado_por = 'admin') AS cientes_admin,
       (SELECT count(*) FROM public.sinal_historico h
         WHERE h.regra = 'ads_acao'
           AND NOT EXISTS (SELECT 1 FROM public.ads_acao_decisao d
                            WHERE 'acao:' || d.id_acao = h.objeto)) AS historico_sem_decisao;
