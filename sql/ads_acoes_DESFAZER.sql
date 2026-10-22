-- =============================================================================
-- DESFAZER sql/ads_acoes.sql   [HISTÓRICO E AÇÕES] etapa 2
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- ANTES: volte o app (sinais_dia.py) para a versão sem a etapa 2 (main e dev);
--   senão a tela procura a regra 'ads_acao' e o motivo 'feito' que somem aqui.
--   Volte também o rodar_ciclo.py / analista sem "# ACOES", se já trocados.
--
-- RECUSA RODAR se:
--   - ads_acao tiver linha (é histórico e só acrescenta). Se for mesmo para
--     apagar, à mão antes: DELETE FROM ads_acao_decisao; depois
--     ALTER TABLE public.ads_acao DISABLE TRIGGER ads_acao_sem_truncate;
--     TRUNCATE public.ads_acao CASCADE;
--   - sinal_historico ou sinal_ciente tiverem linha com regra 'ads_acao', ou
--     sinal_ciente com motivo 'feito' (a CHECK antiga não aceitaria).
-- EFEITO: as 3 tabelas, a view e as funções somem; as CHECKs voltam às listas
--   de antes (sem 'ads_acao' e sem 'feito'); os GRANTs saem junto com as tabelas.
--   O usuário ciclo_ads FICA (é da etapa 1).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    IF to_regclass('public.ads_acao') IS NULL THEN
        RAISE EXCEPTION 'ads_acao não existe. Nada a desfazer.';
    END IF;
    SELECT count(*) INTO n FROM public.ads_acao;
    IF n > 0 THEN
        RAISE EXCEPTION 'ads_acao tem % linha(s). Veja o cabeçalho deste arquivo. Nada foi desfeito.', n;
    END IF;
    SELECT count(*) INTO n FROM public.sinal_historico WHERE regra = 'ads_acao';
    IF n > 0 THEN
        RAISE EXCEPTION 'sinal_historico tem % linha(s) da regra ads_acao. Nada foi desfeito.', n;
    END IF;
    SELECT count(*) INTO n FROM public.sinal_ciente WHERE regra = 'ads_acao' OR motivo = 'feito';
    IF n > 0 THEN
        RAISE EXCEPTION 'sinal_ciente tem % linha(s) com ads_acao/feito. Nada foi desfeito.', n;
    END IF;
END $$;

DROP TABLE IF EXISTS public.ads_acao_decisao;
DROP VIEW IF EXISTS public.v_ads_acao_vigente;
DROP TABLE public.ads_acao;
DROP TABLE IF EXISTS public.dim_loja_responsavel_ads;
DROP FUNCTION IF EXISTS public.ads_acao_decisao_entrada();
DROP FUNCTION IF EXISTS public.ads_acao_entrada();
DROP FUNCTION IF EXISTS public.ads_acao_so_acrescenta();

ALTER TABLE public.sinal_historico DROP CONSTRAINT sinal_historico_regra_valida;
ALTER TABLE public.sinal_historico ADD CONSTRAINT sinal_historico_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado'));

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_regra_valida;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado'));

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_motivo_valido;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_motivo_valido CHECK (motivo IN (
    'falta_fornecedor', 'proposital', 'em_andamento', 'alarme_falso', 'outro'));

COMMIT;

-- Conferência: deve mostrar as 3 CHECKs sem ads_acao/feito e as tabelas = null.
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conname IN ('sinal_historico_regra_valida', 'sinal_ciente_regra_valida', 'sinal_ciente_motivo_valido');
SELECT to_regclass('public.ads_acao') AS ads_acao, to_regclass('public.ads_acao_decisao') AS decisao,
       to_regclass('public.dim_loja_responsavel_ads') AS responsavel;
