-- =============================================================================
-- Full por loja: próxima data de envio liberada + regra "Full parado" no Ciente
-- =============================================================================
-- 12/10/2026, frente [FULL POR LOJA] (plano aprovado pelo Thiago). NÃO RODAR sem
-- o parecer do auditor-tecnico (o Mestre chama).
--
-- POR QUE
--   A API não informa a próxima data de envio liberada do Full (sonda de 12/10:
--   ML 404; Shopee só tem status de adesão ao FBS). A gestora informa a data de
--   cada loja na aba "Full por loja" do Sinais do Dia; o alerta passa a ser "o
--   Full acaba antes da próxima data?". O sinal "Full parado" usa o Ciente
--   (sinal_ciente), cuja CHECK de regra não conhece 'full_parado'.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guardas: aborta se sinal_ciente ou sinal_historico não existirem, se as
--      CHECKs de regra já aceitarem 'full_parado' (já aplicado) ou se a tabela
--      nova já existir.
--   2. Troca as CHECKs sinal_ciente_regra_valida e sinal_historico_regra_valida:
--      as 14 regras de antes + 'full_parado' (igual a sinais_dia.REGRAS). As
--      linhas que já existem continuam válidas (nenhuma muda). O job do histórico
--      não grava 'full_parado' hoje; a CHECK muda junto para as duas listas
--      continuarem iguais a REGRAS (os testes conferem).
--   3. Cria full_proxima_data_envio: SÓ INSERT (cada informação é uma linha; a
--      data em vigor é a última da loja). Guarda quem informou e quando.
--      data_envio NULL = a gestora limpou a data (volta aos 21 dias).
--   4. Conferência: as CHECKs aceitam a regra nova e a tabela responde.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: qualquer hora (CHECKs recriadas em milissegundos; tabelas pequenas).
-- EFEITO: nenhuma linha muda. Antes deste SQL, a aba mostra "data liberada:
--   aguardando o SQL" e usa os 21 dias; o Ciente do "Full parado" fica oculto.
-- ACESSO: o app usa o dono (sem GRANT aqui). Quando existir o usuário mínimo do
--   app, ele precisa de SELECT, INSERT na tabela e USAGE na sequência.
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\full_por_loja_DESFAZER.sql
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    def_c text;
    def_h text;
BEGIN
    IF to_regclass('public.sinal_ciente') IS NULL OR to_regclass('public.sinal_historico') IS NULL THEN
        RAISE EXCEPTION 'sinal_ciente ou sinal_historico não existe. Nada foi aplicado.';
    END IF;
    SELECT pg_get_constraintdef(oid) INTO def_c FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_regra_valida';
    SELECT pg_get_constraintdef(oid) INTO def_h FROM pg_constraint
     WHERE conrelid = 'public.sinal_historico'::regclass AND conname = 'sinal_historico_regra_valida';
    IF def_c IS NULL OR def_h IS NULL THEN
        RAISE EXCEPTION 'CHECK de regra não encontrada em sinal_ciente/sinal_historico. Nada foi aplicado.';
    END IF;
    IF def_c LIKE '%full_parado%' OR def_h LIKE '%full_parado%' THEN
        RAISE EXCEPTION 'A CHECK já aceita full_parado. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.full_proxima_data_envio') IS NOT NULL THEN
        RAISE EXCEPTION 'full_proxima_data_envio já existe. Nada foi aplicado.';
    END IF;
END $$;

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_regra_valida;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado'));

ALTER TABLE public.sinal_historico DROP CONSTRAINT sinal_historico_regra_valida;
ALTER TABLE public.sinal_historico ADD CONSTRAINT sinal_historico_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado'));

CREATE TABLE public.full_proxima_data_envio (
    id             bigserial    PRIMARY KEY,
    marketplace    varchar(30)  NOT NULL,
    loja           varchar(60)  NOT NULL,
    data_envio     date,
    informado_por  varchar(100) NOT NULL,
    informado_em   timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    CONSTRAINT full_data_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT full_data_sem_vazio CHECK (btrim(loja) <> '' AND btrim(informado_por) <> ''),
    CONSTRAINT full_data_futura CHECK (
        data_envio IS NULL
        OR (data_envio > informado_em::date AND data_envio <= informado_em::date + 90))
);

CREATE INDEX ix_full_proxima_data_loja ON public.full_proxima_data_envio
    (marketplace, loja, informado_em DESC, id DESC);

COMMENT ON TABLE public.full_proxima_data_envio IS
    'Full por loja (Sinais do Dia): próxima data de envio ao Full liberada, informada pela gestora '
    '(a API não dá). Só INSERT; a data em vigor é a última linha da loja. NULL = limpou.';

DO $$
DECLARE
    def_c text;
    def_h text;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO def_c FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_regra_valida';
    SELECT pg_get_constraintdef(oid) INTO def_h FROM pg_constraint
     WHERE conrelid = 'public.sinal_historico'::regclass AND conname = 'sinal_historico_regra_valida';
    IF def_c NOT LIKE '%full_parado%' OR def_c NOT LIKE '%visitas_alta%' OR def_c NOT LIKE '%vendas_queda%'
       OR def_h NOT LIKE '%full_parado%' OR def_h NOT LIKE '%visitas_alta%' OR def_h NOT LIKE '%vendas_queda%' THEN
        RAISE EXCEPTION 'As CHECKs novas não têm as 15 regras. Nada foi aplicado.';
    END IF;
    PERFORM 1 FROM public.full_proxima_data_envio LIMIT 1;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): as duas CHECKs com 'full_parado' e a tabela vazia.
SELECT conrelid::regclass AS tabela, conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conname IN ('sinal_ciente_regra_valida', 'sinal_historico_regra_valida');
SELECT count(*) AS linhas_full_proxima_data FROM public.full_proxima_data_envio;
