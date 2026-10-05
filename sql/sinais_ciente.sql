-- =============================================================================
-- sinal_ciente: a gestora responde ao sinal do painel Sinais do Dia   [SINAIS DO DIA]
-- =============================================================================
-- 05/10/2026, v1.1 "Ciente" (plano aprovado pelo Thiago em 05/10/2026).
-- Sem isto o sinal repete todo dia (ex.: abraçadeira em falta no fornecedor).
-- Em cada sinal a gestora marca "Ciente" com MOTIVO, NOTA e "SILENCIAR ATÉ".
-- O silenciado volta sozinho quando passa a data ou quando PIORA (calculado
-- pelo app na leitura, sinais_dia.piorou). Cada linha é um registro de
-- intenção (quem, quando, por quê) que o [MESTRE ANÁLISES] lê para não
-- recomendar o que foi decidido de propósito. Nada é apagado: encerrar um
-- ciente (reativar, ou um ciente novo na mesma chave) só preenche encerrado_*.
--
-- CHAVE DO SINAL = marketplace + loja + regra + objeto.
--   regra  = a regra que disparou (lista na CHECK sinal_ciente_regra_valida,
--            igual a sinais_dia.REGRAS);
--   objeto = estoque_id (Full de estoque), MLB (anúncio), 'peça:X' (família);
--            evento de um dia (ads_config, full_envio) leva '@AAAA-MM-DD'.
-- Um só ciente ABERTO por chave (índice único parcial ux_sinal_ciente_aberto).
--
-- Horários em Brasília sem fuso, como as outras tabelas do sistema
-- (default now() AT TIME ZONE 'America/Sao_Paulo').
--
-- QUEM GRAVA: o app (Streamlit), hoje com o usuário neondb_owner -> sem GRANT.
--   Quando o app sair do dono (card "Usuário de permissão mínima para o app"),
--   o usuário novo precisa de SELECT, INSERT e UPDATE(encerrado_em,
--   encerrado_por, como_encerrou) nesta tabela e USAGE na sequência do id.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se a tabela já existir (nada é tocado).
--   2. CREATE TABLE + CHECKs + índice único parcial.
--   3. Conferência: 7 CHECKs, o índice e a tabela vazia; senão aborta.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: qualquer hora (tabela nova; não trava nada que já existe).
-- EFEITO: nenhuma linha de outra tabela muda. O app mostra "Ciente" assim que
--   a tabela existir (antes disso a tela funciona como a v1).
-- DESFAZER: sql/sinais_ciente_DESFAZER.sql (recusa rodar se houver ciente
--   gravado, para não apagar o registro de intenção das gestoras).
--
-- ATENÇÃO aos testes: tests/test_sinais_dia.py LÊ o CREATE TABLE e o CREATE
-- UNIQUE INDEX DESTE arquivo para criar a TEMP. Mudou aqui, muda lá.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF to_regclass('public.sinal_ciente') IS NOT NULL THEN
        RAISE EXCEPTION 'A tabela sinal_ciente já existe. Nada foi aplicado.';
    END IF;
END $$;

CREATE TABLE public.sinal_ciente (
    id                bigserial    PRIMARY KEY,
    marketplace       varchar(30)  NOT NULL,
    loja              varchar(60)  NOT NULL,
    regra             varchar(30)  NOT NULL,
    objeto            varchar(120) NOT NULL,
    motivo            varchar(20)  NOT NULL,
    nota              text,
    silenciar_ate     date         NOT NULL,
    medida_no_ciente  numeric,
    texto_no_ciente   text,
    criado_por        varchar(100) NOT NULL,
    criado_em         timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    encerrado_em      timestamp,
    encerrado_por     varchar(100),
    como_encerrou     varchar(12),
    CONSTRAINT sinal_ciente_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT sinal_ciente_sem_vazio CHECK (
        btrim(loja) <> '' AND btrim(objeto) <> '' AND btrim(criado_por) <> ''),
    CONSTRAINT sinal_ciente_regra_valida CHECK (regra IN (
        'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
        'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
        'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta')),
    CONSTRAINT sinal_ciente_motivo_valido CHECK (motivo IN (
        'falta_fornecedor', 'proposital', 'em_andamento', 'outro')),
    CONSTRAINT sinal_ciente_nota_no_outro CHECK (
        motivo <> 'outro' OR nullif(btrim(nota), '') IS NOT NULL),
    CONSTRAINT sinal_ciente_prazo CHECK (
        silenciar_ate >= criado_em::date AND silenciar_ate <= criado_em::date + 60),
    CONSTRAINT sinal_ciente_encerramento CHECK (
        (encerrado_em IS NULL AND encerrado_por IS NULL AND como_encerrou IS NULL)
        OR (encerrado_em IS NOT NULL AND btrim(encerrado_por) <> ''
            AND como_encerrou IN ('reativado', 'substituido')))
);

CREATE UNIQUE INDEX ux_sinal_ciente_aberto ON public.sinal_ciente (marketplace, loja, regra, objeto) WHERE encerrado_em IS NULL;

COMMENT ON TABLE public.sinal_ciente IS
    'Sinais do Dia v1.1: ciente da gestora (motivo, nota, silenciar até). Intenção lida pelo [MESTRE ANÁLISES]. Nunca apagar linha.';

DO $$
DECLARE
    n_check int;
    n_linhas int;
BEGIN
    SELECT count(*) INTO n_check FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND contype = 'c';
    IF n_check <> 7 THEN
        RAISE EXCEPTION 'Esperava 7 CHECKs em sinal_ciente, achei %. Nada foi aplicado.', n_check;
    END IF;
    IF to_regclass('public.ux_sinal_ciente_aberto') IS NULL THEN
        RAISE EXCEPTION 'Índice ux_sinal_ciente_aberto não foi criado. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO n_linhas FROM public.sinal_ciente;
    IF n_linhas <> 0 THEN
        RAISE EXCEPTION 'sinal_ciente deveria nascer vazia (tem %). Nada foi aplicado.', n_linhas;
    END IF;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): deve listar as 7 CHECKs e o índice.
SELECT conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conrelid = 'public.sinal_ciente'::regclass AND contype = 'c'
 ORDER BY conname;
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'sinal_ciente';
