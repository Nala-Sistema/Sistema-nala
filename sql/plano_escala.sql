-- =============================================================================
-- plano_escala: o Plano de escala por anúncio (v1, só Mercado Livre)   [PLANO ESCALA]
-- =============================================================================
-- 12/10/2026, Etapa 1 (plano aprovado pelo Thiago em 12/10/2026; regra do
-- [ANALISTA ESCALA] de 08/10). Lida e gravada SÓ pela tela "Plano de escala"
-- (plano_escala.py). A única porta de entrada do plano é o cadastro (.xlsx ou a
-- tabela editável da tela, que passam pela MESMA função plano_escala.gravar):
-- subir de novo SUBSTITUI o plano ativo da chave (marketplace, loja, anúncio),
-- nunca acrescenta.
--
-- TRÊS TABELAS:
--   plano_escala          um plano por anúncio (variações juntas; kit e peça
--                         são anúncios diferentes, logo planos separados). Só
--                         um ATIVO por (marketplace, loja, anuncio): índice
--                         único parcial ux_plano_escala_ativo. Genérica: a
--                         chave é marketplace + anuncio (MLB no ML, item_id na
--                         Shopee); o que for específico de um marketplace vai
--                         em detalhe (JSONB). parametros guarda o que a
--                         gestora informou (comissão/frete opcionais, preços à
--                         mão) e a FOTO dos insumos do cálculo (custo, imposto,
--                         comissão e frete usados com a origem de cada um,
--                         preço de margem zero, TETO, ponto de virada).
--   plano_escala_rodada   o PREVISTO de cada rodada, calculado no cadastro e
--                         recalculado inteiro quando o plano é revisado. Apagar
--                         o plano apaga as rodadas (CASCADE) — mas o app nunca
--                         apaga plano: encerra.
--   plano_escala_decisao  registro das decisões (quem, quando, motivo):
--                         cadastro, revisão (motivo obrigatório), encerramento
--                         pela gestora (motivo obrigatório) e conclusão pelo
--                         sistema. Nunca apagar linha (FK sem CASCADE).
--
-- Horários em Brasília sem fuso, como as outras tabelas do sistema.
--
-- GRANTs: NENHUM é necessário hoje. Quem lê e grava é o app (Streamlit), que
--   conecta com o neondb_owner, dono das tabelas. Nenhum job/coletor toca
--   nelas. Quando o app sair do dono (card "Usuário de permissão mínima para o
--   app"), o usuário novo precisa de: SELECT, INSERT, UPDATE em plano_escala;
--   SELECT, INSERT, DELETE em plano_escala_rodada; SELECT, INSERT em
--   plano_escala_decisao; USAGE nas sequências plano_escala_id_seq e
--   plano_escala_decisao_id_seq.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se qualquer das 3 tabelas já existir (nada é tocado).
--   2. CREATE TABLE x3 + CHECKs + índice único parcial + índice da decisão.
--   3. Conferência: CHECKs, índices e tabelas vazias; senão aborta.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: qualquer hora (tabelas novas; não trava nada que já existe).
-- EFEITO: nenhuma linha de outra tabela muda. A tela "Plano de escala" avisa
--   "tabelas ainda não criadas" até este arquivo rodar.
-- DESFAZER: sql/plano_escala_DESFAZER.sql (recusa rodar se houver plano ou
--   decisão gravada).
--
-- ATENÇÃO aos testes: tests/test_plano_escala.py LÊ os CREATE TABLE e os
-- CREATE INDEX DESTE arquivo para criar as TEMP. Mudou aqui, muda lá.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF to_regclass('public.plano_escala') IS NOT NULL
       OR to_regclass('public.plano_escala_rodada') IS NOT NULL
       OR to_regclass('public.plano_escala_decisao') IS NOT NULL THEN
        RAISE EXCEPTION 'Uma das tabelas do plano de escala já existe. Nada foi aplicado.';
    END IF;
END $$;

CREATE TABLE public.plano_escala (
    id                 bigserial     PRIMARY KEY,
    marketplace        varchar(30)   NOT NULL,
    loja               varchar(60)   NOT NULL,
    anuncio            varchar(40)   NOT NULL,
    sku                varchar(60)   NOT NULL,
    venda_concorrente  integer,
    meta_propria       integer,
    preco_concorrente  numeric(12,2) NOT NULL,
    preco_meta         numeric(12,2) NOT NULL,
    data_inicio        date          NOT NULL,
    gestora            varchar(100)  NOT NULL,
    parametros         jsonb         NOT NULL DEFAULT '{}'::jsonb,
    detalhe            jsonb         NOT NULL DEFAULT '{}'::jsonb,
    situacao           varchar(10)   NOT NULL DEFAULT 'ativo',
    criado_por         varchar(100)  NOT NULL,
    criado_em          timestamp     NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    atualizado_por     varchar(100),
    atualizado_em      timestamp,
    encerrado_por      varchar(100),
    encerrado_em       timestamp,
    como_encerrou      varchar(10),
    CONSTRAINT plano_escala_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT plano_escala_sem_vazio CHECK (
        btrim(loja) <> '' AND btrim(anuncio) <> '' AND btrim(sku) <> ''
        AND btrim(gestora) <> '' AND btrim(criado_por) <> ''),
    CONSTRAINT plano_escala_meta_informada CHECK (
        coalesce(venda_concorrente, 0) > 0 OR coalesce(meta_propria, 0) > 0),
    CONSTRAINT plano_escala_precos_positivos CHECK (
        preco_concorrente > 0 AND preco_meta > 0),
    CONSTRAINT plano_escala_situacao_valida CHECK (situacao IN ('ativo', 'encerrado')),
    CONSTRAINT plano_escala_encerramento CHECK (
        (situacao = 'ativo' AND encerrado_em IS NULL AND encerrado_por IS NULL
            AND como_encerrou IS NULL)
        OR (situacao = 'encerrado' AND encerrado_em IS NOT NULL
            AND btrim(coalesce(encerrado_por, '')) <> ''
            AND como_encerrou IN ('gestora', 'concluido')))
);

CREATE UNIQUE INDEX ux_plano_escala_ativo ON public.plano_escala (marketplace, loja, anuncio) WHERE situacao = 'ativo';

CREATE TABLE public.plano_escala_rodada (
    plano_id        bigint        NOT NULL REFERENCES public.plano_escala (id) ON DELETE CASCADE,
    rodada          smallint      NOT NULL,
    vendas          integer       NOT NULL,
    preco           numeric(12,4) NOT NULL,
    preco_a_mao     boolean       NOT NULL DEFAULT false,
    acos            numeric(6,4)  NOT NULL,
    parcela_ads     numeric(6,4)  NOT NULL,
    tacos           numeric(8,6)  NOT NULL,
    dias_esperados  smallint      NOT NULL,
    margem_unit     numeric(12,4) NOT NULL,
    receita         numeric(14,2) NOT NULL,
    gasto_ads       numeric(14,2) NOT NULL,
    resultado       numeric(14,2) NOT NULL,
    acumulado       numeric(14,2) NOT NULL,
    PRIMARY KEY (plano_id, rodada),
    CONSTRAINT plano_escala_rodada_valores CHECK (
        rodada >= 1 AND vendas > 0 AND preco > 0 AND dias_esperados > 0
        AND acos >= 0 AND acos <= 1 AND parcela_ads >= 0 AND parcela_ads <= 1)
);

CREATE TABLE public.plano_escala_decisao (
    id           bigserial     PRIMARY KEY,
    plano_id     bigint        NOT NULL REFERENCES public.plano_escala (id),
    marketplace  varchar(30)   NOT NULL,
    loja         varchar(60)   NOT NULL,
    anuncio      varchar(40)   NOT NULL,
    tipo         varchar(12)   NOT NULL,
    rodada       smallint,
    motivo       text,
    criado_por   varchar(100)  NOT NULL,
    criado_em    timestamp     NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    CONSTRAINT plano_escala_decisao_tipo_valido CHECK (tipo IN (
        'cadastro', 'revisao', 'encerramento', 'concluido')),
    CONSTRAINT plano_escala_decisao_motivo CHECK (
        tipo IN ('cadastro', 'concluido') OR nullif(btrim(motivo), '') IS NOT NULL),
    CONSTRAINT plano_escala_decisao_sem_vazio CHECK (btrim(criado_por) <> '')
);

CREATE INDEX ix_plano_escala_decisao_plano ON public.plano_escala_decisao (plano_id, criado_em);

COMMENT ON TABLE public.plano_escala IS
    'Plano de escala por anúncio (1 ativo por marketplace+loja+anuncio). Entra só pelo cadastro da tela (xlsx/tabela, mesma função). Nunca apagar: encerrar.';
COMMENT ON TABLE public.plano_escala_rodada IS
    'Previsto por rodada do plano de escala, calculado no cadastro (recalculado inteiro na revisão).';
COMMENT ON TABLE public.plano_escala_decisao IS
    'Decisões do plano de escala (cadastro, revisão, encerramento, conclusão): quem, quando, motivo. Nunca apagar linha.';

DO $$
DECLARE
    n_check int;
    n_linhas int;
BEGIN
    SELECT count(*) INTO n_check FROM pg_constraint
     WHERE conrelid IN ('public.plano_escala'::regclass, 'public.plano_escala_rodada'::regclass,
                        'public.plano_escala_decisao'::regclass)
       AND contype = 'c';
    IF n_check <> 10 THEN
        RAISE EXCEPTION 'Esperava 10 CHECKs nas tabelas do plano de escala, achei %. Nada foi aplicado.', n_check;
    END IF;
    IF to_regclass('public.ux_plano_escala_ativo') IS NULL
       OR to_regclass('public.ix_plano_escala_decisao_plano') IS NULL THEN
        RAISE EXCEPTION 'Índices do plano de escala não foram criados. Nada foi aplicado.';
    END IF;
    SELECT (SELECT count(*) FROM public.plano_escala)
         + (SELECT count(*) FROM public.plano_escala_rodada)
         + (SELECT count(*) FROM public.plano_escala_decisao) INTO n_linhas;
    IF n_linhas <> 0 THEN
        RAISE EXCEPTION 'As tabelas do plano de escala deveriam nascer vazias (têm %). Nada foi aplicado.', n_linhas;
    END IF;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): deve listar as 10 CHECKs e os índices.
SELECT conrelid::regclass AS tabela, conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conrelid IN ('public.plano_escala'::regclass, 'public.plano_escala_rodada'::regclass,
                    'public.plano_escala_decisao'::regclass)
   AND contype = 'c'
 ORDER BY 1, 2;
SELECT tablename, indexname, indexdef FROM pg_indexes
 WHERE tablename IN ('plano_escala', 'plano_escala_rodada', 'plano_escala_decisao')
 ORDER BY 1, 2;
