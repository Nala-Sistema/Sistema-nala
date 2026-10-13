-- =============================================================================
-- RECLAMACOES E DEVOLUCOES COM MOTIVO — fact_reclamacoes + fact_reputacao_loja
-- + usuario coletor_reclamacoes   (frente [DEVOLUCOES], 12/10/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\reclamacoes_fase1.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: DONO do banco (neondb_owner), no SQL Editor.
-- ANTES DE RODAR: passar pelo auditor-tecnico (chamado pela conversa Mestre).
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\reclamacoes_fase1_DESFAZER.sql
--
-- O QUE FAZ
--   1. fact_reclamacoes: UMA tabela para reclamacao E devolucao, ML e Shopee,
--      com coluna `tipo`. Grao: caso x item do pedido. Serve ao cardapio de
--      causas do Sinais do Dia e a cobranca de fornecedor.
--   2. fact_reputacao_loja: foto DIARIA de reputacao/medalha do ML por loja.
--   3. Usuario coletor_reclamacoes (SEM LOGIN ate o passo final), com
--      SELECT/INSERT/UPDATE so nessas duas tabelas + SELECT em dim_fonte_dados
--      (traduzir nome de loja). Sem DELETE, sem venda.
--
-- O QUE NAO FAZ (de proposito)
--   - NAO toca em fact_devolucoes: ela e a copia de venda feita pelo upload
--     (sem motivo). Fonte unica: o motivo mora SO aqui.
--   - NAO mexe em receita, margem nem em nenhuma tabela de venda. A venda
--     devolvida ja sai da venda quando o dinheiro volta (Regra A); esta
--     tabela e MOTIVO e CONTAGEM, nunca subtrai nada.
--   - NAO define senha (passo final, a mao, fora do git).
--
-- E SEGURO RODAR DE NOVO: IF NOT EXISTS / CREATE OR REPLACE.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. fact_reclamacoes
--    marketplace em MAIUSCULAS ('MERCADO LIVRE', 'SHOPEE'), como no resto.
--    tipo (normalizado): devolucao | mediacao | cancelamento_comprador |
--                        cancelamento_vendedor | outro
--    tipo_origem: o texto cru da API (ML: returns/mediations/cancel_purchase/
--                 cancel_sale...; Shopee: 'return').
--    id_caso: ML = id da reclamacao (claim); Shopee = return_sn.
--    item_chave: MLB (ML) ou item_id:model_id (Shopee) do item do caso; ''
--                quando o caso nao casou com o pedido (numero_pedido NULL).
--    numero_pedido: a MESMA chave de fact_pedidos_marketplace e
--                   fact_vendas_snapshot (ML = ID DO PEDIDO, order id; Shopee =
--                   order_sn). Medido em 12/10/2026: 98,5% dos casos do ML
--                   casam. NAO e o pack id (o coletor de vendas grava o id do
--                   pedido; casar pelo pack dava 74% e falso casamento).
--                   pack_id: o pack do pedido, quando ha (so ML).
--    loja: o nome do SISTEMA, igual ao das vendas ('Shopee Lithouse(Nala)',
--          'ML-LPT'), nao o nome da integracao.
--    item_exato: FALSE quando o pedido tem mais de um item e a API nao diz
--                qual deles o caso atinge (so ML): contar por PEDIDO nesse caso.
--    valor: valor do item no pedido (ML: preco x quantidade; Shopee: reembolso
--           do item). NAO e reembolso confirmado no ML.
--    sem dado pessoal: nada de nome, e-mail, endereco nem texto livre do
--                      comprador.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_reclamacoes (
    marketplace        VARCHAR(30)   NOT NULL,
    loja               VARCHAR(60)   NOT NULL,
    id_caso            VARCHAR(40)   NOT NULL,
    item_chave         VARCHAR(60)   NOT NULL DEFAULT '',
    tipo               VARCHAR(30)   NOT NULL,
    tipo_origem        VARCHAR(40),
    status             VARCHAR(40),
    estagio            VARCHAR(40),
    motivo_codigo      VARCHAR(60),
    motivo_nome        VARCHAR(200),
    resultado          VARCHAR(80),
    beneficiado        VARCHAR(40),
    status_dinheiro    VARCHAR(40),
    numero_pedido      VARCHAR(60),
    pedido_id          VARCHAR(60),
    pack_id            VARCHAR(60),
    codigo_anuncio     VARCHAR(30),
    sku                VARCHAR(60),
    quantidade         INTEGER,
    valor              NUMERIC(14, 2),
    item_exato         BOOLEAN       NOT NULL DEFAULT TRUE,
    data_abertura      TIMESTAMP,
    data_atualizacao   TIMESTAMP,
    data_fechamento    TIMESTAMP,
    detalhe            JSONB,
    arquivo_origem     VARCHAR(30)   NOT NULL DEFAULT 'API',
    data_importacao    TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, loja, id_caso, item_chave),
    CONSTRAINT ck_reclam_marketplace_maiusculo CHECK (marketplace = UPPER(marketplace))
);

CREATE INDEX IF NOT EXISTS ix_reclam_abertura ON fact_reclamacoes (data_abertura, loja);
CREATE INDEX IF NOT EXISTS ix_reclam_pedido   ON fact_reclamacoes (loja, numero_pedido);
CREATE INDEX IF NOT EXISTS ix_reclam_anuncio  ON fact_reclamacoes (loja, codigo_anuncio);
CREATE INDEX IF NOT EXISTS ix_reclam_sku      ON fact_reclamacoes (sku);

COMMENT ON TABLE fact_reclamacoes IS
    'Reclamacoes e devolucoes COM MOTIVO, por caso x item. ML: /post-purchase/claims; '
    'Shopee: returns/get_return_list. Coletor coletar_reclamacoes (nala-coletor-ml). '
    'So motivo e contagem: NAO subtrai venda (Regra A). fact_devolucoes e outra coisa '
    '(copia de venda do upload, sem motivo) e nao recebe dado desta fonte.';

-- -----------------------------------------------------------------------------
-- 2. fact_reputacao_loja: uma foto por loja por dia (ML). A API nao tem
--    historico: o historico nasce daqui. Taxas em fracao (0,008 = 0,8%).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_reputacao_loja (
    marketplace          VARCHAR(30)  NOT NULL,
    loja                 VARCHAR(60)  NOT NULL,
    data                 DATE         NOT NULL,
    nivel                VARCHAR(30),
    medalha              VARCHAR(30),
    periodo_dias         SMALLINT,
    vendas_periodo       INTEGER,
    taxa_reclamacao      NUMERIC(8, 5),
    reclamacoes          INTEGER,
    taxa_atraso          NUMERIC(8, 5),
    atrasos              INTEGER,
    taxa_cancelamento    NUMERIC(8, 5),
    cancelamentos        INTEGER,
    detalhe              JSONB,
    data_importacao      TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (marketplace, loja, data),
    CONSTRAINT ck_reput_marketplace_maiusculo CHECK (marketplace = UPPER(marketplace))
);

COMMENT ON TABLE fact_reputacao_loja IS
    'Foto diaria da reputacao do ML por loja (nivel, medalha, taxas de reclamacao, '
    'atraso e cancelamento da janela de periodo_dias). Coletor coletar_reclamacoes.';

-- -----------------------------------------------------------------------------
-- 3. Usuario do coletor: permissao minima, SEM LOGIN, SEM DELETE.
--    (Criado por SQL, nao pelo console do Neon, que daria neon_superuser.)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coletor_reclamacoes') THEN
        CREATE ROLE coletor_reclamacoes NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                                        NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO coletor_reclamacoes;
-- SELECT junto: todo INSERT ... ON CONFLICT precisa dele nas colunas do conflito.
GRANT SELECT, INSERT, UPDATE ON fact_reclamacoes, fact_reputacao_loja
    TO coletor_reclamacoes;
-- Leitura (so SELECT) do cadastro de lojas: o coletor traduz o nome da loja da
-- integracao para o nome do sistema (dim_fonte_dados.nome_na_integracao).
GRANT SELECT ON dim_fonte_dados TO coletor_reclamacoes;

COMMIT;

-- =============================================================================
-- PASSO FINAL, A MAO (nunca commitar a senha) — credencial: DONO
--   1) gerar senha forte e definir pelo psql (nao pelo SQL Editor):
--        \password coletor_reclamacoes
--        ALTER ROLE coletor_reclamacoes WITH LOGIN;
--   2) montar a URL (host da branch production, banco neondb, sslmode=require)
--      e guardar SO no Secret RECLAMACOES_DB_URL do GitHub do repositorio
--      nala-coletor-ml (e no .env local, se for testar daqui).
--   3) testar o UPSERT de verdade como coletor_reclamacoes:
--        python coletar_reclamacoes.py --loja ML-YanniSP --dias 7
-- =============================================================================

-- =============================================================================
-- VERIFICACAO (somente leitura; credencial: DONO; rodar depois)
-- (a) papel sem superuser/RLS e sem papel herdado (esperado: sem linhas no 2o):
--     SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolcanlogin
--       FROM pg_roles WHERE rolname = 'coletor_reclamacoes';
--     SELECT r.rolname FROM pg_auth_members m
--       JOIN pg_roles r ON r.oid = m.roleid
--       JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = 'coletor_reclamacoes';
-- (b) privilegios (ESPERADO: fact_reclamacoes e fact_reputacao_loja com
--     INSERT, SELECT, UPDATE; dim_fonte_dados so SELECT; nenhuma outra tabela,
--     nenhum DELETE/TRUNCATE):
--     SELECT c.relname AS tabela,
--            string_agg(p.priv, ', ' ORDER BY p.priv) AS privilegios
--       FROM pg_class c
--       JOIN pg_namespace n ON n.oid = c.relnamespace
--      CROSS JOIN (VALUES ('SELECT'), ('INSERT'), ('UPDATE'),
--                         ('DELETE'), ('TRUNCATE')) AS p(priv)
--      WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
--        AND has_table_privilege('coletor_reclamacoes', c.oid, p.priv)
--      GROUP BY c.relname ORDER BY c.relname;
--     SELECT has_schema_privilege('coletor_reclamacoes', 'public', 'CREATE');   -- false
--     SELECT has_database_privilege('coletor_reclamacoes', 'neondb', 'CREATE'); -- false
--     -- Se (b) listar outra tabela ou DELETE/TRUNCATE, vem de PUBLIC: NAO seguir
--     -- (ver sql/fase1_revoke_public_tags.sql).
-- =============================================================================
