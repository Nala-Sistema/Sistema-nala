-- =============================================================================
-- fact_tiktok_frete_detalhe — PESO COBRADO E REEMBOLSO DO TIKTOK, POR PEDIDO/SKU
-- =============================================================================
-- Escrito em 29/09/2026 (frente [ALARME FRETE], etapa 2). NÃO RODAR sem o
-- parecer do auditor-tecnico.
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
--
-- COMO RODAR: pelo SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez.
--             A CREDENCIAL É A DO DONO (neondb_owner) — o editor já abre
--             conectado como ele, não há connection string a digitar.
--
-- POR QUE
--   Na Shopee e no TikTok o frete da Nala é zero: todo frete cobrado é
--   penalização por peso/medida do anúncio (aba "🚚 Penalização de frete" em
--   Análise de Produtos). O relatório financeiro do TikTok traz o peso do
--   anúncio, o peso cobrado e o reembolso ao cliente, mas o upload descartava
--   essas colunas. Decisão do Thiago (29/09/2026): guardar SÓ DAQUI PARA
--   FRENTE, nos próximos uploads; nada de reenviar relatório antigo.
--
-- O QUE FAZ
--   Só CRIA a tabela. Não altera fact_vendas_snapshot nem nenhuma outra: a
--   tela liga esta tabela ao snapshot só na leitura, por
--   (loja_origem, pedido_original, codigo_anuncio = sku_tiktok).
--   Quem grava é processar_tiktok.py, num bloco separado que roda DEPOIS do
--   commit da venda: se esta tabela não existir, a venda grava igual.
--   Enquanto a tabela não existir, a aba funciona como antes.
--
-- COLUNAS (nomes do relatório "Detalhes do pedido", linhas "Pedido")
--   peso_estimado_g     "Peso estimado do pacote cobrável" — peso do ANÚNCIO (g)
--   peso_embalagem_g    "Peso da embalagem cobrável"       — peso COBRADO (g)
--   custo_liquido_frete "Custo líquido de frete", como vem  (R$, negativo = custo)
--   reembolso_produtos  "Reembolsos de produtos", em módulo (R$, > 0 = reembolsou)
--   Sem dado de comprador.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF to_regclass('public.fact_tiktok_frete_detalhe') IS NOT NULL THEN
        RAISE EXCEPTION 'fact_tiktok_frete_detalhe já existe — nada a fazer.';
    END IF;
END $$;

CREATE TABLE public.fact_tiktok_frete_detalhe (
    loja_origem         varchar(100)  NOT NULL,
    pedido_original     varchar(50)   NOT NULL,
    sku_tiktok          varchar(50)   NOT NULL,
    peso_estimado_g     numeric(12,3),
    peso_embalagem_g    numeric(12,3),
    custo_liquido_frete numeric(12,2),
    reembolso_produtos  numeric(12,2),
    arquivo_origem      varchar(255),
    gravado_em          timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (loja_origem, pedido_original, sku_tiktok)
);

COMMENT ON TABLE public.fact_tiktok_frete_detalhe IS
    'Peso do anúncio, peso cobrado e reembolso do TikTok por pedido/SKU TikTok, '
    'do relatório financeiro. Gravada por processar_tiktok.py desde 29/09/2026 '
    '(só uploads novos). Lida pela aba Penalização de frete. Ver '
    'sql/tiktok_frete_detalhe.sql.';

-- Conferência: tem de devolver 1 linha com 0 registros.
SELECT 'fact_tiktok_frete_detalhe' AS tabela, count(*) AS registros
FROM public.fact_tiktok_frete_detalhe;

COMMIT;
