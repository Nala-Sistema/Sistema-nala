-- =============================================================================
-- KITS: dim_kit_composicao + dim_kit_composicao_pendente
-- =============================================================================
-- Escrito em 30/09/2026 (frente [KITS], 1ª entrega). NÃO RODAR sem o parecer
-- do auditor-tecnico.
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
--
-- COMO RODAR: pelo SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez e
--             executando de uma vez só. A CREDENCIAL É A DO DONO
--             (neondb_owner) — o editor já abre conectado como ele, então não
--             há connection string a digitar.
--             Tudo roda numa transação só: se a guarda falhar, NADA é
--             aplicado. É seguro rodar de novo: a guarda recusa com
--             "já existe" e nada muda.
--
-- DESFAZER: sql/kits_composicao_DESFAZER.sql
--
-- POR QUE
--   41% da receita é SKU de kit, e o sistema não sabe que K2-L-0320 é 2×
--   L-0320. Sem isso não há venda, estoque nem cobertura em unidade de PEÇA.
--   Decisão do Thiago (30/09/2026): a FONTE ÚNICA da composição é o export
--   de kits do UpSeller (colunas KIT SKU / SKU de Produto / Qtd. SKU de
--   Produto). A "Tabela de preço oficial 2026" fica fora.
--
-- O QUE FAZ
--   Só CRIA duas tabelas. Não altera fact_vendas_snapshot, custo, margem nem
--   nenhuma view. Nenhuma tela lê estas tabelas ainda. Quem vai gravar é a
--   carga do export do UpSeller (Gestão de SKUs, a fazer em dev depois deste
--   SQL):
--     - carga INCREMENTAL com prévia: kit novo entra, composição alterada é
--       atualizada (a linha de peça que saiu do kit é apagada), kit ausente
--       no arquivo NÃO é apagado, só listado;
--     - SKU gravado exatamente como no cadastro (dim_produtos), sem regra de
--       tradução;
--     - kit ou peça sem cadastro vai para dim_kit_composicao_pendente, e o
--       kit inteiro espera lá (não entra pela metade).
--
-- POR QUE NÃO HÁ FOREIGN KEY PARA dim_produtos
--   Uma FK faria a exclusão de SKU em Gestão de SKUs quebrar quando o SKU
--   estiver num kit (mudança de comportamento em outra tela), e exigiria
--   unicidade em dim_produtos.sku, que este arquivo não confere. A validação
--   do cadastro é da carga, e o que não passa vai para a tabela de pendências.
--
-- REGRAS QUE FICAM NA CARGA (não no banco), e que o teste tem de cobrir
--   - kit dentro de kit é recusado (hoje: nenhum no export de 18/09);
--   - composição sempre do arquivo, nunca do nome (K3-L-0359 = 3× L-0358).
--
-- USUÁRIOS QUE NÃO RECEBEM GRANT AQUI, E POR QUÊ
--   - App (Streamlit): hoje conecta como neondb_owner, que é o dono das
--     tabelas novas; não precisa de GRANT. O usuário de permissão mínima do
--     app ainda não existe (card "Usuário de permissão mínima para o app").
--     Quando existir, rodar o bloco comentado no fim deste arquivo.
--   - Usuário de teste (NALA_TEST_DB_URL): o teste cria tabelas TEMPORÁRIAS
--     com os mesmos nomes das reais, confere que resolvem para pg_temp e
--     termina em ROLLBACK (padrão de tests/test_penalizacao_frete.py). Ele
--     não lê nem escreve tabela real, então não precisa de GRANT nelas.
--
-- FORA DESTE ARQUIVO
--   O GRANT do coletor_vendas em fact_vendas_pendentes saiu daqui (decisão do
--   Mestre, 30/09/2026) e está em sql/pendentes_grant_RASCUNHO.sql, para a
--   frente [VENDAS PENDENTES ML].
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guardas
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.dim_kit_composicao') IS NOT NULL
       OR to_regclass('public.dim_kit_composicao_pendente') IS NOT NULL THEN
        RAISE EXCEPTION
'dim_kit_composicao ou dim_kit_composicao_pendente ja existe. Este arquivo ja
rodou (ou foi rodado pela metade a mao). Nada foi aplicado.';
    END IF;
    IF to_regclass('public.dim_produtos') IS NULL THEN
        RAISE EXCEPTION 'dim_produtos nao existe. Banco errado? Nada foi aplicado.';
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. dim_kit_composicao — uma linha por (kit, peça)
-- -----------------------------------------------------------------------------
CREATE TABLE public.dim_kit_composicao (
    kit_sku         text        NOT NULL,
    peca_sku        text        NOT NULL,
    quantidade      integer     NOT NULL,
    arquivo_origem  text        NOT NULL,
    usuario         text,
    carregado_em    timestamptz NOT NULL DEFAULT now(),
    atualizado_em   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kit_sku, peca_sku),
    CONSTRAINT kit_composicao_qtd_positiva CHECK (quantidade > 0),
    CONSTRAINT kit_composicao_kit_nao_e_peca CHECK (kit_sku <> peca_sku),
    -- Excel costuma trazer espaço nas pontas; SKU com espaço no MEIO existe
    -- no cadastro (ex.: 'LBR-050808 CX') e continua permitido.
    CONSTRAINT kit_composicao_sku_sem_espaco_nas_pontas CHECK (
        kit_sku = btrim(kit_sku) AND kit_sku <> ''
        AND peca_sku = btrim(peca_sku) AND peca_sku <> '')
);

-- A PK já atende a leitura por kit (venda JOIN composição ON kit_sku).
-- Este atende a leitura por peça (cobertura e venda em peça, 2ª e 3ª entregas).
CREATE INDEX dim_kit_composicao_peca_idx ON public.dim_kit_composicao (peca_sku);

COMMENT ON TABLE public.dim_kit_composicao IS
    'Composição dos kits: kit_sku = quantidade x peca_sku. FONTE ÚNICA: export '
    'de kits do UpSeller (decisão do Thiago, 30/09/2026), carga incremental '
    'com prévia; kit ausente no arquivo não é apagado. Só SKU cadastrado em '
    'dim_produtos; o resto vai para dim_kit_composicao_pendente. Não mexe em '
    'custo nem margem. Ver sql/kits_composicao.sql.';

-- -----------------------------------------------------------------------------
-- 3. dim_kit_composicao_pendente — linhas do arquivo que esperam cadastro
-- -----------------------------------------------------------------------------
-- Uma linha por (kit, peça) do arquivo. Se UMA peça do kit está sem cadastro,
-- TODAS as linhas do kit ficam aqui (as cadastradas com as duas flags true:
-- "aguardando outra peça do kit"). Resolvido o cadastro, a tela reprocessa:
-- move o kit inteiro para dim_kit_composicao e apaga daqui.
CREATE TABLE public.dim_kit_composicao_pendente (
    kit_sku          text        NOT NULL,
    peca_sku         text        NOT NULL,
    quantidade       integer     NOT NULL,
    kit_cadastrado   boolean     NOT NULL,
    peca_cadastrada  boolean     NOT NULL,
    arquivo_origem   text        NOT NULL,
    usuario          text,
    registrado_em    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kit_sku, peca_sku),
    CONSTRAINT kit_pendente_qtd_positiva CHECK (quantidade > 0),
    CONSTRAINT kit_pendente_kit_nao_e_peca CHECK (kit_sku <> peca_sku),
    CONSTRAINT kit_pendente_sku_sem_espaco_nas_pontas CHECK (
        kit_sku = btrim(kit_sku) AND kit_sku <> ''
        AND peca_sku = btrim(peca_sku) AND peca_sku <> '')
);

COMMENT ON TABLE public.dim_kit_composicao_pendente IS
    'Linhas do export de kits do UpSeller que não entraram em '
    'dim_kit_composicao porque o kit ou alguma peça do kit não tem cadastro '
    'em dim_produtos. O kit inteiro espera aqui. Ver sql/kits_composicao.sql.';

-- Ninguém além do dono lê ou grava as tabelas novas (nem por PUBLIC).
REVOKE ALL ON public.dim_kit_composicao          FROM PUBLIC;
REVOKE ALL ON public.dim_kit_composicao_pendente FROM PUBLIC;

-- -----------------------------------------------------------------------------
-- 4. CONFERÊNCIA (dentro da transação; olhar antes do COMMIT aparecer)
-- -----------------------------------------------------------------------------
-- 4a. As duas tabelas existem, vazias. Esperado: 2 linhas, registros = 0.
SELECT 'dim_kit_composicao' AS tabela, count(*) AS registros FROM public.dim_kit_composicao
UNION ALL
SELECT 'dim_kit_composicao_pendente', count(*) FROM public.dim_kit_composicao_pendente;

-- 4b. Quem tem permissão nas tabelas novas. Esperado: só neondb_owner.
SELECT table_name, grantee, string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privilegios
  FROM information_schema.role_table_grants
 WHERE table_schema = 'public'
   AND table_name IN ('dim_kit_composicao', 'dim_kit_composicao_pendente')
 GROUP BY table_name, grantee
 ORDER BY table_name, grantee;

COMMIT;

-- =============================================================================
-- FUTURO — QUANDO O APP SAIR DO neondb_owner (NÃO RODAR AGORA)
-- =============================================================================
-- Trocar <usuario_app> pelo nome do usuário de permissão mínima do app, que
-- ainda não existe (card "Usuário de permissão mínima para o app"). DELETE em
-- dim_kit_composicao é necessário: quando a composição muda, a peça que saiu
-- do kit é apagada. DELETE na pendente: o reprocessar move e apaga.
--
--   GRANT SELECT, INSERT, UPDATE, DELETE ON public.dim_kit_composicao          TO <usuario_app>;
--   GRANT SELECT, INSERT, UPDATE, DELETE ON public.dim_kit_composicao_pendente TO <usuario_app>;
-- =============================================================================
