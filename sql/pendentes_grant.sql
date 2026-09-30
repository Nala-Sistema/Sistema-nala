-- =============================================================================
-- GRANT do coletor_vendas em fact_vendas_pendentes   [VENDAS PENDENTES ML]
-- =============================================================================
-- 30/09/2026. Sai do rascunho sql/pendentes_grant_RASCUNHO.sql (frente KITS).
-- A trava do rascunho ("risco do reprocessar com origem API") foi RESOLVIDA NO
-- CODIGO, e este arquivo so' pode rodar DEPOIS dela estar no ar:
--   - app (Sistema-nala): pendente com arquivo_origem='API' NAO entra mais no
--     snapshot pela aba; a aba grava o mapeamento e marca 'Aguardando coleta'
--     (database_utils.py, commit 73df2a1 na dev);
--   - coletor (nala-coletor-ml, branch fix/pendentes-api-sku): a coleta rele o
--     pedido cujo SKU ganhou mapeamento/cadastro (teto 20 por loja por noite) e
--     sku_valido de "SEM-SKU:..." mapeado passa a valer.
-- Card do Notion: "[Vendas ML API] GRANT faltando: SKU sem cadastro nao entra
-- em fact_vendas_pendentes".
--
-- ONDE RODAR: Neon "Gestao Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago (o Mestre nao roda).
-- ANTES DE RODAR (nesta ordem):
--   1. auditor-tecnico aprovou este arquivo, o _DESFAZER e o ensaio;
--   2. sql/pendentes_grant_ENSAIO.sql rodado e com o resultado esperado
--      (confirma que o ON CONFLICT exige SELECT de coluna e que o GRANT
--      abaixo basta);
--   3. app (main) e coletor (main) JA' no ar com as correcoes acima;
--   4. fora da janela dos outros coletores (evitar 07h-11h BRT).
--
-- O QUE O GRANT DA (minimo necessario, conferido no codigo):
--   - INSERT na tabela;
--   - SELECT so' nas 3 colunas do ON CONFLICT (numero_pedido, sku,
--     loja_origem). O rascunho dizia que o ON CONFLICT DO NOTHING nao precisa
--     de SELECT; com ALVO de conflito o Postgres le' as colunas do indice, e o
--     ensaio prova isso. SELECT de COLUNA (nao da tabela) evita que o coletor
--     leia valores de venda, status etc.;
--   - USAGE na sequencia do id (fact_vendas_pendentes_id_seq).
--   Sem UPDATE e sem DELETE: quem fecha a pendente e' o app (conciliacao ao
--   abrir a aba), nao o coletor.
--
-- QUEM: coletor_vendas e' o MESMO usuario do ML e da Shopee (VENDAS_DB_URL).
--   Um GRANT serve os dois; o codigo da Shopee so' ganhou a correcao de
--   sku_valido, nenhuma logica nova.
--
-- O QUE ACONTECE NA 1a SINCRONIZACAO DEPOIS: entram de uma vez as pendentes de
--   SKU invalido desde o api_desde de cada loja (medido em 30/09: 64 itens,
--   R$ 3.362,55, quase todos "SEM-SKU:MLB..."; Shopee: zero). Esperado.
--   O coletor ja' confere todas as permissoes abaixo antes de tentar (guarda em
--   vendas_db.SQL_PODE_GRAVAR_PENDENTES): faltando qualquer uma, pula as
--   pendentes em vez de derrubar a sincronizacao.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guardas — o INSERT do coletor tem de funcionar ANTES de ganhar permissao
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    -- Exatamente as colunas de SQL_PENDENTES_INSERT em
    -- nala-coletor-ml/coletor/vendas_db.py. Reconferir no dia.
    cols_coletor text[] := ARRAY[
        'marketplace_origem', 'loja_origem', 'numero_pedido', 'data_venda', 'sku',
        'codigo_anuncio', 'quantidade', 'preco_venda', 'valor_venda_efetivo',
        'imposto', 'comissao', 'frete', 'tarifa_fixa', 'comissao_afiliados',
        'outros_custos', 'total_tarifas', 'valor_liquido', 'arquivo_origem',
        'data_processamento', 'status', 'motivo', 'logistica'];
    faltando text;
    obrigatorias text;
    tem_indice boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coletor_vendas') THEN
        RAISE EXCEPTION 'Usuario coletor_vendas nao existe. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.fact_vendas_pendentes') IS NULL THEN
        RAISE EXCEPTION 'fact_vendas_pendentes nao existe. Nada foi aplicado.';
    END IF;

    -- 1) Toda coluna que o coletor escreve existe.
    SELECT string_agg(c, ', ') INTO faltando
      FROM unnest(cols_coletor) AS c
     WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'fact_vendas_pendentes'
               AND column_name = c);
    IF faltando IS NOT NULL THEN
        RAISE EXCEPTION
'fact_vendas_pendentes nao tem a(s) coluna(s) que o coletor grava: %.
Com o GRANT, a sincronizacao do snapshot falharia toda noite. Nada foi aplicado.',
            faltando;
    END IF;

    -- 2) Nenhuma coluna NOT NULL sem padrao fica de fora do INSERT do coletor.
    SELECT string_agg(a.attname, ', ') INTO obrigatorias
      FROM pg_attribute a
     WHERE a.attrelid = 'public.fact_vendas_pendentes'::regclass
       AND a.attnum > 0 AND NOT a.attisdropped
       AND a.attnotnull AND NOT a.atthasdef
       AND a.attidentity = '' AND a.attgenerated = ''
       AND a.attname <> ALL (cols_coletor);
    IF obrigatorias IS NOT NULL THEN
        RAISE EXCEPTION
'fact_vendas_pendentes tem coluna(s) NOT NULL sem padrao que o coletor nao
preenche: %. Com o GRANT, a sincronizacao falharia. Nada foi aplicado.',
            obrigatorias;
    END IF;

    -- 3) Existe o indice unico que o ON CONFLICT (numero_pedido, sku,
    --    loja_origem) exige: sem predicado, sem expressao, exatamente essas 3.
    SELECT EXISTS (
        SELECT 1
          FROM pg_index i
         WHERE i.indrelid = 'public.fact_vendas_pendentes'::regclass
           AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL
           AND i.indnatts = 3
           AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text)
                  FROM pg_attribute a
                 WHERE a.attrelid = i.indrelid
                   AND a.attnum = ANY (i.indkey::int2[]))
               = ARRAY['loja_origem', 'numero_pedido', 'sku'])
      INTO tem_indice;
    IF NOT tem_indice THEN
        RAISE EXCEPTION
'fact_vendas_pendentes nao tem indice unico em (numero_pedido, sku, loja_origem).
O ON CONFLICT do coletor daria erro e derrubaria a sincronizacao. Nada foi aplicado.';
    END IF;

    -- 4) A sequencia do id existe com o nome que a guarda do coletor espera.
    IF to_regclass('public.fact_vendas_pendentes_id_seq') IS NULL THEN
        RAISE EXCEPTION
'Sequencia public.fact_vendas_pendentes_id_seq nao existe. A guarda do coletor
(vendas_db.SQL_PODE_GRAVAR_PENDENTES) usa esse nome. Nada foi aplicado.';
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. GRANT — o minimo
-- -----------------------------------------------------------------------------
GRANT INSERT ON public.fact_vendas_pendentes TO coletor_vendas;
GRANT SELECT (numero_pedido, sku, loja_origem)
    ON public.fact_vendas_pendentes TO coletor_vendas;
GRANT USAGE ON SEQUENCE public.fact_vendas_pendentes_id_seq TO coletor_vendas;

-- -----------------------------------------------------------------------------
-- 3. Conferencia (dentro da transacao). Esperado:
--    insert = true | select_tabela = false | select_3_colunas = true |
--    select_valor = false | update = false | delete = false | usage_seq = true
--    Se alguma coisa divergir, dar ROLLBACK em vez de COMMIT.
-- -----------------------------------------------------------------------------
SELECT has_table_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'INSERT') AS insert,
       has_table_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'SELECT') AS select_tabela,
       (has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'numero_pedido', 'SELECT')
        AND has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'sku', 'SELECT')
        AND has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'loja_origem', 'SELECT')) AS select_3_colunas,
       has_column_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'valor_venda_efetivo', 'SELECT') AS select_valor,
       has_table_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'UPDATE') AS update,
       has_table_privilege('coletor_vendas', 'public.fact_vendas_pendentes', 'DELETE') AS delete,
       has_sequence_privilege('coletor_vendas', 'public.fact_vendas_pendentes_id_seq', 'USAGE') AS usage_seq;

COMMIT;

-- =============================================================================
-- DEPOIS DO COMMIT (so' leitura), apos a proxima sincronizacao (ML e Shopee):
--   SELECT loja_origem, status, count(*), sum(valor_venda_efetivo)
--     FROM fact_vendas_pendentes
--    WHERE arquivo_origem = 'API'
--    GROUP BY loja_origem, status ORDER BY loja_origem, status;
-- E no ml_execucoes (banco do coletor), a linha da sincronizacao deixa de dizer
-- "permissao pendente" e passa a dizer "N para pendentes".
--
-- DESFAZER: sql/pendentes_grant_DESFAZER.sql.
-- =============================================================================
