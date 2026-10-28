-- =============================================================================
-- USUARIO DO APP — app_nala (o Streamlit sai do neondb_owner)
-- (frente [PEQUENAS APP], 28/10/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\usuario_app.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: DONO do banco (neondb_owner), no SQL Editor.
-- ANTES DE RODAR: passar pelo auditor-tecnico.
-- ANTES DA TROCA DO SECRET: em main os commits 2eddc13 (TikTok sem CREATE) e
--   1b0c0dc (abertura sem DDL). Sem eles o upload do TikTok perde os nomes.
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\usuario_app_DESFAZER.sql
-- SENHA: depois, com C:\Users\thiag\Nala-Analises\definir_senha_app.py
--
-- O QUE FAZ
--   1. Cria app_nala SEM LOGIN ate o script de senha, sem nenhum poder especial.
--   2. Concede, TABELA POR TABELA, so o que o codigo do app usa (lista abaixo,
--      levantada no codigo em 28/10). Leitura de view basta na view: as views
--      rodam com o dono, nao precisa de GRANT nas tabelas por baixo.
--   3. USAGE nas sequencias (id automatico) das tabelas em que ele insere.
--   4. TABELA NOVA criada pelo dono ja nasce LEGIVEL para o app (DEFAULT
--      PRIVILEGES, so SELECT). ESCRITA em tabela nova continua pedindo GRANT
--      no arquivo que cria a tabela — senao a tela falha calada.
--   5. Confere tudo no fim. Se algo sair do desenho, da ERRO e desfaz tudo.
--
-- O QUE NAO FAZ (de proposito)
--   - Nenhum CREATE/ALTER/DROP: o app nao cria mais tabela (commit 1b0c0dc).
--   - NAO escreve em sinal_historico (so o job, usuario sinais_historico),
--     ads_acao (so o rodar_ciclo, usuario ciclo_ads) nem dim_estoque (upload
--     parado). So le.
--   - DELETE em sinal_ciente, ads_acao_decisao e perf_semana_marcacao: nunca.
--     sinal_ciente: UPDATE so nas 3 colunas do encerramento.
--   - Nao le tabela que o app nao usa (backups, ads_log_estrategico, ...).
--   - Sem tempo-limite nem limite de conexao: upload grande nao pode ser
--     cortado no meio. A trava e a falta de GRANT.
--   - NAO define senha (passo do script, fora do git). NAO mexe em outro usuario.
--
-- ATENCAO
--   - Dev e main usam o MESMO banco e o MESMO app_nala: a URL que entrar no
--     app de dev e a mesma que vai para o app de main. Rodar o script de senha
--     de novo troca a senha e DERRUBA quem ja usa a URL antiga.
--   - Gatilhos que gravam (kanban_historico pelo registrar_movimentacao_card)
--     rodam como app_nala: kanban_historico tem INSERT por isso.
--   - Pode criar tabela TEMPORARIA (TEMP de PUBLIC, igual aos outros usuarios).
--
-- E SEGURO RODAR DE NOVO: zera as permissoes do app_nala e concede outra vez;
-- nao mexe no LOGIN nem na senha, se ja tiver.
-- =============================================================================
BEGIN;

-- 1. Usuario (por SQL, nao pelo console do Neon, que daria neon_superuser).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_nala') THEN
        CREATE ROLE app_nala NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                             NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

-- 2. O que o app usa. Ordem dos privilegios sempre SELECT, INSERT, UPDATE,
--    DELETE (a conferencia compara o texto). A MESMA lista esta na consulta
--    de prova no fim do arquivo; tests/test_usuario_app.py confere que as
--    duas sao iguais.
CREATE TEMP TABLE app_nala_previsto (tabela text PRIMARY KEY, privs text NOT NULL)
    ON COMMIT DROP;
INSERT INTO app_nala_previsto (tabela, privs) VALUES
    -- uploads de venda
    ('fact_vendas_snapshot',        'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_vendas_pendentes',       'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_vendas_descartadas',     'SELECT, INSERT'),
    ('fact_devolucoes',             'SELECT, INSERT, DELETE'),
    ('log_uploads',                 'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_tiktok_frete_detalhe',   'SELECT, INSERT, UPDATE'),
    ('dim_tiktok_skus',             'SELECT, INSERT, UPDATE'),
    ('fact_estoque_mensal',         'SELECT, INSERT, DELETE'),
    -- ads da Shopee (upload) e casamento de titulo
    ('fact_ads_shopee',             'SELECT, INSERT, UPDATE'),
    ('log_uploads_ads',             'SELECT, INSERT'),
    ('dim_ads_produto_sku',         'SELECT, INSERT, UPDATE'),
    ('dim_ads_titulo_link',         'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_titulo_sku_shopee',       'SELECT, INSERT, UPDATE'),
    -- cadastros
    ('dim_produtos',                'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_produtos_custos',         'SELECT, INSERT, UPDATE, DELETE'),
    ('fato_compras',                'SELECT, INSERT, DELETE'),
    ('dim_sku_mapeamento',          'SELECT, INSERT, UPDATE'),
    ('dim_kit_composicao',          'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_kit_composicao_pendente', 'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_precos_marketplace',      'SELECT, INSERT, UPDATE'),
    ('dim_tags_anuncio',            'SELECT, INSERT, UPDATE'),
    ('dim_tags_opcoes',             'SELECT, INSERT, UPDATE'),
    ('dim_tags_sku',                'SELECT, INSERT, UPDATE'),
    ('dim_metas_loja',              'SELECT, INSERT, UPDATE'),
    ('dim_metas_anuncio',           'SELECT, INSERT, UPDATE'),
    -- configuracoes e login (o login le password_hash)
    ('dim_lojas',                   'SELECT, INSERT, UPDATE'),
    ('dim_usuarios',                'SELECT, INSERT, UPDATE'),
    ('dim_usuario_lojas',           'SELECT, INSERT, DELETE'),
    ('dim_config_marketplace',      'SELECT, INSERT, DELETE'),
    ('dim_frete_ml',                'SELECT, UPDATE'),
    ('dim_frete_amazon',            'SELECT, UPDATE'),
    -- telas de decisao
    ('sinal_ciente',                'SELECT, INSERT, UPDATE(como_encerrou, encerrado_em, encerrado_por)'),
    ('ads_acao_decisao',            'SELECT, INSERT'),
    ('perf_semana_marcacao',        'SELECT, INSERT'),
    ('full_proxima_data_envio',     'SELECT, INSERT'),
    ('plano_escala',                'SELECT, INSERT, UPDATE'),
    ('plano_escala_rodada',         'SELECT, INSERT, DELETE'),
    ('plano_escala_decisao',        'SELECT, INSERT'),
    -- kanban
    ('kanban_quadros',              'SELECT, INSERT'),
    ('kanban_colunas',              'SELECT, INSERT'),
    ('kanban_cards',                'SELECT, INSERT, UPDATE'),
    ('kanban_checklist',            'SELECT, INSERT, UPDATE'),
    ('kanban_comentarios',          'SELECT, INSERT'),
    ('kanban_historico',            'SELECT, INSERT'),
    -- so leitura (inclui as 3 que NUNCA podem ser escritas pelo app)
    ('sinal_historico',             'SELECT'),
    ('ads_acao',                    'SELECT'),
    ('dim_estoque',                 'SELECT'),
    ('v_ads_acao_vigente',          'SELECT'),
    ('dim_estoque_anuncio',         'SELECT'),
    ('dim_fonte_dados',             'SELECT'),
    ('dim_loja_responsavel_ads',    'SELECT'),
    ('dim_skus',                    'SELECT'),
    ('fact_ads_campanha_config',    'SELECT'),
    ('fact_ads_diario_loja',        'SELECT'),
    ('fact_ads_performance',        'SELECT'),
    ('fact_custos_extras',          'SELECT'),
    ('fact_estoque_diario',         'SELECT'),
    ('fact_pedidos_itens_marketplace', 'SELECT'),
    ('fact_pedidos_marketplace',    'SELECT'),
    ('fact_reclamacoes',            'SELECT'),
    ('fact_saude_anuncio',          'SELECT'),
    ('v_cobertura_full',            'SELECT'),
    ('v_estoque_envio_manual',      'SELECT'),
    ('v_movimento_full_diario',     'SELECT'),
    ('vw_views_shopee_30d',         'SELECT'),
    ('vw_visitas_dia',              'SELECT');

-- 3. Zera e concede de novo. REVOKE ALL na tabela tira tambem permissao por
--    coluna.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM app_nala;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM app_nala;
REVOKE ALL ON SCHEMA public FROM app_nala;
ALTER DEFAULT PRIVILEGES FOR ROLE neondb_owner IN SCHEMA public
    REVOKE ALL ON TABLES FROM app_nala;
GRANT USAGE ON SCHEMA public TO app_nala;

DO $$
DECLARE
    p   record;
    seq record;
BEGIN
    FOR p IN SELECT tabela, privs FROM app_nala_previsto ORDER BY tabela LOOP
        IF to_regclass('public.' || quote_ident(p.tabela)) IS NULL THEN
            RAISE EXCEPTION 'Tabela/view % nao existe. Nada foi gravado.', p.tabela;
        END IF;
        EXECUTE format('GRANT %s ON public.%I TO app_nala', p.privs, p.tabela);
        -- sequencia do id (SERIAL ou IDENTITY) de quem recebe INSERT
        IF p.privs LIKE '%INSERT%' THEN
            FOR seq IN
                SELECT s.relname
                  FROM pg_depend d
                  JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
                 WHERE d.refobjid = ('public.' || quote_ident(p.tabela))::regclass
                   AND d.deptype IN ('a', 'i')
            LOOP
                EXECUTE format('GRANT USAGE ON SEQUENCE public.%I TO app_nala', seq.relname);
            END LOOP;
        END IF;
    END LOOP;
END
$$;

-- 4. Tabela nova do dono: so leitura automatica. Escrita: GRANT no arquivo.
ALTER DEFAULT PRIVILEGES FOR ROLE neondb_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO app_nala;

-- 5. Conferencia. Qualquer desvio => ERRO => nada do arquivo fica gravado.
DO $$
DECLARE
    r        text := 'app_nala';
    problema text;
    n        int;
BEGIN
    -- (a) nenhum poder especial, nenhum grupo herdado
    SELECT string_agg(x, ', ') INTO problema FROM (
        SELECT 'superuser'  AS x FROM pg_roles WHERE rolname = r AND rolsuper
        UNION ALL SELECT 'createrole'  FROM pg_roles WHERE rolname = r AND rolcreaterole
        UNION ALL SELECT 'createdb'    FROM pg_roles WHERE rolname = r AND rolcreatedb
        UNION ALL SELECT 'replication' FROM pg_roles WHERE rolname = r AND rolreplication
        UNION ALL SELECT 'bypassrls'   FROM pg_roles WHERE rolname = r AND rolbypassrls
        UNION ALL SELECT 'membro de ' || g.rolname
                    FROM pg_auth_members m
                    JOIN pg_roles g ON g.oid = m.roleid
                    JOIN pg_roles u ON u.oid = m.member
                   WHERE u.rolname = r
    ) s;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION '% tem poder a mais: %. Nada foi gravado.', r, problema;
    END IF;

    -- (b) sem CREATE no schema nem no banco
    IF has_schema_privilege(r, 'public', 'CREATE')
       OR has_database_privilege(r, current_database(), 'CREATE') THEN
        RAISE EXCEPTION '% pode criar objetos. Nada foi gravado.', r;
    END IF;

    -- (c) cada tabela/view do public: o que ele tem = o previsto (fora da
    --     lista = nada). Mesmo calculo da consulta de prova no fim.
    WITH objs AS MATERIALIZED (
        SELECT c.oid, c.relname FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')),
    tem_hoje AS (
        SELECT o.relname,
               concat_ws(', ',
                   CASE WHEN has_table_privilege(r, o.oid, 'SELECT') THEN 'SELECT' END,
                   CASE WHEN has_table_privilege(r, o.oid, 'INSERT') THEN 'INSERT' END,
                   CASE WHEN has_table_privilege(r, o.oid, 'UPDATE') THEN 'UPDATE'
                        WHEN has_any_column_privilege(r, o.oid, 'UPDATE') THEN
                            'UPDATE(' || (SELECT string_agg(a.attname, ', ' ORDER BY a.attname)
                                            FROM pg_attribute a
                                           WHERE a.attrelid = o.oid AND a.attnum > 0
                                             AND NOT a.attisdropped
                                             AND has_column_privilege(r, o.oid, a.attnum, 'UPDATE')) || ')'
                   END,
                   CASE WHEN has_table_privilege(r, o.oid, 'DELETE') THEN 'DELETE' END,
                   CASE WHEN has_table_privilege(r, o.oid, 'TRUNCATE') THEN 'TRUNCATE' END,
                   CASE WHEN has_table_privilege(r, o.oid, 'REFERENCES')
                          OR has_any_column_privilege(r, o.oid, 'REFERENCES') THEN 'REFERENCES' END,
                   CASE WHEN has_table_privilege(r, o.oid, 'TRIGGER') THEN 'TRIGGER' END,
                   CASE WHEN NOT has_table_privilege(r, o.oid, 'SELECT')
                         AND has_any_column_privilege(r, o.oid, 'SELECT') THEN 'SELECT(colunas)' END,
                   CASE WHEN NOT has_table_privilege(r, o.oid, 'INSERT')
                         AND has_any_column_privilege(r, o.oid, 'INSERT') THEN 'INSERT(colunas)' END
               ) AS privs
          FROM objs o)
    SELECT string_agg(format('%s: tem [%s], previsto [%s]', re.relname,
                             nullif(re.privs, ''), p.privs), ' | '),
           count(*)
      INTO problema, n
      FROM tem_hoje re
      LEFT JOIN app_nala_previsto p ON p.tabela = re.relname
     WHERE coalesce(nullif(re.privs, ''), '-') <> coalesce(p.privs, '-');
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION 'Permissoes fora do desenho (%): %. Nada foi gravado.', n, problema;
    END IF;

    -- (d) as tres que o app NUNCA escreve (redundante com (c), de proposito)
    SELECT string_agg(t, ', ') INTO problema
      FROM unnest(ARRAY['sinal_historico', 'ads_acao', 'dim_estoque']) t
     WHERE has_table_privilege(r, 'public.' || t, 'INSERT, UPDATE, DELETE, TRUNCATE')
        OR has_any_column_privilege(r, 'public.' || t, 'INSERT, UPDATE');
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION '% escreve em %. Nada foi gravado.', r, problema;
    END IF;

    -- (e) sequencias: USAGE so nas das tabelas com INSERT; nunca UPDATE (setval)
    SELECT string_agg(s.relname, ', ') INTO problema
      FROM pg_class s
     WHERE s.relnamespace = 'public'::regnamespace AND s.relkind = 'S'
       AND CASE WHEN s.relkind = 'S' THEN
                has_sequence_privilege(r, s.oid, 'UPDATE')
                OR (has_sequence_privilege(r, s.oid, 'USAGE') <> EXISTS (
                        SELECT 1 FROM pg_depend d
                          JOIN pg_class t ON t.oid = d.refobjid
                          JOIN app_nala_previsto p ON p.tabela = t.relname
                         WHERE d.objid = s.oid AND d.deptype IN ('a', 'i')
                           AND t.relnamespace = 'public'::regnamespace
                           AND p.privs LIKE '%INSERT%'))
                ELSE false END;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION 'Sequencia fora do desenho: %. Nada foi gravado.', problema;
    END IF;

    -- (f) nenhuma das funcoes que gravam custo do Full
    SELECT string_agg(p.proname, ', ') INTO problema
      FROM pg_proc p
     WHERE p.pronamespace = 'public'::regnamespace
       AND CASE WHEN p.proname LIKE 'gravar\_custo\_full\_%'
                THEN has_function_privilege(r, p.oid, 'EXECUTE')
                ELSE false END;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION '% executa funcao que grava: %. Nada foi gravado.', r, problema;
    END IF;

    SELECT count(*) INTO n FROM app_nala_previsto;
    RAISE NOTICE 'OK: app_nala com exatamente o previsto em % tabelas/views; nada nas demais.', n;
END
$$;

COMMIT;

-- =============================================================================
-- CONSULTA DE PROVA (somente leitura; credencial: DONO). Roda depois do COMMIT
-- e a qualquer momento depois (ex.: antes da troca do secret do main).
-- Uma linha por tabela da lista + qualquer tabela em que o app_nala tenha algo
-- sem estar na lista. ESPERADO: todas as linhas com confere = 'OK', e as
-- linhas de sinal_historico, ads_acao e dim_estoque com tem = 'SELECT'.
-- Tabela nova criada depois aparece como 'SO LEITURA AUTOMATICA' (SELECT pela
-- regra do item 4): se o app precisar ESCREVER nela, falta GRANT.
-- =============================================================================
WITH previsto (tabela, privs) AS (VALUES
    ('fact_vendas_snapshot',        'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_vendas_pendentes',       'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_vendas_descartadas',     'SELECT, INSERT'),
    ('fact_devolucoes',             'SELECT, INSERT, DELETE'),
    ('log_uploads',                 'SELECT, INSERT, UPDATE, DELETE'),
    ('fact_tiktok_frete_detalhe',   'SELECT, INSERT, UPDATE'),
    ('dim_tiktok_skus',             'SELECT, INSERT, UPDATE'),
    ('fact_estoque_mensal',         'SELECT, INSERT, DELETE'),
    ('fact_ads_shopee',             'SELECT, INSERT, UPDATE'),
    ('log_uploads_ads',             'SELECT, INSERT'),
    ('dim_ads_produto_sku',         'SELECT, INSERT, UPDATE'),
    ('dim_ads_titulo_link',         'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_titulo_sku_shopee',       'SELECT, INSERT, UPDATE'),
    ('dim_produtos',                'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_produtos_custos',         'SELECT, INSERT, UPDATE, DELETE'),
    ('fato_compras',                'SELECT, INSERT, DELETE'),
    ('dim_sku_mapeamento',          'SELECT, INSERT, UPDATE'),
    ('dim_kit_composicao',          'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_kit_composicao_pendente', 'SELECT, INSERT, UPDATE, DELETE'),
    ('dim_precos_marketplace',      'SELECT, INSERT, UPDATE'),
    ('dim_tags_anuncio',            'SELECT, INSERT, UPDATE'),
    ('dim_tags_opcoes',             'SELECT, INSERT, UPDATE'),
    ('dim_tags_sku',                'SELECT, INSERT, UPDATE'),
    ('dim_metas_loja',              'SELECT, INSERT, UPDATE'),
    ('dim_metas_anuncio',           'SELECT, INSERT, UPDATE'),
    ('dim_lojas',                   'SELECT, INSERT, UPDATE'),
    ('dim_usuarios',                'SELECT, INSERT, UPDATE'),
    ('dim_usuario_lojas',           'SELECT, INSERT, DELETE'),
    ('dim_config_marketplace',      'SELECT, INSERT, DELETE'),
    ('dim_frete_ml',                'SELECT, UPDATE'),
    ('dim_frete_amazon',            'SELECT, UPDATE'),
    ('sinal_ciente',                'SELECT, INSERT, UPDATE(como_encerrou, encerrado_em, encerrado_por)'),
    ('ads_acao_decisao',            'SELECT, INSERT'),
    ('perf_semana_marcacao',        'SELECT, INSERT'),
    ('full_proxima_data_envio',     'SELECT, INSERT'),
    ('plano_escala',                'SELECT, INSERT, UPDATE'),
    ('plano_escala_rodada',         'SELECT, INSERT, DELETE'),
    ('plano_escala_decisao',        'SELECT, INSERT'),
    ('kanban_quadros',              'SELECT, INSERT'),
    ('kanban_colunas',              'SELECT, INSERT'),
    ('kanban_cards',                'SELECT, INSERT, UPDATE'),
    ('kanban_checklist',            'SELECT, INSERT, UPDATE'),
    ('kanban_comentarios',          'SELECT, INSERT'),
    ('kanban_historico',            'SELECT, INSERT'),
    ('sinal_historico',             'SELECT'),
    ('ads_acao',                    'SELECT'),
    ('dim_estoque',                 'SELECT'),
    ('v_ads_acao_vigente',          'SELECT'),
    ('dim_estoque_anuncio',         'SELECT'),
    ('dim_fonte_dados',             'SELECT'),
    ('dim_loja_responsavel_ads',    'SELECT'),
    ('dim_skus',                    'SELECT'),
    ('fact_ads_campanha_config',    'SELECT'),
    ('fact_ads_diario_loja',        'SELECT'),
    ('fact_ads_performance',        'SELECT'),
    ('fact_custos_extras',          'SELECT'),
    ('fact_estoque_diario',         'SELECT'),
    ('fact_pedidos_itens_marketplace', 'SELECT'),
    ('fact_pedidos_marketplace',    'SELECT'),
    ('fact_reclamacoes',            'SELECT'),
    ('fact_saude_anuncio',          'SELECT'),
    ('v_cobertura_full',            'SELECT'),
    ('v_estoque_envio_manual',      'SELECT'),
    ('v_movimento_full_diario',     'SELECT'),
    ('vw_views_shopee_30d',         'SELECT'),
    ('vw_visitas_dia',              'SELECT')
),
objs AS MATERIALIZED (
    SELECT c.oid, c.relname FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
),
tem_hoje AS (
    SELECT o.relname,
           nullif(concat_ws(', ',
               CASE WHEN has_table_privilege('app_nala', o.oid, 'SELECT') THEN 'SELECT' END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'INSERT') THEN 'INSERT' END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'UPDATE') THEN 'UPDATE'
                    WHEN has_any_column_privilege('app_nala', o.oid, 'UPDATE') THEN
                        'UPDATE(' || (SELECT string_agg(a.attname, ', ' ORDER BY a.attname)
                                        FROM pg_attribute a
                                       WHERE a.attrelid = o.oid AND a.attnum > 0
                                         AND NOT a.attisdropped
                                         AND has_column_privilege('app_nala', o.oid, a.attnum, 'UPDATE')) || ')'
               END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'DELETE') THEN 'DELETE' END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'TRUNCATE') THEN 'TRUNCATE' END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'REFERENCES')
                      OR has_any_column_privilege('app_nala', o.oid, 'REFERENCES') THEN 'REFERENCES' END,
               CASE WHEN has_table_privilege('app_nala', o.oid, 'TRIGGER') THEN 'TRIGGER' END,
               CASE WHEN NOT has_table_privilege('app_nala', o.oid, 'SELECT')
                     AND has_any_column_privilege('app_nala', o.oid, 'SELECT') THEN 'SELECT(colunas)' END,
               CASE WHEN NOT has_table_privilege('app_nala', o.oid, 'INSERT')
                     AND has_any_column_privilege('app_nala', o.oid, 'INSERT') THEN 'INSERT(colunas)' END
           ), '') AS privs
      FROM objs o
)
SELECT coalesce(p.tabela, re.relname) AS tabela,
       p.privs                        AS previsto,
       coalesce(re.privs, '(nao existe)') AS tem,
       CASE WHEN re.relname IS NULL THEN 'FALTA A TABELA'
            WHEN p.tabela IS NULL AND re.privs = 'SELECT' THEN 'SO LEITURA AUTOMATICA'
            WHEN coalesce(re.privs, '-') = coalesce(p.privs, '-') THEN 'OK'
            ELSE 'ERRADO' END          AS confere
  FROM previsto p
  FULL JOIN tem_hoje re ON re.relname = p.tabela
 WHERE p.tabela IS NOT NULL OR re.privs IS NOT NULL
 ORDER BY CASE WHEN p.tabela IS NOT NULL AND re.relname IS NOT NULL
                AND coalesce(re.privs, '-') = coalesce(p.privs, '-') THEN 1 ELSE 0 END,
          CASE WHEN p.tabela IN ('sinal_historico', 'ads_acao', 'dim_estoque') THEN 0 ELSE 1 END,
          1;
