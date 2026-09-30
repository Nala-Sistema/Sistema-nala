-- =============================================================================
-- ENSAIO do GRANT de fact_vendas_pendentes   [VENDAS PENDENTES ML]
-- =============================================================================
-- PERGUNTA QUE RESPONDE: o INSERT ... ON CONFLICT (numero_pedido, sku,
-- loja_origem) DO NOTHING do coletor exige SELECT? E o GRANT de pendentes_grant.sql
-- (INSERT + SELECT so' nas 3 colunas + USAGE na sequencia) basta?
--
-- COMO E' SEGURO: roda num unico bloco DO que cria um usuario e uma tabela
-- de MENTIRA (ensaio_pend_*), testa, e termina SEMPRE com RAISE EXCEPTION —
-- o erro desfaz tudo (CREATE ROLE e CREATE TABLE sao transacionais no
-- Postgres). Nada fica gravado; nenhuma tabela real e' lida ou escrita alem
-- do catalogo (LIKE copia a estrutura de fact_vendas_pendentes; ids manuais,
-- sem tocar a sequencia real). O resultado vem NA MENSAGEM DO ERRO.
--
-- ONDE RODAR: Neon "Gestao Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner), antes de pendentes_grant.sql.
--
-- RESULTADO ESPERADO (a mensagem deve conter exatamente isto):
--   A_so_insert=NEGADO | B_insert_mais_select_3_colunas=OK | C_le_valor_venda=NEGADO
-- Se A vier OK, o ON CONFLICT NAO exige SELECT: tirar a linha GRANT SELECT
-- (...) de pendentes_grant.sql (e do _DESFAZER) e as 3 has_column_privilege da
-- guarda em coletor/vendas_db.py. Se B vier NEGADO, o GRANT nao basta: NAO
-- aplicar e voltar para o Mestre.
-- =============================================================================

DO $$
DECLARE
    r text := '';
    sql_ins text := $q$
        INSERT INTO ensaio_pend_t (
            id, marketplace_origem, loja_origem, numero_pedido, data_venda, sku,
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo,
            imposto, comissao, frete, tarifa_fixa, comissao_afiliados, outros_custos,
            total_tarifas, valor_liquido, arquivo_origem, data_processamento,
            status, motivo, logistica)
        VALUES (%s, 'MERCADO LIVRE', 'ENSAIO', 'ENSAIO-1', current_date, 'S1',
                'MLB1', 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 'API', now(),
                'Pendente', 'ensaio', NULL)
        ON CONFLICT (numero_pedido, sku, loja_origem) DO NOTHING$q$;
BEGIN
    CREATE ROLE ensaio_pend_usr NOLOGIN;
    CREATE TABLE ensaio_pend_t
        (LIKE public.fact_vendas_pendentes INCLUDING INDEXES EXCLUDING DEFAULTS);
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
         WHERE tablename = 'ensaio_pend_t' AND indexdef ILIKE '%UNIQUE%'
           AND indexdef ILIKE '%numero_pedido%' AND indexdef ILIKE '%loja_origem%') THEN
        RAISE EXCEPTION 'ensaio invalido: o indice unico nao foi copiado';
    END IF;
    GRANT USAGE ON SCHEMA public TO ensaio_pend_usr;
    -- A) so' INSERT
    GRANT INSERT ON ensaio_pend_t TO ensaio_pend_usr;
    SET LOCAL ROLE ensaio_pend_usr;
    BEGIN
        EXECUTE format(sql_ins, 1);
        r := r || 'A_so_insert=OK | ';
    EXCEPTION WHEN insufficient_privilege THEN
        r := r || 'A_so_insert=NEGADO | ';
    END;
    RESET ROLE;

    -- B) INSERT + SELECT so' nas 3 colunas da chave (o GRANT proposto)
    GRANT SELECT (numero_pedido, sku, loja_origem) ON ensaio_pend_t TO ensaio_pend_usr;
    SET LOCAL ROLE ensaio_pend_usr;
    BEGIN
        EXECUTE format(sql_ins, 2);
        EXECUTE format(sql_ins, 2);   -- repetida: o ON CONFLICT tem de engolir
        r := r || 'B_insert_mais_select_3_colunas=OK | ';
    EXCEPTION WHEN insufficient_privilege THEN
        r := r || 'B_insert_mais_select_3_colunas=NEGADO | ';
    END;

    -- C) o usuario NAO consegue ler valores de venda
    BEGIN
        EXECUTE 'SELECT valor_venda_efetivo FROM ensaio_pend_t';
        r := r || 'C_le_valor_venda=OK | ';
    EXCEPTION WHEN insufficient_privilege THEN
        r := r || 'C_le_valor_venda=NEGADO | ';
    END;
    RESET ROLE;

    -- (A sequencia do id nao e' testada aqui: USAGE nela e' necessario porque o
    -- id e' SERIAL, fato ja' visto no B2 da troca de fonte do snapshot, e a
    -- guarda do coletor confere has_sequence_privilege.)
    r := rtrim(r, ' |');

    RAISE EXCEPTION 'ENSAIO CONCLUIDO (tudo desfeito, nada gravado): %', r;
END $$;
