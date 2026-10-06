-- =============================================================================
-- DESFAZER de sql/v_cobertura_full_so_ml.sql — volta a view ao texto de antes
-- =============================================================================
-- Escrito em 06/10/2026 (frente [DADOS SHOPEE]).
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: a do DONO (neondb_owner). Pelo SQL EDITOR DO NEON, que já abre
--             conectado como ele: não há connection string a digitar.
-- COMO RODAR: colar o arquivo INTEIRO de uma vez e executar de uma vez só. As
--             guardas são blocos DO $$ com ';' dentro, e a conferência do fim
--             depende da tabela temporária criada no começo, na MESMA transação.
--             ANTES DE EXECUTAR: anotar a hora e o minuto (ponto de
--             restauração; o projeto guarda 7 dias de histórico).
--
-- ATENÇÃO À ORDEM: sem a trava, linha da Shopee em fact_estoque_diario aparece
-- na "Cobertura do Full" SEM VENDA. Se o tratamento da Shopee já gravou,
-- desfazer ANTES o lado do coletor (nala-coletor-ml,
-- sql/foto_shopee_fase1_DESFAZER.sql, que apaga as linhas Shopee), ou aceitar
-- conscientemente a Shopee na tela sem venda.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) = '89d171b4d57cc0e986a529a328ecb641' THEN
        RAISE EXCEPTION 'A view ja esta no texto de antes. Nada foi feito.';
    END IF;
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) <> 'd0b34bc7c8c8a5642d091632f1936eff' THEN
        RAISE EXCEPTION 'v_cobertura_full mudou depois da trava. Nada foi feito: desfazer a mao, com cuidado.';
    END IF;
END
$$;

CREATE OR REPLACE VIEW v_cobertura_full AS
WITH ultimo AS (
         SELECT DISTINCT ON (fact_estoque_diario.marketplace, fact_estoque_diario.loja, fact_estoque_diario.estoque_id) fact_estoque_diario.marketplace,
            fact_estoque_diario.loja,
            fact_estoque_diario.estoque_id,
            fact_estoque_diario.data,
            fact_estoque_diario.full_disponivel,
            fact_estoque_diario.full_em_transferencia,
            fact_estoque_diario.full_bloqueado_fiscal,
            fact_estoque_diario.full_outros_indisponivel,
            fact_estoque_diario.galpao_disponivel,
            fact_estoque_diario.unidades_vendidas,
            fact_estoque_diario.unidades_canceladas,
            fact_estoque_diario.unidades_canceladas_pos_envio,
            fact_estoque_diario.unidades_recebidas,
            fact_estoque_diario.unidades_entrada_coleta,
            fact_estoque_diario.operacao_que_zerou,
            fact_estoque_diario.detalhe,
            fact_estoque_diario.origem,
            fact_estoque_diario.fonte_venda,
            fact_estoque_diario.data_captura
           FROM fact_estoque_diario
          ORDER BY fact_estoque_diario.marketplace, fact_estoque_diario.loja, fact_estoque_diario.estoque_id, fact_estoque_diario.data DESC
        ), mov AS MATERIALIZED (
         SELECT v_movimento_full_diario.marketplace,
            v_movimento_full_diario.loja,
            v_movimento_full_diario.estoque_id,
            v_movimento_full_diario.data,
            v_movimento_full_diario.venda_unidades,
            v_movimento_full_diario.entrada_implicita
           FROM v_movimento_full_diario
        ), dia AS (
         SELECT f.marketplace,
            f.loja,
            f.estoque_id,
            f.data,
            f.full_disponivel,
            f.full_em_transferencia,
            f.full_bloqueado_fiscal,
            f.full_outros_indisponivel,
            f.unidades_recebidas,
            f.origem,
                CASE
                    WHEN ((f.marketplace)::text = 'MERCADO LIVRE'::text) THEN m.venda_unidades
                    ELSE f.unidades_vendidas
                END AS venda_bruta,
                CASE
                    WHEN ((f.marketplace)::text = 'MERCADO LIVRE'::text) THEN m.venda_unidades
                    ELSE ((COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0)) - COALESCE(f.unidades_canceladas_pos_envio, 0))
                END AS venda_liquida,
                CASE
                    WHEN ((f.marketplace)::text = 'MERCADO LIVRE'::text) THEN m.entrada_implicita
                    ELSE f.unidades_entrada_coleta
                END AS entrada
           FROM (fact_estoque_diario f
             LEFT JOIN mov m ON ((((m.marketplace)::text = (f.marketplace)::text) AND ((m.loja)::text = (f.loja)::text) AND ((m.estoque_id)::text = (f.estoque_id)::text) AND (m.data = f.data))))
        ), venda_ate AS (
         SELECT fact_vendas_snapshot.loja_origem AS loja,
            max(fact_vendas_snapshot.data_venda) AS venda_ate
           FROM fact_vendas_snapshot
          WHERE ((fact_vendas_snapshot.marketplace_origem)::text = 'MERCADO LIVRE'::text)
          GROUP BY fact_vendas_snapshot.loja_origem
        ), janelas AS (
         SELECT u_1.marketplace,
            u_1.loja,
            u_1.estoque_id,
            bool_or(((COALESCE(f.full_disponivel, 0) > 0) OR (COALESCE(f.full_em_transferencia, 0) > 0) OR (COALESCE(f.full_bloqueado_fiscal, 0) > 0) OR (COALESCE(f.full_outros_indisponivel, 0) > 0) OR (COALESCE(f.unidades_recebidas, 0) > 0) OR (COALESCE(f.entrada, 0) > 0))) AS teve_full_30d,
            sum(f.venda_bruta) FILTER (WHERE (f.data > (u_1.data - 7))) AS vendidas_7d,
            sum(COALESCE(f.venda_liquida, 0)) FILTER (WHERE (f.data > (u_1.data - 7))) AS venda_liquida_7d,
            count(*) FILTER (WHERE ((f.data > (u_1.data - 7)) AND (f.venda_bruta IS NOT NULL) AND ((f.full_disponivel > 0) OR (f.venda_bruta > 0)))) AS dias_com_estoque_7d,
            count(*) FILTER (WHERE (f.data > (u_1.data - 7))) AS dias_com_dado_7d,
            sum(COALESCE(f.venda_liquida, 0)) AS venda_liquida_30d,
            count(*) FILTER (WHERE ((f.venda_bruta IS NOT NULL) AND ((f.full_disponivel > 0) OR (f.venda_bruta > 0)))) AS dias_com_estoque_30d,
            count(*) AS dias_com_dado_30d,
            bool_or(((f.origem)::text = 'operacoes_divergente'::text)) AS serie_divergente
           FROM (ultimo u_1
             JOIN dia f ON ((((f.marketplace)::text = (u_1.marketplace)::text) AND ((f.loja)::text = (u_1.loja)::text) AND ((f.estoque_id)::text = (u_1.estoque_id)::text) AND (f.data > (u_1.data - 30)) AND (f.data <= u_1.data))))
          GROUP BY u_1.marketplace, u_1.loja, u_1.estoque_id
        ), anuncios AS (
         SELECT dim_estoque_anuncio.marketplace,
            dim_estoque_anuncio.loja,
            dim_estoque_anuncio.estoque_id,
            string_agg(DISTINCT (dim_estoque_anuncio.sku)::text, ', '::text) AS skus,
            string_agg(DISTINCT (dim_estoque_anuncio.anuncio_id)::text, ', '::text) AS anuncios,
            count(DISTINCT dim_estoque_anuncio.anuncio_id) AS qtd_anuncios,
            min(dim_estoque_anuncio.titulo) AS titulo,
            bool_or(dim_estoque_anuncio.em_full) AS algum_anuncio_em_full,
            bool_or(dim_estoque_anuncio.ativo) AS algum_anuncio_ativo
           FROM dim_estoque_anuncio
          GROUP BY dim_estoque_anuncio.marketplace, dim_estoque_anuncio.loja, dim_estoque_anuncio.estoque_id
        ), bloqueio AS (
         SELECT u_1.marketplace,
            u_1.loja,
            u_1.estoque_id,
                CASE
                    WHEN (COALESCE(u_1.full_bloqueado_fiscal, 0) = 0) THEN 0
                    ELSE (u_1.data - COALESCE(( SELECT max(f.data) AS max
                       FROM fact_estoque_diario f
                      WHERE (((f.marketplace)::text = (u_1.marketplace)::text) AND ((f.loja)::text = (u_1.loja)::text) AND ((f.estoque_id)::text = (u_1.estoque_id)::text) AND (f.data < u_1.data) AND (COALESCE(f.full_bloqueado_fiscal, 0) = 0))), ( SELECT (min(f.data) - 1)
                       FROM fact_estoque_diario f
                      WHERE (((f.marketplace)::text = (u_1.marketplace)::text) AND ((f.loja)::text = (u_1.loja)::text) AND ((f.estoque_id)::text = (u_1.estoque_id)::text)))))
                END AS dias_bloqueio_fiscal
           FROM ultimo u_1
        ), agora AS (
         SELECT ((now() AT TIME ZONE 'America/Sao_Paulo'::text))::date AS hoje,
                CASE
                    WHEN (EXTRACT(hour FROM (now() AT TIME ZONE 'America/Sao_Paulo'::text)) >= (10)::numeric) THEN 1
                    ELSE 2
                END AS tolerancia_dias
        )
 SELECT u.marketplace,
    u.loja,
    u.estoque_id,
    a.skus,
    a.titulo,
    a.anuncios,
    a.qtd_anuncios,
    a.algum_anuncio_em_full,
    a.algum_anuncio_ativo,
    u.data AS data_do_dado,
    (g.hoje - u.data) AS dias_desde_o_dado,
    ((g.hoje - u.data) > g.tolerancia_dias) AS dado_atrasado,
    u.origem AS origem_ultimo_dado,
    j.serie_divergente,
    u.fonte_venda,
    u.full_disponivel,
    u.full_em_transferencia,
    u.full_bloqueado_fiscal,
    u.full_outros_indisponivel,
    u.galpao_disponivel,
    b.dias_bloqueio_fiscal,
    j.vendidas_7d,
    j.venda_liquida_7d,
    j.dias_com_estoque_7d,
    j.dias_com_dado_7d,
    j.venda_liquida_30d,
    j.dias_com_estoque_30d,
    j.dias_com_dado_30d,
    ((GREATEST(j.venda_liquida_7d, (0)::bigint))::numeric / (NULLIF(j.dias_com_estoque_7d, 0))::numeric) AS venda_dia_7d,
    ((GREATEST(j.venda_liquida_30d, (0)::bigint))::numeric / (NULLIF(j.dias_com_estoque_30d, 0))::numeric) AS venda_dia_30d,
    va.venda_ate
   FROM (((((ultimo u
     JOIN janelas j USING (marketplace, loja, estoque_id))
     JOIN bloqueio b USING (marketplace, loja, estoque_id))
     CROSS JOIN agora g)
     LEFT JOIN anuncios a USING (marketplace, loja, estoque_id))
     LEFT JOIN venda_ate va ON ((((va.loja)::text = (u.loja)::text) AND ((u.marketplace)::text = 'MERCADO LIVRE'::text))))
  WHERE (j.teve_full_30d AND ((g.hoje - u.data) <= 30));

COMMENT ON VIEW v_cobertura_full IS NULL;   -- não tinha comentário antes da trava

DO $$
BEGIN
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) <> '89d171b4d57cc0e986a529a328ecb641' THEN
        RAISE EXCEPTION 'Texto restaurado diferente do original. Nada foi feito.';
    END IF;
END
$$;

SELECT marketplace, count(*) AS linhas FROM v_cobertura_full GROUP BY 1 ORDER BY 1;

COMMIT;
