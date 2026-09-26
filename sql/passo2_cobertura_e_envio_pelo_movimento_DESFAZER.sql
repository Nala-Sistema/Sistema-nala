-- =============================================================================
-- DESFAZER DO PASSO 2 — volta v_cobertura_full e v_estoque_envio_manual ao que
-- eram em 26/09/2026, antes de sql/passo2_cobertura_e_envio_pelo_movimento.sql
-- =============================================================================
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
-- COMO RODAR: SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez.
--             A CREDENCIAL É A DO DONO (neondb_owner).
--
-- É DROP + CREATE porque CREATE OR REPLACE não remove as colunas que o passo 2
-- acrescentou (venda_ate e dias_desconhecidos_na_janela). As duas views não
-- têm dependentes; as permissões são só do dono, que as recria.
-- SE O PASSO 3 (cobertura_full.py lendo as colunas novas) JÁ ESTIVER
-- PUBLICADO, DESFAZER O PASSO 3 ANTES — senão a tela quebra.
--
-- v_cobertura_full volta com a correção do denominador
-- (sql/v_cobertura_full_denominador_sem_null.sql), que já estava aplicada.
-- Texto gerado por script a partir daquele arquivo e de pg_get_viewdef de
-- 26/09/2026, sem redigitar.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF pg_get_viewdef('v_cobertura_full'::regclass, true) NOT ILIKE '%v_movimento_full_diario%' THEN
        RAISE EXCEPTION 'v_cobertura_full nao le v_movimento_full_diario: o passo 2 nao esta aplicado. Nada foi feito.';
    END IF;
END $$;

DROP VIEW v_cobertura_full;
DROP VIEW v_estoque_envio_manual;

CREATE VIEW v_cobertura_full AS
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
        ), janelas AS (
         SELECT u_1.marketplace,
            u_1.loja,
            u_1.estoque_id,
            bool_or(COALESCE(f.full_disponivel, 0) > 0 OR COALESCE(f.full_em_transferencia, 0) > 0 OR COALESCE(f.full_bloqueado_fiscal, 0) > 0 OR COALESCE(f.full_outros_indisponivel, 0) > 0 OR COALESCE(f.unidades_recebidas, 0) > 0 OR COALESCE(f.unidades_entrada_coleta, 0) > 0) AS teve_full_30d,
            sum(f.unidades_vendidas) FILTER (WHERE f.data > (u_1.data - 7)) AS vendidas_7d,
            sum(COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0)) FILTER (WHERE f.data > (u_1.data - 7)) AS venda_liquida_7d,
            count(*) FILTER (WHERE f.data > (u_1.data - 7) AND f.unidades_vendidas IS NOT NULL AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_7d,
            count(*) FILTER (WHERE f.data > (u_1.data - 7)) AS dias_com_dado_7d,
            sum(COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0)) AS venda_liquida_30d,
            count(*) FILTER (WHERE f.unidades_vendidas IS NOT NULL AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_30d,
            count(*) AS dias_com_dado_30d,
            bool_or(f.origem::text = 'operacoes_divergente'::text) AS serie_divergente
           FROM ultimo u_1
             JOIN fact_estoque_diario f ON f.marketplace::text = u_1.marketplace::text AND f.loja::text = u_1.loja::text AND f.estoque_id::text = u_1.estoque_id::text AND f.data > (u_1.data - 30) AND f.data <= u_1.data
          GROUP BY u_1.marketplace, u_1.loja, u_1.estoque_id
        ), anuncios AS (
         SELECT dim_estoque_anuncio.marketplace,
            dim_estoque_anuncio.loja,
            dim_estoque_anuncio.estoque_id,
            string_agg(DISTINCT dim_estoque_anuncio.sku::text, ', '::text) AS skus,
            string_agg(DISTINCT dim_estoque_anuncio.anuncio_id::text, ', '::text) AS anuncios,
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
                    WHEN COALESCE(u_1.full_bloqueado_fiscal, 0) = 0 THEN 0
                    ELSE u_1.data - COALESCE(( SELECT max(f.data) AS max
                       FROM fact_estoque_diario f
                      WHERE f.marketplace::text = u_1.marketplace::text AND f.loja::text = u_1.loja::text AND f.estoque_id::text = u_1.estoque_id::text AND f.data < u_1.data AND COALESCE(f.full_bloqueado_fiscal, 0) = 0), ( SELECT min(f.data) - 1
                       FROM fact_estoque_diario f
                      WHERE f.marketplace::text = u_1.marketplace::text AND f.loja::text = u_1.loja::text AND f.estoque_id::text = u_1.estoque_id::text))
                END AS dias_bloqueio_fiscal
           FROM ultimo u_1
        ), agora AS (
         SELECT (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date AS hoje,
                CASE
                    WHEN EXTRACT(hour FROM (now() AT TIME ZONE 'America/Sao_Paulo'::text)) >= 10::numeric THEN 1
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
    g.hoje - u.data AS dias_desde_o_dado,
    (g.hoje - u.data) > g.tolerancia_dias AS dado_atrasado,
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
    GREATEST(j.venda_liquida_7d, 0::bigint)::numeric / NULLIF(j.dias_com_estoque_7d, 0)::numeric AS venda_dia_7d,
    GREATEST(j.venda_liquida_30d, 0::bigint)::numeric / NULLIF(j.dias_com_estoque_30d, 0)::numeric AS venda_dia_30d
   FROM ultimo u
     JOIN janelas j USING (marketplace, loja, estoque_id)
     JOIN bloqueio b USING (marketplace, loja, estoque_id)
     CROSS JOIN agora g
     LEFT JOIN anuncios a USING (marketplace, loja, estoque_id)
  WHERE j.teve_full_30d AND (g.hoje - u.data) <= 30;

CREATE VIEW v_estoque_envio_manual AS
 SELECT a.id,
    a.marketplace,
    a.loja,
    a.estoque_id,
    a.data_coleta,
    a.quantidade,
    a.observacao,
    a.criado_por,
    a.criado_em,
    a.data_coleta - 1 AS janela_inicio,
    a.data_coleta + 6 AS janela_fim,
    COALESCE(sum(f.unidades_entrada_coleta), 0::bigint) AS coletado_na_janela,
    (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date > (a.data_coleta + 6) AS janela_encerrada,
    (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date - a.data_coleta AS dias_desde_data_agendada,
    (EXISTS ( SELECT 1
           FROM fact_estoque_diario s
          WHERE s.marketplace::text = a.marketplace::text AND s.loja::text = a.loja::text AND s.estoque_id::text = a.estoque_id::text AND s.unidades_entrada_coleta IS NOT NULL)) AS fonte_tem_sinal_coleta
   FROM fact_estoque_envio_manual a
     LEFT JOIN fact_estoque_diario f ON f.marketplace::text = a.marketplace::text AND f.loja::text = a.loja::text AND f.estoque_id::text = a.estoque_id::text AND f.data >= (a.data_coleta - 1) AND f.data <= (a.data_coleta + 6)
  WHERE a.cancelado_em IS NULL
  GROUP BY a.id;

DO $$
BEGIN
    IF (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'v_cobertura_full') <> 30
       OR (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'v_estoque_envio_manual') <> 15 THEN
        RAISE EXCEPTION 'As views nao voltaram com 30 e 15 colunas. Nada foi feito.';
    END IF;
END $$;

SELECT 'DESFEITO' AS momento, (SELECT count(*) FROM v_cobertura_full) AS linhas_cobertura,
       (SELECT count(*) FROM v_estoque_envio_manual) AS linhas_envio;

COMMIT;
