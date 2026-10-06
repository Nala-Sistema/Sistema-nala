-- =============================================================================
-- v_cobertura_full SÓ COM MERCADO LIVRE — trava antes do estoque da Shopee
-- =============================================================================
-- Escrito em 06/10/2026 (frente [DADOS SHOPEE]). NÃO RODAR sem o parecer do
-- auditor-tecnico (o Mestre chama). Tem de rodar ANTES da primeira gravação
-- do tratamento da foto da Shopee (nala-coletor-ml, tratar_foto_shopee.py).
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
-- POR QUE
--   A view lê TODOS os marketplaces de fact_estoque_diario. Fora do ML, ela
--   tira a venda de `unidades_vendidas`, que o tratamento da foto da Shopee
--   NÃO preenche (a foto só tem saldo). Sem esta trava, a primeira linha
--   Shopee gravada já apareceria na tela "Cobertura do Full" sem venda, com a
--   cobertura errada. A Cobertura do Full DA SHOPEE (venda por item:variação
--   via fact_pedidos_itens_marketplace) é frente própria, depois.
--
-- O QUE MUDA (só isto; o resto é o texto EM VIGOR em produção, lido por
-- pg_get_viewdef em 06/10/2026 — impressão digital 89d171b4d57cc0e986a529a328ecb641)
--   O WHERE final ganha: AND u.marketplace = 'MERCADO LIVRE' (linha "<<<").
--   Nomes, ordem e tipos das 31 colunas não mudam. Nenhuma tabela é tocada.
--   Hoje a view não tem nenhuma linha fora do ML: o resultado tem de sair
--   IDÊNTICO (a guarda do fim confere linha a linha).
--   Conferido numa view TEMP em 06/10/2026: o texto abaixo gera a impressão
--   digital d0b34bc7c8c8a5642d091632f1936eff, com 31 colunas.
--
-- QUEM MAIS LÊ fact_estoque_diario (conferido em 06/10/2026)
--   v_movimento_full_diario: já filtra ML. v_estoque_envio_manual: só tem
--   ramo não-ML para agendamento de outro marketplace (nenhum existe).
--   estoque_peca.py e sinais_dia.py filtram ML no código. cobertura_full.py
--   lê a não-ML só onde unidades_entrada_coleta > 0 (a Shopee grava NULL).
--
-- DESFAZER: sql/v_cobertura_full_so_ml_DESFAZER.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Guarda: produção está no texto conhecido (senão alguém mudou a view
--    depois de 06/10 e este arquivo sobrescreveria a mudança)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) = 'd0b34bc7c8c8a5642d091632f1936eff' THEN
        RAISE EXCEPTION 'A trava ja esta aplicada. Nada foi aplicado.';
    END IF;
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) <> '89d171b4d57cc0e986a529a328ecb641' THEN
        RAISE EXCEPTION 'v_cobertura_full mudou desde 06/10/2026. Nada foi aplicado: refazer o arquivo a partir do texto atual.';
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 2. ANTES
-- -----------------------------------------------------------------------------
CREATE TEMP TABLE cobertura_antes ON COMMIT DROP AS SELECT * FROM v_cobertura_full;

-- -----------------------------------------------------------------------------
-- 3. A view (texto em vigor + a linha marcada com "<<<")
-- -----------------------------------------------------------------------------
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
  WHERE (j.teve_full_30d AND ((g.hoje - u.data) <= 30)
         AND ((u.marketplace)::text = 'MERCADO LIVRE'::text));  -- <<< trava [DADOS SHOPEE]

COMMENT ON VIEW v_cobertura_full IS
    'Cobertura do Full por estoque. SÓ MERCADO LIVRE desde 06/10/2026 (trava da '
    'frente [DADOS SHOPEE]): o estoque da Shopee entra em fact_estoque_diario sem '
    'venda, e a Cobertura do Full da Shopee é frente própria.';

-- -----------------------------------------------------------------------------
-- 4. Guarda do DEPOIS: o texto é o esperado e o resultado não mudou
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    a_mais integer;
    a_menos integer;
BEGIN
    IF md5(pg_get_viewdef('v_cobertura_full'::regclass)) <> 'd0b34bc7c8c8a5642d091632f1936eff' THEN
        RAISE EXCEPTION 'Texto da view diferente do esperado. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO a_mais FROM (SELECT * FROM v_cobertura_full EXCEPT ALL SELECT * FROM cobertura_antes) x;
    SELECT count(*) INTO a_menos FROM (SELECT * FROM cobertura_antes EXCEPT ALL SELECT * FROM v_cobertura_full) x;
    IF a_mais + a_menos > 0 THEN
        RAISE EXCEPTION 'v_cobertura_full mudou: % linha(s) a mais, % a menos. Nada foi aplicado.', a_mais, a_menos;
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 5. DEPOIS (aparece no resultado do editor)
-- -----------------------------------------------------------------------------
SELECT (SELECT count(*) FROM cobertura_antes)  AS linhas_antes,
       (SELECT count(*) FROM v_cobertura_full) AS linhas_depois,
       (SELECT count(*) FROM v_cobertura_full WHERE marketplace <> 'MERCADO LIVRE') AS fora_do_ml;
-- ESPERADO: linhas_antes = linhas_depois (178 em 06/10/2026; varia com o dia);
--           fora_do_ml = 0.

COMMIT;
