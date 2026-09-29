-- =============================================================================
-- PASSO 2b — RESSALVAS DO AUDITOR AO PASSO 2 (três views, só definição)
-- =============================================================================
-- Escrito em 28/09/2026 (frente [HORARIO COLETORES]). NÃO RODAR sem o parecer
-- do auditor-tecnico. Depende dos passos 1 e 2 aplicados
-- (sql/v_movimento_full_diario.sql e sql/passo2_cobertura_e_envio_pelo_movimento.sql).
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
--
-- COMO RODAR: pelo SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez e
--             executando de uma vez só. A CREDENCIAL É A DO DONO
--             (neondb_owner) — o editor já abre conectado como ele, então não
--             há connection string a digitar.
--             NÃO colar comando por comando: as guardas são blocos DO $$ com
--             ';' dentro do corpo, e a conferência do fim depende das tabelas
--             temporárias criadas no começo, dentro da MESMA transação.
--             ANTES DE EXECUTAR: anotar a hora e o minuto (ponto de
--             restauração; o projeto guarda 7 dias de histórico).
--
-- O QUE MUDA (linhas marcadas com "<<<"; o resto é o texto aplicado, gerado
-- por script a partir dos arquivos dos passos 1 e 2)
--   (b) [MÉDIA] v_movimento_full_diario: entrada_implicita e
--       saida_sem_explicacao saíam 0 (e não NULL) nos dias de movimento
--       desconhecido, porque GREATEST(NULL, 0) = 0 no Postgres. Passam a NULL.
--       Quem já lia estava protegido (FILTER movimento_conhecido, "> 0",
--       COALESCE(...,0) em teve_full_30d), então nenhuma tela muda hoje.
--   (a) [BAIXA] v_cobertura_full, caminho FORA do ML: a venda líquida volta a
--       COALESCE(vendidas, 0) − canceladas − canceladas pós-envio, como era
--       antes do passo 2 (diferia com vendidas NULL e cancelamento > 0). Hoje
--       não há linha fora do ML: nada muda.
--   (c) [BAIXA] v_estoque_envio_manual: dias_desconhecidos_na_janela só
--       desconta dia conhecido ATÉ ONTEM (um dia conhecido depois de ontem
--       fazia a conta sair para menos).
--
-- CREATE OR REPLACE mantém nomes, ordem e tipos das colunas (as três views).
-- Nenhuma tabela é tocada.
--
-- DESFAZER: sql/passo2b_ressalvas_DESFAZER.sql (reaplica os textos dos passos 1
-- e 2 como estão hoje em produção).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guarda: produção está no estado dos passos 1 e 2
-- -----------------------------------------------------------------------------
DO $$
DECLARE def_mov TEXT; def_cob TEXT; def_env TEXT;
BEGIN
    def_mov := pg_get_viewdef('v_movimento_full_diario'::regclass, true);
    def_cob := pg_get_viewdef('v_cobertura_full'::regclass, true);
    def_env := pg_get_viewdef('v_estoque_envio_manual'::regclass, true);
    IF def_cob NOT ILIKE '%v_movimento_full_diario%' OR def_env NOT ILIKE '%dias_desconhecidos_na_janela%' THEN
        RAISE EXCEPTION 'O passo 2 nao esta aplicado. Nada foi aplicado.';
    END IF;
    IF def_mov ILIKE '%END::integer AS entrada_implicita%' THEN
        RAISE EXCEPTION 'v_movimento_full_diario ja tem a correcao (b). Este arquivo ja rodou. Nada foi aplicado.';
    END IF;
    IF (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'v_movimento_full_diario') <> 11
       OR (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'v_cobertura_full') <> 31
       OR (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'v_estoque_envio_manual') <> 16 THEN
        RAISE EXCEPTION 'Numero de colunas diferente do esperado (11, 31, 16). Nada foi aplicado.';
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. ANTES
-- -----------------------------------------------------------------------------
CREATE TEMP TABLE movimento_antes ON COMMIT DROP AS SELECT * FROM v_movimento_full_diario;
CREATE TEMP TABLE cobertura_antes ON COMMIT DROP AS SELECT * FROM v_cobertura_full;
CREATE TEMP TABLE envio_antes ON COMMIT DROP AS SELECT * FROM v_estoque_envio_manual;

SELECT 'ANTES' AS momento, loja,
       count(*) FILTER (WHERE NOT movimento_conhecido) AS dias_desconhecidos,
       count(*) FILTER (WHERE NOT movimento_conhecido AND entrada_implicita = 0) AS desconhecido_com_entrada_zero
  FROM movimento_antes
 GROUP BY loja ORDER BY loja;

-- -----------------------------------------------------------------------------
-- 3. As três views
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_movimento_full_diario AS
WITH venda_ate AS (
    -- Última venda do ML da loja, em qualquer logística: até onde a venda é
    -- conhecida. Depois disso, movimento desconhecido.
    SELECT loja_origem AS loja, max(data_venda) AS venda_ate
      FROM fact_vendas_snapshot
     WHERE marketplace_origem = 'MERCADO LIVRE'
     GROUP BY loja_origem
), ponte_sku AS (
    SELECT loja, anuncio_id, upper(trim(sku)) AS sku, min(estoque_id) AS estoque_id
      FROM dim_estoque_anuncio
     WHERE marketplace = 'MERCADO LIVRE' AND sku IS NOT NULL
     GROUP BY loja, anuncio_id, upper(trim(sku))
    HAVING count(DISTINCT estoque_id) = 1
), ponte_anuncio AS (
    SELECT loja, anuncio_id, min(estoque_id) AS estoque_id
      FROM dim_estoque_anuncio
     WHERE marketplace = 'MERCADO LIVRE'
     GROUP BY loja, anuncio_id
    HAVING count(DISTINCT estoque_id) = 1
), venda AS (
    SELECT v.loja_origem AS loja,
           COALESCE(ps.estoque_id, pa.estoque_id) AS estoque_id,
           v.data_venda AS data,
           sum(v.quantidade) AS unidades
      FROM fact_vendas_snapshot v
      LEFT JOIN ponte_sku ps
             ON ps.loja = v.loja_origem AND ps.anuncio_id = v.codigo_anuncio
            AND ps.sku = upper(trim(v.sku))
      LEFT JOIN ponte_anuncio pa
             ON pa.loja = v.loja_origem AND pa.anuncio_id = v.codigo_anuncio
     WHERE v.marketplace_origem = 'MERCADO LIVRE'
       AND upper(v.logistica) = 'FULL'
       AND COALESCE(ps.estoque_id, pa.estoque_id) IS NOT NULL
     GROUP BY v.loja_origem, COALESCE(ps.estoque_id, pa.estoque_id), v.data_venda
), saldo AS (
    SELECT f.marketplace, f.loja, f.estoque_id, f.data,
           COALESCE(f.full_disponivel, 0) + COALESCE(f.full_em_transferencia, 0)
         + COALESCE(f.full_bloqueado_fiscal, 0) + COALESCE(f.full_outros_indisponivel, 0)
               AS saldo_total,
           lag(COALESCE(f.full_disponivel, 0) + COALESCE(f.full_em_transferencia, 0)
             + COALESCE(f.full_bloqueado_fiscal, 0) + COALESCE(f.full_outros_indisponivel, 0))
               OVER w AS saldo_total_anterior,
           lag(f.data) OVER w AS data_anterior
      FROM fact_estoque_diario f
     WHERE f.marketplace = 'MERCADO LIVRE'
       AND f.detalhe ? 'inventory_id'          -- só estoque com Full
    WINDOW w AS (PARTITION BY f.marketplace, f.loja, f.estoque_id ORDER BY f.data)
), base AS (
    SELECT s.marketplace, s.loja, s.estoque_id, s.data, s.saldo_total,
           CASE WHEN s.data_anterior = s.data - 1 THEN s.saldo_total_anterior END
               AS saldo_total_anterior,
           va.venda_ate,
           CASE WHEN va.venda_ate IS NOT NULL AND s.data <= va.venda_ate
                THEN COALESCE(vd.unidades, 0) END AS venda_unidades
      FROM saldo s
      LEFT JOIN venda_ate va ON va.loja = s.loja
      LEFT JOIN venda vd
             ON vd.loja = s.loja AND vd.estoque_id = s.estoque_id AND vd.data = s.data
)
SELECT marketplace, loja, estoque_id, data,
       saldo_total,
       saldo_total_anterior,
       venda_ate,
       venda_unidades::integer AS venda_unidades,
       -- <<< (b) GREATEST ignora NULL: sem o CASE, dia desconhecido saía 0
       CASE WHEN saldo_total_anterior IS NOT NULL AND venda_unidades IS NOT NULL
            THEN GREATEST(saldo_total - saldo_total_anterior + venda_unidades, 0)
       END::integer AS entrada_implicita,
       CASE WHEN saldo_total_anterior IS NOT NULL AND venda_unidades IS NOT NULL
            THEN GREATEST(-(saldo_total - saldo_total_anterior + venda_unidades), 0)
       END::integer AS saida_sem_explicacao,
       (saldo_total_anterior IS NOT NULL AND venda_unidades IS NOT NULL)
           AS movimento_conhecido
  FROM base;

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
        ), mov AS MATERIALIZED (  -- <<< NOVO: o movimento calculado UMA vez (sem isto o planejador
                                  --     estima 1 linha e recalcula a view por linha: estoura o tempo)
         SELECT v_movimento_full_diario.marketplace, v_movimento_full_diario.loja,
            v_movimento_full_diario.estoque_id, v_movimento_full_diario.data,
            v_movimento_full_diario.venda_unidades, v_movimento_full_diario.entrada_implicita
           FROM v_movimento_full_diario
        ), dia AS (  -- <<< NOVO: uma linha por estoque e dia, com a venda/entrada da fonte certa
         SELECT f.marketplace, f.loja, f.estoque_id, f.data,
            f.full_disponivel, f.full_em_transferencia, f.full_bloqueado_fiscal,
            f.full_outros_indisponivel, f.unidades_recebidas, f.origem,
                CASE WHEN f.marketplace::text = 'MERCADO LIVRE'::text THEN m.venda_unidades
                     ELSE f.unidades_vendidas END AS venda_bruta,
                CASE WHEN f.marketplace::text = 'MERCADO LIVRE'::text THEN m.venda_unidades
                     ELSE COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0) END AS venda_liquida,  -- <<< (a) como era antes do passo 2
                CASE WHEN f.marketplace::text = 'MERCADO LIVRE'::text THEN m.entrada_implicita
                     ELSE f.unidades_entrada_coleta END AS entrada
           FROM fact_estoque_diario f
             LEFT JOIN mov m ON m.marketplace::text = f.marketplace::text AND m.loja::text = f.loja::text AND m.estoque_id::text = f.estoque_id::text AND m.data = f.data
        ), venda_ate AS (  -- <<< NOVO: coluna 31
         SELECT fact_vendas_snapshot.loja_origem AS loja,
            max(fact_vendas_snapshot.data_venda) AS venda_ate
           FROM fact_vendas_snapshot
          WHERE fact_vendas_snapshot.marketplace_origem::text = 'MERCADO LIVRE'::text
          GROUP BY fact_vendas_snapshot.loja_origem
        ), janelas AS (
         SELECT u_1.marketplace,
            u_1.loja,
            u_1.estoque_id,
            bool_or(COALESCE(f.full_disponivel, 0) > 0 OR COALESCE(f.full_em_transferencia, 0) > 0 OR COALESCE(f.full_bloqueado_fiscal, 0) > 0 OR COALESCE(f.full_outros_indisponivel, 0) > 0 OR COALESCE(f.unidades_recebidas, 0) > 0 OR COALESCE(f.entrada, 0) > 0) AS teve_full_30d,  -- <<< entrada
            sum(f.venda_bruta) FILTER (WHERE f.data > (u_1.data - 7)) AS vendidas_7d,  -- <<<
            sum(COALESCE(f.venda_liquida, 0)) FILTER (WHERE f.data > (u_1.data - 7)) AS venda_liquida_7d,  -- <<<
            count(*) FILTER (WHERE f.data > (u_1.data - 7) AND f.venda_bruta IS NOT NULL AND (f.full_disponivel > 0 OR f.venda_bruta > 0)) AS dias_com_estoque_7d,  -- <<<
            count(*) FILTER (WHERE f.data > (u_1.data - 7)) AS dias_com_dado_7d,
            sum(COALESCE(f.venda_liquida, 0)) AS venda_liquida_30d,  -- <<<
            count(*) FILTER (WHERE f.venda_bruta IS NOT NULL AND (f.full_disponivel > 0 OR f.venda_bruta > 0)) AS dias_com_estoque_30d,  -- <<<
            count(*) AS dias_com_dado_30d,
            bool_or(f.origem::text = 'operacoes_divergente'::text) AS serie_divergente
           FROM ultimo u_1
             JOIN dia f ON f.marketplace::text = u_1.marketplace::text AND f.loja::text = u_1.loja::text AND f.estoque_id::text = u_1.estoque_id::text AND f.data > (u_1.data - 30) AND f.data <= u_1.data  -- <<< dia
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
    GREATEST(j.venda_liquida_30d, 0::bigint)::numeric / NULLIF(j.dias_com_estoque_30d, 0)::numeric AS venda_dia_30d,
    va.venda_ate  -- <<< coluna 31
   FROM ultimo u
     JOIN janelas j USING (marketplace, loja, estoque_id)
     JOIN bloqueio b USING (marketplace, loja, estoque_id)
     CROSS JOIN agora g
     LEFT JOIN anuncios a USING (marketplace, loja, estoque_id)
     LEFT JOIN venda_ate va ON va.loja::text = u.loja::text AND u.marketplace::text = 'MERCADO LIVRE'::text  -- <<<
  WHERE j.teve_full_30d AND (g.hoje - u.data) <= 30;

CREATE OR REPLACE VIEW v_estoque_envio_manual AS
 WITH mov AS MATERIALIZED (  -- <<< o movimento calculado UMA vez (ver v_cobertura_full)
         SELECT v_movimento_full_diario.marketplace, v_movimento_full_diario.loja,
            v_movimento_full_diario.estoque_id, v_movimento_full_diario.data,
            v_movimento_full_diario.entrada_implicita, v_movimento_full_diario.movimento_conhecido
           FROM v_movimento_full_diario
        )
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
        CASE WHEN a.marketplace::text = 'MERCADO LIVRE'::text
             THEN sum(m.entrada_implicita) FILTER (WHERE m.movimento_conhecido)  -- <<< NULL se nenhum dia conhecido
             ELSE COALESCE(sum(f.unidades_entrada_coleta), 0::bigint) END AS coletado_na_janela,
    (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date > (a.data_coleta + 6) AS janela_encerrada,
    (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date - a.data_coleta AS dias_desde_data_agendada,
        CASE WHEN a.marketplace::text = 'MERCADO LIVRE'::text  -- <<<
             THEN (EXISTS ( SELECT 1
                   FROM fact_estoque_diario s
                  WHERE s.marketplace::text = a.marketplace::text AND s.loja::text = a.loja::text AND s.estoque_id::text = a.estoque_id::text AND s.detalhe ? 'inventory_id'))
             ELSE (EXISTS ( SELECT 1
                   FROM fact_estoque_diario s
                  WHERE s.marketplace::text = a.marketplace::text AND s.loja::text = a.loja::text AND s.estoque_id::text = a.estoque_id::text AND s.unidades_entrada_coleta IS NOT NULL)) END AS fonte_tem_sinal_coleta,
        CASE WHEN a.marketplace::text = 'MERCADO LIVRE'::text  -- <<< coluna 16
             THEN GREATEST(LEAST(a.data_coleta + 6, (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date - 1) - (a.data_coleta - 1) + 1
                           - count(m.data) FILTER (WHERE m.movimento_conhecido AND m.data <= (now() AT TIME ZONE 'America/Sao_Paulo'::text)::date - 1), 0)::integer  -- <<< (c)
             END AS dias_desconhecidos_na_janela
   FROM fact_estoque_envio_manual a
     LEFT JOIN fact_estoque_diario f ON f.marketplace::text = a.marketplace::text AND f.loja::text = a.loja::text AND f.estoque_id::text = a.estoque_id::text AND f.data >= (a.data_coleta - 1) AND f.data <= (a.data_coleta + 6)
     LEFT JOIN mov m ON m.marketplace::text = f.marketplace::text AND m.loja::text = f.loja::text AND m.estoque_id::text = f.estoque_id::text AND m.data = f.data  -- <<<
  WHERE a.cancelado_em IS NULL
  GROUP BY a.id;

-- -----------------------------------------------------------------------------
-- 4. Guarda do DEPOIS: só pode mudar o que as ressalvas dizem
-- -----------------------------------------------------------------------------
DO $$
DECLARE mudou INTEGER;
BEGIN
    -- (b): mesmas linhas; dia CONHECIDO igual; dia DESCONHECIDO com entrada e
    -- saída NULL.
    IF (SELECT count(*) FROM movimento_antes) <> (SELECT count(*) FROM v_movimento_full_diario) THEN
        RAISE EXCEPTION 'v_movimento_full_diario mudou de numero de linhas. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO mudou
      FROM movimento_antes a JOIN v_movimento_full_diario d USING (marketplace, loja, estoque_id, data)
     WHERE (a.movimento_conhecido AND (a.*)::text IS DISTINCT FROM (d.*)::text)
        OR (NOT d.movimento_conhecido AND (d.entrada_implicita IS NOT NULL OR d.saida_sem_explicacao IS NOT NULL));
    IF mudou > 0 THEN
        RAISE EXCEPTION '% linha(s) de v_movimento_full_diario fora do esperado. Nada foi aplicado.', mudou;
    END IF;

    -- (a): hoje só há ML em fact_estoque_diario, então v_cobertura_full fica igual.
    IF (SELECT count(*) FROM cobertura_antes) <> (SELECT count(*) FROM v_cobertura_full) THEN
        RAISE EXCEPTION 'v_cobertura_full mudou de numero de linhas. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO mudou
      FROM cobertura_antes a JOIN v_cobertura_full d USING (marketplace, loja, estoque_id)
     WHERE a.marketplace = 'MERCADO LIVRE' AND (a.*)::text IS DISTINCT FROM (d.*)::text;
    IF mudou > 0 THEN
        RAISE EXCEPTION '% linha(s) do ML mudaram em v_cobertura_full. Nada foi aplicado.', mudou;
    END IF;

    -- (c): mesmas linhas; só dias_desconhecidos_na_janela pode mudar.
    IF (SELECT count(*) FROM envio_antes) <> (SELECT count(*) FROM v_estoque_envio_manual) THEN
        RAISE EXCEPTION 'v_estoque_envio_manual mudou de numero de linhas. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO mudou
      FROM envio_antes a JOIN v_estoque_envio_manual d USING (id)
     WHERE (a.coletado_na_janela, a.fonte_tem_sinal_coleta, a.janela_fim)
           IS DISTINCT FROM (d.coletado_na_janela, d.fonte_tem_sinal_coleta, d.janela_fim);
    IF mudou > 0 THEN
        RAISE EXCEPTION '% agendamento(s) mudaram fora de dias_desconhecidos_na_janela. Nada foi aplicado.', mudou;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 5. DEPOIS
-- -----------------------------------------------------------------------------
SELECT 'DEPOIS' AS momento, loja,
       count(*) FILTER (WHERE NOT movimento_conhecido) AS dias_desconhecidos,
       count(*) FILTER (WHERE NOT movimento_conhecido AND entrada_implicita IS NULL) AS desconhecido_com_entrada_null
  FROM v_movimento_full_diario
 GROUP BY loja ORDER BY loja;

COMMIT;

-- =============================================================================
-- DESFAZER
-- =============================================================================
-- Colar INTEIRO sql/passo2b_ressalvas_DESFAZER.sql.
