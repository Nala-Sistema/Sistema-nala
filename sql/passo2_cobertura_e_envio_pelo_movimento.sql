-- =============================================================================
-- PASSO 2 — v_cobertura_full e v_estoque_envio_manual LEEM O MOVIMENTO DA
--           v_movimento_full_diario (ML), e não mais das operações do Full
-- =============================================================================
-- Escrito em 26/09/2026 (frente [HORARIO COLETORES], passo 2 de 4). NÃO RODAR
-- sem o parecer do auditor-tecnico. Depende de v_movimento_full_diario
-- (sql/v_movimento_full_diario.sql), aplicada em 26/09/2026 19:18.
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
-- ORDEM OBRIGATÓRIA DA FRENTE
--   passo 2 (este) -> passo 3 (cobertura_full.py lê as colunas novas) ->
--   passo 4 (coletor para de chamar /operations). Assim a tela nunca fica
--   sem movimento no meio da troca.
--
-- O QUE MUDA — v_cobertura_full (as 30 colunas ficam com os MESMOS nomes,
-- ordem e tipos; a regra de "dado atrasado" não muda)
--   - No MERCADO LIVRE, a venda de 7 e 30 dias (vendidas_7d, venda_liquida_7d,
--     venda_liquida_30d) vem de v_movimento_full_diario.venda_unidades — a
--     venda FULL de fact_vendas_snapshot, que já é líquida de cancelamento.
--     ATENÇÃO: no ML, vendidas_7d passa a ser LÍQUIDA (igual a
--     venda_liquida_7d); não existe mais "bruto − cancelado" no ML.
--     (Corrigido em 28/09/2026, ressalva do auditor.)
--   - dias_com_estoque_7d/30d contam só dia com venda CONHECIDA (dia depois
--     de venda_ate, ou sem saldo na véspera, fica fora do denominador).
--   - teve_full_30d também olha a entrada implícita.
--   - Saldo, trânsito, bloqueio fiscal, galpão, data do dado, origem e
--     serie_divergente continuam vindo de fact_estoque_diario.
--   - Qualquer outro marketplace (Shopee, quando entrar) continua no caminho
--     antigo, linha a linha, pelas colunas de fact_estoque_diario. A venda
--     líquida desse caminho neste arquivo saiu diferente da antiga (vendidas
--     NULL com cancelamento > 0); corrigida em sql/passo2b_ressalvas.sql.
--   - NOVA coluna 31, no fim: venda_ate (a tela mostra "venda até dd/mm").
--
-- O QUE MUDA — v_estoque_envio_manual (as 15 colunas ficam iguais)
--   - No ML, coletado_na_janela = soma da entrada implícita dos dias da
--     janela com movimento CONHECIDO. Se nenhum dia da janela é conhecido,
--     fica NULL — nunca 0 (desconhecido não vira "não coletado").
--   - fonte_tem_sinal_coleta, no ML: o estoque tem saldo do Full em
--     fact_estoque_diario (é dele que a entrada implícita sai). Consulta à
--     tabela, e não à view, de propósito: EXISTS na view a recalcularia
--     inteira para cada agendamento.
--   - NOVA coluna 16, no fim: dias_desconhecidos_na_janela (dias da janela,
--     até ontem, sem movimento conhecido). NULL fora do ML.
--   - Fora do ML, o caminho antigo, igual.
--   - LIMITE: a entrada implícita inclui devolução de comprador e ajuste a
--     favor; um agendamento pode aparecer como "coletado" por isso.
--
-- O QUE NÃO MUDA
--   Nenhuma tabela. Nenhum alarme de "saída sem venda" (fica para depois da
--   medição do ruído foto × foto, 28–30/09/2026).
--
-- REGRA PARA QUEM MEXER DEPOIS (decisão do Mestre, 28/09/2026)
--   A "revisão 4 do DDL" (colunas coletado_manual_em / coletado_manual_por,
--   que o cobertura_full.py já lê desde o commit 225ad52 e que NUNCA foram
--   aplicadas) tem de partir da v_estoque_envio_manual DESTE PASSO — 16
--   colunas, lendo v_movimento_full_diario — e acrescentar as colunas novas
--   NO FIM. Partir da versão antiga (15 colunas, unidades_entrada_coleta)
--   desfaz este passo em silêncio. Idem para qualquer mudança futura na
--   v_cobertura_full: partir de pg_get_viewdef de produção, nunca de arquivo
--   antigo.
--
-- DESFAZER: arquivo sql/passo2_cobertura_e_envio_pelo_movimento_DESFAZER.sql
--   (CREATE OR REPLACE não remove coluna: desfazer é DROP + CREATE com as
--   definições antigas, que estão lá inteiras, prontas para colar).
--
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guarda: as views em produção são as que este arquivo espera
-- -----------------------------------------------------------------------------
DO $$
DECLARE def_cob TEXT; def_env TEXT; n_cob INTEGER; n_env INTEGER;
BEGIN
    IF to_regclass('public.v_movimento_full_diario') IS NULL THEN
        RAISE EXCEPTION
'v_movimento_full_diario nao existe. Aplique sql/v_movimento_full_diario.sql
antes. Nada foi aplicado.';
    END IF;

    def_cob := pg_get_viewdef('v_cobertura_full'::regclass, true);
    def_env := pg_get_viewdef('v_estoque_envio_manual'::regclass, true);
    IF def_cob ILIKE '%v_movimento_full_diario%' OR def_env ILIKE '%v_movimento_full_diario%' THEN
        RAISE EXCEPTION
'As views ja leem v_movimento_full_diario. Este arquivo ja rodou. Nada foi aplicado.';
    END IF;
    IF def_cob NOT ILIKE '%f.unidades_vendidas IS NOT NULL AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_30d%' THEN
        RAISE EXCEPTION
'v_cobertura_full nao e a esperada (sem o sql/v_cobertura_full_denominador_sem_null.sql
ou alterada depois). Nada foi aplicado.';
    END IF;
    IF def_env NOT ILIKE '%COALESCE(sum(f.unidades_entrada_coleta), 0::bigint) AS coletado_na_janela%' THEN
        RAISE EXCEPTION
'v_estoque_envio_manual nao e a esperada. Nada foi aplicado.';
    END IF;

    SELECT count(*) INTO n_cob FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'v_cobertura_full';
    SELECT count(*) INTO n_env FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'v_estoque_envio_manual';
    IF n_cob <> 30 OR n_env <> 15 THEN
        RAISE EXCEPTION
'Esperava 30 colunas em v_cobertura_full e 15 em v_estoque_envio_manual; achei % e %.
Nada foi aplicado.', n_cob, n_env;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. ANTES: foto das duas views (somem sozinhas no COMMIT)
-- -----------------------------------------------------------------------------
CREATE TEMP TABLE cobertura_antes ON COMMIT DROP AS SELECT * FROM v_cobertura_full;
CREATE TEMP TABLE envio_antes ON COMMIT DROP AS SELECT * FROM v_estoque_envio_manual;

SELECT 'ANTES' AS momento, loja, count(*) AS estoques,
       sum(venda_liquida_7d) AS venda_7d, sum(venda_liquida_30d) AS venda_30d,
       count(*) FILTER (WHERE venda_dia_7d IS NULL) AS sem_venda_dia_7d,
       count(*) FILTER (WHERE dado_atrasado) AS dado_atrasado
  FROM cobertura_antes
 GROUP BY loja
 ORDER BY loja;

-- -----------------------------------------------------------------------------
-- 3a. v_cobertura_full (copiada de pg_get_viewdef em 26/09/2026; o que mudou
--     está marcado com "<<<")
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
                     ELSE f.unidades_vendidas - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0) END AS venda_liquida,
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

-- -----------------------------------------------------------------------------
-- 3b. v_estoque_envio_manual (copiada de pg_get_viewdef em 26/09/2026; o que
--     mudou está marcado com "<<<")
-- -----------------------------------------------------------------------------
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
                           - count(m.data) FILTER (WHERE m.movimento_conhecido), 0)::integer
             END AS dias_desconhecidos_na_janela
   FROM fact_estoque_envio_manual a
     LEFT JOIN fact_estoque_diario f ON f.marketplace::text = a.marketplace::text AND f.loja::text = a.loja::text AND f.estoque_id::text = a.estoque_id::text AND f.data >= (a.data_coleta - 1) AND f.data <= (a.data_coleta + 6)
     LEFT JOIN mov m ON m.marketplace::text = f.marketplace::text AND m.loja::text = f.loja::text AND m.estoque_id::text = f.estoque_id::text AND m.data = f.data  -- <<<
  WHERE a.cancelado_em IS NULL
  GROUP BY a.id;

-- -----------------------------------------------------------------------------
-- 4. Guarda do DEPOIS: só as colunas de venda/entrada podem ter mudado
-- -----------------------------------------------------------------------------
DO $$
DECLARE n_antes INTEGER; n_depois INTEGER; mudou INTEGER; n_cob INTEGER; n_env INTEGER;
BEGIN
    SELECT count(*) INTO n_cob FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'v_cobertura_full';
    SELECT count(*) INTO n_env FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'v_estoque_envio_manual';
    IF n_cob <> 31 OR n_env <> 16 THEN
        RAISE EXCEPTION 'Esperava 31 e 16 colunas depois; achei % e %. Nada foi aplicado.',
            n_cob, n_env;
    END IF;

    -- v_cobertura_full: mesmas linhas (o filtro teve_full_30d pode ganhar
    -- estoque pela entrada implícita, nunca perder) e colunas 1–21 iguais.
    SELECT count(*) INTO n_antes FROM cobertura_antes;
    SELECT count(*) INTO n_depois FROM v_cobertura_full;
    IF n_depois < n_antes THEN
        RAISE EXCEPTION 'v_cobertura_full perdeu linhas: % -> %. Nada foi aplicado.',
            n_antes, n_depois;
    END IF;
    SELECT count(*) INTO mudou
      FROM cobertura_antes a
      JOIN v_cobertura_full d USING (marketplace, loja, estoque_id)
     WHERE (a.skus, a.titulo, a.anuncios, a.qtd_anuncios, a.algum_anuncio_em_full,
            a.algum_anuncio_ativo, a.data_do_dado, a.dias_desde_o_dado, a.dado_atrasado,
            a.origem_ultimo_dado, a.serie_divergente, a.fonte_venda, a.full_disponivel,
            a.full_em_transferencia, a.full_bloqueado_fiscal, a.full_outros_indisponivel,
            a.galpao_disponivel, a.dias_bloqueio_fiscal, a.dias_com_dado_7d,
            a.dias_com_dado_30d)
           IS DISTINCT FROM
           (d.skus, d.titulo, d.anuncios, d.qtd_anuncios, d.algum_anuncio_em_full,
            d.algum_anuncio_ativo, d.data_do_dado, d.dias_desde_o_dado, d.dado_atrasado,
            d.origem_ultimo_dado, d.serie_divergente, d.fonte_venda, d.full_disponivel,
            d.full_em_transferencia, d.full_bloqueado_fiscal, d.full_outros_indisponivel,
            d.galpao_disponivel, d.dias_bloqueio_fiscal, d.dias_com_dado_7d,
            d.dias_com_dado_30d);
    IF mudou > 0 THEN
        RAISE EXCEPTION
'% linha(s) de v_cobertura_full mudaram em coluna que nao e de venda. A copia saiu
diferente da original. Nada foi aplicado.', mudou;
    END IF;

    -- v_estoque_envio_manual: mesmas linhas e colunas 1–11, 13 e 14 iguais.
    SELECT count(*) INTO n_antes FROM envio_antes;
    SELECT count(*) INTO n_depois FROM v_estoque_envio_manual;
    IF n_antes <> n_depois THEN
        RAISE EXCEPTION 'v_estoque_envio_manual tinha % linhas e agora tem %. Nada foi aplicado.',
            n_antes, n_depois;
    END IF;
    SELECT count(*) INTO mudou
      FROM envio_antes a JOIN v_estoque_envio_manual d USING (id)
     WHERE (a.marketplace, a.loja, a.estoque_id, a.data_coleta, a.quantidade, a.observacao,
            a.criado_por, a.criado_em, a.janela_inicio, a.janela_fim, a.janela_encerrada,
            a.dias_desde_data_agendada)
           IS DISTINCT FROM
           (d.marketplace, d.loja, d.estoque_id, d.data_coleta, d.quantidade, d.observacao,
            d.criado_por, d.criado_em, d.janela_inicio, d.janela_fim, d.janela_encerrada,
            d.dias_desde_data_agendada);
    IF mudou > 0 THEN
        RAISE EXCEPTION '% agendamento(s) mudaram fora das colunas de coleta. Nada foi aplicado.',
            mudou;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 5. DEPOIS: o que mudou
-- -----------------------------------------------------------------------------
SELECT 'DEPOIS' AS momento, d.loja, count(*) AS estoques,
       sum(a.venda_liquida_7d) AS venda_7d_antes, sum(d.venda_liquida_7d) AS venda_7d_depois,
       sum(a.venda_liquida_30d) AS venda_30d_antes, sum(d.venda_liquida_30d) AS venda_30d_depois,
       count(*) FILTER (WHERE a.venda_dia_7d IS NULL) AS sem_venda_dia_7d_antes,
       count(*) FILTER (WHERE d.venda_dia_7d IS NULL) AS sem_venda_dia_7d_depois,
       count(*) FILTER (WHERE d.dias_com_estoque_7d < 3) AS abaixo_do_piso_7d_depois,
       max(d.venda_ate) AS venda_ate
  FROM v_cobertura_full d
  LEFT JOIN cobertura_antes a USING (marketplace, loja, estoque_id)
 GROUP BY d.loja
 ORDER BY d.loja;

SELECT 'DEPOIS envio' AS momento, d.id, d.loja, d.estoque_id, d.data_coleta, d.quantidade,
       a.coletado_na_janela AS coletado_antes, d.coletado_na_janela AS coletado_depois,
       d.dias_desconhecidos_na_janela, a.fonte_tem_sinal_coleta AS sinal_antes,
       d.fonte_tem_sinal_coleta AS sinal_depois
  FROM v_estoque_envio_manual d
  JOIN envio_antes a USING (id)
 ORDER BY d.data_coleta DESC, d.id
 LIMIT 50;

COMMIT;

-- =============================================================================
-- CONFERÊNCIA DEPOIS DO COMMIT (só leitura)
-- =============================================================================
--   SELECT count(*), max(venda_ate) FROM v_cobertura_full;
--   SELECT count(*), count(coletado_na_janela) FROM v_estoque_envio_manual;
--   Abrir a tela de Cobertura do Full: tem de carregar. As colunas novas só
--   aparecem na tela depois do passo 3 (cobertura_full.py).
--
-- =============================================================================
-- DESFAZER
-- =============================================================================
-- Colar INTEIRO o arquivo sql/passo2_cobertura_e_envio_pelo_movimento_DESFAZER.sql
-- (DROP das duas views e CREATE com as definições de 26/09/2026, antes deste
-- passo). ATENÇÃO: se o passo 3 (cobertura_full.py lendo venda_ate) já estiver
-- publicado, desfazer o passo 3 antes, ou a tela quebra por coluna ausente.
