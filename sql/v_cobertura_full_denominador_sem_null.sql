-- =============================================================================
-- v_cobertura_full — DIA SEM MOVIMENTO CONHECIDO SAI DO DENOMINADOR
-- =============================================================================
-- Escrito em 25/09/2026 (frente [HORARIO COLETORES]). NÃO RODAR sem o parecer
-- do auditor-tecnico. Tem de ser aplicado ANTES de o coletor de estoque do ML
-- (nala-coletor-ml) passar a fazer GRAVAÇÃO PARCIAL.
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
--
-- COMO RODAR: pelo SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez e
--             executando de uma vez só. A CREDENCIAL É A DO DONO
--             (neondb_owner) — o editor já abre conectado como ele, então não
--             há connection string a digitar.
--             NÃO colar comando por comando: as guardas são blocos DO $$ com
--             ';' dentro do corpo, e a conferência do fim depende da tabela
--             temporária criada no começo, dentro da MESMA transação.
--             ANTES DE EXECUTAR: anotar a hora e o minuto (é o ponto de
--             restauração; o projeto guarda 7 dias de histórico).
--
-- POR QUE
--   O coletor de estoque do ML vai passar a gravar, quando a cota do endpoint
--   de operações do Full acabar no meio de uma loja (429 over_quota, 24 e
--   25/09/2026), o SALDO do dia pela foto com o MOVIMENTO em NULL
--   (unidades_vendidas etc. = NULL: "não sei", nunca "zero").
--   Hoje a view conta esse dia em `dias_com_estoque_7d/30d` (porque o Full
--   está > 0) e soma a venda dele como 0 (`COALESCE(unidades_vendidas, 0)`):
--   a venda por dia cai — um dia parcial em 7 derruba até ~14% da
--   venda_dia_7d — e a cobertura sai INFLADA, escondendo quebra.
--
-- O QUE MUDA (só isto; o resto da view é copiado como está em produção)
--   `dias_com_estoque_7d` e `dias_com_estoque_30d` passam a exigir
--   `unidades_vendidas IS NOT NULL`. O numerador não precisa mudar: o dia
--   NULL já soma 0 em venda_liquida_*; o que estava errado era ele contar
--   como dia de estoque no divisor.
--
-- O QUE NÃO MUDA, DE PROPÓSITO
--   - `dias_com_dado_7d/30d` continuam contando o dia: houve dado (o saldo),
--     só não houve movimento. A tela não usa essas colunas no cálculo.
--   - Nomes, ordem e tipos das 30 colunas: CREATE OR REPLACE VIEW exige, e o
--     cobertura_full.py lê por nome.
--   - v_estoque_envio_manual e o SQL_COLETA_POR_DIA do cobertura_full.py
--     também tratam NULL como 0 (coleta do dia parcial some até a rodada boa
--     seguinte sobrescrever). Decisão do Mestre em 25/09/2026: aceitos como
--     estão.
--
-- EFEITO HOJE (medido em 25/09/2026, antes de qualquer gravação parcial)
--   Das 169 linhas da view, só 1 estoque tem dia com unidades_vendidas NULL e
--   Full > 0: ML-Nala MLBU5185740035 (10 unidades, 7 dias só de foto, sem
--   inventory_id). Ele vai de "venda 0/dia em 7 dias com estoque" para
--   "venda NULL em 0 dias com estoque". O cobertura_full.avaliar trata os dois
--   do mesmo jeito (sem venda_dia -> sem cobertura), então a TELA NÃO MUDA.
--   A guarda do passo 3 aborta se mudar qualquer outra linha.
--
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guarda: a view em produção é a que este arquivo espera
-- -----------------------------------------------------------------------------
-- Se alguém já aplicou isto, ou mudou a view por outro caminho, este arquivo
-- copiaria por cima uma versão velha. Para e manda olhar.
DO $$
DECLARE def TEXT; n_colunas INTEGER;
BEGIN
    def := pg_get_viewdef('v_cobertura_full'::regclass, true);
    IF def ILIKE '%unidades_vendidas IS NOT NULL%' THEN
        RAISE EXCEPTION
'v_cobertura_full ja exige unidades_vendidas IS NOT NULL. Este arquivo ja rodou
ou alguem mudou a view. Nada foi aplicado.';
    END IF;
    IF def NOT ILIKE '%count(*) FILTER (WHERE f.full_disponivel > 0 OR f.unidades_vendidas > 0) AS dias_com_estoque_30d%' THEN
        RAISE EXCEPTION
'A definicao de v_cobertura_full nao e a esperada (dias_com_estoque_30d mudou).
Alguem alterou a view depois de 25/09/2026. Nada foi aplicado.';
    END IF;
    SELECT count(*) INTO n_colunas FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'v_cobertura_full';
    IF n_colunas <> 30 THEN
        RAISE EXCEPTION
'v_cobertura_full tem % colunas; este arquivo espera 30. Nada foi aplicado.', n_colunas;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. ANTES: a foto da view, para comparar no fim (some sozinha no COMMIT)
-- -----------------------------------------------------------------------------
CREATE TEMP TABLE cobertura_antes ON COMMIT DROP AS
SELECT * FROM v_cobertura_full;

SELECT 'ANTES' AS momento, count(*) AS linhas,
       count(*) FILTER (WHERE venda_dia_7d IS NULL) AS sem_venda_dia_7d,
       sum(dias_com_estoque_7d) AS soma_dias_estoque_7d,
       sum(dias_com_estoque_30d) AS soma_dias_estoque_30d
  FROM cobertura_antes;

-- -----------------------------------------------------------------------------
-- 3. A troca (copiada de pg_get_viewdef em 25/09/2026; só as duas linhas
--    marcadas com "<<< MUDOU" são diferentes)
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
        ), janelas AS (
         SELECT u_1.marketplace,
            u_1.loja,
            u_1.estoque_id,
            bool_or(COALESCE(f.full_disponivel, 0) > 0 OR COALESCE(f.full_em_transferencia, 0) > 0 OR COALESCE(f.full_bloqueado_fiscal, 0) > 0 OR COALESCE(f.full_outros_indisponivel, 0) > 0 OR COALESCE(f.unidades_recebidas, 0) > 0 OR COALESCE(f.unidades_entrada_coleta, 0) > 0) AS teve_full_30d,
            sum(f.unidades_vendidas) FILTER (WHERE f.data > (u_1.data - 7)) AS vendidas_7d,
            sum(COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0)) FILTER (WHERE f.data > (u_1.data - 7)) AS venda_liquida_7d,
            count(*) FILTER (WHERE f.data > (u_1.data - 7) AND f.unidades_vendidas IS NOT NULL AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_7d,  -- <<< MUDOU
            count(*) FILTER (WHERE f.data > (u_1.data - 7)) AS dias_com_dado_7d,
            sum(COALESCE(f.unidades_vendidas, 0) - COALESCE(f.unidades_canceladas, 0) - COALESCE(f.unidades_canceladas_pos_envio, 0)) AS venda_liquida_30d,
            count(*) FILTER (WHERE f.unidades_vendidas IS NOT NULL AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_30d,  -- <<< MUDOU
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

-- -----------------------------------------------------------------------------
-- 4. Guarda do DEPOIS: só pode mudar estoque que tem dia com movimento NULL
-- -----------------------------------------------------------------------------
-- Qualquer outra diferença quer dizer que a cópia da view saiu errada: aborta
-- e desfaz tudo (a transação inteira volta, inclusive o CREATE OR REPLACE).
DO $$
DECLARE linhas_antes INTEGER; linhas_depois INTEGER; mudou_indevido INTEGER;
BEGIN
    SELECT count(*) INTO linhas_antes FROM cobertura_antes;
    SELECT count(*) INTO linhas_depois FROM v_cobertura_full;
    IF linhas_antes <> linhas_depois THEN
        RAISE EXCEPTION
'A view tinha % linhas e agora tem %. O filtro nao podia mudar. Nada foi aplicado.',
            linhas_antes, linhas_depois;
    END IF;

    SELECT count(*) INTO mudou_indevido
      FROM v_cobertura_full d
      JOIN cobertura_antes a USING (marketplace, loja, estoque_id)
     WHERE (d.*)::text IS DISTINCT FROM (a.*)::text
       AND NOT EXISTS (
            SELECT 1 FROM fact_estoque_diario f
             WHERE f.marketplace = d.marketplace AND f.loja = d.loja
               AND f.estoque_id = d.estoque_id
               AND f.unidades_vendidas IS NULL
               AND f.data > d.data_do_dado - 30 AND f.data <= d.data_do_dado);
    IF mudou_indevido > 0 THEN
        RAISE EXCEPTION
'% linha(s) mudaram sem ter dia com unidades_vendidas NULL. A copia da view saiu
diferente da original. Nada foi aplicado.', mudou_indevido;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 5. DEPOIS: o que mudou, linha a linha (esperado hoje: só ML-Nala MLBU5185740035)
-- -----------------------------------------------------------------------------
SELECT d.loja, d.estoque_id, d.skus,
       a.dias_com_estoque_7d  AS dias_7d_antes,  d.dias_com_estoque_7d  AS dias_7d_depois,
       round(a.venda_dia_7d, 2)  AS venda_dia_7d_antes,  round(d.venda_dia_7d, 2)  AS venda_dia_7d_depois,
       a.dias_com_estoque_30d AS dias_30d_antes, d.dias_com_estoque_30d AS dias_30d_depois,
       round(a.venda_dia_30d, 2) AS venda_dia_30d_antes, round(d.venda_dia_30d, 2) AS venda_dia_30d_depois
  FROM v_cobertura_full d
  JOIN cobertura_antes a USING (marketplace, loja, estoque_id)
 WHERE (d.*)::text IS DISTINCT FROM (a.*)::text
 ORDER BY d.loja, d.estoque_id;

COMMIT;

-- =============================================================================
-- CONFERÊNCIA DEPOIS DO COMMIT (só leitura)
-- =============================================================================
--   SELECT pg_get_viewdef('v_cobertura_full'::regclass, true)
--          ILIKE '%unidades_vendidas IS NOT NULL%' AS aplicado;
--   Esperado: true.
--
--   Abrir a tela de Cobertura do Full no Streamlit e conferir que carrega e
--   que o MLBU5185740035 da ML-Nala continua "sem venda recente" (sem nível
--   de cobertura), como antes.
--
-- =============================================================================
-- DESFAZER
-- =============================================================================
-- Volta as duas linhas ao que eram (colar INTEIRO, como o de cima):
--
-- BEGIN;
-- (repetir o CREATE OR REPLACE VIEW do passo 3 trocando as duas linhas
--  "<<< MUDOU" por estas:)
--     count(*) FILTER (WHERE f.data > (u_1.data - 7) AND (f.full_disponivel > 0 OR f.unidades_vendidas > 0)) AS dias_com_estoque_7d,
--     count(*) FILTER (WHERE f.full_disponivel > 0 OR f.unidades_vendidas > 0) AS dias_com_estoque_30d,
-- COMMIT;
--
-- Nenhum dado é tocado por este arquivo: só a definição da view.
-- ATENÇÃO: desfazer DEPOIS que a gravação parcial do coletor estiver ligada
-- volta o problema da cobertura inflada nos dias parciais.
