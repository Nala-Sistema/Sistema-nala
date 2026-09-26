-- =============================================================================
-- v_movimento_full_diario — MOVIMENTO DO FULL (ML) CALCULADO NA LEITURA
-- =============================================================================
-- Escrito em 26/09/2026 (frente [HORARIO COLETORES], passo 1 de 4). NÃO RODAR
-- sem o parecer do auditor-tecnico.
--
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", como neondb_owner.
--
-- COMO RODAR: pelo SQL EDITOR DO NEON, colando o arquivo INTEIRO de uma vez e
--             executando de uma vez só. A CREDENCIAL É A DO DONO
--             (neondb_owner) — o editor já abre conectado como ele, então não
--             há connection string a digitar.
--             NÃO colar comando por comando: as guardas são blocos DO $$ com
--             ';' dentro do corpo, e tudo roda numa transação só.
--             ANTES DE EXECUTAR: anotar a hora e o minuto (ponto de
--             restauração; o projeto guarda 7 dias de histórico).
--
-- POR QUE
--   O endpoint de operações do Full (/stock/fulfillment/operations/search)
--   passou a responder 429 over_quota nas 4 lojas ML desde ~19/09/2026, com a
--   cota consumida também por outros sistemas autorizados nas contas. O
--   movimento deixa de depender dele (decisão do Mestre, 26/09/2026):
--     SALDO   = foto diária do Full, que o coletor já grava em fact_estoque_diario;
--     VENDA   = fact_vendas_snapshot, logística FULL, ligada ao estoque pela
--               ponte dim_estoque_anuncio;
--     ENTRADA = saldo_total(dia) − saldo_total(dia anterior) + venda(dia),
--               quando > 0. Quando < 0 é SAÍDA SEM EXPLICAÇÃO: alarme, nunca
--               entrada negativa.
--
-- O QUE FAZ
--   Só CRIA a view v_movimento_full_diario. Não altera nenhuma tabela nem
--   nenhuma view existente (v_cobertura_full e v_estoque_envio_manual passam
--   a ler desta no passo 2, em outro arquivo).
--
-- REGRAS QUE A VIEW GARANTE
--   1. FONTE ÚNICA da venda: fact_vendas_snapshot, e só ela. Nunca
--      fact_pedidos_*. Hoje é o upload; depois de 01/10/2026 a API das vendas
--      do ML sincroniza para a mesma tabela — a troca é transparente aqui.
--   2. VENDA DISPONÍVEL ATÉ (`venda_ate`): última data de venda do ML da loja
--      em fact_vendas_snapshot, em QUALQUER logística (um upload cobre todas).
--      Dia depois dela = movimento DESCONHECIDO: venda, entrada e saída saem
--      NULL, nunca zero.
--   3. Saldo total = disponível + em transferência + bloqueio fiscal + outros
--      indisponíveis. A coleta entra no total como "em transferência" antes de
--      ser recebida: por isso o total, e não só o disponível.
--   4. Dia sem saldo na véspera (buraco na série) = movimento desconhecido.
--   5. Ligação venda -> estoque: (loja, anúncio, SKU) quando esse par aponta
--      para UM estoque só; senão (loja, anúncio) quando o anúncio tem UM
--      estoque só; senão a venda não é atribuída (a conferência do fim conta
--      quantas unidades ficaram assim — em 16–18/09/2026 foram zero).
--
-- LIMITES CONHECIDOS (não são bug; estão registrados para a tela)
--   - "Entrada" é tudo o que entrou no Full: coleta, devolução de comprador
--     e ajuste a favor. Não separa só a coleta.
--   - O saldo do dia D é a foto tirada na rodada seguinte (hoje ~10h BRT;
--     na grade nova, ~04h). A venda entre a meia-noite e a foto cai no dia
--     seguinte na conta da entrada. Foto às 04h reduz esse erro.
--   - ML-YanniSP (prep center): em 16–18/09 a entrada deu 21 contra 28 do
--     /operations. Investigação antes do passo 2; se não fechar, a tela marca
--     "entrada não confiável" para ela.
--
-- CONFERIDO EM LEITURA em 26/09/2026: o corpo EXATO desta view (extraído
-- deste arquivo por script) rodou como CTE de mesmo nome na frente das
-- consultas dos passos 4 e 5, sem criar nada em produção.
--   Guarda do passo 4: 0 chaves duplicadas, 0 venda depois de venda_ate,
--   10.289 linhas na view = 10.289 linhas de saldo do Full.
--   Passo 5, 16–18/09/2026 (venda nova × venda líquida do /operations;
--   entrada nova × "entrada de coleta" do /operations):
--     ML-LPT      289 × 280 | entrada  70 ×  15 | saída s/ explicação 6 un
--     ML-Nala     148 × 146 | entrada 307 × 292 | saída s/ explicação 7 un
--     ML-YanniRJ    8 ×   7 | entrada  31 ×  12 | saída s/ explicação 2 un
--     ML-YanniSP    5 ×   5 | entrada  22 ×  28 | saída s/ explicação 1 un
--   A entrada nova é MAIOR que a do /operations na LPT e na YanniRJ porque a
--   regra antiga não contava coleta que chega por TRANSFER_RESERVATION (ex.:
--   MLBU3539056071, total 7 -> 26 com 20 em transferência em 17/09, e o
--   /operations registrou 0).
--   Venda FULL de 01 a 24/09 fora da view: 0 na LPT, YanniRJ e YanniSP; 154
--   na Nala = exatamente a venda de 23/09 (100) e 24/09 (54), dias em que a
--   Nala ficou sem nenhuma linha de saldo (over_quota). A ponte cobre tudo.
--
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- -----------------------------------------------------------------------------
-- 1. Guarda: a view ainda não existe e as colunas de origem estão lá
-- -----------------------------------------------------------------------------
DO $$
DECLARE faltando TEXT;
BEGIN
    IF to_regclass('public.v_movimento_full_diario') IS NOT NULL THEN
        RAISE EXCEPTION
'v_movimento_full_diario ja existe. Este arquivo ja rodou ou alguem criou a view
por outro caminho. Nada foi aplicado.';
    END IF;

    SELECT string_agg(t || '.' || c, ', ') INTO faltando
      FROM (VALUES
            ('fact_estoque_diario', 'full_disponivel'),
            ('fact_estoque_diario', 'full_em_transferencia'),
            ('fact_estoque_diario', 'full_bloqueado_fiscal'),
            ('fact_estoque_diario', 'full_outros_indisponivel'),
            ('fact_estoque_diario', 'detalhe'),
            ('fact_vendas_snapshot', 'codigo_anuncio'),
            ('fact_vendas_snapshot', 'logistica'),
            ('fact_vendas_snapshot', 'quantidade'),
            ('dim_estoque_anuncio', 'estoque_id'),
            ('dim_estoque_anuncio', 'sku')) AS esperado(t, c)
     WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns ic
                        WHERE ic.table_schema = 'public'
                          AND ic.table_name = esperado.t
                          AND ic.column_name = esperado.c);
    IF faltando IS NOT NULL THEN
        RAISE EXCEPTION 'Colunas de origem ausentes: %. Nada foi aplicado.', faltando;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. ANTES: o que alimenta a view (só leitura)
-- -----------------------------------------------------------------------------
SELECT 'ANTES' AS momento, f.loja,
       count(*) FILTER (WHERE f.detalhe ? 'inventory_id') AS linhas_saldo_full,
       max(f.data) AS ultimo_saldo,
       (SELECT max(v.data_venda) FROM fact_vendas_snapshot v
         WHERE v.marketplace_origem = 'MERCADO LIVRE' AND v.loja_origem = f.loja) AS venda_ate
  FROM fact_estoque_diario f
 WHERE f.marketplace = 'MERCADO LIVRE'
 GROUP BY f.loja
 ORDER BY f.loja;

-- -----------------------------------------------------------------------------
-- 3. A view
-- -----------------------------------------------------------------------------
CREATE VIEW v_movimento_full_diario AS
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
       GREATEST(saldo_total - saldo_total_anterior + venda_unidades, 0)::integer
           AS entrada_implicita,
       GREATEST(-(saldo_total - saldo_total_anterior + venda_unidades), 0)::integer
           AS saida_sem_explicacao,
       (saldo_total_anterior IS NOT NULL AND venda_unidades IS NOT NULL)
           AS movimento_conhecido
  FROM base;

COMMENT ON VIEW v_movimento_full_diario IS
'Movimento diario do Full (ML) calculado na leitura: saldo = foto do Full '
'(fact_estoque_diario), venda = fact_vendas_snapshot logistica FULL pela ponte, '
'entrada implicita = saldo - saldo da vespera + venda (>0); <0 = saida sem '
'explicacao (alarme). Depois de venda_ate, ou sem saldo na vespera, movimento '
'desconhecido (NULL). Ver sql/v_movimento_full_diario.sql.';

-- -----------------------------------------------------------------------------
-- 4. Guarda do DEPOIS: uma linha por estoque e dia, e venda só até venda_ate
-- -----------------------------------------------------------------------------
DO $$
DECLARE duplicadas INTEGER; venda_depois INTEGER; linhas_view BIGINT; linhas_fonte BIGINT;
BEGIN
    SELECT count(*) INTO duplicadas FROM (
        SELECT 1 FROM v_movimento_full_diario
         GROUP BY marketplace, loja, estoque_id, data HAVING count(*) > 1) d;
    IF duplicadas > 0 THEN
        RAISE EXCEPTION '% chave(s) duplicada(s) em v_movimento_full_diario. Nada foi aplicado.',
            duplicadas;
    END IF;

    SELECT count(*) INTO venda_depois FROM v_movimento_full_diario
     WHERE data > venda_ate AND venda_unidades IS NOT NULL;
    IF venda_depois > 0 THEN
        RAISE EXCEPTION '% linha(s) com venda depois de venda_ate. Nada foi aplicado.',
            venda_depois;
    END IF;

    SELECT count(*) INTO linhas_view FROM v_movimento_full_diario;
    SELECT count(*) INTO linhas_fonte FROM fact_estoque_diario
     WHERE marketplace = 'MERCADO LIVRE' AND detalhe ? 'inventory_id';
    IF linhas_view <> linhas_fonte THEN
        RAISE EXCEPTION 'A view tem % linhas e o saldo do Full tem %. Nada foi aplicado.',
            linhas_view, linhas_fonte;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 5. DEPOIS: conferência de 16–18/09/2026 contra o /operations (a última
--    janela em que ele passou inteiro nas 4 lojas)
-- -----------------------------------------------------------------------------
SELECT 'DEPOIS' AS momento, m.loja,
       sum(m.venda_unidades) AS venda_nova,
       sum(f.unidades_vendidas - COALESCE(f.unidades_canceladas, 0)
           - COALESCE(f.unidades_canceladas_pos_envio, 0)) AS venda_operacoes_liquida,
       sum(m.entrada_implicita) AS entrada_nova,
       sum(f.unidades_entrada_coleta) AS entrada_coleta_operacoes,
       sum(m.saida_sem_explicacao) AS saida_sem_explicacao,
       count(*) FILTER (WHERE m.saida_sem_explicacao > 0) AS dias_com_saida_sem_explicacao,
       count(*) FILTER (WHERE NOT m.movimento_conhecido) AS dias_desconhecidos
  FROM v_movimento_full_diario m
  JOIN fact_estoque_diario f USING (marketplace, loja, estoque_id, data)
 WHERE m.data BETWEEN DATE '2026-09-16' AND DATE '2026-09-18'
 GROUP BY m.loja
 ORDER BY m.loja;

-- Venda FULL que a ponte não conseguiu ligar a estoque, ou ligou a estoque sem
-- linha de saldo naquele dia (fica fora da view). Esperado: perto de zero.
SELECT 'VENDA FORA DA VIEW' AS momento, u.loja, u.unidades_full,
       COALESCE(m.unidades_na_view, 0) AS unidades_na_view,
       u.unidades_full - COALESCE(m.unidades_na_view, 0) AS unidades_fora
  FROM (SELECT loja_origem AS loja, sum(quantidade) AS unidades_full
          FROM fact_vendas_snapshot
         WHERE marketplace_origem = 'MERCADO LIVRE' AND upper(logistica) = 'FULL'
           AND data_venda BETWEEN DATE '2026-09-01' AND DATE '2026-09-24'
         GROUP BY loja_origem) u
  LEFT JOIN (SELECT loja, sum(venda_unidades) AS unidades_na_view
               FROM v_movimento_full_diario
              WHERE data BETWEEN DATE '2026-09-01' AND DATE '2026-09-24'
              GROUP BY loja) m USING (loja)
 ORDER BY u.loja;

COMMIT;

-- =============================================================================
-- CONFERÊNCIA DEPOIS DO COMMIT (só leitura)
-- =============================================================================
--   SELECT loja, max(venda_ate), count(*) FILTER (WHERE movimento_conhecido)
--     FROM v_movimento_full_diario GROUP BY loja;
--   Esperado: venda_ate = data do último upload de vendas de cada loja.
--
-- =============================================================================
-- DESFAZER
-- =============================================================================
-- Nada depende desta view até o passo 2. Enquanto for assim:
--
-- BEGIN;
--   DROP VIEW v_movimento_full_diario;
-- COMMIT;
--
-- DEPOIS do passo 2, o DROP falha (v_cobertura_full e v_estoque_envio_manual
-- passam a depender dela): desfazer o passo 2 primeiro, pelo bloco dele.
-- Nenhum dado é tocado por este arquivo.
