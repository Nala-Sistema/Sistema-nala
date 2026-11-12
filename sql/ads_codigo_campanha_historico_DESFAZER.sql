-- =============================================================================
-- DESFAZER — preenchimento do historico de codigo_campanha
-- (de C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico.sql)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico_DESFAZER.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: DONO do banco (neondb_owner).
-- QUANDO: NUNCA entre 05:15 e 06:30 (Brasília), janela dos coletores (o arquivo
--         recusa), nem das 12:00 as 13:00 se a coleta da madrugada falhou (reserva).
-- COMO: colar o arquivo INTEIRO e executar de uma vez. Trecho rodado separado
--       perde a transacao. "lock timeout" = nada foi gravado, basta repetir.
--
-- ANTES: se algum leitor ja agrupa por codigo (gerar_ciclo_ml, Sinais, telas), volte
-- o leitor primeiro: sem o historico, a comparacao por campanha quebra de novo.
--
-- O QUE FAZ: volta para vazio o codigo do ML em performance antes de 01/11 e em foto
-- antes de 04/11 14:00, exatamente o que o preenchimento pode ter gravado.
-- O QUE NAO FAZ: nao toca no que o coletor grava (performance de 01/11 em diante, foto
-- de 04/11 14:38 em diante), nao apaga a coluna, nao toca na Shopee.
-- Se alguem tiver rodado backfill_ads.py para um dia antes de 01/11, o codigo dele
-- tambem volta para vazio. Pode rodar de novo.
--
-- RESULTADO ESPERADO: na penultima aba, por tabela e loja, confere = igual e
-- antes_do_corte_com_codigo = 0.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF current_user <> 'neondb_owner' THEN
        RAISE EXCEPTION 'PARADO: rode como DONO (neondb_owner), nao como %', current_user;
    END IF;
    IF (now() AT TIME ZONE 'America/Sao_Paulo')::time BETWEEN TIME '05:15' AND TIME '06:30' THEN
        RAISE EXCEPTION 'PARADO: janela dos coletores (05:15 a 06:30). Rode depois.';
    END IF;
END $$;

SET LOCAL lock_timeout = '5s';

-- Segura a escrita do coletor enquanto roda (segundos). Leitura segue normal.
LOCK TABLE public.fact_ads_performance, public.fact_ads_campanha_config
    IN SHARE ROW EXCLUSIVE MODE;

-- Foto ANTES de cada linha: tudo menos codigo_campanha, inclusive id_campanha,
-- data_importacao e as linhas da Shopee.
CREATE TEMP TABLE _foto ON COMMIT DROP AS
    SELECT 'perf'::text AS tabela, t.id, t.marketplace, t.loja, t.gasto_ads AS gasto,
           t.codigo_campanha AS codigo_antes,
           md5((to_jsonb(t) - 'codigo_campanha')::text) AS resto
      FROM public.fact_ads_performance t
    UNION ALL
    SELECT 'config', t.id, t.marketplace, t.loja, NULL::numeric, t.codigo_campanha,
           md5((to_jsonb(t) - 'codigo_campanha')::text)
      FROM public.fact_ads_campanha_config t;

UPDATE public.fact_ads_performance
   SET codigo_campanha = NULL
 WHERE marketplace = 'MERCADO LIVRE' AND data < DATE '2026-11-01' AND codigo_campanha IS NOT NULL;

UPDATE public.fact_ads_campanha_config
   SET codigo_campanha = NULL
 WHERE marketplace = 'MERCADO LIVRE' AND data_captura < TIMESTAMP '2026-11-04 14:00'
   AND codigo_campanha IS NOT NULL;

DO $$
DECLARE
    n bigint;
BEGIN
    -- nada alem do codigo mudou, nenhuma linha entrou nem saiu
    SELECT count(*) INTO n FROM (
        SELECT f.id FROM _foto f
          LEFT JOIN public.fact_ads_performance t ON t.id = f.id
         WHERE f.tabela = 'perf' AND (t.id IS NULL OR md5((to_jsonb(t) - 'codigo_campanha')::text) <> f.resto)
        UNION ALL
        SELECT f.id FROM _foto f
          LEFT JOIN public.fact_ads_campanha_config t ON t.id = f.id
         WHERE f.tabela = 'config' AND (t.id IS NULL OR md5((to_jsonb(t) - 'codigo_campanha')::text) <> f.resto)
    ) x;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: % linha(s) mudaram fora do codigo_campanha', n;
    END IF;
    IF (SELECT count(*) FROM public.fact_ads_performance) <> (SELECT count(*) FROM _foto WHERE tabela = 'perf')
       OR (SELECT count(*) FROM public.fact_ads_campanha_config) <> (SELECT count(*) FROM _foto WHERE tabela = 'config') THEN
        RAISE EXCEPTION 'PROVA: o numero de linhas mudou';
    END IF;

    -- soma de gasto por loja, antes x depois, nesta rodada
    SELECT count(*) INTO n FROM (
        SELECT loja FROM _foto WHERE tabela = 'perf' GROUP BY 1, marketplace
        HAVING sum(gasto) IS DISTINCT FROM (SELECT sum(p.gasto_ads) FROM public.fact_ads_performance p
                                             WHERE p.loja = _foto.loja AND p.marketplace = _foto.marketplace)
    ) x;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: a soma de gasto mudou em % loja(s)', n;
    END IF;

    -- Shopee continua vazia
    IF EXISTS (SELECT 1 FROM public.fact_ads_performance WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL)
       OR EXISTS (SELECT 1 FROM public.fact_ads_campanha_config WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL) THEN
        RAISE EXCEPTION 'PROVA: apareceu codigo fora do Mercado Livre';
    END IF;

    -- indices da coleta intocados
    IF (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_ads_perf_dia_anuncio_campanha') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_ads_perf_dia_anuncio_campanha ON public.fact_ads_performance USING btree (marketplace, loja, periodo_inicio, periodo_fim, codigo_anuncio, id_campanha)'
       OR (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_campanha_config_id') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_campanha_config_id ON public.fact_ads_campanha_config USING btree (marketplace, loja, id_campanha, data_captura)' THEN
        RAISE EXCEPTION 'PROVA: um indice da coleta mudou';
    END IF;

    -- o que o coletor gravou fica igual
    SELECT count(*) INTO n FROM _foto f
      JOIN public.fact_ads_performance t ON t.id = f.id
     WHERE f.tabela = 'perf' AND t.data >= DATE '2026-11-01' AND t.codigo_campanha IS DISTINCT FROM f.codigo_antes;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: % linha(s) do coletor mudaram (performance)', n;
    END IF;
    SELECT count(*) INTO n FROM _foto f
      JOIN public.fact_ads_campanha_config t ON t.id = f.id
     WHERE f.tabela = 'config' AND t.data_captura >= TIMESTAMP '2026-11-04 14:00' AND t.codigo_campanha IS DISTINCT FROM f.codigo_antes;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: % linha(s) do coletor mudaram (foto)', n;
    END IF;
END $$;

-- PROVA PARA O OLHO (penultima aba)
WITH agora AS (
    SELECT 'perf'::text AS tabela, marketplace, loja, id, gasto_ads AS gasto, codigo_campanha,
           data < DATE '2026-11-01' AS antes_do_corte
      FROM public.fact_ads_performance
    UNION ALL
    SELECT 'config', marketplace, loja, id, NULL::numeric, codigo_campanha,
           data_captura < TIMESTAMP '2026-11-04 14:00'
      FROM public.fact_ads_campanha_config
)
SELECT g.tabela, g.marketplace, g.loja, count(*) AS linhas,
       count(*) FILTER (WHERE g.antes_do_corte AND g.codigo_campanha IS NOT NULL) AS antes_do_corte_com_codigo,
       count(*) FILTER (WHERE NOT g.antes_do_corte AND g.codigo_campanha IS NOT NULL) AS do_coletor_com_codigo,
       sum(f.gasto) AS gasto_antes, sum(g.gasto) AS gasto_depois,
       CASE WHEN sum(f.gasto) IS NOT DISTINCT FROM sum(g.gasto) THEN 'igual' ELSE 'DIFERENTE' END AS confere
  FROM agora g
  JOIN _foto f ON f.tabela = g.tabela AND f.id = g.id
 GROUP BY 1, 2, 3
 ORDER BY 1, 2, 3;

COMMIT;
