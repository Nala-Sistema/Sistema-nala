-- =============================================================================
-- CODIGO DA CAMPANHA — coluna codigo_campanha nas duas tabelas de ads
-- (frente [CAMPANHA ID], Fase 2, 04/11/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: DONO do banco (neondb_owner).
-- QUANDO: NUNCA entre 04:00 e 06:30 (Brasília), janela dos coletores, nem das
--         12:00 as 13:00 se a coleta de ads da madrugada falhou (reserva).
-- COMO: colar o arquivo INTEIRO e executar de uma vez. Trecho rodado separado
--       perde a transacao. "lock timeout" = nada foi gravado, basta repetir.
-- ANTES DE RODAR: passar pelo auditor-tecnico.
-- DEPOIS DE RODAR: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_PROVA.sql
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_DESFAZER.sql
--
-- POR QUE
--   No ML o id_campanha e o NOME da campanha: quando o nome muda, a historia
--   da campanha quebra em duas (Fita Isolante e ABNY 4X300, 28/10). O codigo
--   numerico da API nao muda com o nome (conferido no mapa do coletor).
--
-- O QUE FAZ
--   1. Fotografa linhas e gasto por loja nas duas tabelas.
--   2. Cria codigo_campanha (pode ficar vazia, sem valor padrao) nas duas. Nao
--      reescreve a tabela e nao preenche nada: o coletor e o preenchimento do
--      historico vem depois.
--   3. GRANT SELECT ao app_nala. Ele ja le as duas tabelas; o GRANT fica aqui
--      para o arquivo dizer sozinho quem le a coluna. Nao muda nada hoje.
--   4. Confere tudo. Se algo sair do desenho, da ERRO e nada fica gravado.
--
-- O QUE NAO FAZ (de proposito)
--   - NAO mexe em id_campanha: continua sendo o nome no ML, e tem de ser igual
--     em campanha e em performance, senao a recoleta duplica o gasto.
--   - NAO mexe em ux_ads_perf_dia_anuncio_campanha nem ux_campanha_config_id:
--     a coleta depende deles (erro 42P10 perde a noite de gasto). A prova
--     confere o texto dos dois.
--   - NAO cria indice: tabelas pequenas (cerca de 10 mil linhas cada).
--   - NAO mexe na Shopee: la a coluna fica vazia e os leitores usam o
--     id_campanha, que ja e o codigo numerico.
--
-- ORDEM DA FASE 2
--   1. este arquivo; 2. coletor em main (grava o codigo); 3. conferir a coleta
--   da noite; 4. preencher o historico; 5. leitores, cada um no seu merge.
--
-- RESULTADO ESPERADO: o editor mostra uma aba por comando. A ultima e a do COMMIT;
-- a PENULTIMA traz a tabela, com "igual" em todas as linhas e preenchidas = 0.
-- Pode rodar de novo: nao duplica nada (preenchidas so e 0 na primeira vez).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

CREATE TEMP TABLE _ads_antes ON COMMIT DROP AS
    SELECT 'fact_ads_performance'::text AS tabela, marketplace, loja,
           count(*) AS linhas, sum(gasto_ads) AS gasto
      FROM public.fact_ads_performance GROUP BY 2, 3
    UNION ALL
    SELECT 'fact_ads_campanha_config', marketplace, loja, count(*), NULL::numeric
      FROM public.fact_ads_campanha_config GROUP BY 2, 3;

ALTER TABLE public.fact_ads_performance
    ADD COLUMN IF NOT EXISTS codigo_campanha varchar(200);
ALTER TABLE public.fact_ads_campanha_config
    ADD COLUMN IF NOT EXISTS codigo_campanha varchar(200);

COMMENT ON COLUMN public.fact_ads_performance.codigo_campanha IS
    'ML: campaign_id numerico da API, nao muda quando a campanha e renomeada. Vazia na Shopee (id_campanha ja e o codigo). Entra pelo coletor e pelo preenchimento do historico.';
COMMENT ON COLUMN public.fact_ads_campanha_config.codigo_campanha IS
    'ML: campaign_id numerico da API, nao muda quando a campanha e renomeada. Vazia na Shopee (id_campanha ja e o codigo). Entra pelo coletor e pelo preenchimento do historico.';

GRANT SELECT ON public.fact_ads_performance, public.fact_ads_campanha_config TO app_nala;

DO $$
DECLARE
    t        text;
    r        record;
    diferenca bigint;
BEGIN
    IF current_user <> 'neondb_owner' THEN
        RAISE EXCEPTION 'PARADO: rode como DONO (neondb_owner), nao como %', current_user;
    END IF;

    FOREACH t IN ARRAY ARRAY['fact_ads_performance', 'fact_ads_campanha_config'] LOOP
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = t
                          AND column_name = 'codigo_campanha'
                          AND data_type = 'character varying'
                          AND character_maximum_length = 200
                          AND is_nullable = 'YES' AND column_default IS NULL) THEN
            RAISE EXCEPTION 'PROVA: coluna fora do desenho em %', t;
        END IF;

        -- quem precisa ler
        FOR r IN SELECT unnest(ARRAY['app_nala', 'agentes_leitura', 'patricia_leitura',
                                     'larissa_leitura', 'sinais_historico',
                                     'coletor_ads_ml']) AS papel LOOP
            IF NOT has_column_privilege(r.papel, 'public.' || t, 'codigo_campanha', 'SELECT') THEN
                RAISE EXCEPTION 'PROVA: % nao le %.codigo_campanha', r.papel, t;
            END IF;
        END LOOP;
        -- quem grava: so o coletor (a coleta usa INSERT ... ON CONFLICT DO UPDATE)
        IF NOT has_column_privilege('coletor_ads_ml', 'public.' || t, 'codigo_campanha', 'INSERT')
           OR NOT has_column_privilege('coletor_ads_ml', 'public.' || t, 'codigo_campanha', 'UPDATE') THEN
            RAISE EXCEPTION 'PROVA: coletor_ads_ml nao grava %.codigo_campanha', t;
        END IF;
        -- quem nao pode gravar
        FOR r IN SELECT unnest(ARRAY['app_nala', 'agentes_leitura', 'patricia_leitura',
                                     'larissa_leitura', 'sinais_historico']) AS papel LOOP
            IF has_column_privilege(r.papel, 'public.' || t, 'codigo_campanha', 'INSERT')
               OR has_column_privilege(r.papel, 'public.' || t, 'codigo_campanha', 'UPDATE') THEN
                RAISE EXCEPTION 'PROVA: % nao deveria gravar %.codigo_campanha', r.papel, t;
            END IF;
        END LOOP;
    END LOOP;

    -- as duas chaves da coleta, intocadas
    IF (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'
         AND indexname = 'ux_ads_perf_dia_anuncio_campanha') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_ads_perf_dia_anuncio_campanha ON public.fact_ads_performance USING btree (marketplace, loja, periodo_inicio, periodo_fim, codigo_anuncio, id_campanha)' THEN
        RAISE EXCEPTION 'PROVA: ux_ads_perf_dia_anuncio_campanha mudou';
    END IF;
    IF (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public'
         AND indexname = 'ux_campanha_config_id') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_campanha_config_id ON public.fact_ads_campanha_config USING btree (marketplace, loja, id_campanha, data_captura)' THEN
        RAISE EXCEPTION 'PROVA: ux_campanha_config_id mudou';
    END IF;

    -- linhas e gasto por loja iguais aos de antes do ALTER
    SELECT count(*) INTO diferenca FROM (
        (SELECT * FROM _ads_antes
         EXCEPT
         SELECT 'fact_ads_performance', marketplace, loja, count(*), sum(gasto_ads)
           FROM public.fact_ads_performance GROUP BY 2, 3
         EXCEPT
         SELECT 'fact_ads_campanha_config', marketplace, loja, count(*), NULL::numeric
           FROM public.fact_ads_campanha_config GROUP BY 2, 3)
        UNION ALL
        (SELECT 'fact_ads_performance', marketplace, loja, count(*), sum(gasto_ads)
           FROM public.fact_ads_performance GROUP BY 2, 3
         UNION ALL
         SELECT 'fact_ads_campanha_config', marketplace, loja, count(*), NULL::numeric
           FROM public.fact_ads_campanha_config GROUP BY 2, 3
         EXCEPT
         SELECT * FROM _ads_antes)
    ) x;
    IF diferenca <> 0 THEN
        RAISE EXCEPTION 'PROVA: linhas ou gasto mudaram em % loja(s)', diferenca;
    END IF;
END $$;

-- PROVA PARA O OLHO
WITH agora AS (
    SELECT 'fact_ads_performance'::text AS tabela, marketplace, loja,
           count(*) AS linhas, sum(gasto_ads) AS gasto,
           count(codigo_campanha) AS preenchidas
      FROM public.fact_ads_performance GROUP BY 2, 3
    UNION ALL
    SELECT 'fact_ads_campanha_config', marketplace, loja, count(*), NULL::numeric,
           count(codigo_campanha)
      FROM public.fact_ads_campanha_config GROUP BY 2, 3
)
SELECT g.tabela, g.marketplace, g.loja, a.linhas AS linhas_antes, g.linhas AS linhas_depois,
       a.gasto AS gasto_antes, g.gasto AS gasto_depois, g.preenchidas,
       CASE WHEN a.linhas = g.linhas AND a.gasto IS NOT DISTINCT FROM g.gasto
            THEN 'igual' ELSE 'DIFERENTE' END AS confere
  FROM agora g
  JOIN _ads_antes a USING (tabela, marketplace, loja)
 ORDER BY 1, 2, 3;

COMMIT;
