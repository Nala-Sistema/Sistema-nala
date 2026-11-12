-- =============================================================================
-- DESFAZER — coluna codigo_campanha
-- (de C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha.sql)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_DESFAZER.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: DONO do banco (neondb_owner).
-- QUANDO: NUNCA entre 04:00 e 06:30 (Brasília), janela dos coletores, nem das
--         12:00 as 13:00 se a coleta de ads da madrugada falhou (reserva).
-- COMO: colar o arquivo INTEIRO e executar de uma vez. Trecho rodado separado
--       perde a transacao. "lock timeout" = nada foi gravado, basta repetir.
--
-- ANTES, NESTA ORDEM (o que le ou grava a coluna sai primeiro):
--   1. o app e o gerar_ciclo_ml voltam a nao usar codigo_campanha;
--   2. o coletor (nala-coletor-ml) volta a nao gravar a coluna. Se ele ainda
--      gravar, o INSERT da coleta quebra e a noite de gasto se perde.
--
-- TRAVA: recusa (nada e apagado) se houver linha IMPORTADA nos ultimos 3 dias com
-- codigo: pode ser coletor novo ainda no ar. Depois de tirar o coletor, esperar
-- 3 dias. Preenchimento pelo mapa em linha importada nesses 3 dias tambem dispara
-- a trava; em linha mais antiga, nao. Essa trava nao ve coletor que acabou de
-- entrar em main e ainda nao rodou, nem o app: a ordem acima e responsabilidade
-- de quem roda.
--
-- EFEITO: a coluna some das duas tabelas. O codigo gravado se perde, mas volta
-- (coletor + preenchimento pelo mapa). NAO faz REVOKE: o app_nala ja lia as duas
-- tabelas antes da coluna. Sem CASCADE: se alguma view usar a coluna, da erro e
-- nada e apagado. Indices e id_campanha nao mudam. Pode rodar de novo.
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

DO $$
DECLARE
    t      text;
    n      bigint;
BEGIN
    IF current_user <> 'neondb_owner' THEN
        RAISE EXCEPTION 'PARADO: rode como DONO (neondb_owner), nao como %', current_user;
    END IF;
    FOREACH t IN ARRAY ARRAY['fact_ads_performance', 'fact_ads_campanha_config'] LOOP
        IF EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = t
                      AND column_name = 'codigo_campanha') THEN
            EXECUTE format('SELECT count(*) FROM public.%I
                             WHERE codigo_campanha IS NOT NULL
                               AND data_importacao >= now() - interval ''3 days''', t) INTO n;
            IF n > 0 THEN
                RAISE EXCEPTION 'PARADO: % linha(s) de % importadas nos ultimos 3 dias com codigo. Tire o coletor novo do ar e espere 3 dias.', n, t;
            END IF;
        END IF;
    END LOOP;
END $$;

ALTER TABLE public.fact_ads_performance DROP COLUMN IF EXISTS codigo_campanha;
ALTER TABLE public.fact_ads_campanha_config DROP COLUMN IF EXISTS codigo_campanha;

-- PROVA PARA O OLHO (penultima aba, antes do COMMIT): coluna fora e nada mudou
-- (confere = igual em todas as linhas; colunas_restantes = 0)
WITH agora AS (
    SELECT 'fact_ads_performance'::text AS tabela, marketplace, loja,
           count(*) AS linhas, sum(gasto_ads) AS gasto
      FROM public.fact_ads_performance GROUP BY 2, 3
    UNION ALL
    SELECT 'fact_ads_campanha_config', marketplace, loja, count(*), NULL::numeric
      FROM public.fact_ads_campanha_config GROUP BY 2, 3
)
SELECT g.tabela, g.marketplace, g.loja, g.linhas, g.gasto,
       CASE WHEN a.linhas = g.linhas AND a.gasto IS NOT DISTINCT FROM g.gasto
            THEN 'igual' ELSE 'DIFERENTE' END AS confere,
       (SELECT count(*) FROM information_schema.columns
         WHERE table_schema = 'public' AND column_name = 'codigo_campanha'
           AND table_name IN ('fact_ads_performance', 'fact_ads_campanha_config')) AS colunas_restantes
  FROM agora g
  JOIN _ads_antes a USING (tabela, marketplace, loja)
 ORDER BY 1, 2, 3;

COMMIT;
