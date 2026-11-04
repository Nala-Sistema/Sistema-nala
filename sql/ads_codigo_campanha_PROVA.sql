-- =============================================================================
-- PROVA — o app_nala le de verdade a coluna codigo_campanha e nao grava
-- (so depois de rodar C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha.sql)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_PROVA.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: DONO do banco (neondb_owner).
-- COMO: colar o arquivo INTEIRO e executar de uma vez. Trecho rodado separado
--       perde a transacao e a permissao temporaria ficaria gravada.
--
-- POR QUE ASSIM
--   O app_nala ainda nao tem senha (NOLOGIN), entao ninguem entra com ele. O dono
--   tambem nao pode "virar" app_nala hoje. Aqui, dentro da transacao, o dono ganha
--   essa permissao, vira app_nala, le e tenta gravar. Termina em ROLLBACK: a
--   permissao temporaria e tudo o mais desaparecem. NAO ALTERA NADA NO BANCO.
--
-- RESULTADO ESPERADO: o editor mostra uma aba por comando. Antes da aba do ROLLBACK,
-- tabela com quem = app_nala e as duas tabelas; na ultima aba,
-- dono_pode_virar_app_nala = false.
-- SE DER ERRO: a prova nao passou. Rode a parte a ULTIMA consulta deste arquivo; se
-- der true, rode o REVOKE do comentario acima dela.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

GRANT app_nala TO neondb_owner WITH INHERIT FALSE, SET TRUE;

SET LOCAL ROLE app_nala;

DO $$
DECLARE
    t text;
    n bigint;
BEGIN
    IF current_user <> 'app_nala' THEN
        RAISE EXCEPTION 'PROVA: esperava app_nala, estou como %', current_user;
    END IF;
    FOREACH t IN ARRAY ARRAY['fact_ads_performance', 'fact_ads_campanha_config'] LOOP
        EXECUTE format('SELECT count(codigo_campanha) FROM public.%I', t) INTO n;
        BEGIN
            EXECUTE format('UPDATE public.%I SET codigo_campanha = codigo_campanha WHERE false', t);
            RAISE EXCEPTION 'PROVA: app_nala conseguiu gravar em %', t USING ERRCODE = 'P0002';
        EXCEPTION WHEN insufficient_privilege THEN
            NULL;  -- e o esperado: recusado
        END;
    END LOOP;
END $$;

SELECT current_user AS quem, 'fact_ads_performance' AS tabela, count(*) AS linhas_lidas,
       count(codigo_campanha) AS preenchidas
  FROM public.fact_ads_performance
UNION ALL
SELECT current_user, 'fact_ads_campanha_config', count(*), count(codigo_campanha)
  FROM public.fact_ads_campanha_config;

ROLLBACK;

-- CONFERE QUE NAO FICOU RASTRO: tem de dar dono_pode_virar_app_nala = false.
-- Se der true, avise: e so rodar  REVOKE app_nala FROM neondb_owner GRANTED BY neondb_owner;
SELECT current_user AS quem, pg_has_role('neondb_owner', 'app_nala', 'SET') AS dono_pode_virar_app_nala;
