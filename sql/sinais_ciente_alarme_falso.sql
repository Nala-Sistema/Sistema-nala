-- =============================================================================
-- Motivo "Alarme falso / não serve" no Ciente + view de contagem   [SINAIS AJUSTE]
-- =============================================================================
-- 12/10/2026, Etapa 1 do pacote "[Sinais] Pacote de ajustes v2" (item 1; plano
-- aprovado pelo Thiago). A gestora marca o sinal como "Alarme falso / não serve" (nota
-- opcional): silencia como os outros motivos e conta como calibragem para o
-- [ANALISTA PERFORMANCE] ver quais regras erram em qual loja.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se a CHECK sinal_ciente_motivo_valido não existir, se já
--      aceitar 'alarme_falso' (já aplicado) ou se a view já existir.
--   2. Troca a CHECK sinal_ciente_motivo_valido: os 4 motivos de antes + 'alarme_falso'.
--      As linhas que já existem continuam válidas (nenhuma muda).
--   3. Cria a view v_sinal_alarme_falso (SÓ leitura): por marketplace + loja + regra,
--      quantos cientes "alarme falso", quantos ainda abertos, nos últimos 14 dias, a
--      fração sobre todos os cientes da mesma regra e loja, e as últimas notas.
--   4. Conferência: a CHECK aceita o motivo novo e a view responde.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, depois do auditor-tecnico.
-- QUANDO: qualquer hora (a CHECK é recriada em milissegundos; sinal_ciente é pequena).
-- EFEITO: nenhuma linha muda. O app mostra o motivo novo assim que o código for ao ar;
--   antes disso o motivo novo seria recusado pela CHECK antiga (a tela do main ainda
--   não o oferece, então rodar este SQL antes do merge não quebra nada).
-- LEITURA: a view pertence ao dono. Quem consulta com outro usuário (ex. o
--   [ANALISTA PERFORMANCE]) precisa de GRANT SELECT na view para esse usuário — não
--   criei GRANT aqui porque o nome do usuário de leitura é decisão do Thiago.
-- DESFAZER: sql/sinais_ciente_alarme_falso_DESFAZER.sql (recusa rodar se houver
--   algum ciente 'alarme_falso' gravado).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    def text;
BEGIN
    IF to_regclass('public.sinal_ciente') IS NULL THEN
        RAISE EXCEPTION 'sinal_ciente não existe. Aplique antes sql/sinais_ciente.sql. Nada foi aplicado.';
    END IF;
    SELECT pg_get_constraintdef(oid) INTO def FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_motivo_valido';
    IF def IS NULL THEN
        RAISE EXCEPTION 'CHECK sinal_ciente_motivo_valido não existe. Nada foi aplicado.';
    END IF;
    IF def LIKE '%alarme_falso%' THEN
        RAISE EXCEPTION 'A CHECK já aceita alarme_falso. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.v_sinal_alarme_falso') IS NOT NULL THEN
        RAISE EXCEPTION 'A view v_sinal_alarme_falso já existe. Nada foi aplicado.';
    END IF;
END $$;

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_motivo_valido;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_motivo_valido CHECK (motivo IN (
    'falta_fornecedor', 'proposital', 'em_andamento', 'alarme_falso', 'outro'));

CREATE VIEW public.v_sinal_alarme_falso AS
WITH todos AS (
    SELECT marketplace, loja, regra, count(*) AS total_cientes
      FROM public.sinal_ciente
     GROUP BY marketplace, loja, regra),
falsos AS (
    SELECT marketplace, loja, regra,
           count(*)                                           AS alarmes_falsos,
           count(*) FILTER (WHERE encerrado_em IS NULL)       AS abertos,
           count(*) FILTER (WHERE criado_em >= (now() AT TIME ZONE 'America/Sao_Paulo')
                                                - interval '14 days') AS ultimos_14_dias,
           count(DISTINCT objeto)                             AS objetos_distintos,
           min(criado_em)                                     AS primeiro_em,
           max(criado_em)                                     AS ultimo_em,
           (array_agg(btrim(nota) ORDER BY criado_em DESC)
                FILTER (WHERE nullif(btrim(nota), '') IS NOT NULL))[1:3] AS ultimas_notas
      FROM public.sinal_ciente
     WHERE motivo = 'alarme_falso'
     GROUP BY marketplace, loja, regra)
SELECT f.marketplace, f.loja, f.regra, f.alarmes_falsos, f.abertos, f.ultimos_14_dias,
       f.objetos_distintos, t.total_cientes,
       round(f.alarmes_falsos::numeric / t.total_cientes, 3) AS fracao_dos_cientes,
       f.primeiro_em, f.ultimo_em, f.ultimas_notas
  FROM falsos f
  JOIN todos t USING (marketplace, loja, regra);

COMMENT ON VIEW public.v_sinal_alarme_falso IS
    'Sinais do Dia: cientes marcados "Alarme falso / não serve" por regra e loja (calibragem das réguas). Só leitura.';

DO $$
DECLARE
    def text;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO def FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_motivo_valido';
    IF def NOT LIKE '%alarme_falso%' OR def NOT LIKE '%falta_fornecedor%'
       OR def NOT LIKE '%proposital%' OR def NOT LIKE '%em_andamento%' OR def NOT LIKE '%outro%' THEN
        RAISE EXCEPTION 'A CHECK nova não tem os 5 motivos. Nada foi aplicado.';
    END IF;
    PERFORM 1 FROM public.v_sinal_alarme_falso LIMIT 1;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): a CHECK com os 5 motivos e a view (vazia até o
-- primeiro "alarme falso" ser marcado).
SELECT conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_motivo_valido';
SELECT * FROM public.v_sinal_alarme_falso;
