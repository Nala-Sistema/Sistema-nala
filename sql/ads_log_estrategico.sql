-- =============================================================================
-- ads_log_estrategico + v_ads_log_vigente + usuário ciclo_ads   [HISTÓRICO E AÇÕES] etapa 1
-- =============================================================================
-- 21/10/2026 (plano aprovado pelo Thiago, com as 4 decisões). O LOG ESTRATÉGICO
-- de ads (memória da série, uma linha por evento "[Ciclo] | [Produto/MLB] |
-- [TIPO] | [1 linha]") sai dos arquivos .md e passa a morar aqui. Agnóstico de
-- marketplace (decisão de 23/07): ML e Shopee na mesma tabela, cada linha com
-- marketplace e loja. Depois desta etapa o histórico OFICIAL é a tabela; o
-- arquivo vira só cópia.
--
-- SÓ ACRESCENTA: UPDATE, DELETE e TRUNCATE são recusados por gatilho para
-- TODO MUNDO, inclusive o dono. Correção = linha nova numa rodada nova.
--
-- RODADA = uma gravação de uma versão do log (a sessão do rodar_ciclo, ou um
-- arquivo da importação). A versão VIGENTE de cada (serie, ciclo) é a da rodada
-- mais recente (rodada_em): é o que a view v_ads_log_vigente mostra. Assim o
-- --refazer de um ciclo grava rodada nova e a antiga continua guardada; as
-- linhas reescritas entre versões cumulativas dos logs antigos entram como
-- rodada anterior e não aparecem na leitura padrão.
--
-- SÉRIE = de qual log a linha veio: ML-LPT, ML-Nala (um log por loja) ou SHOPEE
-- (um log só para as 3 lojas da Shopee; a loja vem de cada linha; "Geral
-- (todas as lojas)" = loja TODAS).
--
-- O GATILHO DE ENTRADA (SECURITY DEFINER, só lê dim_lojas) garante:
--   - loja existe em dim_lojas COM O MESMO marketplace (ou é TODAS): uma linha
--     da Shopee nunca fica com loja do ML e vice-versa;
--   - se a série tem nome de loja (ML-LPT, ML-Nala), a loja da linha é ela;
--     se não tem (SHOPEE), o marketplace da linha é a própria série (R1 do
--     auditor, 21/10: sem isso ('SHOPEE','MERCADO LIVRE','ML-Nala') entrava);
--   - gravado_em = agora e gravado_por = o usuário que conectou (não dá para
--     informar outro).
--
-- QUEM GRAVA: SÓ o usuário NOVO ciclo_ads, criado aqui NOLOGIN (o script
--   C:\Users\thiag\Nala-Analises\definir_senha_ciclo.py libera o login com a
--   senha e põe a URL só na área de transferência). Ele tem INSERT nesta
--   tabela e USAGE na sequência; nada de SELECT, UPDATE ou DELETE, nada em
--   outra tabela. Usado pela importação (importar_log_historico.py) e pelo
--   rodar_ciclo.py, com a variável de usuário do Windows NALA_CICLO_DB_URL.
-- QUEM LÊ: agentes_leitura (analistas, pelo nala_sql.py) e patricia_leitura,
--   na tabela e na view.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se a tabela, a view, as funções ou o usuário já
--      existirem, ou se faltar dim_lojas / agentes_leitura / patricia_leitura.
--   2. CREATE TABLE + CHECKs + UNIQUE + índices.
--   3. Gatilhos: entrada (valida loja, carimba quem/quando) e só acrescenta.
--   4. View v_ads_log_vigente.
--   5. CREATE ROLE ciclo_ads NOLOGIN + GRANTs; SELECT para os dois leitores.
--   6. Conferência: CHECKs, índices, gatilhos, tabela vazia, usuário NOLOGIN,
--      cada privilégio esperado e os que NÃO podem existir; senão aborta.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, DEPOIS do auditor-tecnico.
-- QUANDO: qualquer hora fora da janela dos coletores (04:00–06:30). A tabela
--   é nova; os GRANTs não travam nada.
-- DEPOIS: passo a passo em C:\Users\thiag\Nala-Analises\PASSO_A_PASSO_ETAPA1_HISTORICO.md
-- DESFAZER: sql/ads_log_estrategico_DESFAZER.sql.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF to_regclass('public.ads_log_estrategico') IS NOT NULL THEN
        RAISE EXCEPTION 'A tabela ads_log_estrategico já existe. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.v_ads_log_vigente') IS NOT NULL THEN
        RAISE EXCEPTION 'A view v_ads_log_vigente já existe. Nada foi aplicado.';
    END IF;
    IF to_regprocedure('public.ads_log_entrada()') IS NOT NULL
       OR to_regprocedure('public.ads_log_so_acrescenta()') IS NOT NULL THEN
        RAISE EXCEPTION 'As funções do ads_log já existem. Nada foi aplicado.';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ciclo_ads') THEN
        RAISE EXCEPTION 'O usuário ciclo_ads já existe. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.dim_lojas') IS NULL THEN
        RAISE EXCEPTION 'dim_lojas não existe. Nada foi aplicado.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentes_leitura')
       OR NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'patricia_leitura') THEN
        RAISE EXCEPTION 'Falta o usuário agentes_leitura ou patricia_leitura. Nada foi aplicado.';
    END IF;
END $$;

-- ----------------------------------------------------------------------------- 2. tabela
CREATE TABLE public.ads_log_estrategico (
    id                bigserial     PRIMARY KEY,
    serie             varchar(30)   NOT NULL,
    marketplace       varchar(30)   NOT NULL,
    loja              varchar(60)   NOT NULL,
    ciclo             varchar(30)   NOT NULL,
    periodo_ini       date,
    periodo_fim       date,
    tipo_linha        varchar(10)   NOT NULL,
    objeto            text          NOT NULL,
    codigo_anuncio    varchar(30),
    codigo_inferido   boolean       NOT NULL DEFAULT false,
    tipo              text          NOT NULL,
    descricao         text          NOT NULL,
    linha_original    text          NOT NULL,
    linha_hash        char(32)      GENERATED ALWAYS AS (md5(linha_original)) STORED,
    arquivo_origem    text          NOT NULL,
    linha_no_arquivo  integer       NOT NULL,
    rodada            varchar(120)  NOT NULL,
    rodada_em         timestamp     NOT NULL,
    gravado_em        timestamp     NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    gravado_por       varchar(63)   NOT NULL DEFAULT session_user,
    CONSTRAINT ads_log_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT ads_log_sem_vazio CHECK (
        btrim(serie) <> '' AND btrim(loja) <> '' AND btrim(ciclo) <> '' AND btrim(objeto) <> ''
        AND btrim(tipo) <> '' AND btrim(descricao) <> '' AND btrim(linha_original) <> ''
        AND btrim(arquivo_origem) <> '' AND btrim(rodada) <> ''),
    CONSTRAINT ads_log_tipo_linha CHECK (tipo_linha IN ('evento', 'principio')),
    CONSTRAINT ads_log_periodo CHECK (
        (periodo_ini IS NULL AND periodo_fim IS NULL)
        OR (periodo_ini IS NOT NULL AND periodo_fim IS NOT NULL
            AND periodo_fim >= periodo_ini AND periodo_fim <= periodo_ini + 14)),
    CONSTRAINT ads_log_codigo_inferido CHECK (NOT codigo_inferido OR codigo_anuncio IS NOT NULL),
    CONSTRAINT ads_log_linha_no_arquivo CHECK (linha_no_arquivo > 0),
    -- a mesma linha não entra 2x na mesma rodada (importação idempotente:
    -- INSERT ... ON CONFLICT DO NOTHING, sem alvo, não exige SELECT)
    CONSTRAINT ads_log_uma_vez_por_rodada UNIQUE (serie, ciclo, rodada, linha_hash)
);

CREATE INDEX ix_ads_log_serie_ciclo ON public.ads_log_estrategico (serie, ciclo, rodada_em);
CREATE INDEX ix_ads_log_codigo ON public.ads_log_estrategico (codigo_anuncio)
    WHERE codigo_anuncio IS NOT NULL;
CREATE INDEX ix_ads_log_loja ON public.ads_log_estrategico (marketplace, loja, periodo_ini);

COMMENT ON TABLE public.ads_log_estrategico IS
    'Log estratégico de ads (memória da série), ML e Shopee. SÓ ACRESCENTA (gatilho recusa UPDATE/DELETE/TRUNCATE). Grava só ciclo_ads. Versão vigente: v_ads_log_vigente.';
COMMENT ON COLUMN public.ads_log_estrategico.serie IS
    'De qual log veio: ML-LPT, ML-Nala (um por loja) ou SHOPEE (um log para as 3 lojas).';
COMMENT ON COLUMN public.ads_log_estrategico.rodada IS
    'Uma gravação de uma versão do log (sessão do rodar_ciclo ou arquivo da importação). Vigente = a de rodada_em mais recente por (serie, ciclo).';
COMMENT ON COLUMN public.ads_log_estrategico.codigo_inferido IS
    'true = o MLB não estava na linha; foi herdado de outra linha da mesma série com o mesmo nome de produto (nome com um MLB só).';

-- ----------------------------------------------------------------------------- 3. gatilhos
-- Entrada: valida a loja contra dim_lojas e carimba quem/quando. SECURITY
-- DEFINER só para ler dim_lojas (o ciclo_ads não tem SELECT nela); session_user
-- é quem conectou (dentro do SECURITY DEFINER current_user seria o dono).
CREATE FUNCTION public.ads_log_entrada() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
BEGIN
    NEW.gravado_em  := now() AT TIME ZONE 'America/Sao_Paulo';
    NEW.gravado_por := session_user;
    IF NEW.loja = 'TODAS' THEN
        IF NOT EXISTS (SELECT 1 FROM public.dim_lojas WHERE marketplace = NEW.marketplace) THEN
            RAISE EXCEPTION 'ads_log: marketplace % não existe em dim_lojas.', NEW.marketplace;
        END IF;
    ELSIF NOT EXISTS (SELECT 1 FROM public.dim_lojas
                       WHERE loja = NEW.loja AND marketplace = NEW.marketplace) THEN
        RAISE EXCEPTION 'ads_log: loja % não existe em dim_lojas com o marketplace %.',
            NEW.loja, NEW.marketplace;
    END IF;
    IF EXISTS (SELECT 1 FROM public.dim_lojas WHERE loja = NEW.serie) THEN
        IF NEW.loja <> NEW.serie THEN
            RAISE EXCEPTION 'ads_log: a série % é de uma loja só; a linha veio com a loja %.',
                NEW.serie, NEW.loja;
        END IF;
    ELSIF NEW.marketplace <> NEW.serie THEN
        -- série que não é loja (SHOPEE) = um marketplace inteiro: a linha é dele
        RAISE EXCEPTION 'ads_log: a série % não é loja; o marketplace da linha tem de ser %, veio %.',
            NEW.serie, NEW.serie, NEW.marketplace;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.ads_log_entrada() FROM PUBLIC;

CREATE FUNCTION public.ads_log_so_acrescenta() RETURNS trigger
    LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'ads_log_estrategico só aceita linha nova: % recusado. Correção = linha nova numa rodada nova.', TG_OP;
END $$;
REVOKE ALL ON FUNCTION public.ads_log_so_acrescenta() FROM PUBLIC;

CREATE TRIGGER ads_log_entrada BEFORE INSERT ON public.ads_log_estrategico
    FOR EACH ROW EXECUTE FUNCTION public.ads_log_entrada();
CREATE TRIGGER ads_log_sem_update_delete BEFORE UPDATE OR DELETE ON public.ads_log_estrategico
    FOR EACH ROW EXECUTE FUNCTION public.ads_log_so_acrescenta();
CREATE TRIGGER ads_log_sem_truncate BEFORE TRUNCATE ON public.ads_log_estrategico
    FOR EACH STATEMENT EXECUTE FUNCTION public.ads_log_so_acrescenta();

-- ----------------------------------------------------------------------------- 4. view
CREATE VIEW public.v_ads_log_vigente AS
SELECT l.*
  FROM public.ads_log_estrategico l
 WHERE l.rodada = (SELECT l2.rodada
                     FROM public.ads_log_estrategico l2
                    WHERE l2.serie = l.serie AND l2.ciclo = l.ciclo
                    ORDER BY l2.rodada_em DESC, l2.rodada DESC
                    LIMIT 1);

COMMENT ON VIEW public.v_ads_log_vigente IS
    'Log estratégico de ads, só a versão vigente de cada (serie, ciclo): a rodada mais recente. Leitura padrão dos analistas.';

-- ----------------------------------------------------------------------------- 5. usuários
CREATE ROLE ciclo_ads NOLOGIN;

GRANT USAGE ON SCHEMA public TO ciclo_ads;
GRANT INSERT ON public.ads_log_estrategico TO ciclo_ads;
GRANT USAGE ON SEQUENCE public.ads_log_estrategico_id_seq TO ciclo_ads;

GRANT SELECT ON public.ads_log_estrategico, public.v_ads_log_vigente TO agentes_leitura, patricia_leitura;

-- ----------------------------------------------------------------------------- 6. conferência
DO $$
DECLARE
    n int;
    t text;
BEGIN
    SELECT count(*) INTO n FROM pg_constraint
     WHERE conrelid = 'public.ads_log_estrategico'::regclass AND contype = 'c';
    IF n <> 6 THEN
        RAISE EXCEPTION 'Esperava 6 CHECKs em ads_log_estrategico, achei %. Nada foi aplicado.', n;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.ads_log_estrategico'::regclass
                      AND conname = 'ads_log_uma_vez_por_rodada' AND contype = 'u') THEN
        RAISE EXCEPTION 'UNIQUE ads_log_uma_vez_por_rodada não foi criada. Nada foi aplicado.';
    END IF;
    FOREACH t IN ARRAY ARRAY['ix_ads_log_serie_ciclo', 'ix_ads_log_codigo', 'ix_ads_log_loja'] LOOP
        IF to_regclass('public.' || t) IS NULL THEN
            RAISE EXCEPTION 'Índice % não foi criado. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    SELECT count(*) INTO n FROM pg_trigger
     WHERE tgrelid = 'public.ads_log_estrategico'::regclass AND NOT tgisinternal AND tgenabled = 'O';
    IF n <> 3 THEN
        RAISE EXCEPTION 'Esperava 3 gatilhos ligados em ads_log_estrategico, achei %. Nada foi aplicado.', n;
    END IF;
    SELECT count(*) INTO n FROM public.ads_log_estrategico;
    IF n <> 0 THEN
        RAISE EXCEPTION 'ads_log_estrategico deveria nascer vazia (tem %). Nada foi aplicado.', n;
    END IF;
    IF (SELECT rolcanlogin FROM pg_roles WHERE rolname = 'ciclo_ads') THEN
        RAISE EXCEPTION 'ciclo_ads deveria nascer NOLOGIN. Nada foi aplicado.';
    END IF;
    -- o que TEM que existir
    IF NOT has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'INSERT')
       OR NOT has_sequence_privilege('ciclo_ads', 'public.ads_log_estrategico_id_seq', 'USAGE') THEN
        RAISE EXCEPTION 'ciclo_ads sem INSERT na tabela ou USAGE na sequência. Nada foi aplicado.';
    END IF;
    FOREACH t IN ARRAY ARRAY['agentes_leitura', 'patricia_leitura'] LOOP
        IF NOT has_table_privilege(t, 'public.ads_log_estrategico', 'SELECT')
           OR NOT has_table_privilege(t, 'public.v_ads_log_vigente', 'SELECT') THEN
            RAISE EXCEPTION '% sem SELECT na tabela ou na view. Nada foi aplicado.', t;
        END IF;
        IF has_table_privilege(t, 'public.ads_log_estrategico', 'INSERT')
           OR has_table_privilege(t, 'public.ads_log_estrategico', 'UPDATE')
           OR has_table_privilege(t, 'public.ads_log_estrategico', 'DELETE') THEN
            RAISE EXCEPTION '% com escrita em ads_log_estrategico. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    -- o que NÃO pode existir para o ciclo_ads
    IF has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'UPDATE')
       OR has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'DELETE')
       OR has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'TRUNCATE')
       OR has_table_privilege('ciclo_ads', 'public.v_ads_log_vigente', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.dim_lojas', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.sinal_historico', 'INSERT')
       OR (to_regclass('public.fact_vendas_snapshot') IS NOT NULL
           AND has_table_privilege('ciclo_ads', 'public.fact_vendas_snapshot', 'SELECT'))
       OR (to_regclass('public.dim_usuarios') IS NOT NULL
           AND has_table_privilege('ciclo_ads', 'public.dim_usuarios', 'SELECT'))
       OR has_function_privilege('ciclo_ads', 'public.ads_log_entrada()', 'EXECUTE') THEN
        RAISE EXCEPTION 'ciclo_ads com privilégio a mais (herdado de PUBLIC?). Nada foi aplicado.';
    END IF;
END $$;

COMMIT;

-- Conferência (depois do COMMIT): CHECKs, gatilhos e privilégios.
SELECT conname, pg_get_constraintdef(oid)
  FROM pg_constraint
 WHERE conrelid = 'public.ads_log_estrategico'::regclass AND contype IN ('c', 'u')
 ORDER BY conname;
SELECT tgname, tgenabled FROM pg_trigger
 WHERE tgrelid = 'public.ads_log_estrategico'::regclass AND NOT tgisinternal ORDER BY tgname;
SELECT grantee, table_name, string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privilegios
  FROM information_schema.role_table_grants
 WHERE table_name IN ('ads_log_estrategico', 'v_ads_log_vigente')
   AND grantee IN ('ciclo_ads', 'agentes_leitura', 'patricia_leitura')
 GROUP BY grantee, table_name
 ORDER BY grantee, table_name;
