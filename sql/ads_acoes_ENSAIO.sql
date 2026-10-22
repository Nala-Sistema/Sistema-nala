-- =============================================================================
-- ENSAIO de sql/ads_acoes.sql   [HISTÓRICO E AÇÕES] etapa 2
-- =============================================================================
-- O MESMO conteúdo do ads_acoes.sql (gerado a partir dele, até a conferência),
-- mas SEM COMMIT: no fim roda 8 testes e termina em ROLLBACK. NADA FICA no banco.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Rodar TUDO de uma vez, fora das 12h (job).
-- RESULTADO ESPERADO: aparece "ENSAIO OK: 8 testes passaram..." e, no fim,
--   ads_acao = null, responsavel = null e checks_novas = 0.
--   Teste que falha para com "ENSAIO FALHOU: Tn ..." e nada fica.
-- =============================================================================

-- =============================================================================
-- ads_acao + v_ads_acao_vigente + ads_acao_decisao + dim_loja_responsavel_ads
-- + regra 'ads_acao' e motivo 'feito' no Sinais do Dia   [HISTÓRICO E AÇÕES] etapa 2
-- =============================================================================
-- 21/10/2026 (plano aprovado pelo Thiago, com as 2 decisões: tabela nova de
-- responsável por loja e motivo "Feito" no Ciente). Fluxo:
--   analista propõe o plano de ação do ciclo -> rodar_ciclo grava as propostas
--   em ads_acao (usuário ciclo_ads, só INSERT) -> a tela Sinais do Dia mostra o
--   plano no bloco da loja -> a RESPONSÁVEL da loja marca a caixinha e clica OK
--   (ads_acao_decisao, gravada pelo app) -> cada ação marcada vira o sinal
--   'ads_acao' ("Ação da análise de ads"), com prazo -> ela responde no Ciente
--   (motivo novo 'feito') -> o ciclo seguinte lê as respostas.
--
-- ads_acao: SÓ ACRESCENTA (gatilho recusa UPDATE/DELETE/TRUNCATE até do dono),
--   como ads_log_estrategico. Versão vigente de cada (loja, ciclo) = a rodada
--   mais recente (v_ads_acao_vigente); o --refazer grava rodada nova. Serve a
--   qualquer loja e aos dois agentes (analista-ads e analista-ads-nala): a
--   chave é loja + ciclo + período, o ciclo é texto (C29, U...) e o "mais
--   recente da loja" sai do periodo_ini, não do nome do ciclo.
--   O gatilho de entrada garante loja x marketplace x série (mesma regra do
--   log: série com nome de loja = a loja; série que não é loja = o marketplace).
--
-- ads_acao_decisao: a caixinha. Uma por ação. Gravada pelo app (usuário do app);
--   o gatilho recusa decisão de quem não é a responsável da loja da ação
--   (dim_loja_responsavel_ads) nem ADMIN, e prazo fora de hoje..hoje+60.
--   Não é "só acrescenta": decisão de TESTE se desfaz pelo dono (DELETE).
--
-- dim_loja_responsavel_ads: loja -> usuária que decide as ações de ads. Carga:
--   as lojas do MERCADO LIVRE -> Larissa; as da SHOPEE -> Patricia. Loja nova
--   entra aqui (INSERT pelo dono). NÃO mexe em dim_usuario_lojas.
--
-- Sinais do Dia: regra 'ads_acao' nas CHECKs sinal_historico_regra_valida e
--   sinal_ciente_regra_valida; motivo 'feito' na sinal_ciente_motivo_valido
--   (iguais a sinais_dia.REGRAS / MOTIVOS e a sql/sinais_historico.sql e
--   sql/sinais_ciente.sql, que mudam junto).
--
-- GRANTs: ciclo_ads INSERT em ads_acao (+ sequência); sinais_historico (job do
--   histórico; em 13/10 parou 2 dias por uma leitura sem permissão),
--   agentes_leitura e patricia_leitura: SELECT em ads_acao, v_ads_acao_vigente,
--   ads_acao_decisao e dim_loja_responsavel_ads.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se algo já existir, se faltar usuário/loja/tabela, ou se
--      as CHECKs do Sinais do Dia não forem as esperadas.
--   2. dim_loja_responsavel_ads + carga.
--   3. ads_acao + gatilhos + view.  4. ads_acao_decisao + gatilho.
--   5. CHECKs do Sinais do Dia.     6. GRANTs.     7. Conferência.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, DEPOIS do auditor-tecnico
--             e do sql/ads_acoes_ENSAIO.sql.
-- QUANDO: fora da janela dos coletores (04:00–06:30) e fora das 12h (job do
--   histórico). As trocas de CHECK seguram sinal_ciente/sinal_historico por
--   instantes (lock_timeout 5 s: se não pegar, aborta sem aplicar nada).
-- DESFAZER: sql/ads_acoes_DESFAZER.sql.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- ----------------------------------------------------------------------------- 1. guarda
DO $$
DECLARE
    def_h text;
    def_c text;
    def_m text;
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['ads_acao', 'v_ads_acao_vigente', 'ads_acao_decisao',
                             'dim_loja_responsavel_ads'] LOOP
        IF to_regclass('public.' || t) IS NOT NULL THEN
            RAISE EXCEPTION '% já existe. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    IF to_regprocedure('public.ads_acao_entrada()') IS NOT NULL
       OR to_regprocedure('public.ads_acao_so_acrescenta()') IS NOT NULL
       OR to_regprocedure('public.ads_acao_decisao_entrada()') IS NOT NULL THEN
        RAISE EXCEPTION 'As funções do ads_acao já existem. Nada foi aplicado.';
    END IF;
    FOREACH t IN ARRAY ARRAY['dim_lojas', 'dim_usuarios', 'sinal_historico', 'sinal_ciente',
                             'ads_log_estrategico'] LOOP
        IF to_regclass('public.' || t) IS NULL THEN
            RAISE EXCEPTION '% não existe. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    FOREACH t IN ARRAY ARRAY['ciclo_ads', 'sinais_historico', 'agentes_leitura', 'patricia_leitura'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = t) THEN
            RAISE EXCEPTION 'Falta o usuário de banco %. Nada foi aplicado.', t;
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM public.dim_usuarios WHERE username = 'Larissa')
       OR NOT EXISTS (SELECT 1 FROM public.dim_usuarios WHERE username = 'Patricia') THEN
        RAISE EXCEPTION 'Falta a usuária Larissa ou Patricia em dim_usuarios. Nada foi aplicado.';
    END IF;
    SELECT pg_get_constraintdef(oid) INTO def_h FROM pg_constraint
     WHERE conrelid = 'public.sinal_historico'::regclass AND conname = 'sinal_historico_regra_valida';
    SELECT pg_get_constraintdef(oid) INTO def_c FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_regra_valida';
    SELECT pg_get_constraintdef(oid) INTO def_m FROM pg_constraint
     WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_motivo_valido';
    IF def_h IS NULL OR def_c IS NULL OR def_m IS NULL THEN
        RAISE EXCEPTION 'Falta uma CHECK do Sinais do Dia (regra/motivo). Nada foi aplicado.';
    END IF;
    IF def_h NOT LIKE '%full_parado%' OR def_c NOT LIKE '%full_parado%'
       OR def_m NOT LIKE '%alarme_falso%' THEN
        RAISE EXCEPTION 'As CHECKs do Sinais do Dia não são as esperadas (falta full_parado/alarme_falso). Nada foi aplicado.';
    END IF;
    IF def_h LIKE '%ads_acao%' OR def_c LIKE '%ads_acao%' OR def_m LIKE '%feito%' THEN
        RAISE EXCEPTION 'As CHECKs já aceitam ads_acao/feito. Nada foi aplicado.';
    END IF;
END $$;

-- ----------------------------------------------------------------------------- 2. responsável por loja
CREATE TABLE public.dim_loja_responsavel_ads (
    id_loja     integer   PRIMARY KEY REFERENCES public.dim_lojas (id),
    id_usuario  integer   NOT NULL REFERENCES public.dim_usuarios (id_usuario),
    criado_em   timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo')
);

COMMENT ON TABLE public.dim_loja_responsavel_ads IS
    'Quem decide as ações da análise de ads de cada loja (caixinha do Sinais do Dia). ML -> Larissa, Shopee -> Patricia. Loja nova entra aqui. Não é dim_usuario_lojas (essa é visibilidade).';

INSERT INTO public.dim_loja_responsavel_ads (id_loja, id_usuario)
SELECT l.id, u.id_usuario
  FROM public.dim_lojas l
  JOIN public.dim_usuarios u
    ON u.username = CASE l.marketplace WHEN 'MERCADO LIVRE' THEN 'Larissa'
                                       WHEN 'SHOPEE' THEN 'Patricia' END
 WHERE l.marketplace IN ('MERCADO LIVRE', 'SHOPEE');

-- ----------------------------------------------------------------------------- 3. ads_acao
CREATE TABLE public.ads_acao (
    id               bigserial     PRIMARY KEY,
    marketplace      varchar(30)   NOT NULL,
    loja             varchar(60)   NOT NULL,
    serie            varchar(30)   NOT NULL,
    ciclo            varchar(30)   NOT NULL,
    periodo_ini      date          NOT NULL,
    periodo_fim      date          NOT NULL,
    ordem            smallint      NOT NULL,
    objeto           text          NOT NULL,
    codigo_anuncio   varchar(30),
    sku              varchar(60),
    nome_produto     text,
    acao             text          NOT NULL,
    dono             varchar(60)   NOT NULL,
    numero_esperado  text          NOT NULL,
    checar_em        date          NOT NULL,
    linha_original   text          NOT NULL,
    arquivo_origem   text          NOT NULL,
    rodada           varchar(120)  NOT NULL,
    rodada_em        timestamp     NOT NULL,
    gravado_em       timestamp     NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    gravado_por      varchar(63)   NOT NULL DEFAULT session_user,
    CONSTRAINT ads_acao_marketplace_caixa_alta CHECK (marketplace = upper(marketplace)),
    CONSTRAINT ads_acao_sem_vazio CHECK (
        btrim(loja) <> '' AND btrim(serie) <> '' AND btrim(ciclo) <> '' AND btrim(objeto) <> ''
        AND btrim(acao) <> '' AND btrim(dono) <> '' AND btrim(numero_esperado) <> ''
        AND btrim(linha_original) <> '' AND btrim(arquivo_origem) <> '' AND btrim(rodada) <> ''),
    CONSTRAINT ads_acao_periodo CHECK (periodo_fim >= periodo_ini AND periodo_fim <= periodo_ini + 14),
    CONSTRAINT ads_acao_ordem CHECK (ordem BETWEEN 1 AND 30),
    CONSTRAINT ads_acao_checar_em CHECK (checar_em > periodo_fim AND checar_em <= periodo_fim + 60),
    CONSTRAINT ads_acao_uma_vez_por_rodada UNIQUE (loja, ciclo, rodada, ordem)
);

CREATE INDEX ix_ads_acao_loja_periodo ON public.ads_acao (marketplace, loja, periodo_ini, rodada_em);
CREATE INDEX ix_ads_acao_codigo ON public.ads_acao (codigo_anuncio) WHERE codigo_anuncio IS NOT NULL;

COMMENT ON TABLE public.ads_acao IS
    'Plano de ação proposto pela análise de ads de cada ciclo, por loja. SÓ ACRESCENTA. Grava só ciclo_ads (rodar_ciclo). Vigente: v_ads_acao_vigente.';

CREATE FUNCTION public.ads_acao_entrada() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
BEGIN
    NEW.gravado_em  := now() AT TIME ZONE 'America/Sao_Paulo';
    NEW.gravado_por := session_user;
    IF NOT EXISTS (SELECT 1 FROM public.dim_lojas
                    WHERE loja = NEW.loja AND marketplace = NEW.marketplace) THEN
        RAISE EXCEPTION 'ads_acao: loja % não existe em dim_lojas com o marketplace %.',
            NEW.loja, NEW.marketplace;
    END IF;
    IF EXISTS (SELECT 1 FROM public.dim_lojas WHERE loja = NEW.serie) THEN
        IF NEW.loja <> NEW.serie THEN
            RAISE EXCEPTION 'ads_acao: a série % é de uma loja só; a ação veio com a loja %.',
                NEW.serie, NEW.loja;
        END IF;
    ELSIF NEW.marketplace <> NEW.serie THEN
        RAISE EXCEPTION 'ads_acao: a série % não é loja; o marketplace da ação tem de ser %, veio %.',
            NEW.serie, NEW.serie, NEW.marketplace;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.ads_acao_entrada() FROM PUBLIC;

CREATE FUNCTION public.ads_acao_so_acrescenta() RETURNS trigger
    LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'ads_acao só aceita linha nova: % recusado. Correção = rodada nova.', TG_OP;
END $$;
REVOKE ALL ON FUNCTION public.ads_acao_so_acrescenta() FROM PUBLIC;

CREATE TRIGGER ads_acao_entrada BEFORE INSERT ON public.ads_acao
    FOR EACH ROW EXECUTE FUNCTION public.ads_acao_entrada();
CREATE TRIGGER ads_acao_sem_update_delete BEFORE UPDATE OR DELETE ON public.ads_acao
    FOR EACH ROW EXECUTE FUNCTION public.ads_acao_so_acrescenta();
CREATE TRIGGER ads_acao_sem_truncate BEFORE TRUNCATE ON public.ads_acao
    FOR EACH STATEMENT EXECUTE FUNCTION public.ads_acao_so_acrescenta();

CREATE VIEW public.v_ads_acao_vigente AS
SELECT a.*
  FROM public.ads_acao a
 WHERE a.rodada = (SELECT a2.rodada
                     FROM public.ads_acao a2
                    WHERE a2.loja = a.loja AND a2.ciclo = a.ciclo
                    ORDER BY a2.rodada_em DESC, a2.rodada DESC
                    LIMIT 1);

COMMENT ON VIEW public.v_ads_acao_vigente IS
    'Plano de ação de ads, só a rodada vigente de cada (loja, ciclo).';

-- ----------------------------------------------------------------------------- 4. decisão (caixinha)
CREATE TABLE public.ads_acao_decisao (
    id_acao       bigint      PRIMARY KEY REFERENCES public.ads_acao (id),
    vai_executar  boolean     NOT NULL,
    prazo         date        NOT NULL,
    decidido_por  varchar(63) NOT NULL,
    decidido_em   timestamp   NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    CONSTRAINT ads_acao_decisao_sem_vazio CHECK (btrim(decidido_por) <> '')
);

COMMENT ON TABLE public.ads_acao_decisao IS
    'A caixinha da responsável da loja: vai executar a ação, com prazo. Gravada pelo app; o gatilho só aceita a responsável da loja (dim_loja_responsavel_ads) ou ADMIN.';

CREATE FUNCTION public.ads_acao_decisao_entrada() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    hoje date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
    NEW.decidido_em := now() AT TIME ZONE 'America/Sao_Paulo';
    IF NEW.prazo < hoje OR NEW.prazo > hoje + 60 THEN
        RAISE EXCEPTION 'ads_acao_decisao: prazo % fora de hoje..hoje+60.', NEW.prazo;
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM public.ads_acao a
          JOIN public.dim_lojas l ON l.loja = a.loja
          JOIN public.dim_loja_responsavel_ads r ON r.id_loja = l.id
          JOIN public.dim_usuarios u ON u.id_usuario = r.id_usuario
         WHERE a.id = NEW.id_acao AND u.username = NEW.decidido_por)
       AND NOT EXISTS (SELECT 1 FROM public.dim_usuarios
                        WHERE username = NEW.decidido_por AND role = 'ADMIN') THEN
        RAISE EXCEPTION 'ads_acao_decisao: % não é a responsável da loja desta ação.', NEW.decidido_por;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.ads_acao_decisao_entrada() FROM PUBLIC;

CREATE TRIGGER ads_acao_decisao_entrada BEFORE INSERT OR UPDATE ON public.ads_acao_decisao
    FOR EACH ROW EXECUTE FUNCTION public.ads_acao_decisao_entrada();

-- ----------------------------------------------------------------------------- 5. CHECKs do Sinais do Dia
ALTER TABLE public.sinal_historico DROP CONSTRAINT sinal_historico_regra_valida;
ALTER TABLE public.sinal_historico ADD CONSTRAINT sinal_historico_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado',
    'ads_acao'));

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_regra_valida;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta', 'full_parado',
    'ads_acao'));

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_motivo_valido;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_motivo_valido CHECK (motivo IN (
    'falta_fornecedor', 'proposital', 'em_andamento', 'alarme_falso', 'outro', 'feito'));

-- ----------------------------------------------------------------------------- 6. GRANTs
GRANT INSERT ON public.ads_acao TO ciclo_ads;
GRANT USAGE ON SEQUENCE public.ads_acao_id_seq TO ciclo_ads;
GRANT SELECT ON public.ads_acao, public.v_ads_acao_vigente, public.ads_acao_decisao,
                public.dim_loja_responsavel_ads
   TO sinais_historico, agentes_leitura, patricia_leitura;

-- ----------------------------------------------------------------------------- 7. conferência
DO $$
DECLARE
    n int;
    t text;
    u text;
BEGIN
    SELECT count(*) INTO n FROM public.dim_loja_responsavel_ads r
      JOIN public.dim_lojas l ON l.id = r.id_loja
      JOIN public.dim_usuarios us ON us.id_usuario = r.id_usuario
     WHERE (l.marketplace = 'MERCADO LIVRE' AND us.username = 'Larissa')
        OR (l.marketplace = 'SHOPEE' AND us.username = 'Patricia');
    IF n <> (SELECT count(*) FROM public.dim_lojas WHERE marketplace IN ('MERCADO LIVRE', 'SHOPEE'))
       OR n <> (SELECT count(*) FROM public.dim_loja_responsavel_ads) OR n = 0 THEN
        RAISE EXCEPTION 'Carga de dim_loja_responsavel_ads errada (% linhas certas). Nada foi aplicado.', n;
    END IF;
    SELECT count(*) INTO n FROM pg_trigger
     WHERE tgrelid = 'public.ads_acao'::regclass AND NOT tgisinternal AND tgenabled = 'O';
    IF n <> 3 THEN
        RAISE EXCEPTION 'Esperava 3 gatilhos em ads_acao, achei %. Nada foi aplicado.', n;
    END IF;
    SELECT count(*) INTO n FROM pg_trigger
     WHERE tgrelid = 'public.ads_acao_decisao'::regclass AND NOT tgisinternal AND tgenabled = 'O';
    IF n <> 1 THEN
        RAISE EXCEPTION 'Esperava 1 gatilho em ads_acao_decisao, achei %. Nada foi aplicado.', n;
    END IF;
    SELECT count(*) INTO n FROM pg_constraint
     WHERE conrelid = 'public.ads_acao'::regclass AND contype = 'c';
    IF n <> 5 THEN
        RAISE EXCEPTION 'Esperava 5 CHECKs em ads_acao, achei %. Nada foi aplicado.', n;
    END IF;
    IF (SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'public.sinal_historico'::regclass AND conname = 'sinal_historico_regra_valida')
         NOT LIKE '%ads_acao%'
       OR (SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_regra_valida')
         NOT LIKE '%ads_acao%'
       OR (SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'public.sinal_ciente'::regclass AND conname = 'sinal_ciente_motivo_valido')
         NOT LIKE '%feito%' THEN
        RAISE EXCEPTION 'As CHECKs do Sinais do Dia não mudaram. Nada foi aplicado.';
    END IF;
    -- o que TEM que existir
    IF NOT has_table_privilege('ciclo_ads', 'public.ads_acao', 'INSERT')
       OR NOT has_sequence_privilege('ciclo_ads', 'public.ads_acao_id_seq', 'USAGE') THEN
        RAISE EXCEPTION 'ciclo_ads sem INSERT em ads_acao ou USAGE na sequência. Nada foi aplicado.';
    END IF;
    FOREACH u IN ARRAY ARRAY['sinais_historico', 'agentes_leitura', 'patricia_leitura'] LOOP
        FOREACH t IN ARRAY ARRAY['ads_acao', 'v_ads_acao_vigente', 'ads_acao_decisao',
                                 'dim_loja_responsavel_ads'] LOOP
            IF NOT has_table_privilege(u, 'public.' || t, 'SELECT') THEN
                RAISE EXCEPTION '% sem SELECT em %. Nada foi aplicado.', u, t;
            END IF;
            IF has_table_privilege(u, 'public.' || t, 'INSERT')
               OR has_table_privilege(u, 'public.' || t, 'UPDATE')
               OR has_table_privilege(u, 'public.' || t, 'DELETE') THEN
                RAISE EXCEPTION '% com escrita em %. Nada foi aplicado.', u, t;
            END IF;
        END LOOP;
    END LOOP;
    -- o que NÃO pode existir para o ciclo_ads
    IF has_table_privilege('ciclo_ads', 'public.ads_acao', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao', 'UPDATE')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao', 'DELETE')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao', 'TRUNCATE')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao_decisao', 'INSERT')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao_decisao', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.dim_loja_responsavel_ads', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.dim_usuarios', 'SELECT')
       OR has_function_privilege('ciclo_ads', 'public.ads_acao_entrada()', 'EXECUTE')
       OR has_function_privilege('ciclo_ads', 'public.ads_acao_decisao_entrada()', 'EXECUTE') THEN
        RAISE EXCEPTION 'ciclo_ads com privilégio a mais. Nada foi aplicado.';
    END IF;
END $$;


-- =============================================================================
-- TESTES DO ENSAIO (mesma transação; o ROLLBACK do fim desfaz tudo). Teste que
-- falha ABORTA com "ENSAIO FALHOU: ...". Se aparecer "ENSAIO OK", passaram.
-- =============================================================================

GRANT ciclo_ads TO neondb_owner WITH SET TRUE, INHERIT FALSE;   -- só neste ensaio

-- T1. Responsáveis: toda loja do ML com a Larissa, toda da Shopee com a Patricia.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.dim_lojas l
                LEFT JOIN public.dim_loja_responsavel_ads r ON r.id_loja = l.id
                LEFT JOIN public.dim_usuarios u ON u.id_usuario = r.id_usuario
               WHERE l.marketplace IN ('MERCADO LIVRE', 'SHOPEE')
                 AND u.username IS DISTINCT FROM
                     CASE l.marketplace WHEN 'MERCADO LIVRE' THEN 'Larissa' ELSE 'Patricia' END) THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T1 responsável errado ou faltando';
    END IF;
END $$;

-- T2. Como ciclo_ads: ações válidas entram (ML-LPT e Shopee-LPT); repetida não
--     duplica; loja/série/marketplace trocados são recusados pelo gatilho.
SET LOCAL ROLE ciclo_ads;
DO $$
DECLARE
    n int;
    caso text[];
BEGIN
    INSERT INTO public.ads_acao (marketplace, loja, serie, ciclo, periodo_ini, periodo_fim, ordem,
        objeto, codigo_anuncio, acao, dono, numero_esperado, checar_em, linha_original,
        arquivo_origem, rodada, rodada_em, gravado_por)
    VALUES
      ('MERCADO LIVRE', 'ML-LPT', 'ML-LPT', 'C99', '2026-10-12', '2026-10-18', 1, 'Teste A',
       'MLB0000000001', 'Pausar', 'Larissa', 'gasto 0', '2026-10-25', 'linha 1', 'ENSAIO',
       'ensaio-r1', '2026-10-21 10:00', 'quem_eu_quiser'),
      ('MERCADO LIVRE', 'ML-LPT', 'ML-LPT', 'C99', '2026-10-12', '2026-10-18', 2, 'Teste B',
       'MLB0000000002', 'Subir ROAS Obj', 'Larissa', 'ACOS < margem', '2026-10-25', 'linha 2',
       'ENSAIO', 'ensaio-r1', '2026-10-21 10:00', NULL),
      ('SHOPEE', 'Shopee-LPT', 'SHOPEE', 'S99', '2026-10-12', '2026-10-18', 1, 'Teste S',
       NULL, 'Pausar', 'Patricia', 'gasto 0', '2026-10-25', 'linha s', 'ENSAIO',
       'ensaio-s1', '2026-10-21 10:00', NULL);
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n <> 3 THEN RAISE EXCEPTION 'ENSAIO FALHOU: T2 entraram % de 3', n; END IF;

    INSERT INTO public.ads_acao (marketplace, loja, serie, ciclo, periodo_ini, periodo_fim, ordem,
        objeto, acao, dono, numero_esperado, checar_em, linha_original, arquivo_origem, rodada, rodada_em)
    VALUES ('MERCADO LIVRE', 'ML-LPT', 'ML-LPT', 'C99', '2026-10-12', '2026-10-18', 1, 'Teste A',
            'Pausar', 'Larissa', 'gasto 0', '2026-10-25', 'linha 1', 'ENSAIO', 'ensaio-r1',
            '2026-10-21 10:00')
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n <> 0 THEN RAISE EXCEPTION 'ENSAIO FALHOU: T2 a ação repetida entrou de novo'; END IF;

    FOREACH caso SLICE 1 IN ARRAY ARRAY[
        ARRAY['MERCADO LIVRE', 'ML-Nala', 'ML-LPT'],        -- loja de outra série do ML
        ARRAY['MERCADO LIVRE', 'Shopee-LPT', 'ML-LPT'],     -- loja da Shopee com marketplace ML
        ARRAY['SHOPEE', 'Shopee-LPT', 'ML-LPT'],            -- ação da Shopee na série da ML-LPT
        ARRAY['MERCADO LIVRE', 'ML-Nala', 'SHOPEE'],        -- série SHOPEE com marketplace ML
        ARRAY['SHOPEE', 'Shopee-Inexistente', 'SHOPEE']] LOOP
        BEGIN
            INSERT INTO public.ads_acao (marketplace, loja, serie, ciclo, periodo_ini, periodo_fim,
                ordem, objeto, acao, dono, numero_esperado, checar_em, linha_original,
                arquivo_origem, rodada, rodada_em)
            VALUES (caso[1], caso[2], caso[3], 'C99', '2026-10-12', '2026-10-18', 9, 'x', 'x', 'x',
                    'x', '2026-10-25', 'x', 'ENSAIO', 'ensaio-rx', now());
            RAISE EXCEPTION 'ENSAIO FALHOU: T2 entrou % / % / série %', caso[1], caso[2], caso[3];
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM LIKE 'ENSAIO FALHOU%' OR SQLERRM NOT LIKE 'ads_acao:%' THEN RAISE; END IF;
        END;
    END LOOP;

    -- rodada nova do mesmo ciclo (o --refazer) entra ao lado da antiga
    INSERT INTO public.ads_acao (marketplace, loja, serie, ciclo, periodo_ini, periodo_fim, ordem,
        objeto, acao, dono, numero_esperado, checar_em, linha_original, arquivo_origem, rodada, rodada_em)
    VALUES ('MERCADO LIVRE', 'ML-LPT', 'ML-LPT', 'C99', '2026-10-12', '2026-10-18', 1, 'Teste A2',
            'Pausar já', 'Larissa', 'gasto 0', '2026-10-25', 'linha 1 refeita', 'ENSAIO',
            'ensaio-r2', '2026-10-21 11:00');
END $$;

-- T3. Como ciclo_ads: não lê, não altera, não decide.
DO $$
DECLARE cmd text;
BEGIN
    FOREACH cmd IN ARRAY ARRAY[
        'SELECT 1 FROM public.ads_acao',
        'SELECT 1 FROM public.v_ads_acao_vigente',
        'UPDATE public.ads_acao SET acao = acao',
        'DELETE FROM public.ads_acao',
        'TRUNCATE public.ads_acao',
        'SELECT 1 FROM public.ads_acao_decisao',
        'INSERT INTO public.ads_acao_decisao (id_acao, vai_executar, prazo, decidido_por) VALUES (1, true, current_date, ''x'')',
        'SELECT 1 FROM public.dim_loja_responsavel_ads'] LOOP
        BEGIN
            EXECUTE cmd;
            RAISE EXCEPTION 'ENSAIO FALHOU: T3 ciclo_ads conseguiu: %', cmd;
        EXCEPTION WHEN insufficient_privilege THEN
            NULL;
        END;
    END LOOP;
END $$;

RESET ROLE;

-- T4. Como DONO: o gatilho recusa UPDATE, DELETE e TRUNCATE em ads_acao.
DO $$
DECLARE cmd text;
BEGIN
    FOREACH cmd IN ARRAY ARRAY[
        'UPDATE public.ads_acao SET acao = acao',
        'DELETE FROM public.ads_acao',
        'TRUNCATE public.ads_acao CASCADE'] LOOP
        BEGIN
            EXECUTE cmd;
            RAISE EXCEPTION 'ENSAIO FALHOU: T4 o dono conseguiu: %', cmd;
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM NOT LIKE 'ads_acao só aceita linha nova%' THEN RAISE; END IF;
        END;
    END LOOP;
END $$;

-- T5. Como DONO (o app): a caixinha. Larissa decide ação do ML; Patricia NÃO
--     decide ação do ML; Larissa NÃO decide ação da Shopee; Patricia decide a da
--     Shopee; admin (ADMIN) decide; prazo no passado é recusado.
DO $$
DECLARE
    a1 bigint; a2 bigint; s1 bigint;
    caso text[];
    hoje date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
    SELECT id INTO a1 FROM public.ads_acao WHERE rodada = 'ensaio-r2';
    SELECT id INTO a2 FROM public.ads_acao WHERE rodada = 'ensaio-r1' AND ordem = 2;
    SELECT id INTO s1 FROM public.ads_acao WHERE rodada = 'ensaio-s1';
    FOREACH caso SLICE 1 IN ARRAY ARRAY[
        ARRAY[a1::text, 'Patricia', '0'],      -- Patricia na ação do ML
        ARRAY[s1::text, 'Larissa', '0'],       -- Larissa na ação da Shopee
        ARRAY[a1::text, 'Larissa', '-1']] LOOP -- prazo no passado
        BEGIN
            INSERT INTO public.ads_acao_decisao (id_acao, vai_executar, prazo, decidido_por)
            VALUES (caso[1]::bigint, true, hoje + caso[3]::int, caso[2]);
            RAISE EXCEPTION 'ENSAIO FALHOU: T5 aceitou % na ação % (prazo %)', caso[2], caso[1], caso[3];
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM LIKE 'ENSAIO FALHOU%' OR SQLERRM NOT LIKE 'ads_acao_decisao:%' THEN RAISE; END IF;
        END;
    END LOOP;
    INSERT INTO public.ads_acao_decisao (id_acao, vai_executar, prazo, decidido_por)
    VALUES (a1, true, hoje + 3, 'Larissa'),
           (s1, true, hoje + 3, 'Patricia');
    IF EXISTS (SELECT 1 FROM public.dim_usuarios WHERE username = 'admin' AND role = 'ADMIN') THEN
        INSERT INTO public.ads_acao_decisao (id_acao, vai_executar, prazo, decidido_por)
        VALUES (a2, true, hoje + 3, 'admin');
    END IF;
END $$;

-- T6. A view mostra só a rodada nova do C99 da ML-LPT (r2) e a ação da Shopee.
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM public.v_ads_acao_vigente WHERE loja = 'ML-LPT' AND ciclo = 'C99';
    IF n <> 1 OR NOT EXISTS (SELECT 1 FROM public.v_ads_acao_vigente WHERE rodada = 'ensaio-r2') THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T6 a view não mostrou só a rodada nova (achou %)', n;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.v_ads_acao_vigente WHERE loja = 'Shopee-LPT') THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T6 a ação da Shopee sumiu da view';
    END IF;
    IF (SELECT gravado_por FROM public.ads_acao WHERE rodada = 'ensaio-r1' AND ordem = 1) <> session_user THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T6 gravado_por não é quem conectou';
    END IF;
END $$;

-- T7. As CHECKs do Sinais do Dia aceitam 'ads_acao' e 'feito' e seguem recusando o resto.
DO $$
DECLARE hoje date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
    INSERT INTO public.sinal_historico (data, marketplace, loja, regra, objeto)
    VALUES (hoje, 'MERCADO LIVRE', 'ML-LPT', 'ads_acao', 'acao:ensaio');
    INSERT INTO public.sinal_ciente (marketplace, loja, regra, objeto, motivo, silenciar_ate, criado_por)
    VALUES ('MERCADO LIVRE', 'ML-LPT', 'ads_acao', 'acao:ensaio', 'feito', hoje + 7, 'ensaio');
    BEGIN
        INSERT INTO public.sinal_historico (data, marketplace, loja, regra, objeto)
        VALUES (hoje, 'MERCADO LIVRE', 'ML-LPT', 'regra_que_nao_existe', 'x');
        RAISE EXCEPTION 'ENSAIO FALHOU: T7 sinal_historico aceitou regra inválida';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        INSERT INTO public.sinal_ciente (marketplace, loja, regra, objeto, motivo, silenciar_ate, criado_por)
        VALUES ('MERCADO LIVRE', 'ML-LPT', 'ads_acao', 'acao:ensaio2', 'motivo_que_nao_existe', hoje + 7, 'ensaio');
        RAISE EXCEPTION 'ENSAIO FALHOU: T7 sinal_ciente aceitou motivo inválido';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END $$;

-- T8. GRANTs (matriz).
DO $$
DECLARE
    u text; t text;
BEGIN
    FOREACH u IN ARRAY ARRAY['sinais_historico', 'agentes_leitura', 'patricia_leitura'] LOOP
        FOREACH t IN ARRAY ARRAY['ads_acao', 'v_ads_acao_vigente', 'ads_acao_decisao',
                                 'dim_loja_responsavel_ads'] LOOP
            IF NOT has_table_privilege(u, 'public.' || t, 'SELECT')
               OR has_table_privilege(u, 'public.' || t, 'INSERT')
               OR has_table_privilege(u, 'public.' || t, 'UPDATE')
               OR has_table_privilege(u, 'public.' || t, 'DELETE') THEN
                RAISE EXCEPTION 'ENSAIO FALHOU: T8 % em %: esperado só SELECT', u, t;
            END IF;
        END LOOP;
    END LOOP;
    IF NOT has_table_privilege('ciclo_ads', 'public.ads_acao', 'INSERT')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao', 'SELECT')
       OR has_table_privilege('ciclo_ads', 'public.ads_acao_decisao', 'INSERT')
       OR NOT has_sequence_privilege('ciclo_ads', 'public.ads_acao_id_seq', 'USAGE') THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T8 privilégios do ciclo_ads errados';
    END IF;
    -- a etapa 1 continua igual
    IF NOT has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'INSERT')
       OR has_table_privilege('ciclo_ads', 'public.ads_log_estrategico', 'SELECT') THEN
        RAISE EXCEPTION 'ENSAIO FALHOU: T8 privilégios da etapa 1 mudaram';
    END IF;
END $$;

SELECT 'ENSAIO OK: 8 testes passaram; o ROLLBACK a seguir desfaz tudo' AS resultado;

ROLLBACK;

-- Depois do ROLLBACK nada pode ter ficado (tabelas = null; CHECKs sem ads_acao/feito):
SELECT to_regclass('public.ads_acao') AS ads_acao,
       to_regclass('public.dim_loja_responsavel_ads') AS responsavel,
       (SELECT count(*) FROM pg_constraint
         WHERE conname IN ('sinal_historico_regra_valida', 'sinal_ciente_regra_valida', 'sinal_ciente_motivo_valido')
           AND (pg_get_constraintdef(oid) LIKE '%ads_acao%' OR pg_get_constraintdef(oid) LIKE '%feito%')) AS checks_novas;
