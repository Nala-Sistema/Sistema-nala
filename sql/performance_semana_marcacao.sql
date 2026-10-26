-- =============================================================================
-- perf_semana_marcacao: a marcação da gestora na aba "Semana" do Sinais do Dia
-- [PERFORMANCE SEMANAL] E1
-- =============================================================================
-- 26/10/2026 (especificação v2 aprovada pelo Thiago em 23/10; E1 liberada em
-- 26/10). Fluxo: a tela calcula as regras da semana seg–dom fechada (nada é
-- gravado pelo cálculo) -> a RESPONSÁVEL da loja escolhe o motivo de cada item
-- e clica Gravar -> uma linha por item aqui. A tela lê a mais recente de cada
-- semana + loja + anúncio + regra; "ruido", "proposital" e "ja_resolvido"
-- calam o mesmo item por 4 semanas (salvo se o R$ em jogo passar de 1,5× a
-- "medida" gravada aqui).
--
-- SÓ ACRESCENTA para o app: o gatilho recusa UPDATE (marcação nova substitui a
--   antiga, a antiga fica como histórico). DELETE fica só com o dono, para
--   limpar marcação de TESTE.
-- Quem grava: o app (hoje com o usuário dono). O gatilho de entrada recusa
--   quem não é a responsável da loja (dim_loja_responsavel_ads: Larissa nas
--   lojas do ML, Patricia nas da Shopee) nem ADMIN, a loja que não existe
--   naquele marketplace em dim_lojas, e prazo fora de amanhã..hoje+60 (o app
--   exige D+2 útil). Mesmo padrão de ads_acao_decisao (sql/ads_acoes.sql).
-- regra: T1..T16 (ataque, E1) e D1..D17 (defesa, E2), para a E2 não precisar
--   trocar a CHECK. motivo: os 8 da v2 (performance_semana.MOTIVOS).
--
-- GRANTs: SELECT para agentes_leitura e patricia_leitura (leitura dos
--   analistas e da Patricia) e sinais_historico (job do histórico, para a E2).
--   O app usa o dono: não precisa de GRANT. Quando o app ganhar usuário
--   próprio: SELECT, INSERT nesta tabela + USAGE na sequência.
--
-- O QUE ESTE ARQUIVO FAZ, NUMA TRANSAÇÃO SÓ:
--   1. Guarda: aborta se a tabela já existir ou se faltar usuário/tabela.
--   2. Tabela + índice.  3. Gatilhos.  4. GRANTs.  5. Conferência.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Quem roda: o Thiago, DEPOIS do auditor-tecnico.
-- QUANDO: qualquer hora (tabela nova, não trava nenhuma outra).
-- DESFAZER: sql/performance_semana_marcacao_DESFAZER.sql.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

-- ----------------------------------------------------------------------------- 1. guarda
DO $$
DECLARE
    u text;
BEGIN
    IF to_regclass('public.perf_semana_marcacao') IS NOT NULL THEN
        RAISE EXCEPTION 'perf_semana_marcacao já existe. Nada foi aplicado.';
    END IF;
    IF to_regclass('public.dim_loja_responsavel_ads') IS NULL
       OR to_regclass('public.dim_lojas') IS NULL
       OR to_regclass('public.dim_usuarios') IS NULL THEN
        RAISE EXCEPTION 'Falta dim_loja_responsavel_ads, dim_lojas ou dim_usuarios. Nada foi aplicado.';
    END IF;
    FOREACH u IN ARRAY ARRAY['agentes_leitura', 'patricia_leitura', 'sinais_historico'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = u) THEN
            RAISE EXCEPTION 'Usuário % não existe. Nada foi aplicado.', u;
        END IF;
    END LOOP;
END $$;

-- ----------------------------------------------------------------------------- 2. tabela
CREATE TABLE public.perf_semana_marcacao (
    id              bigserial     PRIMARY KEY,
    marketplace     varchar(30)   NOT NULL,
    loja            varchar(80)   NOT NULL,
    semana_ini      date          NOT NULL,
    codigo_anuncio  varchar(60)   NOT NULL,
    sku             varchar(200),
    regra           varchar(4)    NOT NULL,
    motivo          varchar(20)   NOT NULL,
    nota            varchar(500),
    prazo           date,
    medida          numeric(14,2),
    evidencia       varchar(500),
    sugestao        varchar(1000),
    decidido_por    varchar(63)   NOT NULL,
    decidido_em     timestamp     NOT NULL DEFAULT (now() AT TIME ZONE 'America/Sao_Paulo'),
    CONSTRAINT perf_semana_marcacao_marketplace CHECK (marketplace IN ('MERCADO LIVRE', 'SHOPEE')),
    CONSTRAINT perf_semana_marcacao_segunda CHECK (extract(isodow FROM semana_ini) = 1),
    CONSTRAINT perf_semana_marcacao_regra CHECK (regra ~ '^(T([1-9]|1[0-6])|D([1-9]|1[0-7]))$'),
    CONSTRAINT perf_semana_marcacao_motivo CHECK (motivo IN (
        'vou_agir', 'em_andamento', 'feito', 'ruido', 'proposital', 'falta_fornecedor',
        'ja_resolvido', 'outro')),
    CONSTRAINT perf_semana_marcacao_prazo_vou_agir CHECK (motivo <> 'vou_agir' OR prazo IS NOT NULL),
    CONSTRAINT perf_semana_marcacao_nota_outro CHECK (motivo <> 'outro' OR btrim(COALESCE(nota, '')) <> ''),
    CONSTRAINT perf_semana_marcacao_sem_vazio CHECK (
        btrim(loja) <> '' AND btrim(codigo_anuncio) <> '' AND btrim(decidido_por) <> '')
);

CREATE INDEX ix_perf_semana_marcacao_loja_semana
    ON public.perf_semana_marcacao (loja, semana_ini);

COMMENT ON TABLE public.perf_semana_marcacao IS
    'Marcação da gestora nos itens da aba Semana do Sinais do Dia (performance_semana.py). Só acrescenta; vale a mais recente de cada semana + loja + anúncio + regra. Gravada pelo app; o gatilho só aceita a responsável da loja (dim_loja_responsavel_ads) ou ADMIN.';

-- ----------------------------------------------------------------------------- 3. gatilhos
CREATE FUNCTION public.perf_semana_marcacao_entrada() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    hoje date := (now() AT TIME ZONE 'America/Sao_Paulo')::date;
BEGIN
    NEW.decidido_em := now() AT TIME ZONE 'America/Sao_Paulo';
    IF NEW.prazo IS NOT NULL AND (NEW.prazo <= hoje OR NEW.prazo > hoje + 60) THEN
        RAISE EXCEPTION 'perf_semana_marcacao: prazo % fora de amanhã..hoje+60 (nada para hoje).', NEW.prazo;
    END IF;
    IF NEW.semana_ini > hoje THEN
        RAISE EXCEPTION 'perf_semana_marcacao: semana % ainda não começou.', NEW.semana_ini;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.dim_lojas
                    WHERE loja = NEW.loja AND marketplace = NEW.marketplace) THEN
        RAISE EXCEPTION 'perf_semana_marcacao: loja % não existe no %.', NEW.loja, NEW.marketplace;
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM public.dim_lojas l
          JOIN public.dim_loja_responsavel_ads r ON r.id_loja = l.id
          JOIN public.dim_usuarios u ON u.id_usuario = r.id_usuario
         WHERE l.loja = NEW.loja AND l.marketplace = NEW.marketplace
           AND u.username = NEW.decidido_por)
       AND NOT EXISTS (SELECT 1 FROM public.dim_usuarios
                        WHERE username = NEW.decidido_por AND role = 'ADMIN') THEN
        RAISE EXCEPTION 'perf_semana_marcacao: % não é a responsável da loja %.', NEW.decidido_por, NEW.loja;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.perf_semana_marcacao_entrada() FROM PUBLIC;

CREATE FUNCTION public.perf_semana_marcacao_sem_update() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'perf_semana_marcacao só acrescenta: grave uma marcação nova.';
END $$;
REVOKE ALL ON FUNCTION public.perf_semana_marcacao_sem_update() FROM PUBLIC;

CREATE TRIGGER perf_semana_marcacao_entrada BEFORE INSERT ON public.perf_semana_marcacao
    FOR EACH ROW EXECUTE FUNCTION public.perf_semana_marcacao_entrada();
CREATE TRIGGER perf_semana_marcacao_sem_update BEFORE UPDATE ON public.perf_semana_marcacao
    FOR EACH ROW EXECUTE FUNCTION public.perf_semana_marcacao_sem_update();

-- ----------------------------------------------------------------------------- 4. GRANTs
GRANT SELECT ON public.perf_semana_marcacao TO agentes_leitura, patricia_leitura, sinais_historico;

-- ----------------------------------------------------------------------------- 5. conferência
DO $$
DECLARE
    n int;
    u text;
BEGIN
    SELECT count(*) INTO n FROM pg_trigger
     WHERE tgrelid = 'public.perf_semana_marcacao'::regclass AND NOT tgisinternal AND tgenabled = 'O';
    IF n <> 2 THEN
        RAISE EXCEPTION 'Esperava 2 gatilhos em perf_semana_marcacao, achei %. Nada foi aplicado.', n;
    END IF;
    SELECT count(*) INTO n FROM pg_constraint
     WHERE conrelid = 'public.perf_semana_marcacao'::regclass AND contype = 'c';
    IF n <> 7 THEN
        RAISE EXCEPTION 'Esperava 7 CHECKs em perf_semana_marcacao, achei %. Nada foi aplicado.', n;
    END IF;
    FOREACH u IN ARRAY ARRAY['agentes_leitura', 'patricia_leitura', 'sinais_historico'] LOOP
        IF NOT has_table_privilege(u, 'public.perf_semana_marcacao', 'SELECT') THEN
            RAISE EXCEPTION '% sem SELECT em perf_semana_marcacao. Nada foi aplicado.', u;
        END IF;
        IF has_table_privilege(u, 'public.perf_semana_marcacao', 'INSERT')
           OR has_table_privilege(u, 'public.perf_semana_marcacao', 'UPDATE')
           OR has_table_privilege(u, 'public.perf_semana_marcacao', 'DELETE')
           OR has_function_privilege(u, 'public.perf_semana_marcacao_entrada()', 'EXECUTE') THEN
            RAISE EXCEPTION '% com privilégio a mais em perf_semana_marcacao. Nada foi aplicado.', u;
        END IF;
    END LOOP;
END $$;

COMMIT;

-- Conferência (depois do COMMIT)
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid = 'public.perf_semana_marcacao'::regclass ORDER BY conname;
SELECT grantee, string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privilegios
  FROM information_schema.role_table_grants
 WHERE table_name = 'perf_semana_marcacao'
 GROUP BY grantee ORDER BY grantee;
