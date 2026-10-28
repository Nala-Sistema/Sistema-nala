-- =============================================================================
-- ACESSO SO LEITURA — usuarios agentes_leitura e patricia_leitura
-- (frente [ACESSO LEITURA], 13/10/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\acesso_leitura.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: DONO do banco (neondb_owner), no SQL Editor.
-- ANTES DE RODAR: passar pelo auditor-tecnico.
-- DESFAZER: no fim deste arquivo (comentado, um bloco por usuario).
-- SENHA: depois, com C:\Users\thiag\Nala-Analises\definir_senha_leitura.py
--
-- O QUE FAZ
--   1. Cria dois usuarios SEPARADOS (revogar um nao derruba o outro), SEM LOGIN
--      ate o script de senha, sem nenhum poder especial.
--   2. agentes_leitura: SELECT em todas as tabelas e views do schema public,
--      MENOS:
--        - dim_usuarios                       (logins das gestoras)
--        - v_kanban_cards_por_responsavel     (a view roda com a permissao do
--                                              dono e mostraria os usernames)
--        - fact_vendas_snapshot_backup_ml_202609     (copias de venda: somar
--        - fact_vendas_snapshot_backup_shopee_202609  junto com a oficial conta
--        - bkp_magalu_20260817                        a venda duas vezes)
--   3. patricia_leitura: SELECT em TUDO, inclusive custo, margem e backups;
--      dim_usuarios SO por coluna, sem password_hash.
--      => SELECT * FROM dim_usuarios FALHA para ela. E esperado: listar as
--         colunas.
--   4. Limites por usuario: transacao so leitura, consulta cortada em 30 s
--      (agentes) / 60 s (Patricia), transacao parada cortada em 60 s,
--      no maximo 3 conexoes cada.
--   5. Confere tudo no fim. Se algo sair do desenho, da ERRO e desfaz tudo.
--
-- O QUE NAO FAZ (de proposito)
--   - Nenhum INSERT/UPDATE/DELETE/TRUNCATE em nada, nenhum CREATE no schema.
--   - NAO usa pg_read_all_data (daria o password_hash) nem permissao automatica
--     para tabela futura: TABELA NOVA PRECISA DE GRANT A MAO (consulta de
--     conferencia no fim do arquivo).
--   - NAO define senha (passo do script, fora do git).
--
-- ATENCAO
--   - O banco e o mesmo para producao e dev: os dois usuarios veem dado de
--     producao nos dois ambientes. Nao ha isolamento.
--   - "So leitura" e os tempos-limite sao cinto de seguranca contra descuido:
--     o proprio usuario consegue desligar na sessao dele. A trava de verdade
--     e nao ter GRANT de escrita. O limite de conexoes ele nao consegue mudar.
--   - As 5 funcoes do public que PUBLIC executa sao de gatilho / SECURITY
--     INVOKER (rodam com a permissao de quem chama): nao abrem escrita. As 2
--     SECURITY DEFINER (gravar_custo_full_*) ja sao fechadas ao coletor.
--   - Os dois usuarios PODEM criar tabela TEMPORARIA (privilegio TEMP que o
--     banco da a PUBLIC por padrao). Ela some no fim da sessao e nao toca em
--     tabela do sistema. NAO se revoga: revogar TEMP de PUBLIC tiraria tambem
--     dos coletores (que dependem de PUBLIC), e nao da para tirar so destes dois.
--
-- E SEGURO RODAR DE NOVO: zera as permissoes de leitura dos dois e concede
-- outra vez; nao mexe no LOGIN nem na senha de quem ja tiver.
-- =============================================================================
BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Usuarios (criados por SQL, nao pelo console do Neon, que daria
--    neon_superuser).
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentes_leitura') THEN
        CREATE ROLE agentes_leitura NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                                    NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'patricia_leitura') THEN
        CREATE ROLE patricia_leitura NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                                     NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 2. Limites (valem a cada nova conexao do usuario).
-- -----------------------------------------------------------------------------
ALTER ROLE agentes_leitura  CONNECTION LIMIT 3;
ALTER ROLE agentes_leitura  SET default_transaction_read_only = on;
ALTER ROLE agentes_leitura  SET statement_timeout = '30s';
ALTER ROLE agentes_leitura  SET idle_in_transaction_session_timeout = '60s';

ALTER ROLE patricia_leitura CONNECTION LIMIT 3;
ALTER ROLE patricia_leitura SET default_transaction_read_only = on;
ALTER ROLE patricia_leitura SET statement_timeout = '60s';
ALTER ROLE patricia_leitura SET idle_in_transaction_session_timeout = '60s';

-- -----------------------------------------------------------------------------
-- 3. Permissoes: zera e concede de novo (rodar outra vez da o mesmo resultado).
--    REVOKE ALL na tabela tambem tira permissao por coluna.
-- -----------------------------------------------------------------------------
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM agentes_leitura, patricia_leitura;
REVOKE ALL ON SCHEMA public FROM agentes_leitura, patricia_leitura;
GRANT USAGE ON SCHEMA public TO agentes_leitura, patricia_leitura;

DO $$
DECLARE
    fora_agentes  text[] := ARRAY[
        'dim_usuarios',
        'v_kanban_cards_por_responsavel',
        'fact_vendas_snapshot_backup_ml_202609',
        'fact_vendas_snapshot_backup_shopee_202609',
        'bkp_magalu_20260817'];
    fora_patricia text[] := ARRAY['dim_usuarios'];   -- recebe por coluna abaixo
    obj record;
BEGIN
    FOR obj IN
        SELECT c.relname
          FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
         ORDER BY c.relname
    LOOP
        IF NOT obj.relname = ANY (fora_agentes) THEN
            EXECUTE format('GRANT SELECT ON public.%I TO agentes_leitura', obj.relname);
        END IF;
        IF NOT obj.relname = ANY (fora_patricia) THEN
            EXECUTE format('GRANT SELECT ON public.%I TO patricia_leitura', obj.relname);
        END IF;
    END LOOP;
END
$$;

-- Patricia: dim_usuarios coluna por coluna, SEM password_hash. Lista escrita a
-- mao de proposito: coluna nova em dim_usuarios NAO entra sozinha.
GRANT SELECT (id_usuario, username, nome_completo, nome, email, role, ativo,
              data_criacao, ultimo_acesso, criado_por, created_at, updated_at)
    ON public.dim_usuarios TO patricia_leitura;

-- -----------------------------------------------------------------------------
-- 4. Conferencia. Qualquer desvio => ERRO => nada do arquivo fica gravado.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    r        text;
    n_obj    int;
    n_ag     int;
    n_pa     int;
    problema text;
BEGIN
    SELECT count(*) INTO n_obj
      FROM pg_class
     WHERE relnamespace = 'public'::regnamespace
       AND relkind IN ('r', 'p', 'v', 'm', 'f');

    FOREACH r IN ARRAY ARRAY['agentes_leitura', 'patricia_leitura'] LOOP
        -- (a) nenhum poder especial, nenhum grupo herdado (pg_read_all_data etc.)
        SELECT string_agg(x, ', ') INTO problema FROM (
            SELECT 'superuser'  AS x FROM pg_roles WHERE rolname = r AND rolsuper
            UNION ALL SELECT 'createrole'  FROM pg_roles WHERE rolname = r AND rolcreaterole
            UNION ALL SELECT 'createdb'    FROM pg_roles WHERE rolname = r AND rolcreatedb
            UNION ALL SELECT 'replication' FROM pg_roles WHERE rolname = r AND rolreplication
            UNION ALL SELECT 'bypassrls'   FROM pg_roles WHERE rolname = r AND rolbypassrls
            UNION ALL SELECT 'membro de ' || g.rolname
                        FROM pg_auth_members m
                        JOIN pg_roles g ON g.oid = m.roleid
                        JOIN pg_roles u ON u.oid = m.member
                       WHERE u.rolname = r
        ) s;
        IF problema IS NOT NULL THEN
            RAISE EXCEPTION '% tem poder a mais: %. Nada foi gravado.', r, problema;
        END IF;

        -- (b) nenhuma escrita em nenhuma tabela/view (nem por coluna, nem via PUBLIC)
        SELECT string_agg(c.relname, ', ') INTO problema
          FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
           -- CASE garante a ordem: a funcao so roda em linha do tipo certo
           -- (o WHERE nao garante ordem; foi o erro "dim_estoque_pkey is not a sequence")
           AND CASE WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
                    THEN has_table_privilege(r, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                         OR has_any_column_privilege(r, c.oid, 'INSERT, UPDATE, REFERENCES')
                    ELSE false END;
        IF problema IS NOT NULL THEN
            RAISE EXCEPTION '% pode escrever em: %. Nada foi gravado.', r, problema;
        END IF;

        -- (c) sem CREATE no schema nem no banco
        IF has_schema_privilege(r, 'public', 'CREATE')
           OR has_database_privilege(r, current_database(), 'CREATE') THEN
            RAISE EXCEPTION '% pode criar objetos. Nada foi gravado.', r;
        END IF;

        -- (c2) nenhuma sequencia (nextval/setval/currval) e nenhuma das
        --      funcoes que gravam custo do Full (SECURITY DEFINER)
        SELECT string_agg(c.relname, ', ') INTO problema
          FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relkind = 'S'
           AND CASE WHEN c.relkind = 'S'
                    THEN has_sequence_privilege(r, c.oid, 'USAGE, SELECT, UPDATE')
                    ELSE false END;
        IF problema IS NOT NULL THEN
            RAISE EXCEPTION '% mexe em sequencia: %. Nada foi gravado.', r, problema;
        END IF;
        SELECT string_agg(p.proname, ', ') INTO problema
          FROM pg_proc p
         WHERE p.pronamespace = 'public'::regnamespace
           AND CASE WHEN p.proname LIKE 'gravar\_custo\_full\_%'
                    THEN has_function_privilege(r, p.oid, 'EXECUTE')
                    ELSE false END;
        IF problema IS NOT NULL THEN
            RAISE EXCEPTION '% executa funcao que grava: %. Nada foi gravado.', r, problema;
        END IF;
    END LOOP;

    -- (d) agentes: nada de dim_usuarios (nem uma coluna) nem dos 4 objetos fora
    SELECT string_agg(o, ', ') INTO problema
      FROM unnest(ARRAY['dim_usuarios', 'v_kanban_cards_por_responsavel',
                        'fact_vendas_snapshot_backup_ml_202609',
                        'fact_vendas_snapshot_backup_shopee_202609',
                        'bkp_magalu_20260817']) AS o
     WHERE CASE WHEN to_regclass('public.' || o) IS NOT NULL
                THEN has_any_column_privilege('agentes_leitura', to_regclass('public.' || o), 'SELECT')
                ELSE false END;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION 'agentes_leitura le o que nao devia: %. Nada foi gravado.', problema;
    END IF;

    -- (e) Patricia: password_hash nunca; o resto de dim_usuarios sim
    IF has_column_privilege('patricia_leitura', 'public.dim_usuarios', 'password_hash', 'SELECT') THEN
        RAISE EXCEPTION 'patricia_leitura le password_hash. Nada foi gravado.';
    END IF;
    IF NOT has_column_privilege('patricia_leitura', 'public.dim_usuarios', 'email', 'SELECT') THEN
        RAISE EXCEPTION 'patricia_leitura ficou sem as colunas de dim_usuarios. Nada foi gravado.';
    END IF;

    -- (f) contagem: agentes = tudo menos 5; Patricia = tudo menos dim_usuarios
    --     (que ela le por coluna, nao pela tabela inteira)
    SELECT count(*) FILTER (WHERE CASE WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
                                       THEN has_table_privilege('agentes_leitura',  c.oid, 'SELECT')
                                       ELSE false END),
           count(*) FILTER (WHERE CASE WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
                                       THEN has_table_privilege('patricia_leitura', c.oid, 'SELECT')
                                       ELSE false END)
      INTO n_ag, n_pa
      FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f');
    IF n_ag <> n_obj - 5 OR n_pa <> n_obj - 1 THEN
        RAISE EXCEPTION 'Contagem fora do esperado: % objetos; agentes leem % (esperado %), Patricia le % (esperado %). Nada foi gravado.',
            n_obj, n_ag, n_obj - 5, n_pa, n_obj - 1;
    END IF;

    RAISE NOTICE 'OK: % tabelas+views no public. agentes_leitura le %, patricia_leitura le % (+ dim_usuarios sem password_hash).',
        n_obj, n_ag, n_pa;
END
$$;

COMMIT;

-- =============================================================================
-- PASSO SEGUINTE (fora do SQL Editor) — credencial: DONO, so na area de
-- transferencia; o script NAO grava nada em disco:
--     python C:\Users\thiag\Nala-Analises\definir_senha_leitura.py agentes_leitura
--     python C:\Users\thiag\Nala-Analises\definir_senha_leitura.py patricia_leitura
-- O script libera o LOGIN, define a senha, deixa a URL do usuario na area de
-- transferencia e roda a prova (INSERT falha, dim_usuarios, password_hash,
-- fact_vendas_snapshot).
-- =============================================================================

-- =============================================================================
-- TABELA NOVA (somente leitura; credencial: DONO). Lista o que ficou sem GRANT.
-- Esperado: so os 5 de fora (agentes) e dim_usuarios (Patricia). Qualquer outra
-- linha = tabela nova: decidir se e sensivel e dar o GRANT SELECT a mao.
--     (MATERIALIZED: filtra o tipo ANTES de chamar has_table_privilege)
--     WITH objs AS MATERIALIZED (
--         SELECT c.oid, c.relname FROM pg_class c
--          WHERE c.relnamespace = 'public'::regnamespace
--            AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))
--     SELECT * FROM (
--         SELECT relname,
--                has_table_privilege('agentes_leitura',  oid, 'SELECT') AS agentes,
--                has_table_privilege('patricia_leitura', oid, 'SELECT') AS patricia
--           FROM objs) s
--      WHERE NOT (agentes AND patricia)
--      ORDER BY 1;
-- =============================================================================

-- =============================================================================
-- DESFAZER — um bloco por usuario (apagar um nao mexe no outro).
-- Credencial: DONO. Para usar: tirar o /* e o */ do bloco escolhido.
-- TRAVA: recusa (nada e apagado) se o usuario for dono de algum objeto, for
-- membro ou tiver membro em algum grupo (fora a ligacao automatica com o
-- neondb_owner, que o criou), tiver permissao em outro banco, ou estiver
-- conectado agora.
-- Pode rodar de novo: se o usuario ja nao existir, nao faz nada.
-- =============================================================================

/* ---------- DESFAZER agentes_leitura ----------
BEGIN;
DO $$
DECLARE
    r        text := 'agentes_leitura';
    rid      oid;
    problema text;
BEGIN
    SELECT oid INTO rid FROM pg_roles WHERE rolname = r;
    IF rid IS NULL THEN
        RAISE NOTICE '% nao existe. Nada a fazer.', r;
        RETURN;
    END IF;
    SELECT string_agg(x, ' | ') INTO problema FROM (
        SELECT 'e dono de ' || count(*) || ' objeto(s)' AS x
          FROM pg_shdepend WHERE refobjid = rid AND deptype = 'o' HAVING count(*) > 0
        UNION ALL
        SELECT 'tem dependencia em outro banco (' || count(*) || ')'
          FROM pg_shdepend
         WHERE refobjid = rid AND dbid NOT IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
        HAVING count(*) > 0
        UNION ALL
        SELECT 'membro de ' || g.rolname
          FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = rid
        UNION ALL
        SELECT 'tem membro ' || u.rolname
          FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member
         WHERE m.roleid = rid AND u.rolname <> 'neondb_owner'
        UNION ALL
        SELECT 'conectado agora (' || count(*) || ' sessao/oes)'
          FROM pg_stat_activity WHERE usename = r HAVING count(*) > 0
    ) s;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION 'DESFAZER RECUSADO para %: %. Nada foi apagado.', r, problema;
    END IF;
    EXECUTE format('DROP OWNED BY %I', r);   -- so tira os GRANTs (nao e dono de nada)
    EXECUTE format('DROP ROLE %I', r);
    RAISE NOTICE '% apagado.', r;
END
$$;
COMMIT;
---------- fim DESFAZER agentes_leitura ---------- */

/* ---------- DESFAZER patricia_leitura ----------
BEGIN;
DO $$
DECLARE
    r        text := 'patricia_leitura';
    rid      oid;
    problema text;
BEGIN
    SELECT oid INTO rid FROM pg_roles WHERE rolname = r;
    IF rid IS NULL THEN
        RAISE NOTICE '% nao existe. Nada a fazer.', r;
        RETURN;
    END IF;
    SELECT string_agg(x, ' | ') INTO problema FROM (
        SELECT 'e dono de ' || count(*) || ' objeto(s)' AS x
          FROM pg_shdepend WHERE refobjid = rid AND deptype = 'o' HAVING count(*) > 0
        UNION ALL
        SELECT 'tem dependencia em outro banco (' || count(*) || ')'
          FROM pg_shdepend
         WHERE refobjid = rid AND dbid NOT IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
        HAVING count(*) > 0
        UNION ALL
        SELECT 'membro de ' || g.rolname
          FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = rid
        UNION ALL
        SELECT 'tem membro ' || u.rolname
          FROM pg_auth_members m JOIN pg_roles u ON u.oid = m.member
         WHERE m.roleid = rid AND u.rolname <> 'neondb_owner'
        UNION ALL
        SELECT 'conectado agora (' || count(*) || ' sessao/oes)'
          FROM pg_stat_activity WHERE usename = r HAVING count(*) > 0
    ) s;
    IF problema IS NOT NULL THEN
        RAISE EXCEPTION 'DESFAZER RECUSADO para %: %. Nada foi apagado.', r, problema;
    END IF;
    EXECUTE format('DROP OWNED BY %I', r);   -- so tira os GRANTs (nao e dono de nada)
    EXECUTE format('DROP ROLE %I', r);
    RAISE NOTICE '% apagado.', r;
END
$$;
COMMIT;
---------- fim DESFAZER patricia_leitura ---------- */
