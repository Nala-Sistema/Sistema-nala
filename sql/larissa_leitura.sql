-- =============================================================================
-- ACESSO SO LEITURA — usuario larissa_leitura (socia, Mercado Livre)
-- (frente [GESTORAS CLAUDE CODE], 27/10/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\larissa_leitura.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: DONO do banco (neondb_owner), no SQL Editor.
-- ANTES DE RODAR: passar pelo auditor-tecnico.
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\larissa_leitura_DESFAZER.sql
-- SENHA: depois, com C:\Users\<usuario>\Nala-Analises\definir_senha_leitura.py larissa_leitura
--
-- O QUE FAZ (igual ao patricia_leitura de sql\acesso_leitura.sql)
--   1. Cria o usuario larissa_leitura SEM LOGIN ate o script de senha, sem
--      nenhum poder especial.
--   2. SELECT em TUDO do schema public, inclusive custo, margem e backups;
--      dim_usuarios SO por coluna, sem password_hash.
--      => SELECT * FROM dim_usuarios FALHA para ela. E esperado.
--      (O nala_sql.py recusa, para qualquer usuario, consulta que cite backup
--      de venda; aqui o banco deixa ler, igual a Patricia.)
--   3. Limites: transacao so leitura, consulta cortada em 60 s, transacao
--      parada cortada em 60 s, no maximo 3 conexoes.
--   4. Confere tudo no fim. Se algo sair do desenho, da ERRO e desfaz tudo.
--
-- O QUE NAO FAZ (de proposito)
--   - NAO toca em agentes_leitura nem em patricia_leitura.
--   - Nenhum INSERT/UPDATE/DELETE/TRUNCATE em nada, nenhum CREATE no schema.
--   - NAO usa pg_read_all_data (daria o password_hash) nem permissao automatica
--     para tabela futura: TABELA NOVA PRECISA DE GRANT A MAO.
--   - NAO define senha (passo do script, fora do git).
--
-- ATENCAO
--   - O banco e o mesmo para producao e dev: ela ve dado de producao.
--   - "So leitura" e os tempos-limite sao cinto de seguranca: a trava de
--     verdade e nao ter GRANT de escrita. O limite de conexoes ela nao muda.
--   - Pode criar tabela TEMPORARIA (TEMP de PUBLIC, igual aos outros dois;
--     some no fim da sessao). Nao se revoga sem tirar dos coletores.
--
-- E SEGURO RODAR DE NOVO: zera as permissoes de leitura dela e concede outra
-- vez; nao mexe no LOGIN nem na senha, se ja tiver.
-- =============================================================================
BEGIN;

-- 1. Usuario (por SQL, nao pelo console do Neon, que daria neon_superuser).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'larissa_leitura') THEN
        CREATE ROLE larissa_leitura NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                                    NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

-- 2. Limites (valem a cada nova conexao).
ALTER ROLE larissa_leitura CONNECTION LIMIT 3;
ALTER ROLE larissa_leitura SET default_transaction_read_only = on;
ALTER ROLE larissa_leitura SET statement_timeout = '60s';
ALTER ROLE larissa_leitura SET idle_in_transaction_session_timeout = '60s';

-- 3. Permissoes: zera e concede de novo. REVOKE ALL na tabela tira tambem
--    permissao por coluna.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM larissa_leitura;
REVOKE ALL ON SCHEMA public FROM larissa_leitura;
GRANT USAGE ON SCHEMA public TO larissa_leitura;

DO $$
DECLARE
    obj record;
BEGIN
    FOR obj IN
        SELECT c.relname
          FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
           AND c.relname <> 'dim_usuarios'          -- recebe por coluna abaixo
         ORDER BY c.relname
    LOOP
        EXECUTE format('GRANT SELECT ON public.%I TO larissa_leitura', obj.relname);
    END LOOP;
END
$$;

-- dim_usuarios coluna por coluna, SEM password_hash. Mesma lista da Patricia,
-- escrita a mao de proposito: coluna nova NAO entra sozinha.
GRANT SELECT (id_usuario, username, nome_completo, nome, email, role, ativo,
              data_criacao, ultimo_acesso, criado_por, created_at, updated_at)
    ON public.dim_usuarios TO larissa_leitura;

-- 4. Conferencia. Qualquer desvio => ERRO => nada do arquivo fica gravado.
DO $$
DECLARE
    r        text := 'larissa_leitura';
    n_obj    int;
    n_la     int;
    n_pa     int;
    problema text;
BEGIN
    SELECT count(*) INTO n_obj
      FROM pg_class
     WHERE relnamespace = 'public'::regnamespace
       AND relkind IN ('r', 'p', 'v', 'm', 'f');

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

    -- (c2) nenhuma sequencia e nenhuma das funcoes que gravam custo do Full
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

    -- (d) password_hash nunca; o resto de dim_usuarios sim
    IF has_column_privilege(r, 'public.dim_usuarios', 'password_hash', 'SELECT') THEN
        RAISE EXCEPTION '% le password_hash. Nada foi gravado.', r;
    END IF;
    IF NOT has_column_privilege(r, 'public.dim_usuarios', 'email', 'SELECT') THEN
        RAISE EXCEPTION '% ficou sem as colunas de dim_usuarios. Nada foi gravado.', r;
    END IF;

    -- (e) contagem: tudo menos dim_usuarios (que ela le por coluna), e o mesmo
    --     numero da Patricia
    SELECT count(*) FILTER (WHERE CASE WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
                                       THEN has_table_privilege(r, c.oid, 'SELECT')
                                       ELSE false END),
           count(*) FILTER (WHERE CASE WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
                                       THEN has_table_privilege('patricia_leitura', c.oid, 'SELECT')
                                       ELSE false END)
      INTO n_la, n_pa
      FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f');
    IF n_la <> n_obj - 1 THEN
        RAISE EXCEPTION 'Contagem fora do esperado: % objetos; larissa_leitura le % (esperado %). Nada foi gravado.',
            n_obj, n_la, n_obj - 1;
    END IF;

    RAISE NOTICE 'OK: % tabelas+views no public. larissa_leitura le % (+ dim_usuarios sem password_hash). patricia_leitura le % (se diferente, falta GRANT de tabela nova para ela).',
        n_obj, n_la, n_pa;
END
$$;

COMMIT;

-- =============================================================================
-- PASSO SEGUINTE (fora do SQL Editor), no PC da Larissa, com o Thiago ao lado.
-- Credencial: DONO, so na area de transferencia; o script NAO grava nada em disco:
--     python C:\Users\<usuario>\Nala-Analises\definir_senha_leitura.py larissa_leitura
-- Ver PASSO_A_PASSO_SOCIAS.md (passo 6) no nala-analises.
-- =============================================================================

-- =============================================================================
-- TABELA NOVA (somente leitura; credencial: DONO). Lista o que ficou sem GRANT.
-- Esperado: so dim_usuarios para as duas socias.
--     WITH objs AS MATERIALIZED (
--         SELECT c.oid, c.relname FROM pg_class c
--          WHERE c.relnamespace = 'public'::regnamespace
--            AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))
--     SELECT * FROM (
--         SELECT relname,
--                has_table_privilege('agentes_leitura',  oid, 'SELECT') AS agentes,
--                has_table_privilege('patricia_leitura', oid, 'SELECT') AS patricia,
--                has_table_privilege('larissa_leitura',  oid, 'SELECT') AS larissa
--           FROM objs) s
--      WHERE NOT (agentes AND patricia AND larissa)
--      ORDER BY 1;
-- =============================================================================
