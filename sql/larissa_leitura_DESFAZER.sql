-- =============================================================================
-- DESFAZER — usuario larissa_leitura (de sql\larissa_leitura.sql)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\larissa_leitura_DESFAZER.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb".
-- CREDENCIAL: DONO do banco (neondb_owner), no SQL Editor.
--
-- So apaga larissa_leitura; nao mexe em agentes_leitura nem em patricia_leitura.
-- Para so cortar o acesso sem apagar (ex.: chave vazou), basta:
--     ALTER ROLE larissa_leitura WITH NOLOGIN;
-- e depois gerar senha nova com definir_senha_leitura.py larissa_leitura.
--
-- TRAVA: recusa (nada e apagado) se o usuario for dono de algum objeto, for
-- membro ou tiver membro em algum grupo (fora a ligacao automatica com o
-- neondb_owner, que o criou), tiver permissao em outro banco, ou estiver
-- conectado agora. Pode rodar de novo: se ja nao existir, nao faz nada.
-- =============================================================================
BEGIN;
DO $$
DECLARE
    r        text := 'larissa_leitura';
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
