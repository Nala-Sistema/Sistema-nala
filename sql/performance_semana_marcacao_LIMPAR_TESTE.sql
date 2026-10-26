-- =============================================================================
-- LIMPAR O TESTE DA MARCAÇÃO (usuário admin)   [PERFORMANCE SEMANAL] E1
-- =============================================================================
-- 26/10/2026. O teste da marcação na aba Semana é feito SÓ com o usuário admin
-- (o único ADMIN em dim_usuarios). Este arquivo apaga as marcações do admin em
-- perf_semana_marcacao, e nada mais: as de todos os outros usuários ficam.
-- A tabela só acrescenta para o app (o gatilho recusa UPDATE); o DELETE é do dono.
--
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner). Rodar tudo de uma vez.
-- RESULTADO ESPERADO: a conferência no fim mostra 0 marcações do admin.
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DELETE FROM public.perf_semana_marcacao WHERE decidido_por = 'admin';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.perf_semana_marcacao WHERE decidido_por = 'admin') THEN
        RAISE EXCEPTION 'Sobrou teste do admin. Nada foi apagado.';
    END IF;
END $$;

COMMIT;

SELECT (SELECT count(*) FROM public.perf_semana_marcacao WHERE decidido_por = 'admin') AS marcacoes_admin,
       (SELECT count(*) FROM public.perf_semana_marcacao) AS marcacoes_total;
