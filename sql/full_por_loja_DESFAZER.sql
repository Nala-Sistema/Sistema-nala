-- =============================================================================
-- DESFAZER C:\Users\thiag\Sistema-nala\sql\full_por_loja.sql   [FULL POR LOJA]
-- =============================================================================
-- ONDE RODAR: Neon "Gestão Marketplaces" (still-shape-14526725), branch
--             "production", banco "neondb", SQL EDITOR, credencial do DONO
--             (neondb_owner).
--
-- RECUSA RODAR se existir QUALQUER ciente ou histórico com regra 'full_parado',
-- ou qualquer data informada em full_proxima_data_envio: a CHECK antiga não
-- aceitaria a linha, e apagar destruiria o registro das gestoras. Nesse caso,
-- exporte (SELECT * ... -> CSV) e decida com o Thiago.
-- EFEITO: a tabela some e as CHECKs voltam às 14 regras. Antes de rodar, o código
-- da aba "Full por loja" tem que ter saído do ar (senão a gravação é recusada;
-- a leitura cai para "aguardando o SQL" sozinha).
-- =============================================================================

BEGIN;

SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n FROM public.sinal_ciente WHERE regra = 'full_parado';
    IF n > 0 THEN
        RAISE EXCEPTION 'Há % ciente(s) full_parado. Exporte antes e decida com o Thiago. Nada foi desfeito.', n;
    END IF;
    SELECT count(*) INTO n FROM public.sinal_historico WHERE regra = 'full_parado';
    IF n > 0 THEN
        RAISE EXCEPTION 'Há % linha(s) full_parado no histórico. Nada foi desfeito.', n;
    END IF;
    IF to_regclass('public.full_proxima_data_envio') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM public.full_proxima_data_envio' INTO n;
        IF n > 0 THEN
            RAISE EXCEPTION 'Há % data(s) informada(s) pelas gestoras. Exporte antes e decida com o Thiago. Nada foi desfeito.', n;
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS public.full_proxima_data_envio;

ALTER TABLE public.sinal_ciente DROP CONSTRAINT sinal_ciente_regra_valida;
ALTER TABLE public.sinal_ciente ADD CONSTRAINT sinal_ciente_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta'));

ALTER TABLE public.sinal_historico DROP CONSTRAINT sinal_historico_regra_valida;
ALTER TABLE public.sinal_historico ADD CONSTRAINT sinal_historico_regra_valida CHECK (regra IN (
    'vendas_queda', 'vendas_alta', 'full_cobertura', 'full_zerado_ads', 'full_envio',
    'full_ruptura', 'ads_config', 'ads_sem_venda', 'ads_custo', 'ads_espiral',
    'exp_anuncio', 'exp_familia', 'visitas_queda', 'visitas_alta'));

COMMIT;
