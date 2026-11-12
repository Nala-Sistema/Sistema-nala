-- =============================================================================
-- CODIGO DA CAMPANHA — preenche o historico do Mercado Livre
-- (frente [CAMPANHA ID], Fase 2, 05/11/2026)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: DONO do banco (neondb_owner).
-- QUANDO: NUNCA entre 05:15 e 06:30 (Brasília), janela dos coletores (o arquivo
--         recusa), nem das 12:00 as 13:00 se a coleta da madrugada falhou (reserva).
-- COMO: colar o arquivo INTEIRO e executar de uma vez. Trecho rodado separado
--       perde a transacao. "lock timeout" = nada foi gravado, basta repetir.
-- ANTES DE RODAR: passar pelo auditor-tecnico.
-- DEPOIS DE RODAR: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico_PROVA.sql
-- DESFAZER: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico_DESFAZER.sql
--
-- DE ONDE VEM A LISTA
--   Do mapa id->nome do coletor (banco gentle-water-78611482, ml_bruto, recurso
--   ads_mapa_campanhas, 28/09 a 04/11, valendo a 1a rodada de cada dia, como na
--   recoleta). 104 trechos; md5 da lista = c508b068f6c5f640e400f1635c034b2f. Esta e
--   uma carga unica: dado novo continua entrando so pelo coletor.
--
-- REGRA, so em linha do ML com codigo vazio
--   Performance, dia 28/09 a 31/10: o codigo que o mapa dava para aquele nome no dia.
--   Foto de campanha, 29/09 ate antes de 04/11 14:00: o mapa da VESPERA, gravado na
--     mesma rodada da foto.
--   INFERENCIA, performance antes de 28/09 e foto antes de 29/09: nao ha mapa; herda
--     pelo nome. Se uma campanha foi renomeada antes disso, segue como duas.
--   Fora disso a linha fica VAZIA e e listada: nada e chutado. Em novembro a coleta
--   ja grava: nao mexe. A foto extra de 04/11 14:38 ja tem codigo: nao mexe.
--
-- O QUE NAO FAZ: nao mexe em id_campanha, data_importacao, gasto nem indices; nao
-- toca na Shopee; nao trata o defeito do id 324696468 da ML-YanniRJ, so reporta.
--
-- TRAVAS (qualquer uma da ERRO e nada fica gravado): nome com 2 codigos na lista;
-- linha com 2 alvos; codigo ja gravado diferente do mapa; linha alterada fora do
-- codigo; soma de gasto por loja diferente antes x depois; mesmo dia, anuncio e
-- codigo em 2 linhas; nome com 2 codigos; codigo com 2 nomes fora de Fita e ABNY;
-- Fita ou ABNY com nome antigo e novo em codigos diferentes; indice alterado.
--
-- RESULTADO ESPERADO: o editor mostra uma aba por comando. A ultima e a do COMMIT;
-- a PENULTIMA lista as linhas que ficaram vazias; a ANTEPENULTIMA traz o resumo por
-- tabela e loja, com "igual" em todas. Pode rodar de novo: so preenche o que falta.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF current_user <> 'neondb_owner' THEN
        RAISE EXCEPTION 'PARADO: rode como DONO (neondb_owner), nao como %', current_user;
    END IF;
    IF (now() AT TIME ZONE 'America/Sao_Paulo')::time BETWEEN TIME '05:15' AND TIME '06:30' THEN
        RAISE EXCEPTION 'PARADO: janela dos coletores (05:15 a 06:30). Rode depois.';
    END IF;
END $$;

SET LOCAL lock_timeout = '5s';

-- Segura a escrita do coletor enquanto roda (segundos). Leitura segue normal.
LOCK TABLE public.fact_ads_performance, public.fact_ads_campanha_config
    IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE _mapa (loja text, codigo text, nome text, de date, ate date) ON COMMIT DROP;
INSERT INTO _mapa (loja, codigo, nome, de, ate) VALUES
    ('ML-LPT', '7295246', 'Kit 1000 abraç 3,6x200', DATE '2026-10-07', DATE '2026-11-04'),
    ('ML-LPT', '356935333', 'MLB4098106221 Dispenser+talher', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944043', 'MLB5372096652 k10 calço rolam', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944049', 'MLB5379140666 Pote Divisoria', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944071', 'MLB5414599016 k2 kit 5 escov', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944090', 'MLB4034187665 2 kits 3 escovas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944107', 'MLB4048566495 k5 Calço Rolam', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944274', 'MLB5791679996 Embaralhador', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356944300', 'MLB6203130802 Chave de Roda', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356945011', 'MLB4592572611 k4 fio de ny 2mm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356949915', 'MLB5371993452 k10 calço rolam', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356949922', 'MLB4048317803 k10 calço rolam', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356949947', 'MLB5048875864 Defletor', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356950953', 'MLB3873776447 Sup. Vass Inox', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356950989', 'MLB4291557007 K4alicates+ Mart', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356951229', 'MLB4291555145 k100 Abrc. Ameri', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356958878', 'MLB4032155295 Machado', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356958896', 'MLB6105508698 Fita Isolante', DATE '2026-09-28', DATE '2026-10-27'),
    ('ML-LPT', '356958896', '0 MLB6105508698 Fita Isolante', DATE '2026-10-28', DATE '2026-11-04'),
    ('ML-LPT', '356958931', 'MLB4033433891 kit 5 escovas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356959262', 'MLB4291669487 k100 abrac Alemã', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356959433', 'MLB4098104775 k10 bandejas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356959494', 'MLB4042009773 Rebitadeira+100', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356965815', 'MLB5930509322 B Abr ny 3,6x300', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '356965841', 'MLB6203372802 Chave Allen', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357059495', 'MLB4291630121 Martelo 500g', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357091429', 'KIT CHAVE FENDA MLB6203090906', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357108412', 'MLB6136695556 Kit Alicates 8', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357116481', 'Escova Circular MLB4198095381', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357145052', 'MLB4609592277 K1000 ABNY 4X200', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357145111', 'MLB5408537270 K1000 ABNY4*300', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357145124', 'MLB4078334767 AB NY 1000 3*200', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357159472', 'MLB6443622290 ABNY 1000 4X300', DATE '2026-09-28', DATE '2026-10-27'),
    ('ML-LPT', '357159472', '0 MLB6443622290 ABNY1000 4X300', DATE '2026-10-28', DATE '2026-11-04'),
    ('ML-LPT', '357173051', 'MLB5908887786 K ALIC. E MART', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357173591', 'MLB4291645159 K 3 ALICATES', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357203971', 'ALIC. GRIMPAR MLB6203258760', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357220366', 'KIT ESC 321/ 360 MLB4198121295', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357386948', 'MLB4639959197 Torx + Allen', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357387228', 'MLB5872191022 Escv75 kit2', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357387574', 'MLB5779586070 Esc kit tools', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357387791', 'MLB5872215764 Escv 100 lato', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357402119', 'MLB4639907699 kit 24 lapis mar', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357409569', 'MLB4629029167 Serrote', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357413691', 'MLB4291552459 Alicate univer 6', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357770210', 'MLB5408538670 kit 500 abc 4,8', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357796919', 'MLB5908886192 kit30 abc amer', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357894051', 'MLB4291645785 KIT 4 ALICATES', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357919264', 'MLB5371947476 K20 CALÇOS', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357937178', 'MLB4405096581 Cabo Chupeta', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357979416', 'MLB4048446663 K5 Calço e Rolam', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '357986986', 'MLB4639900495 K2 ESTILETE RASP', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358134287', 'TESTE VENDAS ÚLTIMOS 30D', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358140872', 'MLB53313978 Anel Plates', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358394517', 'MLB5872190674 kit 2 esc. 100mm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358607052', 'MLB7184616924 KIT RS Cal e rol', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358675106', 'MLB4992232525 kit 3 ventosas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358758213', 'MLB7218627106 k500 abç 2,5x200', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '358758250', 'MLB7218654826 abç ny 3,6 x 350', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359079992', 'Kit fita colorida e preta', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359179644', 'Kit 500 abraç 3,6x250', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359188019', 'kit 1000 abraç 2,5x200', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359188027', 'kit 40 rolos etiqueta', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359254339', 'Kit alicates 6" + pressão', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359284215', 'kit 400 abraç pequenas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359284268', 'kit 300 abraç pequenas', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359299867', 'kit 12 grampos com mola', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359308067', 'kit alicates + chave fenda', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-LPT', '359415168', 'kit 10 fitas isolantes preta', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '7290427', 'Lupa led', DATE '2026-10-07', DATE '2026-11-04'),
    ('ML-Nala', '7295965', 'Estante multiuso L-0298', DATE '2026-10-07', DATE '2026-11-04'),
    ('ML-Nala', '351413011', 'Porta detergente inox', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '352256049', 'Acrílico 4 andares', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '352676310', 'Suporte vassoura', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '353205236', 'Defletor', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '357151357', 'Anel pilates', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '357244140', 'Kit 10 calços + 10 rolamentos', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '357259697', 'Sup vassoura inox', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '357268642', 'Fios de nylon', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358375533', 'Garrafa térmica 3,2L', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358375537', 'Assadeira biscoito', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358375541', 'Bastão trilha', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358390252', 'Lancheira térmica', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358390264', 'Garrafa inox 750mL', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358390316', 'Acrílico 4 and', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358621615', 'Cuba com tampa', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358629384', 'Kit com 5 bandejas 36x27cm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358639608', 'kit 3 cubas com tampa', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '358646131', 'Bandeja 36x27cm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359161311', 'kit 3 bandejas 40x30cm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359172664', 'kit 2 bastões trilha', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359263509', 'kit 3 bandejas 36x27cm', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359445160', 'kit 2 latas + 2 papeis higien', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359445194', 'Laço natal pequeno', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359453930', 'Papel toalha natal', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359453968', 'kit 2 meias natal cinto noel', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359461147', 'arvore natal mdf', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359461198', 'kit 4 papel higienico natal', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359461290', 'Enfeite caixa natal verde', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359470748', 'lata natal', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359470849', 'Kit laços natal', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359470851', 'Kit 2 latas natal', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-Nala', '359470856', 'Porta papel toalha', DATE '2026-09-28', DATE '2026-11-04'),
    ('ML-YanniRJ', '359494255', 'Campanha Mercado Livre', DATE '2026-09-28', DATE '2026-11-04');

-- Foto ANTES de cada linha: tudo menos codigo_campanha, inclusive id_campanha,
-- data_importacao e as linhas da Shopee.
CREATE TEMP TABLE _foto ON COMMIT DROP AS
    SELECT 'perf'::text AS tabela, t.id, t.marketplace, t.loja, t.gasto_ads AS gasto,
           t.codigo_campanha AS codigo_antes,
           md5((to_jsonb(t) - 'codigo_campanha')::text) AS resto
      FROM public.fact_ads_performance t
    UNION ALL
    SELECT 'config', t.id, t.marketplace, t.loja, NULL::numeric, t.codigo_campanha,
           md5((to_jsonb(t) - 'codigo_campanha')::text)
      FROM public.fact_ads_campanha_config t;

-- Alvo de cada linha vazia
CREATE TEMP TABLE _alvo ON COMMIT DROP AS
    SELECT 'perf'::text AS tabela, p.id, m.codigo, 'mapa'::text AS origem
      FROM public.fact_ads_performance p
      JOIN _mapa m ON m.loja = p.loja AND m.nome = p.campanha AND p.data BETWEEN m.de AND m.ate
     WHERE p.marketplace = 'MERCADO LIVRE' AND p.codigo_campanha IS NULL
       AND p.data >= DATE '2026-09-28' AND p.data < DATE '2026-11-01'
    UNION ALL
    SELECT 'perf', p.id, m.codigo, 'inferencia'
      FROM public.fact_ads_performance p
      JOIN (SELECT DISTINCT loja, nome, codigo FROM _mapa) m ON m.loja = p.loja AND m.nome = p.campanha
     WHERE p.marketplace = 'MERCADO LIVRE' AND p.codigo_campanha IS NULL
       AND p.data < DATE '2026-09-28'
    UNION ALL
    SELECT 'config', c.id, m.codigo, 'mapa'
      FROM public.fact_ads_campanha_config c
      JOIN _mapa m ON m.loja = c.loja AND m.nome = c.campanha
                  AND (c.data_captura::date - 1) BETWEEN m.de AND m.ate
     WHERE c.marketplace = 'MERCADO LIVRE' AND c.codigo_campanha IS NULL
       AND c.data_captura >= TIMESTAMP '2026-09-29 00:00'
       AND c.data_captura < TIMESTAMP '2026-11-04 14:00'
    UNION ALL
    SELECT 'config', c.id, m.codigo, 'inferencia'
      FROM public.fact_ads_campanha_config c
      JOIN (SELECT DISTINCT loja, nome, codigo FROM _mapa) m ON m.loja = c.loja AND m.nome = c.campanha
     WHERE c.marketplace = 'MERCADO LIVRE' AND c.codigo_campanha IS NULL
       AND c.data_captura < TIMESTAMP '2026-09-29 00:00';

DO $$
DECLARE
    lista text;
BEGIN
    SELECT string_agg(loja || ': ' || nome, '; ') INTO lista FROM (
        SELECT loja, nome FROM _mapa GROUP BY 1, 2 HAVING count(DISTINCT codigo) > 1) x;
    IF lista IS NOT NULL THEN
        RAISE EXCEPTION 'PARADO: nome com 2 codigos, nao preencho por nome: %', lista;
    END IF;
    IF EXISTS (SELECT 1 FROM _alvo GROUP BY tabela, id HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'PARADO: linha com mais de um alvo';
    END IF;
END $$;

UPDATE public.fact_ads_performance p
   SET codigo_campanha = a.codigo
  FROM _alvo a
 WHERE a.tabela = 'perf' AND a.id = p.id AND p.codigo_campanha IS NULL;

UPDATE public.fact_ads_campanha_config c
   SET codigo_campanha = a.codigo
  FROM _alvo a
 WHERE a.tabela = 'config' AND a.id = c.id AND c.codigo_campanha IS NULL;

DO $$
DECLARE
    n     bigint;
    lista text;
BEGIN
    -- nada alem do codigo mudou, nenhuma linha entrou nem saiu
    SELECT count(*) INTO n FROM (
        SELECT f.id FROM _foto f
          LEFT JOIN public.fact_ads_performance t ON t.id = f.id
         WHERE f.tabela = 'perf' AND (t.id IS NULL OR md5((to_jsonb(t) - 'codigo_campanha')::text) <> f.resto)
        UNION ALL
        SELECT f.id FROM _foto f
          LEFT JOIN public.fact_ads_campanha_config t ON t.id = f.id
         WHERE f.tabela = 'config' AND (t.id IS NULL OR md5((to_jsonb(t) - 'codigo_campanha')::text) <> f.resto)
    ) x;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: % linha(s) mudaram fora do codigo_campanha', n;
    END IF;
    IF (SELECT count(*) FROM public.fact_ads_performance) <> (SELECT count(*) FROM _foto WHERE tabela = 'perf')
       OR (SELECT count(*) FROM public.fact_ads_campanha_config) <> (SELECT count(*) FROM _foto WHERE tabela = 'config') THEN
        RAISE EXCEPTION 'PROVA: o numero de linhas mudou';
    END IF;

    -- soma de gasto por loja, antes x depois, nesta rodada
    SELECT count(*) INTO n FROM (
        SELECT loja FROM _foto WHERE tabela = 'perf' GROUP BY 1, marketplace
        HAVING sum(gasto) IS DISTINCT FROM (SELECT sum(p.gasto_ads) FROM public.fact_ads_performance p
                                             WHERE p.loja = _foto.loja AND p.marketplace = _foto.marketplace)
    ) x;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: a soma de gasto mudou em % loja(s)', n;
    END IF;

    -- Shopee continua vazia
    IF EXISTS (SELECT 1 FROM public.fact_ads_performance WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL)
       OR EXISTS (SELECT 1 FROM public.fact_ads_campanha_config WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL) THEN
        RAISE EXCEPTION 'PROVA: apareceu codigo fora do Mercado Livre';
    END IF;

    -- indices da coleta intocados
    IF (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_ads_perf_dia_anuncio_campanha') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_ads_perf_dia_anuncio_campanha ON public.fact_ads_performance USING btree (marketplace, loja, periodo_inicio, periodo_fim, codigo_anuncio, id_campanha)'
       OR (SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_campanha_config_id') IS DISTINCT FROM
       'CREATE UNIQUE INDEX ux_campanha_config_id ON public.fact_ads_campanha_config USING btree (marketplace, loja, id_campanha, data_captura)' THEN
        RAISE EXCEPTION 'PROVA: um indice da coleta mudou';
    END IF;

    -- codigo que ja estava gravado continua igual
    SELECT count(*) INTO n FROM _foto f
      JOIN public.fact_ads_performance t ON t.id = f.id
     WHERE f.tabela = 'perf' AND f.codigo_antes IS NOT NULL AND t.codigo_campanha IS DISTINCT FROM f.codigo_antes;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PROVA: % codigo(s) ja gravados mudaram', n;
    END IF;

    -- codigo gravado pela coleta tem de bater com o mapa do mesmo dia
    SELECT count(*) INTO n FROM public.fact_ads_performance p
      JOIN _mapa m ON m.loja = p.loja AND m.nome = p.campanha AND p.data BETWEEN m.de AND m.ate
     WHERE p.marketplace = 'MERCADO LIVRE' AND p.codigo_campanha IS NOT NULL AND p.codigo_campanha <> m.codigo;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PARADO: % linha(s) com codigo diferente do mapa', n;
    END IF;

    -- mesmo dia, anuncio e codigo em 2 linhas seria gasto em dobro
    SELECT count(*) INTO n FROM (
        SELECT 1 FROM public.fact_ads_performance
         WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL
         GROUP BY loja, data, codigo_anuncio, codigo_campanha HAVING count(*) > 1) x;
    IF n <> 0 THEN
        RAISE EXCEPTION 'PARADO: % caso(s) de mesmo dia, anuncio e codigo em 2 linhas', n;
    END IF;

    -- nome com 2 codigos, nas duas tabelas
    SELECT string_agg(loja || ': ' || campanha, '; ') INTO lista FROM (
        SELECT loja, campanha FROM (
            SELECT loja, campanha, codigo_campanha FROM public.fact_ads_performance
             WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL
            UNION
            SELECT loja, campanha, codigo_campanha FROM public.fact_ads_campanha_config
             WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL) u
         GROUP BY 1, 2 HAVING count(DISTINCT codigo_campanha) > 1) x;
    IF lista IS NOT NULL THEN
        RAISE EXCEPTION 'PARADO: nome com 2 codigos: %', lista;
    END IF;

    -- codigo com 2 nomes: so as 2 renomeacoes de 28/10
    SELECT string_agg(loja || ': ' || codigo_campanha, '; ') INTO lista FROM (
        SELECT loja, codigo_campanha FROM (
            SELECT loja, campanha, codigo_campanha FROM public.fact_ads_performance
             WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL
            UNION
            SELECT loja, campanha, codigo_campanha FROM public.fact_ads_campanha_config
             WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL) u
         GROUP BY 1, 2 HAVING count(DISTINCT campanha) > 1
                          AND codigo_campanha NOT IN ('356958896', '357159472')) x;
    IF lista IS NOT NULL THEN
        RAISE EXCEPTION 'PARADO: codigo com 2 nomes fora de Fita e ABNY: %', lista;
    END IF;

    -- Fita e ABNY: nome antigo e novo no mesmo codigo
    IF EXISTS (SELECT 1 FROM public.fact_ads_performance
                WHERE loja = 'ML-LPT'
                  AND campanha IN ('MLB6105508698 Fita Isolante', '0 MLB6105508698 Fita Isolante')
                  AND codigo_campanha IS DISTINCT FROM '356958896')
       OR EXISTS (SELECT 1 FROM public.fact_ads_performance
                   WHERE loja = 'ML-LPT'
                     AND campanha IN ('MLB6443622290 ABNY 1000 4X300', '0 MLB6443622290 ABNY1000 4X300')
                     AND codigo_campanha IS DISTINCT FROM '357159472')
       OR EXISTS (SELECT 1 FROM public.fact_ads_campanha_config
                   WHERE loja = 'ML-LPT'
                     AND campanha IN ('MLB6105508698 Fita Isolante', '0 MLB6105508698 Fita Isolante')
                     AND codigo_campanha IS DISTINCT FROM '356958896')
       OR EXISTS (SELECT 1 FROM public.fact_ads_campanha_config
                   WHERE loja = 'ML-LPT'
                     AND campanha IN ('MLB6443622290 ABNY 1000 4X300', '0 MLB6443622290 ABNY1000 4X300')
                     AND codigo_campanha IS DISTINCT FROM '357159472') THEN
        RAISE EXCEPTION 'PROVA: Fita ou ABNY com linha fora do codigo certo';
    END IF;
END $$;

-- RESUMO (antepenultima aba). Por_mapa e por_nome contam o que ESTA rodada preencheu.
WITH agora AS (
    SELECT 'perf'::text AS tabela, marketplace, loja, id, gasto_ads AS gasto, codigo_campanha
      FROM public.fact_ads_performance
    UNION ALL
    SELECT 'config', marketplace, loja, id, NULL::numeric, codigo_campanha
      FROM public.fact_ads_campanha_config
)
SELECT g.tabela, g.marketplace, g.loja,
       count(*) AS linhas,
       count(*) FILTER (WHERE f.codigo_antes IS NOT NULL) AS ja_tinham,
       count(*) FILTER (WHERE a.origem = 'mapa') AS por_mapa,
       count(*) FILTER (WHERE a.origem = 'inferencia') AS por_nome_inferencia,
       coalesce(sum(g.gasto) FILTER (WHERE a.origem = 'inferencia'), 0) AS gasto_por_nome_inferencia,
       count(*) FILTER (WHERE g.codigo_campanha IS NULL) AS vazias,
       sum(f.gasto) AS gasto_antes,
       sum(g.gasto) AS gasto_depois,
       CASE WHEN sum(f.gasto) IS NOT DISTINCT FROM sum(g.gasto) THEN 'igual' ELSE 'DIFERENTE' END AS confere
  FROM agora g
  JOIN _foto f ON f.tabela = g.tabela AND f.id = g.id
  LEFT JOIN _alvo a ON a.tabela = g.tabela AND a.id = g.id
 GROUP BY 1, 2, 3
 ORDER BY 1, 2, 3;

-- O QUE FICOU VAZIO NO ML (penultima aba)
SELECT 'perf' AS tabela, loja, campanha, min(data)::text AS de, max(data)::text AS ate,
       count(*) AS linhas, sum(gasto_ads) AS gasto
  FROM public.fact_ads_performance
 WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NULL
 GROUP BY 2, 3
UNION ALL
SELECT 'config', loja, campanha, min(data_captura)::date::text, max(data_captura)::date::text,
       count(*), NULL
  FROM public.fact_ads_campanha_config
 WHERE marketplace = 'MERCADO LIVRE' AND codigo_campanha IS NULL
 GROUP BY 2, 3
 ORDER BY 1, 2, 3;

COMMIT;
