-- =============================================================================
-- PROVA — preenchimento do historico de codigo_campanha. SO LEITURA.
-- (depois de rodar C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico.sql)
-- =============================================================================
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\ads_codigo_campanha_historico_PROVA.sql
-- ONDE RODAR: projeto Neon "Gestão Marketplaces" (still-shape-14526725),
--             branch "production", banco "neondb", SQL Editor.
-- CREDENCIAL: de preferencia um usuario de LEITURA (roda como agentes_leitura);
--             o DONO tambem serve.
--
-- Nao altera nada e pode rodar a qualquer hora. Refaz, linha a linha, a conta do
-- codigo esperado contra o que esta gravado.
-- RESULTADO ESPERADO: uma tabela com confere = ok ou informativo em todas as linhas.
-- Os numeros "informativo" valem para o preenchimento de 05/11: performance por nome
-- (inferencia) 3.837 linhas e R$ 9.666,42; foto por nome 1.771 linhas; vazias 23 linhas
-- e R$ 1,39, na ML-YanniRJ. Se a coleta recolher esses dias de novo, as vazias mudam.
-- As checagens 6 (so Fita e ABNY com 2 nomes) e 7 (k10 com 3 nomes e 3 codigos) tambem
-- valem so para 05/11: uma renomeacao nova faz as duas darem DIFERENTE sem haver erro.
-- =============================================================================

WITH mapa (loja, codigo, nome, de, ate) AS (VALUES
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
    ('ML-YanniRJ', '359494255', 'Campanha Mercado Livre', DATE '2026-09-28', DATE '2026-11-04')
),
nomes AS (SELECT DISTINCT loja, nome, codigo FROM mapa),
perf AS (
    SELECT p.id, p.loja, p.data, p.campanha, p.codigo_anuncio, p.gasto_ads, p.codigo_campanha,
           CASE WHEN p.data >= DATE '2026-11-01' THEN p.codigo_campanha
                WHEN p.data >= DATE '2026-09-28' THEN
                     (SELECT m.codigo FROM mapa m WHERE m.loja = p.loja AND m.nome = p.campanha AND p.data BETWEEN m.de AND m.ate)
                ELSE (SELECT n.codigo FROM nomes n WHERE n.loja = p.loja AND n.nome = p.campanha) END AS esperado
      FROM public.fact_ads_performance p
     WHERE p.marketplace = 'MERCADO LIVRE'
),
config AS (
    SELECT c.id, c.loja, c.data_captura, c.campanha, c.codigo_campanha,
           CASE WHEN c.data_captura >= TIMESTAMP '2026-11-04 14:00' THEN c.codigo_campanha
                WHEN c.data_captura >= TIMESTAMP '2026-09-29 00:00' THEN
                     (SELECT m.codigo FROM mapa m WHERE m.loja = c.loja AND m.nome = c.campanha AND (c.data_captura::date - 1) BETWEEN m.de AND m.ate)
                ELSE (SELECT n.codigo FROM nomes n WHERE n.loja = c.loja AND n.nome = c.campanha) END AS esperado
      FROM public.fact_ads_campanha_config c
     WHERE c.marketplace = 'MERCADO LIVRE'
),
nome_codigo AS (
    SELECT loja, campanha, codigo_campanha FROM perf WHERE codigo_campanha IS NOT NULL
    UNION
    SELECT loja, campanha, codigo_campanha FROM config WHERE codigo_campanha IS NOT NULL
),
checagem (ordem, o_que, valor, esperado_valor) AS (
    SELECT 1, 'performance ML: codigo diferente do esperado', count(*)::text, '0'
      FROM perf WHERE codigo_campanha IS DISTINCT FROM esperado
    UNION ALL
    SELECT 2, 'foto ML: codigo diferente do esperado', count(*)::text, '0'
      FROM config WHERE codigo_campanha IS DISTINCT FROM esperado
    UNION ALL
    SELECT 3, 'Shopee: linhas com codigo (2 tabelas)',
           ((SELECT count(*) FROM public.fact_ads_performance WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL)
            + (SELECT count(*) FROM public.fact_ads_campanha_config WHERE marketplace <> 'MERCADO LIVRE' AND codigo_campanha IS NOT NULL))::text, '0'
    UNION ALL
    SELECT 4, 'mesmo dia, anuncio e codigo em 2 linhas', count(*)::text, '0'
      FROM (SELECT 1 FROM perf WHERE codigo_campanha IS NOT NULL
             GROUP BY loja, data, codigo_anuncio, codigo_campanha HAVING count(*) > 1) x
    UNION ALL
    SELECT 5, 'nome com 2 codigos', count(*)::text, '0'
      FROM (SELECT 1 FROM nome_codigo GROUP BY loja, campanha HAVING count(DISTINCT codigo_campanha) > 1) x
    UNION ALL
    SELECT 6, 'codigos com 2 nomes (so Fita e ABNY)',
           coalesce(string_agg(codigo_campanha, ', ' ORDER BY codigo_campanha), ''), '356958896, 357159472'
      FROM (SELECT codigo_campanha FROM nome_codigo GROUP BY loja, codigo_campanha HAVING count(DISTINCT campanha) > 1) x
    UNION ALL
    SELECT 7, 'k10 calco rolam: nomes e codigos distintos',
           count(DISTINCT campanha) || ' nomes, ' || count(DISTINCT codigo_campanha) || ' codigos', '3 nomes, 3 codigos'
      FROM nome_codigo WHERE loja = 'ML-LPT' AND campanha LIKE '%k10 calço rolam'
    UNION ALL
    SELECT 8, 'foto ML: linhas vazias', count(*)::text, '0'
      FROM config WHERE codigo_campanha IS NULL
    UNION ALL
    SELECT 20, 'INFORMATIVO performance por nome (inferencia): linhas e R$',
           count(*) || ' linhas, R$ ' || coalesce(sum(gasto_ads), 0), NULL
      FROM perf WHERE data < DATE '2026-09-28' AND codigo_campanha IS NOT NULL
    UNION ALL
    SELECT 21, 'INFORMATIVO foto por nome (inferencia): linhas', count(*)::text, NULL
      FROM config WHERE data_captura < TIMESTAMP '2026-09-29 00:00' AND codigo_campanha IS NOT NULL
    UNION ALL
    SELECT 22, 'INFORMATIVO performance ML vazia: linhas e R$',
           count(*) || ' linhas, R$ ' || coalesce(sum(gasto_ads), 0), NULL
      FROM perf WHERE codigo_campanha IS NULL
    UNION ALL
    SELECT 23, 'INFORMATIVO performance ML vazia, por nome',
           coalesce((SELECT string_agg(loja || ' / ' || campanha || ': ' || n, '; ' ORDER BY loja, campanha)
                       FROM (SELECT loja, campanha, count(*) n FROM perf WHERE codigo_campanha IS NULL GROUP BY 1, 2) y), ''), NULL
)
SELECT ordem, o_que, valor, esperado_valor,
       CASE WHEN esperado_valor IS NULL THEN 'informativo'
            WHEN valor = esperado_valor THEN 'ok' ELSE 'DIFERENTE' END AS confere
  FROM checagem
 ORDER BY ordem;
