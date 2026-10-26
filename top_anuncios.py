"""
TOP ANÚNCIOS — a consulta da tabela de ataque (frente [TOP 30], main d9d4315),
num módulo comum desde 26/10/2026 ([PERFORMANCE SEMANAL] E1).

Quem usa (a MESMA consulta, nunca copiada):
  - analise_produtos.py, aba Mais Vendidos, modo "Anúncio (loja)": Top N GERAL
    das lojas filtradas, no período escolhido (por_loja=False).
  - performance_semana.py, aba "Semana" do Sinais do Dia: Top 30 POR LOJA por
    receita de 30 dias, mais a semana seg–dom fechada e as 4 semanas antes
    (por_loja=True). As regras de ataque leem as colunas da semana.

Regras de dado (iguais à [TOP 30]):
  - Venda só de fact_vendas_snapshot (fonte única). Na Shopee a venda não traz
    o item_id: ele vem pela PONTE fact_pedidos_itens_marketplace (loja,
    pedido, sku) e, se faltar, de dim_estoque_anuncio quando o SKU tem um
    único anúncio. A ponte nunca soma receita. ML: o anúncio é o
    codigo_anuncio da própria venda.
  - Ads, visitas e views entram já agregados por (loja, anúncio) para o JOIN
    não multiplicar gasto nem visita.
  - Views da Shopee = janela móvel de 30 dias (última foto até o fim do
    período); nunca comparadas com a visita do ML.
  - Pedidos = numero_pedido distintos (no ML é o pack).
"""

from datetime import timedelta

import pandas as pd

# Lojas com coleta por API (nome no banco -> sigla de exibição). Vendas, ads,
# visitas e views usam o MESMO nome de loja no banco; só a Shopee da Nala foge
# da sigla ("Shopee Lithouse(Nala)").
LOJAS_ATAQUE = {
    'ML-LPT': 'ML-LPT',
    'ML-Nala': 'ML-Nala',
    'ML-YanniRJ': 'ML-YanniRJ',
    'ML-YanniSP': 'ML-YanniSP',
    'Shopee Lithouse(Nala)': 'Shopee-Nala',
    'Shopee-LPT': 'Shopee-LPT',
}
LOJAS_SEM_ADS = {'ML-YanniSP'}
ORIGENS_SUGESTAO = ('performance', 'ads', 'escala')

COLS_NUM = ['un', 'rec', 'marg', 'ped', 'un_ant', 'rec_ant',
            'imp', 'cli', 'gasto', 'rec_ads', 'visitas', 'views_30d',
            'rec_sem', 'un_sem', 'ped_sem', 'rec_base', 'ped_base',
            'vis_sem', 'vis_base', 'imp_sem', 'cli_sem', 'gasto_sem', 'rec_ads_sem',
            'imp_base', 'cli_base', 'gasto_base', 'pos']

# Janelas: período (ini..fim), anterior (ini_ant..fim_ant), semana
# (sem_ini..sem_fim) e base (base_ini..base_fim). A Mais Vendidos passa a
# semana = período e a base = anterior (não lê nada a mais). O corte do Top N
# é geral ou por loja (por_loja), pela receita do período.
SQL_ATAQUE = """
WITH vend AS (
    SELECT f.loja_origem AS loja, f.marketplace_origem AS mkt, f.data_venda, f.sku,
           f.numero_pedido, f.quantidade, f.valor_venda_efetivo, f.margem_total,
           CASE WHEN f.marketplace_origem = 'SHOPEE'
                THEN COALESCE(p.id_anuncio_plataforma, d.anuncio_id,
                              'sem item ' || f.sku)
                ELSE f.codigo_anuncio END AS chave
    FROM fact_vendas_snapshot f
    LEFT JOIN (SELECT DISTINCT loja, numero_pedido, sku, id_anuncio_plataforma
               FROM fact_pedidos_itens_marketplace
               WHERE marketplace = 'SHOPEE' AND loja = ANY(%(lojas)s)) p
           ON f.marketplace_origem = 'SHOPEE' AND p.loja = f.loja_origem
          AND p.numero_pedido = f.numero_pedido AND p.sku = f.sku
    LEFT JOIN (SELECT loja, sku, MIN(anuncio_id) AS anuncio_id
               FROM dim_estoque_anuncio
               WHERE marketplace = 'SHOPEE' AND loja = ANY(%(lojas)s)
               GROUP BY loja, sku HAVING COUNT(DISTINCT anuncio_id) = 1) d
           ON f.marketplace_origem = 'SHOPEE' AND d.loja = f.loja_origem
          AND d.sku = f.sku
    WHERE f.loja_origem = ANY(%(lojas)s)
      AND f.data_venda BETWEEN %(ini_le)s AND %(fim_le)s
),
atual AS (
    SELECT loja, mkt, chave,
           SUM(quantidade)::bigint AS un,
           SUM(valor_venda_efetivo) AS rec,
           SUM(margem_total) AS marg,
           COUNT(DISTINCT numero_pedido) AS ped,
           string_agg(DISTINCT sku, ', ' ORDER BY sku) AS skus,
           (array_agg(sku ORDER BY valor_venda_efetivo DESC))[1] AS sku_top
    FROM vend WHERE data_venda BETWEEN %(ini)s AND %(fim)s
    GROUP BY loja, mkt, chave
),
ranq AS (
    SELECT a.*, ROW_NUMBER() OVER (
               PARTITION BY CASE WHEN %(por_loja)s THEN a.loja ELSE '' END
               ORDER BY a.rec DESC, a.chave) AS pos
    FROM atual a
),
anterior AS (
    SELECT loja, chave, SUM(quantidade)::bigint AS un, SUM(valor_venda_efetivo) AS rec
    FROM vend WHERE data_venda BETWEEN %(ini_ant)s AND %(fim_ant)s
    GROUP BY loja, chave
),
semana AS (
    SELECT loja, chave,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda BETWEEN %(sem_ini)s AND %(sem_fim)s) AS rec_sem,
           SUM(quantidade) FILTER (WHERE data_venda BETWEEN %(sem_ini)s AND %(sem_fim)s) AS un_sem,
           COUNT(DISTINCT numero_pedido) FILTER (WHERE data_venda BETWEEN %(sem_ini)s AND %(sem_fim)s) AS ped_sem,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda BETWEEN %(base_ini)s AND %(base_fim)s) AS rec_base,
           COUNT(DISTINCT numero_pedido) FILTER (WHERE data_venda BETWEEN %(base_ini)s AND %(base_fim)s) AS ped_base
    FROM vend GROUP BY loja, chave
),
ads AS (
    SELECT loja, codigo_anuncio AS chave,
           SUM(impressoes) FILTER (WHERE data BETWEEN %(ini)s AND %(fim)s) AS imp,
           SUM(cliques) FILTER (WHERE data BETWEEN %(ini)s AND %(fim)s) AS cli,
           SUM(gasto_ads) FILTER (WHERE data BETWEEN %(ini)s AND %(fim)s) AS gasto,
           SUM(receita_ads) FILTER (WHERE data BETWEEN %(ini)s AND %(fim)s) AS rec_ads,
           SUM(impressoes) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS imp_sem,
           SUM(cliques) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS cli_sem,
           SUM(gasto_ads) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS gasto_sem,
           SUM(receita_ads) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS rec_ads_sem,
           SUM(impressoes) FILTER (WHERE data BETWEEN %(base_ini)s AND %(base_fim)s) AS imp_base,
           SUM(cliques) FILTER (WHERE data BETWEEN %(base_ini)s AND %(base_fim)s) AS cli_base,
           SUM(gasto_ads) FILTER (WHERE data BETWEEN %(base_ini)s AND %(base_fim)s) AS gasto_base
    FROM fact_ads_performance
    WHERE loja = ANY(%(lojas)s) AND data BETWEEN %(ini_le)s AND %(fim_le)s
    GROUP BY loja, codigo_anuncio
),
vis AS (
    SELECT loja, codigo_anuncio AS chave,
           SUM(visitas) FILTER (WHERE data BETWEEN %(ini)s AND %(fim)s) AS visitas,
           SUM(visitas) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS vis_sem,
           SUM(visitas) FILTER (WHERE data BETWEEN %(base_ini)s AND %(base_fim)s) AS vis_base
    FROM vw_visitas_dia
    WHERE loja = ANY(%(lojas)s) AND data BETWEEN %(ini_le)s AND %(fim_le)s
    GROUP BY loja, codigo_anuncio
),
vws AS (
    SELECT DISTINCT ON (loja, codigo_anuncio) loja, codigo_anuncio AS chave, views_30d
    FROM vw_views_shopee_30d
    WHERE loja = ANY(%(lojas)s) AND data <= %(fim)s
    ORDER BY loja, codigo_anuncio, data DESC
)
SELECT a.loja, a.mkt, a.chave, a.skus, a.sku_top, a.pos,
       COALESCE(pr.nome, '(sem cadastro)') AS nome,
       a.un, a.rec, a.marg, a.ped,
       ant.un AS un_ant, ant.rec AS rec_ant,
       ads.imp, ads.cli, ads.gasto, ads.rec_ads,
       vis.visitas, vws.views_30d,
       s.rec_sem, s.un_sem, s.ped_sem, s.rec_base, s.ped_base,
       vis.vis_sem, vis.vis_base,
       ads.imp_sem, ads.cli_sem, ads.gasto_sem, ads.rec_ads_sem,
       ads.imp_base, ads.cli_base, ads.gasto_base
FROM ranq a
LEFT JOIN anterior ant ON ant.loja = a.loja AND ant.chave = a.chave
LEFT JOIN semana s ON s.loja = a.loja AND s.chave = a.chave
LEFT JOIN ads ON ads.loja = a.loja AND ads.chave = a.chave
LEFT JOIN vis ON vis.loja = a.loja AND vis.chave = a.chave
LEFT JOIN vws ON vws.loja = a.loja AND vws.chave = a.chave
LEFT JOIN dim_produtos pr ON pr.sku = a.sku_top
WHERE a.pos <= %(limite)s
ORDER BY a.rec DESC
"""


def periodo_anterior(ini, fim):
    """Mesmo nº de dias imediatamente antes de `ini` (fim já é dia fechado)."""
    dias = (fim - ini).days + 1
    fim_ant = ini - timedelta(days=1)
    return fim_ant - timedelta(days=dias - 1), fim_ant


def params_ataque(lojas_db, ini, fim, limite, por_loja=False, semana=None, base=None):
    """Parâmetros da SQL_ATAQUE. Sem semana/base: semana = período e base =
    período anterior (a Mais Vendidos não lê nada a mais)."""
    ini_ant, fim_ant = periodo_anterior(ini, fim)
    sem_ini, sem_fim = semana or (ini, fim)
    base_ini, base_fim = base or (ini_ant, fim_ant)
    return {
        'lojas': list(lojas_db), 'ini': ini, 'fim': fim,
        'ini_ant': ini_ant, 'fim_ant': fim_ant,
        'sem_ini': sem_ini, 'sem_fim': sem_fim, 'base_ini': base_ini, 'base_fim': base_fim,
        'ini_le': min(ini_ant, sem_ini, base_ini), 'fim_le': max(fim, sem_fim, base_fim),
        'limite': limite, 'por_loja': bool(por_loja),
    }


def consultar_ataque(engine, lojas_db, ini, fim, limite, por_loja=False, semana=None,
                     base=None):
    """(DataFrame numérico, (ini_ant, fim_ant))."""
    p = params_ataque(lojas_db, ini, fim, limite, por_loja, semana, base)
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(SQL_ATAQUE, p)
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        cur.close()
    finally:
        conn.close()
    for c in COLS_NUM:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df, (p['ini_ant'], p['fim_ant'])


def fmt_sugestoes(sugestoes):
    """Várias sugestões por linha, cada uma com a origem (performance / ads / escala)."""
    if not sugestoes:
        return "—"
    return " · ".join(f"[{s['origem']}] {s['texto']}" for s in sugestoes)


def fmt_seta(atual, anterior):
    if atual is None or pd.isna(atual):
        return "—"
    if anterior is None or pd.isna(anterior) or anterior == 0:
        return "novo" if atual > 0 else "—"
    if anterior < 0:  # devolução maior que a venda: variação % não tem sentido
        return "—"
    v = (atual / anterior - 1) * 100
    return f"{'▲' if v >= 0 else '▼'} {v:+.0f}%"


def pct_ou_nada(num, den):
    if den is None or pd.isna(den) or den == 0 or num is None or pd.isna(num):
        return None
    return float(num) / float(den) * 100


def tabela_ataque(df, dias, sugestoes=None):
    """Aplica as contas por linha e devolve DataFrame numérico (para tela e Excel).
    sugestoes: {(loja, chave): [{'origem', 'texto'}]} (None = nenhuma)."""
    sugestoes = sugestoes or {}
    out = pd.DataFrame({
        'Loja': df['loja'].map(LOJAS_ATAQUE),
        'SKU': df['skus'],
        'Produto': df['nome'],
        'Anúncio': df['chave'],
        'Unidades': df['un'],
        'Receita': df['rec'],
    })
    out['Ticket médio'] = df['rec'] / df['un'].where(df['un'] != 0)
    out['Margem %'] = [pct_ou_nada(m, r) for m, r in zip(df['marg'], df['rec'])]
    sem_ads = df['loja'].isin(LOJAS_SEM_ADS)
    ml = df['mkt'] == 'MERCADO LIVRE'
    out['Visitas (ML)'] = df['visitas'].where(ml).fillna(0).where(ml)
    out['Views 30d (Shopee)'] = df['views_30d'].where(~ml)
    for rot, col in [('Impressões ads', 'imp'), ('Cliques ads', 'cli'),
                     ('Invest. ads', 'gasto')]:
        out[rot] = df[col].fillna(0).where(~sem_ads)
    out['ACOS %'] = [None if sa else pct_ou_nada(g, ra)
                     for g, ra, sa in zip(df['gasto'], df['rec_ads'], sem_ads)]
    out['TACOS %'] = [None if sa else pct_ou_nada(0 if pd.isna(g) else g, r)
                      for g, r, sa in zip(df['gasto'], df['rec'], sem_ads)]
    # Conversão = unidades ÷ visitas (ML). Shopee: só quando o período é de 30
    # dias, que é a janela das views.
    conv = []
    for un, vis, vw, is_ml in zip(df['un'], df['visitas'], df['views_30d'], ml):
        if is_ml:
            conv.append(pct_ou_nada(un, 0 if pd.isna(vis) else vis))
        else:
            conv.append(pct_ou_nada(un, vw) if dias == 30 else None)
    out['Conversão %'] = conv
    out['Receita ant.'] = df['rec_ant']
    out['Unid. ant.'] = df['un_ant']
    out['Δ Receita'] = [fmt_seta(a, b) for a, b in zip(df['rec'], df['rec_ant'])]
    out['Δ Unid.'] = [fmt_seta(a, b) for a, b in zip(df['un'], df['un_ant'])]
    out['Sugestão'] = [fmt_sugestoes(sugestoes.get((l, c)))
                       for l, c in zip(df['loja'], df['chave'])]
    return out
