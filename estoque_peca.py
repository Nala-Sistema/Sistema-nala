"""
COBERTURA DE ESTOQUE EM PEÇA — ML (frente [KITS], 2ª entrega, 01/10/2026)

Aba "📦 Cobertura em peça (ML)" de Análise de Produtos. Substitui a aba
antiga, que lia dim_estoque (upload do UpSeller, parado desde 17/05).

FONTE ÚNICA: estoque só da API do Mercado Livre (fact_estoque_diario +
ponte dim_estoque_anuncio), composição de dim_kit_composicao, venda de
fact_vendas_snapshot. Só leitura, sem chamada à API.

REGRAS (plano aprovado pelo Mestre em 01/10/2026, medido no banco)
  - Foto: o último dia de CADA loja. Estoque que não aparece nele saiu do ML
    (o coletor grava todo estoque todo dia).
  - Full em peça = Σ Full do kit × qtd + Full da própria peça, somado por
    estoque_id DISTINTO: vários anúncios dividem o mesmo estoque, e somar por
    anúncio dobraria o Full.
  - Galpão em peça: SÓ de anúncio cuja SKU é a própria peça. O galpão do
    anúncio de kit já vem dividido pelo UpSeller (LKE-3104-4030 = 1.315 →
    K10 = 131); multiplicar de volta conta a peça várias vezes.
  - Galpão é um só: não soma entre lojas. Usa o MAIOR entre os anúncios da
    peça (o UpSeller pode publicar com limite numa loja) e avisa quando as
    lojas divergem. ML-YanniSP é prep center: coluna própria, fora do galpão.
  - Peça sem anúncio próprio: galpão "desconhecido", nunca zero. Piso
    "≥ N pelo kit" = galpão do anúncio de um kit de UMA peça só × qtd,
    sempre marcado como estimativa; a cobertura dela sai marcada "mínimo".
  - Venda em peça = venda do kit × qtd + venda da peça sozinha, em TODOS os
    marketplaces (o galpão abastece todos). Full da Shopee/Amazon ainda não
    entra no estoque: a cobertura sai conservadora (dito na tela).
  - Kit sem composição (pendente) entra como ele mesmo, com aviso.
  - Nunca soma unidades de produtos diferentes. R$ em jogo = receita de 30
    dias dos SKUs (kits e a própria peça) que param se a peça acabar; o
    total do topo soma SKUs DISTINTOS.
"""

from datetime import date, datetime, timedelta, timezone

import pandas as pd

MARKETPLACE = 'MERCADO LIVRE'
LOJA_PREP_CENTER = 'ML-YanniSP'
PRAZO_REPOSICAO_PADRAO = 14


# ============================================================
# SQL — parâmetros ligados (psycopg2); sem schema de propósito: o teste usa
# tabelas TEMPORÁRIAS com os mesmos nomes.
# ============================================================

# Último dia de cada loja; um estoque por linha (ponte com DISTINCT: dois
# anúncios do mesmo estoque não duplicam o saldo).
SQL_ESTOQUE = """
    WITH ult AS (
        SELECT loja, max(data) AS data
          FROM fact_estoque_diario
         WHERE marketplace = %(marketplace)s
         GROUP BY loja),
    ponte AS (
        SELECT DISTINCT loja, estoque_id, sku
          FROM dim_estoque_anuncio
         WHERE marketplace = %(marketplace)s AND estoque_id IS NOT NULL
           AND sku IS NOT NULL)
    SELECT f.loja, f.estoque_id, p.sku, f.data,
           COALESCE(f.full_disponivel, 0)       AS full_disponivel,
           COALESCE(f.full_em_transferencia, 0) AS full_em_transferencia,
           f.galpao_disponivel
      FROM fact_estoque_diario f
      JOIN ult u ON u.loja = f.loja AND u.data = f.data
      JOIN ponte p ON p.loja = f.loja AND p.estoque_id = f.estoque_id
     WHERE f.marketplace = %(marketplace)s
"""

# Venda de todos os marketplaces, por SKU vendido.
SQL_VENDAS = """
    SELECT sku,
           SUM(quantidade) FILTER (WHERE data_venda >= %(ini_7)s)  AS qtd_7d,
           SUM(quantidade)                                         AS qtd_30d,
           SUM(valor_venda_efetivo)                                AS receita_30d
      FROM fact_vendas_snapshot
     WHERE data_venda >= %(ini_30)s AND data_venda <= %(fim)s
     GROUP BY sku
"""

# Quantos estoques cada loja tem no último dia e no anterior: foto parcial
# (coleta que parou no meio) some com estoque sem erro nenhum.
SQL_CONTAGEM_DIAS = """
    WITH d AS (
        SELECT loja, data, count(*) AS n
          FROM fact_estoque_diario
         WHERE marketplace = %(marketplace)s
         GROUP BY loja, data),
    r AS (
        SELECT loja, data, n,
               row_number() OVER (PARTITION BY loja ORDER BY data DESC) AS k
          FROM d)
    SELECT loja,
           max(n)    FILTER (WHERE k = 1) AS n_ultimo,
           max(data) FILTER (WHERE k = 1) AS ultimo,
           max(n)    FILTER (WHERE k = 2) AS n_anterior,
           max(data) FILTER (WHERE k = 2) AS anterior
      FROM r
     WHERE k <= 2
     GROUP BY loja
"""

SQL_COMPOSICAO = "SELECT kit_sku, peca_sku, quantidade FROM dim_kit_composicao"

SQL_KITS_PENDENTES = "SELECT DISTINCT kit_sku FROM dim_kit_composicao_pendente"

SQL_MAPEAMENTO = "SELECT sku_errado, sku_correto FROM dim_sku_mapeamento"

SQL_NOMES = "SELECT sku, nome FROM dim_produtos WHERE sku = ANY(%(skus)s)"

TODAS_AS_SQL = (SQL_ESTOQUE, SQL_VENDAS, SQL_COMPOSICAO, SQL_KITS_PENDENTES,
                SQL_MAPEAMENTO, SQL_NOMES, SQL_CONTAGEM_DIAS)

QUEDA_FOTO_PARCIAL = 0.20


def params_vendas(hoje):
    """30 dias fechados (até ontem): hoje ainda está vendendo."""
    fim = hoje - timedelta(days=1)
    return {'ini_30': fim - timedelta(days=29), 'ini_7': fim - timedelta(days=6), 'fim': fim}


def tolerancia_atraso(agora_sp):
    """Mesma regra de v_cobertura_full (copiada, não lida): depois das 10h de
    Brasília o dado de ontem já devia ter chegado (1 dia); antes, 2."""
    return 1 if agora_sp.hour >= 10 else 2


def ler(conn, hoje):
    """Lê tudo o que a conta precisa. Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_ESTOQUE, {'marketplace': MARKETPLACE})
        estoque = cur.fetchall()
        cur.execute(SQL_VENDAS, params_vendas(hoje))
        vendas = cur.fetchall()
        cur.execute(SQL_COMPOSICAO)
        composicao = cur.fetchall()
        cur.execute(SQL_KITS_PENDENTES)
        pendentes = {r[0] for r in cur.fetchall()}
        cur.execute(SQL_MAPEAMENTO)
        mapa = dict(cur.fetchall())
        return estoque, vendas, composicao, pendentes, mapa
    finally:
        cur.close()
        conn.rollback()


def ler_contagens(conn):
    """[(loja, n_ultimo, ultimo, n_anterior, anterior)]. Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_CONTAGEM_DIAS, {'marketplace': MARKETPLACE})
        return cur.fetchall()
    finally:
        cur.close()
        conn.rollback()


def fotos_parciais(contagens, queda=QUEDA_FOTO_PARCIAL):
    """Lojas cujo último dia tem bem menos estoques que o anterior (já houve:
    ML-Nala 25/09 com 15 linhas, ML-YanniRJ 25/09 com 2). A foto é usada
    assim mesmo; a tela avisa que pode faltar estoque."""
    return [(loja, n_ult, ult, n_ant, ant)
            for loja, n_ult, ult, n_ant, ant in contagens
            if n_ant and n_ult < (1 - queda) * n_ant]


def ler_nomes(conn, skus):
    if not skus:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(SQL_NOMES, {'skus': sorted(skus)})
        return dict(cur.fetchall())
    finally:
        cur.close()
        conn.rollback()


# ============================================================
# CONTA EM PEÇA (sem banco)
# ============================================================

def _diverge(valores):
    """Lojas publicando galpão diferente, fora o ruído da hora da coleta."""
    if len(valores) < 2:
        return False
    alto, baixo = max(valores), min(valores)
    return alto - baixo > max(2, 0.01 * alto)


def montar(estoque, vendas, composicao, kits_pendentes=(), mapa=None,
           prazo=PRAZO_REPOSICAO_PADRAO):
    """
    estoque: [(loja, estoque_id, sku, data, full, transferencia, galpao)]
    vendas: [(sku, qtd_7d, qtd_30d, receita_30d)]
    composicao: [(kit, peca, qtd)]

    Devolve (pecas, kits_por_peca, avisos):
      pecas: DataFrame, uma linha por peça;
      kits_por_peca: {peca: [dict por kit que usa a peça]};
      avisos: dict de listas para a tela.
    """
    mapa = mapa or {}
    comp = {}
    for kit, peca, qtd in composicao:
        comp.setdefault(kit, {})[peca] = int(qtd)
    sem_comp = {mapa.get(k, k) for k in kits_pendentes} | set(kits_pendentes)

    def _pecas_de(sku):
        return comp[sku].items() if sku in comp else [(sku, 1)]

    p = {}

    def _p(peca):
        return p.setdefault(peca, {
            'full': 0, 'transf': 0, 'galpao_lojas': {}, 'prep': None,
            'qtd_7d': 0.0, 'qtd_30d': 0.0, 'skus_dependentes': set(),
            'kits': set(), 'vem_de_kit_sem_comp': False})

    # ---- estoque ---------------------------------------------------------
    galpao_kit = {}       # kit -> maior galpão publicado (fora o prep center)
    datas_loja = {}
    # SKU do anúncio passa pela mesma correção das vendas (um salto só), e só
    # DEPOIS o estoque é contado uma vez: um anúncio com o SKU errado e outro
    # com o certo no mesmo estoque_id viram uma linha, não duas (Full dobrado).
    unicos = {}
    for l, e, s, d, f, t, g in estoque:
        unicos.setdefault((l, e, mapa.get(s, s)), (l, e, mapa.get(s, s), d, f, t, g))
    estoque = list(unicos.values())
    for loja, _eid, sku, data, full, transf, galpao in estoque:
        datas_loja[loja] = data
        for peca, q in _pecas_de(sku):
            reg = _p(peca)
            reg['full'] += int(full) * q
            reg['transf'] += int(transf) * q
            if sku in comp:
                reg['kits'].add(sku)
        if galpao is None:
            continue
        if sku in comp:
            if loja != LOJA_PREP_CENTER:
                galpao_kit[sku] = max(galpao_kit.get(sku, 0), int(galpao))
            continue
        reg = _p(sku)
        if loja == LOJA_PREP_CENTER:
            reg['prep'] = max(reg['prep'] or 0, int(galpao))
        else:
            reg['galpao_lojas'].setdefault(loja, set()).add(int(galpao))

    # ---- venda -----------------------------------------------------------
    receita_sku = {}
    for sku, q7, q30, rec in vendas:
        receita_sku[sku] = float(rec or 0)
        for peca, q in _pecas_de(sku):
            reg = _p(peca)
            reg['qtd_7d'] += float(q7 or 0) * q
            reg['qtd_30d'] += float(q30 or 0) * q
            reg['skus_dependentes'].add(sku)
            if sku in comp:
                reg['kits'].add(sku)
            elif sku in sem_comp:
                reg['vem_de_kit_sem_comp'] = True
    for sku in {r[2] for r in estoque} & sem_comp:
        _p(sku)['vem_de_kit_sem_comp'] = True

    # Piso do galpão: kit de UMA peça só, galpão do kit × qtd (aprovado).
    piso = {}
    for kit, g in galpao_kit.items():
        if len(comp[kit]) == 1:
            (peca, q), = comp[kit].items()
            piso[peca] = max(piso.get(peca, 0), g * q)

    # ---- linha por peça --------------------------------------------------
    linhas, divergentes = [], []
    for peca, reg in p.items():
        valores = [v for vs in reg['galpao_lojas'].values() for v in vs]
        if valores:
            galpao, galpao_tipo = max(valores), 'conhecido'
            if _diverge(valores):
                divergentes.append(peca)
        elif peca in piso:
            galpao, galpao_tipo = piso[peca], 'piso'
        else:
            galpao, galpao_tipo = None, 'desconhecido'
        dia_30 = reg['qtd_30d'] / 30.0
        dia_7 = reg['qtd_7d'] / 7.0
        estoque_total = (galpao or 0) + reg['full'] + (reg['prep'] or 0)
        cobertura = estoque_total / dia_30 if dia_30 > 0 else None
        minimo = galpao_tipo != 'conhecido'
        linhas.append({
            'peca': peca,
            'galpao': galpao,
            'galpao_tipo': galpao_tipo,
            'galpao_diverge': peca in divergentes,
            'full': reg['full'],
            'transferencia': reg['transf'],
            'prep_center': reg['prep'],
            'venda_dia_7d': dia_7,
            'venda_dia_30d': dia_30,
            'cobertura_dias': cobertura,
            'cobertura_minima': minimo,
            'cobertura_full_dias': reg['full'] / dia_30 if dia_30 > 0 else None,
            'em_jogo_30d': sum(receita_sku.get(s, 0.0) for s in reg['skus_dependentes']),
            'skus_dependentes': tuple(sorted(reg['skus_dependentes'])),
            'qtd_kits': len(reg['kits']),
            'kit_sem_composicao': reg['vem_de_kit_sem_comp'],
            'ruptura': cobertura is not None and cobertura < prazo,
        })
    pecas = pd.DataFrame(linhas)
    if not pecas.empty:
        pecas = pecas.sort_values(['ruptura', 'em_jogo_30d', 'peca'],
                                  ascending=[False, False, True]).reset_index(drop=True)

    # ---- detalhe: kits que usam cada peça --------------------------------
    full_kit = {}
    for _loja, _eid, sku, _d, full, _t, _g in estoque:
        if sku in comp:
            full_kit[sku] = full_kit.get(sku, 0) + int(full)
    venda_kit = {s: (q30, rec) for s, _q7, q30, rec in vendas}
    kits_por_peca = {}
    for kit, c in comp.items():
        for peca, q in c.items():
            if peca not in p or kit not in p[peca]['kits']:
                continue
            q30, rec = venda_kit.get(kit, (0, 0))
            kits_por_peca.setdefault(peca, []).append({
                'Kit': kit, 'Qtd da peça no kit': q,
                'Full do kit (kits)': full_kit.get(kit, 0),
                'Galpão publicado do kit': galpao_kit.get(kit),
                'Kits vendidos 30d': int(q30 or 0),
                'Receita 30d do kit': float(rec or 0)})

    avisos = {
        'datas_loja': datas_loja,
        'galpao_divergente': sorted(divergentes),
        'kits_sem_composicao': sorted(pecas.loc[pecas['kit_sem_composicao'], 'peca'])
        if not pecas.empty else [],
    }
    return pecas, kits_por_peca, avisos


def skus_em_jogo(pecas, mascara):
    """SKUs DISTINTOS que dependem das peças marcadas (kit misto não
    conta duas vezes)."""
    skus = set()
    for t in pecas.loc[mascara, 'skus_dependentes']:
        skus |= set(t)
    return skus


def lojas_atrasadas(datas_loja, hoje, agora_sp):
    tol = tolerancia_atraso(agora_sp)
    return sorted(l for l, d in datas_loja.items()
                  if (hoje - pd.Timestamp(d).date()).days > tol)


# ============================================================
# TELA
# ============================================================

def _int(v):
    return f'{int(round(v)):,}'.replace(',', '.')


def _brl(v):
    return 'R$ ' + f'{v:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def texto_galpao(linha):
    if linha['galpao_tipo'] == 'conhecido':
        return _int(linha['galpao']) + (' ⚠ publicado diferente entre lojas'
                                        if linha['galpao_diverge'] else '')
    if linha['galpao_tipo'] == 'piso':
        return f"desconhecido (≥ {_int(linha['galpao'])} pelo kit, estimativa)"
    return 'desconhecido'


def texto_cobertura(dias, minimo):
    if dias is None:
        return 'sem giro'
    return (f'≥ {dias:.0f} (mínimo)' if minimo else f'{dias:.0f}')


def _tabela_tela(df, nomes):
    return pd.DataFrame({
        'Peça': df['peca'],
        'Produto': df['peca'].map(lambda s: nomes.get(s) or '(sem cadastro)'),
        'Galpão': df.apply(texto_galpao, axis=1),
        'Full ML (peças)': df['full'].map(_int),
        'Em transferência (fora da cobertura)': df['transferencia'].map(_int),
        'Prep center SP': df['prep_center'].map(lambda v: '—' if pd.isna(v) else _int(v)),
        'Venda/dia 7d': df['venda_dia_7d'].map(lambda v: f'{v:.1f}'),
        'Venda/dia 30d': df['venda_dia_30d'].map(lambda v: f'{v:.1f}'),
        'Cobertura (dias)': [texto_cobertura(d, m) for d, m in
                             zip(df['cobertura_dias'], df['cobertura_minima'])],
        'Cobertura só Full (dias)': [texto_cobertura(d, False) for d in df['cobertura_full_dias']],
        'R$ em jogo (30d)': df['em_jogo_30d'].map(_brl),
        'Kits que usam': df['qtd_kits'],
        'Aviso': df['kit_sem_composicao'].map(lambda v: 'kit sem composição' if v else ''),
    })


def render(engine):
    import streamlit as st

    st.subheader("📦 Cobertura em peça (ML)")
    st.caption(
        "Estoque da API do Mercado Livre, em UNIDADES DE PEÇA: o Full de cada kit vira "
        "peças pela composição. Galpão só do anúncio da própria peça (o do kit vem "
        "dividido pelo UpSeller). Venda de todos os marketplaces, 30 dias até ontem. "
        "O Full da Shopee e da Amazon ainda não entra, e o que está EM TRANSFERÊNCIA "
        "para o Full aparece em coluna própria mas não conta na cobertura: ela é "
        "conservadora.")

    from permissoes import ve_todas_lojas
    if not ve_todas_lojas():
        st.info("Esta visão é da empresa inteira (galpão único e venda de todas as "
                "lojas): disponível para quem vê todas as lojas.")
        return

    c1, c2 = st.columns([1, 2])
    prazo = c1.number_input("Prazo de reposição (dias)", min_value=1, max_value=120,
                            value=PRAZO_REPOSICAO_PADRAO, step=1, key="cobp_prazo")
    busca = c2.text_input("Buscar peça ou kit (SKU ou parte do nome)", key="cobp_busca")

    hoje = date.today()
    conn = engine.raw_connection()
    try:
        estoque, vendas, composicao, pendentes, mapa = ler(conn, hoje)
        pecas, kits_por_peca, avisos = montar(estoque, vendas, composicao, pendentes,
                                              mapa, prazo)
        nomes = ler_nomes(conn, set(pecas['peca']) if not pecas.empty else set())
        parciais = fotos_parciais(ler_contagens(conn))
    finally:
        conn.close()
    if pecas.empty:
        st.info("Sem estoque nem venda para mostrar.")
        return

    agora_sp = datetime.now(timezone(timedelta(hours=-3)))
    atrasadas = lojas_atrasadas(avisos['datas_loja'], agora_sp.date(), agora_sp)
    datas = ', '.join(f"{l} {pd.Timestamp(d):%d/%m}" for l, d in sorted(avisos['datas_loja'].items()))
    if atrasadas:
        st.warning(f"⚠️ Estoque atrasado em {', '.join(atrasadas)}. Foto usada: {datas}.")
    else:
        st.caption(f"Foto do estoque: {datas}.")
    for loja, n_ult, ult, n_ant, ant in parciais:
        st.warning(f"⚠️ Foto possivelmente PARCIAL em {loja}: {n_ult} estoques em "
                   f"{pd.Timestamp(ult):%d/%m} contra {n_ant} em {pd.Timestamp(ant):%d/%m}. "
                   "Pode faltar estoque dessa loja nesta tela.")

    if busca:
        b = busca.strip().lower()
        alvo = pecas['peca'].str.lower().str.contains(b, regex=False) | \
            pecas['peca'].map(lambda s: b in (nomes.get(s) or '').lower()) | \
            pecas['skus_dependentes'].map(lambda t: any(b in s.lower() for s in t))
        pecas = pecas[alvo]

    rup = pecas['ruptura']
    skus_risco = skus_em_jogo(pecas, rup)
    receita_risco = sum(v for s, v in _receita_por_sku(vendas).items() if s in skus_risco)
    m1, m2, m3 = st.columns(3)
    m1.metric(f"Peças com cobertura < {prazo} dias", _int(rup.sum()))
    m2.metric("R$ em jogo (30d, SKUs distintos)", _brl(receita_risco))
    m3.metric("Peças com galpão desconhecido",
              _int((pecas['galpao_tipo'] != 'conhecido').sum()))

    st.markdown(f"### 🔥 Ruptura iminente (cobertura < {prazo} dias)")
    st.caption("Ordenado pelo R$ em jogo: receita de 30 dias dos SKUs que param se a "
               "peça acabar (a peça e os kits que a usam). Cobertura \"mínimo\" = galpão "
               "desconhecido ou estimado pelo kit; pode ser maior.")
    if rup.any():
        st.dataframe(_tabela_tela(pecas[rup], nomes), use_container_width=True,
                     hide_index=True)
    else:
        st.success("Nenhuma peça abaixo do prazo.")

    with st.expander(f"📋 Todas as peças ({len(pecas)})"):
        st.dataframe(_tabela_tela(pecas, nomes), use_container_width=True,
                     hide_index=True, height=420)

    com_kit = [s for s in pecas['peca'] if s in kits_por_peca]
    if com_kit:
        st.markdown("### 🧩 Kits que usam a peça")
        escolhida = st.selectbox("Peça", com_kit, key="cobp_peca",
                                 format_func=lambda s: f"{s} — {nomes.get(s) or ''}")
        det = pd.DataFrame(kits_por_peca[escolhida]).sort_values('Receita 30d do kit',
                                                                 ascending=False)
        det['Receita 30d do kit'] = det['Receita 30d do kit'].map(_brl)
        st.dataframe(det, use_container_width=True, hide_index=True)

    if avisos['galpao_divergente']:
        st.warning("⚠ Galpão publicado diferente entre lojas (usado o maior): "
                   + ', '.join(avisos['galpao_divergente']))
    if avisos['kits_sem_composicao']:
        st.warning("Kit sem composição (pendente em Gestão de SKUs → 🧩 Kits), contado "
                   "como ele mesmo: " + ', '.join(avisos['kits_sem_composicao']))


def _receita_por_sku(vendas):
    return {s: float(r or 0) for s, _q7, _q30, r in vendas}
