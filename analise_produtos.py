"""
ANÁLISE DE PRODUTOS - Sistema Nala
Versão: 1.0 (17/05/2026)

Módulo único com 4 tabs voltadas a entender o desempenho por produto (SKU):

  Tab 1 — 🏆 Mais Vendidos
        Ranking por período/lojas/marketplaces com receita, quantidade,
        margem média (AVG(margem_percentual)), ticket, nº pedidos,
        Curva ABC (dim_tags_anuncio) e mix por canal.

  Tab 2 — 📈 Crescimento & Queda
        Compara dois períodos contíguos. Mostra top 20 em alta e top 20
        em queda por delta % de quantidade vendida.

  Tab 3 — 📦 Cobertura em peça (ML + Shopee)  (01/10/2026; Full da Shopee em 06/10/2026)
        Estoque da API do ML (galpão + Full) e venda de todos os
        marketplaces em UNIDADES DE PEÇA (kit × composição). Ruptura
        iminente ordenada pelo R$ em jogo. Conta em estoque_peca.py.
        Substituiu a cobertura por dim_estoque e a aba de upload do
        UpSeller que a alimentava (fonte única: estoque só da API).

  (Tab 5 — 💸 Despesas de Full SAIU em 07/10/2026: o custo do Full do ML vem
   da API de billing pelo coletor coletar_custo_full (nala-coletor-ml), toda
   semana. Fonte única: o upload morreu junto. Ver dim_fonte_dados, assuntos
   custo_full_armazenagem e custo_full_eventos.)

  Tab 6 — 🧾 Fechamento de Estoque
        Foto mensal valorizada do galpão e de cada Full, ao preço de compra
        congelado no upload, em fact_estoque_mensal. É outra coisa da tab 3:
        a tab 3 é o saldo de hoje para calcular giro, esta é o dia 1º por
        local, que ninguém sobrescreve. A leitura dos quatro formatos de
        relatório mora em processar_estoque_full.py.

  Tab 7 — 🚚 Penalização de frete
        Alarme por SKU do frete cobrado na Shopee (API) e no TikTok, onde o
        frete da Nala é zero: todo frete é penalização de peso/medida do
        anúncio. Só leitura, sobre fact_vendas_snapshot.

Dependências internas:
    database_utils.get_engine       — engine cacheado (v3.6)
    database_utils.gravar_log_upload — log de uploads
    permissoes.filtrar_query_por_loja — RBAC nas queries
    processar_estoque_full          — leitura/valorização do fechamento
"""

import streamlit as st
import pandas as pd
import io

import estoque_peca
from datetime import date, timedelta

from database_utils import get_engine, gravar_log_upload
from filtro_periodo import filtro_periodo
from permissoes import (
    ve_todas_lojas, get_lojas_usuario, filtrar_query_por_loja,
)


# ============================================================
# HELPERS
# ============================================================

def _fmt_brl(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(v):
    if v is None:
        return "—"
    try:
        return f"{float(v):.1f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_int(v):
    try:
        return f"{int(v):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def _query_to_df(engine, query, params=None):
    """Executa query raw e devolve DataFrame."""
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        colunas = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(rows, columns=colunas)
    finally:
        conn.close()


def _coerce_num(df, cols):
    """
    psycopg2 traz NUMERIC do Postgres como Decimal — pandas vê como dtype=object,
    o que quebra .nlargest(), .sum() etc. Convertemos para float aqui.
    """
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _opcoes_lojas_marketplaces():
    """Lista distinct de lojas e marketplaces vindos de dim_lojas."""
    engine = get_engine()
    try:
        df = pd.read_sql(
            "SELECT DISTINCT marketplace, loja FROM dim_lojas ORDER BY marketplace, loja",
            engine,
        )
        marketplaces = sorted(df['marketplace'].dropna().unique().tolist())
        lojas = sorted(df['loja'].dropna().unique().tolist())
        return marketplaces, lojas
    except Exception:
        return [], []


def _resolver_periodo(label):
    hoje = date.today()
    if label == "Últimos 7 dias":
        return hoje - timedelta(days=7), hoje
    if label == "Últimos 30 dias":
        return hoje - timedelta(days=30), hoje
    if label == "Últimos 60 dias":
        return hoje - timedelta(days=60), hoje
    if label == "Últimos 90 dias":
        return hoje - timedelta(days=90), hoje
    return None, None  # personalizado


def _filtros_periodo_loja_marketplace(key_prefix):
    """Renderiza filtros padronizados e devolve (data_inicio, data_fim, lojas, marketplaces)."""
    presets = [
        "Últimos 30 dias", "Últimos 7 dias",
        "Últimos 60 dias", "Últimos 90 dias",
        "Personalizado",
    ]
    marketplaces_opt, lojas_opt = _opcoes_lojas_marketplaces()
    if not ve_todas_lojas():
        lojas_permitidas = set(get_lojas_usuario())
        lojas_opt = [l for l in lojas_opt if l in lojas_permitidas]

    col1, col2, col3, col4 = st.columns([1.2, 1.2, 1.4, 1.4])
    with col1:
        preset = st.selectbox("📅 Período", presets, key=f"{key_prefix}_preset")
    di, df_ = _resolver_periodo(preset)
    if preset == "Personalizado":
        with col2:
            di = st.date_input("De", value=date.today() - timedelta(days=30),
                               key=f"{key_prefix}_di")
            df_ = st.date_input("Até", value=date.today(), key=f"{key_prefix}_df")
    else:
        with col2:
            st.caption(f"De **{di.strftime('%d/%m/%Y')}** até **{df_.strftime('%d/%m/%Y')}**")
    with col3:
        marketplaces = st.multiselect("🛒 Marketplaces", marketplaces_opt,
                                      default=[], key=f"{key_prefix}_mkts")
    with col4:
        lojas = st.multiselect("🏪 Lojas", lojas_opt,
                               default=[], key=f"{key_prefix}_lojas")
    return di, df_, lojas, marketplaces


def _montar_where_filtros(data_ini, data_fim, lojas, marketplaces, engine,
                          alias=''):
    """Monta lista de WHERE + params usando RBAC (filtrar_query_por_loja)."""
    where_parts = []
    params = []
    col_loja = f"{alias}loja_origem" if alias else "loja_origem"
    col_mkt = f"{alias}marketplace_origem" if alias else "marketplace_origem"
    col_data = f"{alias}data_venda" if alias else "data_venda"

    where_parts.append(f"{col_data} >= %s")
    params.append(data_ini)
    where_parts.append(f"{col_data} <= %s")
    params.append(data_fim)

    if lojas:
        placeholders = ', '.join(['%s'] * len(lojas))
        where_parts.append(f"{col_loja} IN ({placeholders})")
        params.extend(lojas)
    else:
        filtrar_query_por_loja(where_parts, params, col_loja, engine)

    if marketplaces:
        placeholders = ', '.join(['%s'] * len(marketplaces))
        where_parts.append(f"{col_mkt} IN ({placeholders})")
        params.extend(marketplaces)

    return where_parts, params


# ============================================================
# TAB 1 — MAIS VENDIDOS
# ============================================================

def _tab_mais_vendidos(engine):
    st.subheader("🏆 Produtos Mais Vendidos")
    st.caption("Ranking por SKU. Margem é média aritmética de margem_percentual.")

    data_ini, data_fim, lojas, mkts = _filtros_periodo_loja_marketplace("mv")

    ver_por = st.radio("Ver por", ["SKU vendido", "Produto (peça)"], horizontal=True,
                       key="mv_ver_por")
    if ver_por == "Produto (peça)":
        _mais_vendidos_por_peca(engine, data_ini, data_fim, lojas, mkts)
        return

    col_a, col_b = st.columns([1, 3])
    with col_a:
        ordenar_por = st.selectbox(
            "Ordenar por",
            ["Receita", "Quantidade", "Margem média", "Nº pedidos"],
            key="mv_ord",
        )
    with col_b:
        limit = st.slider("Top N", min_value=10, max_value=200, value=50, step=10,
                          key="mv_limit")

    if data_ini is None or data_fim is None or data_fim < data_ini:
        st.warning("Período inválido — ajuste as datas.")
        return

    where_parts, params = _montar_where_filtros(data_ini, data_fim, lojas, mkts, engine,
                                                 alias='f.')
    where_sql = " AND ".join(where_parts)

    ord_col = {
        "Receita":      "receita DESC",
        "Quantidade":   "quantidade DESC",
        "Margem média": "margem_pct DESC NULLS LAST",
        "Nº pedidos":   "pedidos DESC",
    }[ordenar_por]

    query = f"""
        SELECT
            f.sku,
            COALESCE(p.nome, '(sem cadastro)')                   AS nome,
            SUM(f.quantidade)::bigint                            AS quantidade,
            SUM(f.valor_venda_efetivo)::numeric                  AS receita,
            COUNT(*)::bigint                                     AS pedidos,
            AVG(f.margem_percentual)                             AS margem_pct,
            SUM(f.valor_venda_efetivo) / NULLIF(SUM(f.quantidade), 0) AS ticket_medio,
            COUNT(DISTINCT f.loja_origem)::int                   AS lojas_ativas,
            COUNT(DISTINCT f.marketplace_origem)::int            AS marketplaces_ativos,
            MIN(t.tag_curva)                                     AS curva
        FROM fact_vendas_snapshot f
        LEFT JOIN dim_produtos p ON p.sku = f.sku
        LEFT JOIN dim_tags_anuncio t ON t.codigo_anuncio = f.codigo_anuncio
                                      AND t.marketplace = f.marketplace_origem
        WHERE {where_sql}
        GROUP BY f.sku, p.nome
        ORDER BY {ord_col}
        LIMIT %s
    """
    params_ext = params + [limit]

    try:
        df = _query_to_df(engine, query, params_ext)
    except Exception as e:
        st.error(f"Erro ao consultar vendas: {e}")
        return

    if df.empty:
        st.info("Nenhuma venda encontrada para os filtros selecionados.")
        return

    # NUMERIC do Postgres vem como Decimal/object → converter para float
    df = _coerce_num(df, ['quantidade', 'receita', 'pedidos', 'margem_pct',
                          'ticket_medio', 'lojas_ativas', 'marketplaces_ativos'])

    # Métricas resumo no topo
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SKUs", _fmt_int(len(df)))
    c2.metric("Receita Total", _fmt_brl(df['receita'].sum()))
    c3.metric("Unidades", _fmt_int(df['quantidade'].sum()))
    c4.metric("Pedidos", _fmt_int(df['pedidos'].sum()))
    margem_geral = df['margem_pct'].dropna().astype(float).mean() if df['margem_pct'].notna().any() else None
    c5.metric("Margem média", _fmt_pct(margem_geral))

    st.markdown("---")

    # Tabela formatada
    df_disp = df.copy()
    df_disp['Receita']        = df_disp['receita'].apply(_fmt_brl)
    df_disp['Ticket Médio']   = df_disp['ticket_medio'].apply(_fmt_brl)
    df_disp['Margem']         = df_disp['margem_pct'].apply(lambda v: _fmt_pct(v) if pd.notna(v) else "—")
    df_disp['Quantidade']     = df_disp['quantidade'].apply(_fmt_int)
    df_disp['Pedidos']        = df_disp['pedidos'].apply(_fmt_int)
    df_disp['Curva']          = df_disp['curva'].fillna('—')

    cols_show = ['sku', 'nome', 'Quantidade', 'Receita', 'Ticket Médio',
                 'Margem', 'Pedidos', 'lojas_ativas', 'marketplaces_ativos', 'Curva']
    rename = {'sku': 'SKU', 'nome': 'Produto',
              'lojas_ativas': 'Lojas', 'marketplaces_ativos': 'Mkts'}
    st.dataframe(df_disp[cols_show].rename(columns=rename),
                 use_container_width=True, hide_index=True)

    # Top 20 receita — gráfico
    with st.expander("📊 Gráfico — Top 20 por Receita", expanded=False):
        df_chart = df.nlargest(20, 'receita').copy()
        df_chart['label'] = df_chart['sku'].astype(str) + ' — ' + df_chart['nome'].str.slice(0, 35)
        st.bar_chart(df_chart.set_index('label')['receita'])

    # Export
    buf = io.BytesIO()
    df.to_excel(buf, index=False, sheet_name='MaisVendidos')
    st.download_button(
        "⬇️ Baixar Excel completo",
        data=buf.getvalue(),
        file_name=f"mais_vendidos_{data_ini}_{data_fim}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _mais_vendidos_por_peca(engine, data_ini, data_fim, lojas, mkts):
    """
    Mais Vendidos em UNIDADES DE PEÇA (frente [KITS], 3ª entrega, 01/10/2026).
    A conta é estoque_peca.venda_em_peca, a mesma da Cobertura em peça. Só
    unidades: receita/margem por peça ficam fora (rateio do kit misto). Nenhum
    total que some peças diferentes.
    """
    st.caption("Unidades: venda do kit × quantidade da peça na composição (🧩 Kits em "
               "Gestão de SKUs) + a peça vendida sozinha. Receita e margem: a da peça "
               "sozinha entra inteira; a do kit é RATEADA entre as peças pelo peso do "
               "custo (custo da peça × qtd ÷ soma do kit), o mesmo custo que a venda usa. "
               "Soma das peças = soma das vendas, ao centavo.")
    limit = st.slider("Top N", min_value=10, max_value=200, value=50, step=10,
                      key="mv_peca_limit")
    if data_ini is None or data_fim is None or data_fim < data_ini:
        st.warning("Período inválido — ajuste as datas.")
        return
    where_parts, params = _montar_where_filtros(data_ini, data_fim, lojas, mkts, engine,
                                                 alias='f.')
    try:
        conn = engine.raw_connection()
        try:
            vendas = estoque_peca.ler_vendas_por_sku(conn, " AND ".join(where_parts), params)
            composicao, pendentes, mapa = estoque_peca.ler_composicao(conn)
            comp = estoque_peca.agrupar_composicao(composicao)
            venda = estoque_peca.venda_em_peca(
                [(s, q) for s, q, _r, _m in vendas], comp,
                estoque_peca.kits_sem_composicao(pendentes, mapa))
            pecas_dos_kits = {p for s, *_ in vendas if s in comp for p in comp[s]}
            custos = estoque_peca.ler_custos(conn, pecas_dos_kits)
            valores, por_quantidade = estoque_peca.valor_em_peca(
                [(s, r, m) for s, _q, r, m in vendas], comp, custos)
            df = estoque_peca.tabela_por_peca(venda, valores=valores)
            nomes = estoque_peca.ler_nomes(conn, set(df['peca']))
        finally:
            conn.close()
    except Exception as e:
        st.error(f"Erro ao consultar vendas por peça: {e}")
        return
    if df.empty:
        st.info("Nenhuma venda encontrada para os filtros selecionados.")
        return

    # Cards: contagem de peças e R$ (somar R$ entre peças pode; unidades não).
    receita = float(df['receita_total'].sum())
    margem = float(df['margem'].sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Peças com venda", _fmt_int(len(df)))
    c2.metric("Peças vendidas também dentro de kit", _fmt_int((df['em_kit'] > 0).sum()))
    c3.metric("Receita", _fmt_brl(receita))
    c4.metric("Margem", _fmt_brl(margem),
              _fmt_pct(margem / receita * 100) if receita else None, delta_color="off")

    top = df.head(limit)
    st.dataframe(pd.DataFrame({
        'Peça': top['peca'],
        'Produto': top['peca'].map(lambda s: nomes.get(s) or '(sem cadastro)'),
        'Vendida sozinha': top['sozinha'].map(_fmt_int),
        'Dentro de kit': top['em_kit'].map(_fmt_int),
        'Total (peças)': top['total'].map(_fmt_int),
        '% em kit': top['pct_em_kit'].map(lambda v: f"{v * 100:.0f}%"),
        'Receita sozinha': top['receita_sozinha'].map(lambda v: _fmt_brl(float(v))),
        'Receita em kit (rateada)': top['receita_kit'].map(lambda v: _fmt_brl(float(v))),
        'Receita total': top['receita_total'].map(lambda v: _fmt_brl(float(v))),
        'Margem R$': top['margem'].map(lambda v: _fmt_brl(float(v))),
        'Margem %': top['margem_pct'].map(
            lambda v: '—' if v is None else _fmt_pct(float(v) * 100)),
        'Kits que venderam': top['qtd_kits'],
        'Aviso': top['kit_sem_composicao'].map(lambda v: 'kit sem composição' if v else ''),
    }), use_container_width=True, hide_index=True)

    if por_quantidade:
        st.warning("Rateio por QUANTIDADE (alguma peça sem custo, zero ou inválido) "
                   "nestes kits: " + ', '.join(por_quantidade)
                   + ". Cadastre o custo da peça em Gestão de SKUs para ratear pelo custo.")

    sem = list(df.loc[df['kit_sem_composicao'], 'peca'])
    if sem:
        st.warning("Kit sem composição (pendente em Gestão de SKUs → 🧩 Kits), contado "
                   "como ele mesmo: " + ', '.join(sem))

    buf = io.BytesIO()
    df.assign(kits=df['kits'].map(', '.join),
              **{c: df[c].map(float) for c in ['receita_sozinha', 'receita_kit',
                                                'receita_total', 'margem']},
              margem_pct=df['margem_pct'].map(lambda v: None if v is None else float(v))
              ).to_excel(buf, index=False, sheet_name='PorPeca')
    st.download_button("⬇️ Baixar Excel completo", data=buf.getvalue(),
                       file_name=f"mais_vendidos_por_peca_{data_ini}_{data_fim}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="mv_peca_xlsx")


# ============================================================
# TAB 2 — CRESCIMENTO & QUEDA
# ============================================================

def _tab_crescimento(engine):
    st.subheader("📈 Crescimento & Queda de SKUs")
    st.caption("Compara dois períodos contíguos de igual duração. Delta por quantidade.")

    col1, col2 = st.columns(2)
    with col1:
        dias = st.selectbox("Janela (cada período)",
                            [7, 14, 30, 60, 90],
                            index=2, key="cresc_dias")
    with col2:
        top_n = st.slider("Top N (em alta e em queda)",
                          5, 50, 20, 5, key="cresc_topn")

    _, _, lojas, mkts = _filtros_periodo_loja_marketplace("cresc")

    hoje = date.today()
    fim_atual = hoje
    ini_atual = hoje - timedelta(days=dias)
    fim_ant = ini_atual - timedelta(days=1)
    ini_ant = fim_ant - timedelta(days=dias - 1)

    st.caption(
        f"🟢 Atual: **{ini_atual.strftime('%d/%m')} → {fim_atual.strftime('%d/%m/%Y')}** "
        f" | 🔁 Comparado a: **{ini_ant.strftime('%d/%m')} → {fim_ant.strftime('%d/%m/%Y')}**"
    )

    where_at, params_at = _montar_where_filtros(ini_atual, fim_atual, lojas, mkts, engine, alias='f.')
    where_an, params_an = _montar_where_filtros(ini_ant, fim_ant, lojas, mkts, engine, alias='f.')

    query = f"""
        WITH atual AS (
            SELECT f.sku,
                   SUM(f.quantidade)::bigint AS qtd_atual,
                   SUM(f.valor_venda_efetivo)::numeric AS rec_atual
            FROM fact_vendas_snapshot f
            WHERE {' AND '.join(where_at)}
            GROUP BY f.sku
        ),
        anterior AS (
            SELECT f.sku,
                   SUM(f.quantidade)::bigint AS qtd_ant,
                   SUM(f.valor_venda_efetivo)::numeric AS rec_ant
            FROM fact_vendas_snapshot f
            WHERE {' AND '.join(where_an)}
            GROUP BY f.sku
        )
        SELECT
            COALESCE(a.sku, b.sku) AS sku,
            COALESCE(p.nome, '(sem cadastro)') AS nome,
            COALESCE(a.qtd_atual, 0) AS qtd_atual,
            COALESCE(b.qtd_ant,   0) AS qtd_ant,
            COALESCE(a.rec_atual, 0) AS rec_atual,
            COALESCE(b.rec_ant,   0) AS rec_ant
        FROM atual a
        FULL OUTER JOIN anterior b ON a.sku = b.sku
        LEFT JOIN dim_produtos p ON p.sku = COALESCE(a.sku, b.sku)
        WHERE COALESCE(a.qtd_atual, 0) + COALESCE(b.qtd_ant, 0) > 0
    """

    try:
        df = _query_to_df(engine, query, params_at + params_an)
    except Exception as e:
        st.error(f"Erro ao consultar: {e}")
        return

    if df.empty:
        st.info("Sem dados nos períodos selecionados.")
        return

    df = _coerce_num(df, ['qtd_atual', 'qtd_ant', 'rec_atual', 'rec_ant'])

    # Calcula deltas
    df['delta_qtd'] = df['qtd_atual'] - df['qtd_ant']
    df['delta_pct'] = df.apply(
        lambda r: ((float(r['qtd_atual']) / float(r['qtd_ant']) - 1) * 100)
        if r['qtd_ant'] and float(r['qtd_ant']) > 0
        else (float('inf') if r['qtd_atual'] > 0 else 0),
        axis=1,
    )

    # Filtra ruído (vendas zero em ambos)
    df_movimento = df[(df['qtd_atual'] > 0) | (df['qtd_ant'] > 0)].copy()

    # Em alta: ordenar por delta_pct (excluindo SKUs novos com inf no topo só se preferir)
    em_alta = df_movimento[df_movimento['delta_qtd'] > 0].copy()
    em_alta = em_alta.sort_values('delta_qtd', ascending=False).head(top_n)

    em_queda = df_movimento[df_movimento['delta_qtd'] < 0].copy()
    em_queda = em_queda.sort_values('delta_qtd', ascending=True).head(top_n)

    def _fmt_tabela(d):
        d = d.copy()
        d['Atual (qtd)']    = d['qtd_atual'].apply(_fmt_int)
        d['Anterior (qtd)'] = d['qtd_ant'].apply(_fmt_int)
        d['Δ Qtd']          = d['delta_qtd'].apply(lambda v: f"{int(v):+,}".replace(",", "."))
        d['Δ %']            = d['delta_pct'].apply(
            lambda v: "novo" if v == float('inf') else f"{v:+.1f}%"
        )
        d['Receita Atual']  = d['rec_atual'].apply(_fmt_brl)
        d['Receita Ant.']   = d['rec_ant'].apply(_fmt_brl)
        return d[['sku', 'nome', 'Atual (qtd)', 'Anterior (qtd)',
                  'Δ Qtd', 'Δ %', 'Receita Atual', 'Receita Ant.']].rename(
            columns={'sku': 'SKU', 'nome': 'Produto'}
        )

    col_alta, col_queda = st.columns(2)
    with col_alta:
        st.markdown(f"### 🟢 Em alta (Top {top_n})")
        if em_alta.empty:
            st.info("Nenhum SKU em crescimento.")
        else:
            st.dataframe(_fmt_tabela(em_alta),
                         use_container_width=True, hide_index=True)
    with col_queda:
        st.markdown(f"### 🔴 Em queda (Top {top_n})")
        if em_queda.empty:
            st.info("Nenhum SKU em queda.")
        else:
            st.dataframe(_fmt_tabela(em_queda),
                         use_container_width=True, hide_index=True)

    with st.expander("💾 Baixar comparativo completo (Excel)"):
        buf = io.BytesIO()
        df_movimento.to_excel(buf, index=False, sheet_name='Crescimento')
        st.download_button(
            "⬇️ Download",
            data=buf.getvalue(),
            file_name=f"crescimento_skus_{dias}d.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# TAB 3 — COBERTURA EM PEÇA (ML)  (frente [KITS], 01/10/2026)
# ============================================================
# Substitui a cobertura que lia dim_estoque (upload do UpSeller, parado desde
# 17/05) e a aba de upload que a alimentava: estoque agora só da API do ML.
# dim_estoque fica no banco, sem tela. A conta mora em estoque_peca.py.

def _tab_cobertura_peca(engine):
    try:
        estoque_peca.render(engine)
    except Exception as e:
        st.error(f"❌ Não consegui montar a cobertura em peça. Tente de novo; se "
                 f"continuar, avise com esta mensagem:\n\n{type(e).__name__}: {str(e)[:300]}")


# ============================================================
# ENTRYPOINT
# ============================================================

# ============================================================
# TAB 6 — FECHAMENTO DE ESTOQUE
# ============================================================

# (rótulo na tela, marketplace em dim_lojas, leitor, extensões aceitas)
_ORIGENS_FECHAMENTO = {
    'Galpão (Upseller)':   (None,            'upseller', ['xlsx', 'xls']),
    'Mercado Livre (Full)': ('MERCADO LIVRE', 'ml',       ['xlsx', 'xls']),
    'Amazon (FBA)':         ('AMAZON',        'amazon',   ['csv']),
    'Shopee (FBS)':         ('SHOPEE',        'shopee',   ['xlsx', 'xls']),
}


def _ultimo_dia_mes_anterior(hoje=None):
    """
    Data de referência padrão do fechamento.

    Convenção do Thiago: a posição é puxada no dia 1º e vale como fechamento
    do mês que acabou. Puxar dia 1º e etiquetar como setembro faria o
    fechamento de agosto nunca existir.
    """
    hoje = hoje or date.today()
    return hoje.replace(day=1) - timedelta(days=1)


def _meses_com_fechamento(engine):
    try:
        df = _query_to_df(engine, """
            SELECT DISTINCT data_referencia FROM fact_estoque_mensal
            ORDER BY data_referencia DESC
        """)
        return [d for d in df['data_referencia']] if not df.empty else []
    except Exception:
        return []


def _tab_fechamento_estoque(engine):
    st.subheader("📦 Fechamento de Estoque")
    st.caption(
        "Posição valorizada do galpão e de cada Full, ao custo de compra "
        "congelado no dia do upload."
    )

    sub_painel, sub_upload = st.tabs(["📊 Painel do mês", "⬆️ Subir relatórios"])

    # O upload roda ANTES do painel de propósito. As duas sub-tabs são
    # renderizadas no mesmo ciclo do Streamlit, na ordem em que o código
    # executa — e não na ordem em que aparecem na tela. Com o painel primeiro,
    # ele consultava o banco antes da gravação do mesmo clique acontecer e
    # ficava sempre um upload atrasado.
    with sub_upload:
        _fechamento_upload(engine)
    with sub_painel:
        _fechamento_painel(engine)


def _fechamento_painel(engine):
    meses = _meses_com_fechamento(engine)
    if not meses:
        st.info(
            "Nenhum fechamento gravado ainda. Use **Subir relatórios** para "
            "carregar a posição de cada local."
        )
        return

    ref = st.selectbox(
        "📅 Mês de referência", meses,
        format_func=lambda d: d.strftime('%m/%Y') if hasattr(d, 'strftime') else str(d),
        key="fech_ref",
    )

    where, params = ["data_referencia = %s"], [ref]
    if not ve_todas_lojas():
        # Quem só vê algumas lojas não vê o galpão: ele é da empresa inteira.
        permitidas = get_lojas_usuario(engine)
        if not permitidas:
            st.warning("Você não tem loja liberada para ver o fechamento.")
            return
        where.append("loja = ANY(%s)")
        params.append(list(permitidas))

    df = _query_to_df(engine, f"""
        SELECT local_estoque, marketplace, loja,
               COUNT(*)                          AS skus,
               SUM(qtd_disponivel)               AS disponivel,
               SUM(qtd_indisponivel)             AS indisponivel,
               SUM(valor_disponivel + valor_indisponivel) AS valor_estoque,
               SUM(CASE WHEN transito_tipo = 'ENTRADA_FULL'
                        THEN qtd_transito ELSE 0 END)     AS transito_full,
               SUM(CASE WHEN transito_tipo = 'ENTRADA_FULL'
                        THEN valor_transito ELSE 0 END)   AS valor_transito_full,
               SUM(CASE WHEN transito_tipo = 'IMPORTACAO'
                        THEN qtd_transito ELSE 0 END)     AS importacao,
               SUM(CASE WHEN transito_tipo = 'IMPORTACAO'
                        THEN valor_transito ELSE 0 END)   AS valor_importacao,
               SUM(CASE WHEN sku_cadastrado THEN 0
                        ELSE qtd_disponivel + qtd_indisponivel END) AS un_sem_custo,
               MAX(data_upload)                  AS atualizado_em
        FROM fact_estoque_mensal
        WHERE {' AND '.join(where)}
        GROUP BY local_estoque, marketplace, loja
        ORDER BY SUM(valor_disponivel + valor_indisponivel) DESC
    """, params)

    if df.empty:
        st.info("Sem dados neste mês para as lojas que você acessa.")
        return

    num = ['skus', 'disponivel', 'indisponivel', 'valor_estoque', 'transito_full',
           'valor_transito_full', 'importacao', 'valor_importacao', 'un_sem_custo']
    df = _coerce_num(df, num).fillna({c: 0 for c in num})

    valor_estoque = float(df['valor_estoque'].sum())
    valor_tr = float(df['valor_transito_full'].sum())
    valor_imp = float(df['valor_importacao'].sum())
    un_imp = int(df['importacao'].sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Estoque", _fmt_brl(valor_estoque),
              help="Disponível + indisponível, nos locais que você acessa.")
    c2.metric("A caminho dos Fulls", _fmt_brl(valor_tr),
              help="Saiu do galpão e ainda não chegou ao CD. Soma ao imobilizado.")
    c3.metric("Imobilizado", _fmt_brl(valor_estoque + valor_tr),
              help="Estoque + o que está a caminho dos Fulls.")
    c4.metric("Importação", _fmt_brl(valor_imp),
              help="Compra a caminho do galpão. NÃO entra no imobilizado: "
                   "mercadoria que ainda não chegou não é estoque.")
    c5.metric("Unidades", _fmt_int(df['disponivel'].sum() + df['indisponivel'].sum()),
              help="Unidades em estoque, sem contar as que estão a caminho.")

    sem_custo = int(df['un_sem_custo'].sum())
    if sem_custo:
        st.warning(
            f"⚠️ {_fmt_int(sem_custo)} unidade(s) entraram valendo **zero** por "
            f"não terem preço de compra cadastrado. O valor acima está "
            f"subestimado nessa medida."
        )

    # Uma linha por local, depois o TOTAL, depois a importação — que fica
    # separada logo abaixo do total, e não somada nele, de propósito.
    linhas = [{
        'Local': r['local_estoque'],
        'SKUs': _fmt_int(r['skus']),
        'Disponível': _fmt_int(r['disponivel']),
        'Indisponível': _fmt_int(r['indisponivel']),
        'Estoque (R$)': _fmt_brl(r['valor_estoque']),
        'Em trânsito (un)': _fmt_int(r['transito_full']),
        'Em trânsito (R$)': _fmt_brl(r['valor_transito_full']),
        'Sem custo (un)': _fmt_int(r['un_sem_custo']),
    } for _, r in df.iterrows()]

    linhas.append({
        'Local': 'TOTAL',
        'SKUs': _fmt_int(df['skus'].sum()),
        'Disponível': _fmt_int(df['disponivel'].sum()),
        'Indisponível': _fmt_int(df['indisponivel'].sum()),
        'Estoque (R$)': _fmt_brl(valor_estoque),
        'Em trânsito (un)': _fmt_int(df['transito_full'].sum()),
        'Em trânsito (R$)': _fmt_brl(valor_tr),
        'Sem custo (un)': _fmt_int(sem_custo),
    })

    if un_imp or valor_imp:
        linhas.append({
            'Local': '➕ Importação a caminho do galpão (fora do total)',
            'SKUs': '', 'Disponível': '', 'Indisponível': '', 'Estoque (R$)': '',
            'Em trânsito (un)': _fmt_int(un_imp),
            'Em trânsito (R$)': _fmt_brl(valor_imp),
            'Sem custo (un)': '',
        })

    st.dataframe(pd.DataFrame(linhas), use_container_width=True, hide_index=True)
    st.caption(
        f"**Imobilizado hoje: {_fmt_brl(valor_estoque + valor_tr)}** — estoque "
        f"mais o que está a caminho dos Fulls. A importação de "
        f"{_fmt_brl(valor_imp)} fica de fora porque ainda não chegou ao galpão."
    )

    _fechamento_detalhes(engine, ref, where, params)


def _fechamento_detalhes(engine, ref, where, params):
    """Quebra por motivo de indisponibilidade e lista de SKUs sem custo."""
    with st.expander("🔍 Por que há estoque indisponível"):
        df = _query_to_df(engine, f"""
            SELECT local_estoque, m.key AS motivo, SUM(m.value::int) AS unidades
            FROM fact_estoque_mensal,
                 LATERAL jsonb_each_text(COALESCE(motivos_indisponivel, '{{}}'::jsonb)) m
            WHERE {' AND '.join(where)}
            GROUP BY local_estoque, m.key
            HAVING SUM(m.value::int) > 0
            ORDER BY SUM(m.value::int) DESC
        """, params)
        if df.empty:
            st.caption("Nenhuma unidade indisponível neste fechamento.")
        else:
            df = _coerce_num(df, ['unidades'])
            st.dataframe(df.rename(columns={
                'local_estoque': 'Local', 'motivo': 'Motivo', 'unidades': 'Unidades',
            }), use_container_width=True, hide_index=True)

    with st.expander("⚠️ SKUs sem preço de compra"):
        df = _query_to_df(engine, f"""
            SELECT local_estoque, sku,
                   qtd_disponivel + qtd_indisponivel + qtd_transito AS unidades
            FROM fact_estoque_mensal
            WHERE {' AND '.join(where)} AND sku_cadastrado = FALSE
              AND qtd_disponivel + qtd_indisponivel + qtd_transito > 0
            ORDER BY 3 DESC
        """, params)
        if df.empty:
            st.caption("Todos os SKUs deste fechamento têm preço de compra. 🎉")
        else:
            df = _coerce_num(df, ['unidades'])
            st.caption(
                "Cadastrar o preço de compra destes SKUs e subir o arquivo de "
                "novo corrige o fechamento — o upload substitui o local inteiro."
            )
            st.dataframe(df.rename(columns={
                'local_estoque': 'Local', 'sku': 'SKU', 'unidades': 'Unidades',
            }), use_container_width=True, hide_index=True)


def _fechamento_upload(engine):
    from processar_estoque_full import (
        ler_ml, ler_amazon, ler_shopee, ler_upseller,
        buscar_mapa_asin, buscar_precos_compra, valorizar, gravar_fechamento,
        LOCAL_GALPAO, TRANSITO_ENTRADA_FULL, TRANSITO_IMPORTACAO,
    )
    from database_utils import buscar_mapeamento_skus

    c1, c2 = st.columns([1.4, 1])
    with c1:
        origem = st.selectbox("📥 Origem do relatório",
                              list(_ORIGENS_FECHAMENTO.keys()), key="fech_origem")
    with c2:
        ref = st.date_input("📅 Mês de referência", value=_ultimo_dia_mes_anterior(),
                            key="fech_data",
                            help="A posição puxada no dia 1º fecha o mês anterior.")

    marketplace, leitor, extensoes = _ORIGENS_FECHAMENTO[origem]

    loja = None
    if marketplace is None:
        local = LOCAL_GALPAO
        transito_tipo = TRANSITO_IMPORTACAO
        st.caption("O Upseller enxerga só o galpão, então o local já está definido.")
    else:
        transito_tipo = TRANSITO_ENTRADA_FULL
        lojas_mkt = _lojas_do_marketplace(engine, marketplace)
        if not ve_todas_lojas():
            permitidas = set(get_lojas_usuario(engine))
            lojas_mkt = [l for l in lojas_mkt if l in permitidas]
        if not lojas_mkt:
            st.warning("Nenhuma loja deste marketplace liberada para você.")
            return
        loja = st.selectbox("🏪 Loja", lojas_mkt, key="fech_loja")
        local = loja

    arquivo = st.file_uploader(
        f"Relatório de estoque — {origem}", type=extensoes, key="fech_arquivo",
        help="Um arquivo por local. Subir de novo o mesmo local substitui o "
             "que já estava gravado naquele mês.",
    )
    if arquivo is None:
        _fechamento_historico(engine)
        return

    # O arquivo é lido a cada rerun do Streamlit — e o buffer fica no fim da
    # leitura anterior. Sem rebobinar, a segunda leitura vem vazia e a tela
    # volta sem preview e sem botão, como se o clique não tivesse feito nada.
    if hasattr(arquivo, 'seek'):
        arquivo.seek(0)

    try:
        if leitor == 'ml':
            df, avisos = ler_ml(arquivo)
        elif leitor == 'shopee':
            df, avisos = ler_shopee(arquivo)
        elif leitor == 'upseller':
            df, avisos = ler_upseller(arquivo)
        else:
            df, avisos = ler_amazon(arquivo, buscar_mapa_asin(engine),
                                    buscar_mapeamento_skus(engine))
    except Exception as exc:
        st.error(f"Falha ao ler o arquivo: {exc}")
        return

    for aviso in avisos:
        st.warning(f"⚠️ {aviso}")

    if df.empty:
        st.error("O arquivo não produziu nenhuma linha de estoque.")
        return

    df = valorizar(df, buscar_precos_compra(engine))

    unidades = int(df['disponivel'].sum() + df['indisponivel'].sum())
    valor = float(df['valor_disponivel'].sum() + df['valor_indisponivel'].sum())
    transito = int(df['transito'].sum())
    sem_custo = df[~df['sku_cadastrado']]
    un_sem_custo = int(sem_custo['disponivel'].sum() + sem_custo['indisponivel'].sum())

    st.markdown(f"### Prévia — `{local}`, fechamento de **{ref.strftime('%m/%Y')}**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKUs", _fmt_int(len(df)))
    c2.metric("Unidades", _fmt_int(unidades))
    c3.metric("Valor", _fmt_brl(valor))
    c4.metric(
        "A caminho" if transito_tipo == TRANSITO_ENTRADA_FULL else "Importação",
        _fmt_int(transito),
    )

    if un_sem_custo:
        st.warning(
            f"⚠️ {_fmt_int(len(sem_custo))} SKU(s) sem preço de compra "
            f"({_fmt_int(un_sem_custo)} unidades) vão entrar valendo zero."
        )
        with st.expander("Ver SKUs sem preço de compra"):
            st.dataframe(
                sem_custo[['sku', 'disponivel', 'indisponivel', 'transito']]
                .rename(columns={'sku': 'SKU', 'disponivel': 'Disponível',
                                 'indisponivel': 'Indisponível',
                                 'transito': 'Trânsito'}),
                use_container_width=True, hide_index=True,
            )

    with st.expander("Ver as linhas que serão gravadas"):
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("---")
    if st.button(f"💾 Gravar fechamento de `{local}`", type="primary",
                 key="fech_gravar"):
        res = gravar_fechamento(engine, df, ref, local, marketplace, loja,
                                transito_tipo, arquivo.name)
        gravar_log_upload(engine, {
            'marketplace': marketplace or 'UPSELLER',
            'loja': loja or LOCAL_GALPAO,
            'arquivo_nome': arquivo.name,
            'periodo_inicio': None,
            'periodo_fim': None,
            'total_linhas': int(len(df)),
            'linhas_importadas': res['inseridos'],
            'linhas_erro': int(len(df)) - res['inseridos'],
        })
        # Sem st.rerun() aqui: ele reinicia o script na hora e joga fora a
        # mensagem abaixo, que é justamente a confirmação de que gravou.
        st.success(f"✅ {res['mensagem']}")
        st.caption("Confira o resultado em **Painel do mês**.")

    _fechamento_historico(engine)


@st.cache_data(ttl=300, show_spinner=False)
def _lojas_do_marketplace(_engine, marketplace):
    try:
        df = pd.read_sql(
            "SELECT DISTINCT loja FROM dim_lojas WHERE marketplace = %s ORDER BY loja",
            _engine, params=(marketplace,),
        )
        return df['loja'].dropna().tolist()
    except Exception:
        return []


def _fechamento_historico(engine):
    st.markdown("### 🗂️ O que já está fechado")
    try:
        df = _query_to_df(engine, """
            SELECT data_referencia, local_estoque,
                   COUNT(*) AS skus,
                   SUM(qtd_disponivel + qtd_indisponivel) AS unidades,
                   SUM(valor_disponivel + valor_indisponivel) AS valor,
                   MAX(data_upload) AS quando
            FROM fact_estoque_mensal
            GROUP BY data_referencia, local_estoque
            ORDER BY data_referencia DESC, local_estoque
            LIMIT 40
        """)
        if df.empty:
            st.caption("Nenhum fechamento gravado ainda.")
            return
        df = _coerce_num(df, ['skus', 'unidades', 'valor'])
        st.dataframe(pd.DataFrame({
            'Mês': df['data_referencia'].apply(
                lambda d: d.strftime('%m/%Y') if hasattr(d, 'strftime') else str(d)),
            'Local': df['local_estoque'],
            'SKUs': df['skus'].apply(_fmt_int),
            'Unidades': df['unidades'].apply(_fmt_int),
            'Valor': df['valor'].apply(_fmt_brl),
            'Subido em': pd.to_datetime(df['quando']).dt.strftime('%d/%m/%Y %H:%M'),
        }), use_container_width=True, hide_index=True)
    except Exception:
        st.caption("Histórico indisponível.")


# ============================================================
# TAB 7 — PENALIZAÇÃO DE FRETE (Shopee + TikTok)
# ============================================================
#
# Regra do negócio (Thiago, 24/09/2026): na Shopee e no TikTok o frete da Nala
# é ZERO (no TikTok a Nala paga comissão maior para ter frete grátis). Qualquer
# frete cobrado nessas plataformas é penalização por peso/medida errados no
# cadastro do ANÚNCIO. O sistema não tem peso cadastrado (dim_produtos está
# vazio nisso), então o alarme usa só o que o marketplace cobrou.
#
# De onde vem o valor, em fact_vendas_snapshot (fonte única do gestor):
#   - Shopee pela API (arquivo_origem = 'API'): o coletor põe a multa de
#     peso/medida no `frete` da linha, rateada pelo valor das linhas do pedido.
#     O upload da Shopee grava frete = 0 (o export não traz a multa), por isso
#     só as linhas da API entram: isso exclui a Shopee-Yanni e a Shopee
#     anterior a 01/09 sem citar loja nenhuma aqui.
#   - TikTok pelo upload: `frete` = −"Custo líquido de frete" do relatório.
#     Valor negativo é crédito a favor da Nala e fica FORA da soma.
# O espelho da API (fact_pedidos_marketplace) entra só com o peso cobrado ×
# peso do anúncio, informativo: nenhum valor em R$ sai dele.
#
# TikTok, etapa 2 (29/09/2026): fact_tiktok_frete_detalhe guarda o peso do
# anúncio, o peso cobrado e o reembolso ao cliente, gravados pelo upload SÓ
# DAQUI PARA FRENTE (relatório antigo não é reenviado). Liga ao snapshot só na
# leitura, por (loja, pedido_original, codigo_anuncio = SKU TikTok). Pedido
# com reembolso sai da soma e aparece à parte. Enquanto a tabela não existir
# no banco, a SQL usa um `tk` vazio e a aba funciona como antes.

_LOJA_EXIBICAO = {
    'Shopee Lithouse(Nala)': 'Shopee-Nala',
    'Shopee Litstore(Yanni)': 'Shopee-Yanni',
}

_TK_TABELA = """
    tk AS (
        SELECT loja_origem AS loja, pedido_original, sku_tiktok,
               MAX(peso_estimado_g)    AS peso_estimado_g,
               MAX(peso_embalagem_g)   AS peso_embalagem_g,
               MAX(reembolso_produtos) AS reembolso_produtos
        FROM fact_tiktok_frete_detalhe
        GROUP BY loja_origem, pedido_original, sku_tiktok
    )"""

_TK_VAZIO = """
    tk AS (
        SELECT NULL::varchar AS loja, NULL::varchar AS pedido_original,
               NULL::varchar AS sku_tiktok, NULL::numeric AS peso_estimado_g,
               NULL::numeric AS peso_embalagem_g, NULL::numeric AS reembolso_produtos
        WHERE FALSE
    )"""

# Parâmetros NOMEADOS do psycopg2 — nenhum % fora deles (um % solto já
# derrubou SQL da Shopee em produção). `lojas` NULL = todas as lojas.
# `{tk}` é trocado em Python (str.format) antes de ir ao banco.
_SQL_PENALIZACAO_MODELO = """
    WITH {tk},
    base AS (
        SELECT s.marketplace_origem AS marketplace,
               s.loja_origem        AS loja,
               s.sku,
               s.numero_pedido,
               COALESCE(NULLIF(s.pedido_original, ''), s.numero_pedido) AS pedido,
               COALESCE(s.frete, 0) AS frete,
               COALESCE(tk.reembolso_produtos, 0) > 0 AS reembolsado,
               tk.peso_embalagem_g,
               tk.peso_estimado_g
        FROM fact_vendas_snapshot s
        LEFT JOIN tk
               ON s.marketplace_origem = 'TIKTOK'
              AND tk.loja = s.loja_origem
              AND tk.pedido_original = s.pedido_original
              AND tk.sku_tiktok = s.codigo_anuncio
        WHERE s.data_venda >= %(data_ini)s
          AND s.data_venda <= %(data_fim)s
          AND (s.marketplace_origem = 'TIKTOK'
               OR (s.marketplace_origem = 'SHOPEE' AND s.arquivo_origem = 'API'))
          AND (%(lojas)s::text[] IS NULL OR s.loja_origem = ANY(%(lojas)s::text[]))
    ),
    -- Nome e peso agregados ANTES do JOIN: chave repetida nessas tabelas não
    -- pode multiplicar a linha de venda e dobrar a soma de R$.
    nomes AS (
        SELECT sku, MAX(nome) AS nome
        FROM dim_produtos
        GROUP BY sku
    ),
    pesos AS (
        SELECT loja, numero_pedido,
               MAX(peso_cobrado_g)     AS peso_cobrado_g,
               MAX(peso_cadastrado_kg) AS peso_cadastrado_kg
        FROM fact_pedidos_marketplace
        WHERE marketplace = 'SHOPEE'
          AND data_venda >= %(data_ini)s
          AND data_venda <= %(data_fim)s
        GROUP BY loja, numero_pedido
    )
    SELECT b.marketplace,
           b.loja,
           b.sku,
           MAX(dp.nome) AS nome,
           COUNT(DISTINCT b.pedido) AS pedidos_total,
           COUNT(DISTINCT b.pedido) FILTER (WHERE b.frete > 0 AND NOT b.reembolsado)
               AS pedidos_penalizados,
           -- Para o card contar PEDIDOS distintos: carrinho com 2 SKUs
           -- multados é 1 pedido, não 2.
           ARRAY_AGG(DISTINCT b.pedido) FILTER (WHERE b.frete > 0 AND NOT b.reembolsado)
               AS lista_pedidos_penalizados,
           COALESCE(SUM(b.frete) FILTER (WHERE b.frete > 0 AND NOT b.reembolsado), 0)
               AS frete_rs,
           -- Frete em pedido com reembolso ao cliente (TikTok): fora da soma.
           COUNT(DISTINCT b.pedido) FILTER (WHERE b.frete > 0 AND b.reembolsado)
               AS pedidos_reembolso,
           COALESCE(SUM(b.frete) FILTER (WHERE b.frete > 0 AND b.reembolsado), 0)
               AS frete_reembolso_rs,
           COUNT(DISTINCT b.pedido) FILTER (WHERE b.frete < 0) AS pedidos_credito,
           COALESCE(-SUM(b.frete) FILTER (WHERE b.frete < 0), 0) AS credito_rs,
           -- Shopee: espelho da API. TikTok: detalhe do upload (tk).
           AVG(COALESCE(pe.peso_cobrado_g, b.peso_embalagem_g) / 1000.0)
               FILTER (WHERE b.frete > 0 AND NOT b.reembolsado) AS peso_cobrado_kg,
           AVG(COALESCE(pe.peso_cadastrado_kg, b.peso_estimado_g / 1000.0))
               FILTER (WHERE b.frete > 0 AND NOT b.reembolsado) AS peso_anuncio_kg
    FROM base b
    LEFT JOIN nomes dp ON dp.sku = b.sku
    LEFT JOIN pesos pe
           ON b.marketplace = 'SHOPEE'
          AND pe.loja = b.loja
          AND pe.numero_pedido = b.numero_pedido
    GROUP BY b.marketplace, b.loja, b.sku
    HAVING COUNT(*) FILTER (WHERE b.frete <> 0) > 0
    ORDER BY frete_rs DESC, b.loja, b.sku
"""

SQL_PENALIZACAO_FRETE = _SQL_PENALIZACAO_MODELO.format(tk=_TK_TABELA)
SQL_PENALIZACAO_FRETE_SEM_TK = _SQL_PENALIZACAO_MODELO.format(tk=_TK_VAZIO)

# Existe E o usuário do app pode ler (R3: se o app sair do neondb_owner sem o
# GRANT, a aba cai no `tk` vazio em vez de quebrar). O CASE evita chamar
# has_table_privilege com tabela inexistente, que dá erro.
SQL_EXISTE_TK = """
    SELECT CASE WHEN to_regclass('public.fact_tiktok_frete_detalhe') IS NULL THEN FALSE
                ELSE has_table_privilege('public.fact_tiktok_frete_detalhe', 'SELECT')
           END
"""

# Mesmo universo e mesmo filtro de loja da SQL principal: o "dado disponível
# até" de um gestor é o das lojas dele.
SQL_PENALIZACAO_DADO_ATE = """
    SELECT MAX(data_venda)
    FROM fact_vendas_snapshot
    WHERE (marketplace_origem = 'TIKTOK'
           OR (marketplace_origem = 'SHOPEE' AND arquivo_origem = 'API'))
      AND (%(lojas)s::text[] IS NULL OR loja_origem = ANY(%(lojas)s::text[]))
"""


def params_penalizacao_frete(data_ini, data_fim, lojas=None):
    """Parâmetros da SQL_PENALIZACAO_FRETE. `lojas` None = todas; [] = nenhuma."""
    return {
        'data_ini': data_ini,
        'data_fim': data_fim,
        'lojas': None if lojas is None else list(lojas),
    }


def params_penalizacao_dado_ate(lojas=None):
    """Parâmetros da SQL_PENALIZACAO_DADO_ATE."""
    return {'lojas': None if lojas is None else list(lojas)}


def contar_pedidos_penalizados(alarme):
    """Pedidos penalizados DISTINTOS (por loja) nas linhas do alarme."""
    pedidos = set()
    for loja, lista in zip(alarme['loja'], alarme['lista_pedidos_penalizados']):
        for p in (lista or []):
            pedidos.add((loja, p))
    return len(pedidos)


def sql_penalizacao_frete(tem_detalhe_tiktok):
    """SQL da aba: com o detalhe do TikTok se a tabela existe, senão sem."""
    return SQL_PENALIZACAO_FRETE if tem_detalhe_tiktok else SQL_PENALIZACAO_FRETE_SEM_TK


def separar_alarme_creditos(df):
    """
    Divide o resultado em (alarme, créditos, reembolsos).

    Alarme: SKUs com pedido penalizado, com R$ médio por pedido e % dos
    pedidos do SKU. Créditos: frete negativo (a favor da Nala). Reembolsos:
    frete de pedido TikTok com reembolso ao cliente. Os dois últimos ficam
    fora da soma.
    """
    df = _coerce_num(df.copy(), [
        'pedidos_total', 'pedidos_penalizados', 'frete_rs',
        'pedidos_credito', 'credito_rs', 'peso_cobrado_kg', 'peso_anuncio_kg',
        'pedidos_reembolso', 'frete_reembolso_rs',
    ])
    reembolsos = df[df['pedidos_reembolso'] > 0].copy()
    alarme = df[df['pedidos_penalizados'] > 0].copy()
    alarme['media_rs'] = (alarme['frete_rs'] / alarme['pedidos_penalizados']).round(2)
    alarme['pct_penalizados'] = (
        100.0 * alarme['pedidos_penalizados'] / alarme['pedidos_total']).round(1)
    alarme = alarme.sort_values(['frete_rs', 'loja', 'sku'],
                                ascending=[False, True, True])
    creditos = df[df['pedidos_credito'] > 0].copy()
    return (alarme.reset_index(drop=True), creditos.reset_index(drop=True),
            reembolsos.reset_index(drop=True))


def _tab_penalizacao_frete(engine):
    """Mesma blindagem da aba de Full: erro aqui não derruba as outras abas."""
    try:
        _render_penalizacao_frete(engine)
    except Exception as e:
        st.error(
            "Esta aba encontrou um erro e foi isolada — as demais abas continuam "
            "funcionando normalmente."
        )
        st.caption(f"Detalhe técnico: {type(e).__name__}: {e}")


def _fmt_kg(v):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):.2f} kg".replace(".", ",")


def _render_penalizacao_frete(engine):
    st.subheader("🚚 Penalização de frete — Shopee e TikTok")
    st.caption(
        "Na Shopee e no TikTok o frete da Nala é zero. Todo frete cobrado aqui é "
        "penalização por peso/medida errados no **cadastro do anúncio** no "
        "marketplace — é lá que se corrige."
    )

    # RBAC: gestor de loja só vê as lojas dele (mesmo padrão do status do Full)
    lojas = None if ve_todas_lojas() else list(get_lojas_usuario(engine) or [])
    if lojas is not None and not lojas:
        st.caption("Nenhuma loja atribuída ao seu perfil.")
        return

    dado_ate = _query_to_df(engine, SQL_PENALIZACAO_DADO_ATE,
                            params_penalizacao_dado_ate(lojas)).iloc[0, 0]
    ini, fim = filtro_periodo("pen_frete", dado_ate=dado_ate,
                              padrao="Últimos 30 dias")
    if ini is None:
        return

    st.info(
        "**Cobertura do dado:** Shopee-Nala e Shopee-LPT só a partir de "
        "01/09/2026 (vendas pela API). Shopee-Yanni: sem dado de multa (o upload "
        "não traz). TikTok-Nala: pelo upload do relatório financeiro; peso "
        "cobrado e reembolso do TikTok só nos relatórios enviados a partir de "
        "29/09/2026 (antes disso, \"—\"). Pedido com reembolso ou devolução ao "
        "cliente é devolução, não penalização: fica fora da soma."
    )
    if ini < date(2026, 9, 1):
        st.warning(
            "O período começa antes de 01/09/2026: nesses dias a Shopee não tem "
            "dado de multa, só o TikTok aparece."
        )

    tem_detalhe_tiktok = bool(_query_to_df(engine, SQL_EXISTE_TK).iloc[0, 0])
    df = _query_to_df(engine, sql_penalizacao_frete(tem_detalhe_tiktok),
                      params_penalizacao_frete(ini, fim, lojas))
    if df.empty:
        st.success("Nenhum frete cobrado na Shopee ou no TikTok no período.")
        return

    alarme, creditos, reembolsos = separar_alarme_creditos(df)
    for d in (alarme, creditos, reembolsos):
        d['loja'] = d['loja'].map(lambda l: _LOJA_EXIBICAO.get(l, l))
        d['nome'] = d['nome'].fillna('(sem nome no cadastro)')

    lojas_opt = sorted(set(alarme['loja']) | set(creditos['loja'])
                       | set(reembolsos['loja']))
    lojas_sel = st.multiselect("🏪 Lojas", lojas_opt, default=[],
                               placeholder="Todas as lojas",
                               key="pen_frete_lojas")
    if lojas_sel:
        alarme = alarme[alarme['loja'].isin(lojas_sel)]
        creditos = creditos[creditos['loja'].isin(lojas_sel)]
        reembolsos = reembolsos[reembolsos['loja'].isin(lojas_sel)]

    c1, c2, c3 = st.columns(3)
    c1.metric("Frete cobrado no período (R$)", _fmt_brl(alarme['frete_rs'].sum()))
    c2.metric("Pedidos penalizados (nº)", _fmt_int(contar_pedidos_penalizados(alarme)))
    c3.metric("SKUs a corrigir (nº)", _fmt_int(len(alarme)))

    if alarme.empty:
        st.success("Nenhum SKU penalizado no período.")
    else:
        # Já vem ordenado por Frete R$ decrescente (separar_alarme_creditos).
        tabela = pd.DataFrame({
            'SKU': alarme['sku'],
            'Produto': alarme['nome'],
            'Loja': alarme['loja'],
            'Frete R$': alarme['frete_rs'].map(_fmt_brl),
            'Média R$': alarme['media_rs'].map(_fmt_brl),
            'Peso cobrado': alarme['peso_cobrado_kg'].map(_fmt_kg),
            'Peso anúncio': alarme['peso_anuncio_kg'].map(_fmt_kg),
            'Pedidos pen.': alarme['pedidos_penalizados'].astype(int),
            'Pedidos SKU': alarme['pedidos_total'].astype(int),
            '% pen.': alarme['pct_penalizados'].map(_fmt_pct),
        })
        _col = st.column_config
        st.dataframe(
            tabela, use_container_width=True, hide_index=True,
            column_config={
                'SKU': _col.TextColumn(width="medium", help="SKU da Nala"),
                'Produto': _col.TextColumn(width="large", help="Nome do produto no cadastro"),
                'Loja': _col.TextColumn(width="medium", help="Loja com a sigla do marketplace"),
                'Frete R$': _col.TextColumn(
                    width="small", help="Frete cobrado no período (R$), soma dos pedidos penalizados"),
                'Média R$': _col.TextColumn(
                    width="small", help="Média de frete por pedido penalizado (R$)"),
                'Peso cobrado': _col.TextColumn(
                    width="small",
                    help="Peso cobrado médio (kg) nos pedidos penalizados. Shopee: "
                         "do pedido inteiro (API). TikTok: peso da embalagem cobrável, "
                         "só relatórios enviados a partir de 29/09/2026"),
                'Peso anúncio': _col.TextColumn(
                    width="small",
                    help="Peso do anúncio (kg), média nos pedidos penalizados. TikTok: "
                         "peso estimado do pacote, só relatórios a partir de 29/09/2026"),
                'Pedidos pen.': _col.NumberColumn(
                    width="small", help="Pedidos penalizados (nº): pedidos do SKU com frete cobrado"),
                'Pedidos SKU': _col.NumberColumn(
                    width="small", help="Pedidos do SKU no período (nº), com e sem frete"),
                '% pen.': _col.TextColumn(
                    width="small", help="% dos pedidos do SKU que foram penalizados"),
            })
        st.caption(
            "Peso cobrado × peso do anúncio: informativo (Shopee: espelho da API, "
            "peso do pedido inteiro; TikTok: relatório financeiro), média simples das linhas de venda "
            "penalizadas do SKU. Na Shopee, pedido com vários SKUs tem a multa "
            "dividida pelo valor de cada linha — por isso a soma da coluna "
            "\"Pedidos pen.\" pode passar do card, que conta cada pedido uma vez. "
            "Passe o mouse no título da coluna para ver o que ela mede."
        )

    if not creditos.empty:
        st.markdown("#### Créditos de frete (a favor da Nala) — fora da soma")
        st.dataframe(pd.DataFrame({
            'SKU': creditos['sku'],
            'Produto': creditos['nome'],
            'Loja': creditos['loja'],
            'Pedidos com crédito (nº)': creditos['pedidos_credito'].astype(int),
            'Crédito (R$)': creditos['credito_rs'].map(_fmt_brl),
        }), use_container_width=True, hide_index=True)

    if not reembolsos.empty:
        st.markdown("#### Frete em pedido com reembolso ao cliente (TikTok) — fora da soma")
        st.caption(
            "Regra (Thiago, 29/09/2026): pedido com reembolso ou devolução ao "
            "cliente é DEVOLUÇÃO, não penalização de peso/medida. O frete desses "
            "pedidos fica fora da soma e dos cards e aparece só aqui."
        )
        st.dataframe(pd.DataFrame({
            'SKU': reembolsos['sku'],
            'Produto': reembolsos['nome'],
            'Loja': reembolsos['loja'],
            'Pedidos reembolsados (nº)': reembolsos['pedidos_reembolso'].astype(int),
            'Frete (R$)': reembolsos['frete_reembolso_rs'].map(_fmt_brl),
        }), use_container_width=True, hide_index=True)


def main():
    st.header("📈 Análise de Produtos")
    engine = get_engine()

    t1, t2, t3, t6, t7 = st.tabs([
        "🏆 Mais Vendidos",
        "📈 Crescimento & Queda",
        "📦 Cobertura em peça (ML + Shopee)",
        "🧾 Fechamento de Estoque",
        "🚚 Penalização de frete",
    ])
    with t1:
        _tab_mais_vendidos(engine)
    with t2:
        _tab_crescimento(engine)
    with t3:
        _tab_cobertura_peca(engine)
    with t6:
        _tab_fechamento_estoque(engine)
    with t7:
        _tab_penalizacao_frete(engine)


if __name__ == "__main__":
    main()
