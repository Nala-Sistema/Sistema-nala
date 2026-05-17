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

  Tab 3 — 📦 Cobertura de Estoque
        Para cada SKU em dim_estoque: estoque_atual, vendas_30d,
        venda/dia, dias de cobertura, status visual.

  Tab 4 — ⬆️ Atualizar Estoque (Upseller)
        Upload do relatório semanal (.xlsx). Detecta colunas SKU/Estoque,
        valida contra dim_produtos, faz UPSERT em dim_estoque, registra
        em log_uploads.

Dependências internas:
    database_utils.get_engine       — engine cacheado (v3.6)
    database_utils.gravar_log_upload — log de uploads
    permissoes.filtrar_query_por_loja — RBAC nas queries
"""

import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta

from database_utils import get_engine, gravar_log_upload
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
            MAX(t.tag_curva)                                     AS curva
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
# TAB 3 — COBERTURA DE ESTOQUE
# ============================================================

def _tab_cobertura(engine):
    st.subheader("📦 Cobertura de Estoque")
    st.caption("Dias restantes = estoque atual ÷ (vendas dos últimos 30 dias ÷ 30).")

    # Última atualização do estoque
    try:
        df_meta = _query_to_df(engine,
            "SELECT MAX(data_atualizacao) AS ult, COUNT(*) AS itens, "
            "       SUM(quantidade) AS soma FROM dim_estoque")
        if not df_meta.empty and pd.notna(df_meta.iloc[0]['ult']):
            ult = df_meta.iloc[0]['ult']
            ult_str = ult.strftime('%d/%m/%Y %H:%M') if hasattr(ult, 'strftime') else str(ult)
            c1, c2, c3 = st.columns(3)
            c1.metric("Última atualização", ult_str)
            c2.metric("SKUs com estoque", _fmt_int(df_meta.iloc[0]['itens']))
            c3.metric("Unidades totais", _fmt_int(df_meta.iloc[0]['soma'] or 0))
        else:
            st.warning("⚠️ Nenhum estoque carregado. Use a tab **Atualizar Estoque** para subir o relatório Upseller.")
            return
    except Exception:
        st.error("Não foi possível ler dim_estoque.")
        return

    # Filtros (período é fixo em 30d para o cálculo de cobertura)
    _, lojas_opt = _opcoes_lojas_marketplaces()
    if not ve_todas_lojas():
        lojas_permitidas = set(get_lojas_usuario())
        lojas_opt = [l for l in lojas_opt if l in lojas_permitidas]

    col1, col2, col3 = st.columns(3)
    with col1:
        marketplaces_opt, _ = _opcoes_lojas_marketplaces()
        mkts = st.multiselect("🛒 Marketplaces (vendas)", marketplaces_opt,
                              default=[], key="cob_mkts")
    with col2:
        lojas = st.multiselect("🏪 Lojas (vendas)", lojas_opt, default=[],
                               key="cob_lojas")
    with col3:
        status_filter = st.multiselect(
            "🎯 Status",
            ["🔴 Crítico (<15d)", "🟡 Atenção (15-30d)", "🟢 OK (>30d)", "⚪ Sem giro"],
            default=[],
            key="cob_status",
        )

    hoje = date.today()
    ini = hoje - timedelta(days=30)

    where_parts, params = _montar_where_filtros(ini, hoje, lojas, mkts, engine, alias='f.')
    where_sql = " AND ".join(where_parts)

    query = f"""
        WITH v30 AS (
            SELECT f.sku, SUM(f.quantidade)::bigint AS vendas_30d
            FROM fact_vendas_snapshot f
            WHERE {where_sql}
            GROUP BY f.sku
        )
        SELECT
            e.sku,
            COALESCE(p.nome, '(sem cadastro)') AS nome,
            e.quantidade            AS estoque,
            COALESCE(v.vendas_30d, 0) AS vendas_30d,
            e.data_atualizacao
        FROM dim_estoque e
        LEFT JOIN v30 v        ON v.sku = e.sku
        LEFT JOIN dim_produtos p ON p.sku = e.sku
        ORDER BY e.sku
    """

    try:
        df = _query_to_df(engine, query, params)
    except Exception as exc:
        st.error(f"Erro ao calcular cobertura: {exc}")
        return

    if df.empty:
        st.info("Nenhum SKU no estoque.")
        return

    df['venda_dia']  = df['vendas_30d'].astype(float) / 30.0
    df['dias_cobertura'] = df.apply(
        lambda r: (float(r['estoque']) / r['venda_dia']) if r['venda_dia'] > 0 else None,
        axis=1,
    )

    def _status(d, vendas):
        if vendas == 0:
            return "⚪ Sem giro"
        if d is None:
            return "⚪ Sem giro"
        if d < 15:
            return "🔴 Crítico (<15d)"
        if d < 30:
            return "🟡 Atenção (15-30d)"
        return "🟢 OK (>30d)"

    df['status'] = df.apply(
        lambda r: _status(r['dias_cobertura'], r['vendas_30d']),
        axis=1,
    )

    if status_filter:
        df = df[df['status'].isin(status_filter)]

    # Métricas
    s_critico = (df['status'] == "🔴 Crítico (<15d)").sum()
    s_aten    = (df['status'] == "🟡 Atenção (15-30d)").sum()
    s_ok      = (df['status'] == "🟢 OK (>30d)").sum()
    s_sem     = (df['status'] == "⚪ Sem giro").sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 Crítico", _fmt_int(s_critico))
    c2.metric("🟡 Atenção", _fmt_int(s_aten))
    c3.metric("🟢 OK", _fmt_int(s_ok))
    c4.metric("⚪ Sem giro", _fmt_int(s_sem))

    st.markdown("---")

    # Tabela
    df_disp = df.copy()
    df_disp['Estoque']      = df_disp['estoque'].apply(_fmt_int)
    df_disp['Vendas 30d']   = df_disp['vendas_30d'].apply(_fmt_int)
    df_disp['Venda/dia']    = df_disp['venda_dia'].apply(lambda v: f"{v:.2f}".replace(".", ","))
    df_disp['Dias rest.']   = df_disp['dias_cobertura'].apply(
        lambda v: "∞" if v is None else f"{v:.0f}"
    )
    df_disp['Atualizado']   = df_disp['data_atualizacao'].apply(
        lambda v: v.strftime('%d/%m/%Y') if hasattr(v, 'strftime') else str(v)
    )
    cols = ['sku', 'nome', 'Estoque', 'Vendas 30d', 'Venda/dia',
            'Dias rest.', 'status', 'Atualizado']
    st.dataframe(
        df_disp[cols].rename(columns={'sku': 'SKU', 'nome': 'Produto',
                                       'status': 'Status'}),
        use_container_width=True, hide_index=True,
    )

    with st.expander("💾 Exportar Excel"):
        buf = io.BytesIO()
        df.to_excel(buf, index=False, sheet_name='Cobertura')
        st.download_button(
            "⬇️ Download",
            data=buf.getvalue(),
            file_name=f"cobertura_estoque_{hoje}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# TAB 4 — UPLOAD ESTOQUE (UPSELLER)
# ============================================================

def _detectar_coluna(df, candidatos):
    """Devolve o nome da coluna que casa (case-insensitive, contém) com algum candidato."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        c_low = cand.lower()
        if c_low in cols_lower:
            return cols_lower[c_low]
    for c in df.columns:
        c_low = c.lower()
        for cand in candidatos:
            if cand.lower() in c_low:
                return c
    return None


def _tab_upload_estoque(engine):
    st.subheader("⬆️ Atualizar Estoque (Upseller)")
    st.caption("Suba o relatório semanal (.xlsx) do Upseller. UPSERT em dim_estoque por SKU.")

    arquivo = st.file_uploader("Selecione o arquivo .xlsx", type=['xlsx', 'xls'],
                                key="upl_estoque")

    if arquivo is None:
        st.info("Aguardando arquivo...")
        _historico_uploads_estoque(engine)
        return

    # Lê tentando detectar cabeçalho
    try:
        df_raw = pd.read_excel(arquivo, sheet_name=0, dtype=str)
    except Exception as exc:
        st.error(f"Falha ao ler arquivo: {exc}")
        return

    # Se as colunas vierem como 'Unnamed: 0', tentamos avançar header
    if all(str(c).startswith("Unnamed") for c in df_raw.columns):
        for header_idx in range(1, 10):
            arquivo.seek(0)
            try:
                df_try = pd.read_excel(arquivo, sheet_name=0, header=header_idx, dtype=str)
                if not all(str(c).startswith("Unnamed") for c in df_try.columns):
                    df_raw = df_try
                    break
            except Exception:
                pass

    st.write(f"📋 **{len(df_raw)} linhas** lidas. Pré-visualização:")
    st.dataframe(df_raw.head(10), use_container_width=True)

    # Mapeamento de colunas (autodetect com override)
    col_sku_auto = _detectar_coluna(df_raw, ['sku', 'codigo', 'código', 'cod'])
    col_estoque_auto = _detectar_coluna(df_raw, ['estoque', 'saldo', 'qtd', 'quantidade',
                                                  'disponivel', 'disponível'])

    colunas = list(df_raw.columns)
    c1, c2 = st.columns(2)
    with c1:
        col_sku = st.selectbox(
            "Coluna do SKU",
            colunas,
            index=colunas.index(col_sku_auto) if col_sku_auto in colunas else 0,
            key="upl_col_sku",
        )
    with c2:
        col_estoque = st.selectbox(
            "Coluna do Estoque (quantidade)",
            colunas,
            index=colunas.index(col_estoque_auto) if col_estoque_auto in colunas else 0,
            key="upl_col_est",
        )

    # Normaliza
    df = pd.DataFrame({
        'sku': df_raw[col_sku].astype(str).str.strip(),
        'quantidade_raw': df_raw[col_estoque],
    })
    df = df[df['sku'].notna() & (df['sku'] != '') & (df['sku'].str.lower() != 'nan')]

    def _to_int(v):
        try:
            s = str(v).strip().replace('.', '').replace(',', '.')
            return int(float(s)) if s else 0
        except (TypeError, ValueError):
            return 0

    df['quantidade'] = df['quantidade_raw'].apply(_to_int)
    df = df[['sku', 'quantidade']].drop_duplicates(subset=['sku'], keep='last')

    # Valida contra dim_produtos
    try:
        df_skus_cadastrados = pd.read_sql("SELECT sku FROM dim_produtos", engine)
        cadastrados = set(df_skus_cadastrados['sku'].astype(str).str.strip())
    except Exception:
        cadastrados = set()

    df['cadastrado'] = df['sku'].isin(cadastrados)
    qtd_total = len(df)
    qtd_ok = int(df['cadastrado'].sum())
    qtd_naocad = qtd_total - qtd_ok

    c1, c2, c3 = st.columns(3)
    c1.metric("Linhas válidas", _fmt_int(qtd_total))
    c2.metric("SKUs cadastrados", _fmt_int(qtd_ok))
    c3.metric("Não cadastrados", _fmt_int(qtd_naocad),
              delta=f"{qtd_naocad}" if qtd_naocad else None,
              delta_color="inverse")

    if qtd_naocad > 0:
        with st.expander(f"⚠️ Ver {qtd_naocad} SKU(s) não cadastrados — serão gravados mesmo assim"):
            st.dataframe(df[~df['cadastrado']][['sku', 'quantidade']],
                          use_container_width=True, hide_index=True)

    st.markdown("---")

    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("💾 Gravar estoque", type="primary",
                     key="upl_btn_save", disabled=(qtd_total == 0)):
            _gravar_estoque(engine, df, arquivo.name)
            st.cache_data.clear()
            st.success(f"✅ {qtd_total} SKU(s) atualizado(s) em dim_estoque.")
            st.rerun()

    _historico_uploads_estoque(engine)


def _gravar_estoque(engine, df, arquivo_nome):
    """Faz UPSERT em dim_estoque + grava em log_uploads."""
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO dim_estoque (sku, quantidade, data_atualizacao, arquivo_origem)
            VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (sku) DO UPDATE
            SET quantidade = EXCLUDED.quantidade,
                data_atualizacao = NOW(),
                arquivo_origem = EXCLUDED.arquivo_origem
        """
        importados = 0
        erros = 0
        for _, r in df.iterrows():
            try:
                cursor.execute(sql, (
                    str(r['sku']).strip(),
                    int(r['quantidade']),
                    arquivo_nome,
                ))
                importados += 1
            except Exception:
                erros += 1
        conn.commit()
        cursor.close()
    except Exception as exc:
        st.error(f"Erro ao gravar: {exc}")
        importados = 0
        erros = len(df)
    finally:
        conn.close()

    gravar_log_upload(engine, {
        'marketplace': 'UPSELLER',
        'loja': 'ESTOQUE',
        'arquivo_nome': arquivo_nome,
        'periodo_inicio': None,
        'periodo_fim': None,
        'total_linhas': int(len(df)),
        'linhas_importadas': importados,
        'linhas_erro': erros,
    })


def _historico_uploads_estoque(engine):
    st.markdown("### 🗂️ Últimos uploads de estoque")
    try:
        df = _query_to_df(
            engine,
            """
            SELECT data_upload, arquivo_nome, total_linhas,
                   linhas_importadas, linhas_erro, status
            FROM log_uploads
            WHERE marketplace = 'UPSELLER'
            ORDER BY data_upload DESC
            LIMIT 10
            """,
        )
        if df.empty:
            st.caption("Nenhum upload de estoque registrado ainda.")
        else:
            df['data_upload'] = pd.to_datetime(df['data_upload']).dt.strftime('%d/%m/%Y %H:%M')
            st.dataframe(df.rename(columns={
                'data_upload': 'Quando',
                'arquivo_nome': 'Arquivo',
                'total_linhas': 'Linhas',
                'linhas_importadas': 'Importadas',
                'linhas_erro': 'Erros',
                'status': 'Status',
            }), use_container_width=True, hide_index=True)
    except Exception:
        st.caption("Histórico indisponível (log_uploads ainda sem registros de estoque).")


# ============================================================
# ENTRYPOINT
# ============================================================

def main():
    st.header("📈 Análise de Produtos")
    engine = get_engine()

    t1, t2, t3, t4 = st.tabs([
        "🏆 Mais Vendidos",
        "📈 Crescimento & Queda",
        "📦 Cobertura de Estoque",
        "⬆️ Atualizar Estoque (Upseller)",
    ])
    with t1:
        _tab_mais_vendidos(engine)
    with t2:
        _tab_crescimento(engine)
    with t3:
        _tab_cobertura(engine)
    with t4:
        _tab_upload_estoque(engine)


if __name__ == "__main__":
    main()
