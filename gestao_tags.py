"""
GESTÃO DE TAGS DE ANÚNCIO - Sistema Nala
Classificação estratégica de anúncios por marketplace.

CONCEITO:
- Curva ABC: CALCULADA automaticamente via Pareto (receita acumulada)
  A = top 80% da receita, B = próximos 15%, C = últimos 5%
- Status: ATRIBUÍDO manualmente pelo usuário
  Novo / Escalando / Estável / Descontinuado

TABS:
  1. Lista de Anúncios Tagueados (visualização + edição de status)
  2. Atribuir Status (anúncios sem classificação manual)
  3. Visão Geral + Resumo Agregado (receita/margem por curva e status)

TABELA: dim_config_marketplace
DEPENDÊNCIA: fact_vendas_snapshot (codigo_anuncio + marketplace_origem)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import get_engine, recalcular_curva_abc


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _buscar_tags(engine, marketplace=None, curva=None, status=None):
    """
    Busca anúncios tagueados da dim_config_marketplace.

    Args:
        engine: SQLAlchemy engine
        marketplace: filtro por marketplace (None = todos)
        curva: filtro por curva 'A', 'B', 'C' (None = todos)
        status: filtro por status (None = todos)

    Retorna:
        DataFrame com os registros
    """
    query = "SELECT * FROM dim_config_marketplace WHERE 1=1"
    params = []

    if marketplace:
        query += " AND marketplace = %s"
        params.append(marketplace)
    if curva:
        query += " AND tag_curva = %s"
        params.append(curva)
    if status:
        query += " AND tag_status = %s"
        params.append(status)

    query += " ORDER BY tag_curva ASC, marketplace, codigo_anuncio"

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao buscar tags: {e}")
        return pd.DataFrame()


def _buscar_anuncios_sem_status(engine, dias=30):
    """
    Busca anúncios que existem em fact_vendas_snapshot mas
    NÃO têm tag_status atribuído em dim_config_marketplace.
    Inclui receita agregada para referência.

    Retorna:
        DataFrame com anúncios órfãos
    """
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    query = """
        SELECT
            v.marketplace_origem as marketplace,
            v.codigo_anuncio,
            COALESCE(c.tag_curva, '-') as tag_curva,
            COUNT(*) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total,
            AVG(v.margem_percentual) as margem_media
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_config_marketplace c
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
          AND v.codigo_anuncio IS NOT NULL
          AND TRIM(v.codigo_anuncio) != ''
          AND (c.tag_status IS NULL OR c.tag_status = '')
        GROUP BY v.marketplace_origem, v.codigo_anuncio, c.tag_curva
        ORDER BY receita_total DESC
    """

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (data_corte,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao buscar anúncios sem status: {e}")
        return pd.DataFrame()


def _atualizar_status(engine, marketplace, codigo_anuncio, tag_status, observacoes=''):
    """
    Atualiza o status de um anúncio na dim_config_marketplace.
    Se não existir, cria o registro.
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dim_config_marketplace (marketplace, codigo_anuncio, tag_status, observacoes, data_atualizacao)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (marketplace, codigo_anuncio)
            DO UPDATE SET tag_status = EXCLUDED.tag_status,
                          observacoes = EXCLUDED.observacoes,
                          data_atualizacao = NOW()
        """, (marketplace, codigo_anuncio, tag_status, observacoes))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao atualizar status: {e}")
        return False


def _buscar_resumo_agregado(engine, dias=30):
    """
    Busca resumo de receita e margem por curva e por status.
    Cruza dim_config_marketplace com fact_vendas_snapshot.
    """
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    # Resumo por Curva
    query_curva = """
        SELECT
            COALESCE(c.tag_curva, 'Sem curva') as curva,
            COUNT(DISTINCT v.codigo_anuncio) as total_anuncios,
            COUNT(*) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total,
            AVG(v.margem_percentual) as margem_media
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_config_marketplace c
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
          AND v.codigo_anuncio IS NOT NULL
          AND TRIM(v.codigo_anuncio) != ''
        GROUP BY c.tag_curva
        ORDER BY receita_total DESC
    """

    # Resumo por Status
    query_status = """
        SELECT
            COALESCE(c.tag_status, 'Sem status') as status,
            COUNT(DISTINCT v.codigo_anuncio) as total_anuncios,
            COUNT(*) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total,
            AVG(v.margem_percentual) as margem_media
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_config_marketplace c
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
          AND v.codigo_anuncio IS NOT NULL
          AND TRIM(v.codigo_anuncio) != ''
        GROUP BY c.tag_status
        ORDER BY receita_total DESC
    """

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        cursor.execute(query_curva, (data_corte,))
        cols_curva = [desc[0] for desc in cursor.description]
        rows_curva = cursor.fetchall()
        df_curva = pd.DataFrame(rows_curva, columns=cols_curva)

        cursor.execute(query_status, (data_corte,))
        cols_status = [desc[0] for desc in cursor.description]
        rows_status = cursor.fetchall()
        df_status = pd.DataFrame(rows_status, columns=cols_status)

        cursor.close()
        conn.close()

        return df_curva, df_status

    except Exception as e:
        st.error(f"Erro ao buscar resumo agregado: {e}")
        return pd.DataFrame(), pd.DataFrame()


# ============================================================
# TAB 1: LISTA DE ANÚNCIOS TAGUEADOS
# ============================================================

def tab_lista_tags(engine):
    """Lista de anúncios com suas tags (Curva + Status)."""

    st.subheader("📋 Anúncios Classificados")

    # Filtros
    col1, col2, col3, col4 = st.columns(4)

    # Marketplace
    try:
        df_mktps = pd.read_sql(
            "SELECT DISTINCT marketplace FROM dim_config_marketplace ORDER BY marketplace",
            engine
        )
        mktps = df_mktps['marketplace'].tolist() if not df_mktps.empty else []
    except:
        mktps = []

    mktp_filtro = col1.selectbox("Marketplace:", ["Todos"] + mktps, key="tag_mktp")
    curva_filtro = col2.selectbox("Curva:", ["Todas", "A", "B", "C"], key="tag_curva")
    status_filtro = col3.selectbox(
        "Status:", ["Todos", "Novo", "Escalando", "Estável", "Descontinuado", "Sem status"],
        key="tag_status"
    )

    # Período para recálculo
    periodo_abc = col4.selectbox("Período ABC:", ["30 dias", "60 dias", "90 dias"], key="tag_periodo")
    dias_abc = int(periodo_abc.split()[0])

    # Botão recalcular
    if st.button("🔄 Recalcular Curva ABC", type="secondary"):
        with st.spinner(f"Recalculando com base nos últimos {dias_abc} dias..."):
            resultado = recalcular_curva_abc(engine, dias=dias_abc)
            if resultado['total_anuncios'] > 0:
                st.success(
                    f"✅ Curva recalculada: {resultado['a']} A, "
                    f"{resultado['b']} B, {resultado['c']} C "
                    f"({resultado['total_anuncios']} anúncios)"
                )
                st.rerun()
            else:
                st.warning("⚠️ Nenhum anúncio encontrado no período.")

    # Buscar dados
    mktp_param = mktp_filtro if mktp_filtro != "Todos" else None
    curva_param = curva_filtro if curva_filtro != "Todas" else None
    status_param = None
    if status_filtro == "Sem status":
        status_param = None  # Tratado especialmente abaixo
    elif status_filtro != "Todos":
        status_param = status_filtro

    df_tags = _buscar_tags(engine, marketplace=mktp_param, curva=curva_param, status=status_param)

    # Filtro especial "Sem status"
    if status_filtro == "Sem status" and not df_tags.empty:
        df_tags = df_tags[df_tags['tag_status'].isna() | (df_tags['tag_status'] == '')]

    if df_tags.empty:
        st.info("ℹ️ Nenhum anúncio encontrado com os filtros selecionados.")
        st.caption(
            "Dica: A Curva ABC é calculada automaticamente a cada upload de vendas. "
            "Clique em 'Recalcular' para atualizar manualmente."
        )
        return

    # Indicadores rápidos
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", formatar_quantidade(len(df_tags)))
    c2.metric("Curva A", formatar_quantidade(len(df_tags[df_tags['tag_curva'] == 'A'])))
    c3.metric("Curva B", formatar_quantidade(len(df_tags[df_tags['tag_curva'] == 'B'])))
    c4.metric("Curva C", formatar_quantidade(len(df_tags[df_tags['tag_curva'] == 'C'])))

    # Tabela
    df_exibir = df_tags[[
        'marketplace', 'codigo_anuncio', 'sku', 'tag_curva', 'tag_status',
        'data_atualizacao', 'observacoes'
    ]].copy()

    # Formatar data
    df_exibir['data_atualizacao'] = pd.to_datetime(
        df_exibir['data_atualizacao'], errors='coerce'
    ).dt.strftime('%d/%m/%Y %H:%M').fillna('-')

    df_exibir = df_exibir.fillna('-')

    df_exibir = df_exibir.rename(columns={
        'marketplace': 'Marketplace',
        'codigo_anuncio': 'Código Anúncio',
        'sku': 'SKU',
        'tag_curva': 'Curva',
        'tag_status': 'Status',
        'data_atualizacao': 'Atualizado em',
        'observacoes': 'Observações',
    })

    st.dataframe(df_exibir, use_container_width=True, height=500, hide_index=True)

    # Edição individual
    st.divider()
    with st.expander("✏️ Editar Status de um anúncio", expanded=False):
        col_e1, col_e2 = st.columns(2)

        # Selecionar anúncio
        anuncios_lista = df_tags.apply(
            lambda r: f"{r['marketplace']} | {r['codigo_anuncio']}", axis=1
        ).tolist()

        if anuncios_lista:
            selecionado = col_e1.selectbox("Anúncio:", anuncios_lista, key="edit_anuncio")

            # Extrair marketplace e codigo_anuncio
            partes = selecionado.split(' | ')
            mktp_edit = partes[0].strip()
            cod_edit = partes[1].strip()

            novo_status = col_e2.selectbox(
                "Novo Status:",
                ["Novo", "Escalando", "Estável", "Descontinuado"],
                key="edit_status"
            )

            obs_edit = st.text_input("Observações:", key="edit_obs")

            if st.button("💾 Salvar Status", key="btn_salvar_status"):
                if _atualizar_status(engine, mktp_edit, cod_edit, novo_status, obs_edit):
                    st.success(f"✅ Status de '{cod_edit}' atualizado para '{novo_status}'")
                    st.rerun()


# ============================================================
# TAB 2: ATRIBUIR STATUS
# ============================================================

def tab_atribuir_status(engine):
    """Anúncios que têm vendas mas ainda não têm Status atribuído."""

    st.subheader("🏷️ Atribuir Status a Anúncios")

    st.markdown(
        "Anúncios que aparecem nas vendas mas ainda **não** têm Status "
        "(Novo / Escalando / Estável / Descontinuado) definido."
    )

    # Período
    periodo_filtro = st.selectbox(
        "Período de vendas:", ["Últimos 30 dias", "Últimos 60 dias", "Últimos 90 dias"],
        key="atrib_periodo"
    )
    dias = int(periodo_filtro.split()[1])

    # Buscar anúncios sem status
    df_sem = _buscar_anuncios_sem_status(engine, dias=dias)

    if df_sem.empty:
        st.success("✅ Todos os anúncios já têm Status atribuído!")
        return

    st.info(f"📊 {len(df_sem)} anúncio(s) sem Status nos últimos {dias} dias.")

    # Exibir tabela
    df_exibir = df_sem.copy()
    df_exibir['receita_total'] = df_exibir['receita_total'].apply(formatar_valor)
    df_exibir['margem_media'] = df_exibir['margem_media'].apply(formatar_percentual)
    df_exibir['total_vendas'] = df_exibir['total_vendas'].apply(formatar_quantidade)

    df_exibir = df_exibir.rename(columns={
        'marketplace': 'Marketplace',
        'codigo_anuncio': 'Código Anúncio',
        'tag_curva': 'Curva',
        'total_vendas': 'Vendas',
        'receita_total': 'Receita',
        'margem_media': 'Margem Média',
    })

    st.dataframe(df_exibir, use_container_width=True, height=400, hide_index=True)

    # Atribuição em lote
    st.divider()
    st.markdown("### Atribuir Status")

    col1, col2 = st.columns(2)

    # Selecionar anúncio
    opcoes = df_sem.apply(
        lambda r: f"{r['marketplace']} | {r['codigo_anuncio']} (Curva {r['tag_curva']})",
        axis=1
    ).tolist()

    selecionados = col1.multiselect("Selecionar anúncio(s):", opcoes, key="atrib_multi")

    status_atrib = col2.selectbox(
        "Status a atribuir:",
        ["Novo", "Escalando", "Estável", "Descontinuado"],
        key="atrib_status"
    )

    obs_atrib = st.text_input("Observações (opcional):", key="atrib_obs")

    if st.button("💾 Atribuir Status", type="primary", key="btn_atribuir"):
        if not selecionados:
            st.warning("⚠️ Selecione pelo menos um anúncio.")
            return

        sucesso = 0
        for sel in selecionados:
            partes = sel.split(' | ')
            mktp = partes[0].strip()
            cod = partes[1].split(' (')[0].strip()

            if _atualizar_status(engine, mktp, cod, status_atrib, obs_atrib):
                sucesso += 1

        if sucesso > 0:
            st.success(f"✅ Status '{status_atrib}' atribuído a {sucesso} anúncio(s)!")
            st.rerun()


# ============================================================
# TAB 3: VISÃO GERAL + RESUMO AGREGADO
# ============================================================

def tab_visao_geral(engine):
    """Resumo do portfólio: distribuição por Curva e Status + receita/margem."""

    st.subheader("📊 Visão Geral do Catálogo")

    # Período
    periodo_filtro = st.selectbox(
        "Período de análise:", ["Últimos 30 dias", "Últimos 60 dias", "Últimos 90 dias"],
        key="visao_periodo"
    )
    dias = int(periodo_filtro.split()[1])

    # Buscar dados
    df_curva, df_status = _buscar_resumo_agregado(engine, dias=dias)

    if df_curva.empty and df_status.empty:
        st.warning("⚠️ Nenhum dado encontrado no período.")
        return

    # ---- RESUMO POR CURVA ----
    st.markdown("### 📈 Por Curva ABC")

    if not df_curva.empty:
        # Indicadores por curva
        cols = st.columns(len(df_curva))
        for i, (_, row) in enumerate(df_curva.iterrows()):
            curva_label = row['curva']
            with cols[i]:
                st.markdown(f"**Curva {curva_label}**")
                st.metric("Anúncios", formatar_quantidade(int(row['total_anuncios'])))
                st.metric("Vendas", formatar_quantidade(int(row['total_vendas'])))
                st.metric("Receita", formatar_valor(float(row['receita_total'])))
                margem = float(row['margem_media']) if row['margem_media'] else 0
                st.metric("Margem Média", formatar_percentual(margem))

        # Tabela formatada
        df_curva_fmt = df_curva.copy()
        df_curva_fmt['receita_total'] = df_curva_fmt['receita_total'].apply(formatar_valor)
        df_curva_fmt['margem_media'] = df_curva_fmt['margem_media'].apply(
            lambda x: formatar_percentual(float(x)) if x else formatar_percentual(0)
        )
        df_curva_fmt['total_vendas'] = df_curva_fmt['total_vendas'].apply(formatar_quantidade)
        df_curva_fmt['total_anuncios'] = df_curva_fmt['total_anuncios'].apply(formatar_quantidade)

        df_curva_fmt = df_curva_fmt.rename(columns={
            'curva': 'Curva',
            'total_anuncios': 'Anúncios',
            'total_vendas': 'Vendas',
            'receita_total': 'Receita',
            'margem_media': 'Margem Média',
        })

        st.dataframe(df_curva_fmt, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado de curva disponível.")

    st.divider()

    # ---- RESUMO POR STATUS ----
    st.markdown("### 🏷️ Por Status")

    if not df_status.empty:
        # Indicadores por status
        cols = st.columns(min(len(df_status), 5))
        for i, (_, row) in enumerate(df_status.iterrows()):
            status_label = row['status']
            col_idx = i % len(cols)
            with cols[col_idx]:
                st.markdown(f"**{status_label}**")
                st.metric("Anúncios", formatar_quantidade(int(row['total_anuncios'])))
                st.metric("Receita", formatar_valor(float(row['receita_total'])))
                margem = float(row['margem_media']) if row['margem_media'] else 0
                st.metric("Margem Média", formatar_percentual(margem))

        # Tabela formatada
        df_status_fmt = df_status.copy()
        df_status_fmt['receita_total'] = df_status_fmt['receita_total'].apply(formatar_valor)
        df_status_fmt['margem_media'] = df_status_fmt['margem_media'].apply(
            lambda x: formatar_percentual(float(x)) if x else formatar_percentual(0)
        )
        df_status_fmt['total_vendas'] = df_status_fmt['total_vendas'].apply(formatar_quantidade)
        df_status_fmt['total_anuncios'] = df_status_fmt['total_anuncios'].apply(formatar_quantidade)

        df_status_fmt = df_status_fmt.rename(columns={
            'status': 'Status',
            'total_anuncios': 'Anúncios',
            'total_vendas': 'Vendas',
            'receita_total': 'Receita',
            'margem_media': 'Margem Média',
        })

        st.dataframe(df_status_fmt, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado de status disponível.")

    # Contagem geral de tags
    st.divider()
    st.markdown("### 📊 Cobertura de Classificação")
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM dim_config_marketplace")
        total_config = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_config_marketplace WHERE tag_status IS NOT NULL AND tag_status != ''")
        com_status = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_config_marketplace WHERE tag_curva IS NOT NULL")
        com_curva = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total de Anúncios na Config", formatar_quantidade(total_config))
        c2.metric("Com Curva ABC", formatar_quantidade(com_curva))
        c3.metric("Com Status", formatar_quantidade(com_status))

        if total_config > 0:
            pct_curva = com_curva / total_config * 100
            pct_status = com_status / total_config * 100
            st.progress(pct_status / 100, text=f"Cobertura de Status: {formatar_percentual(pct_status)}")
    except Exception:
        pass


# ============================================================
# MAIN
# ============================================================

def main():
    """Função principal do módulo"""

    st.title("🏷️ Gestão de Tags de Anúncio")

    engine = get_engine()

    tab1, tab2, tab3 = st.tabs([
        "📋 Lista Tagueada",
        "🏷️ Atribuir Status",
        "📊 Visão Geral"
    ])

    with tab1:
        tab_lista_tags(engine)

    with tab2:
        tab_atribuir_status(engine)

    with tab3:
        tab_visao_geral(engine)


if __name__ == "__main__":
    main()
