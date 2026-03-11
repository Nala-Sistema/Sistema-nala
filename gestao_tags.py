"""
GESTÃO DE TAGS DE ANÚNCIO - Sistema Nala
Versão: 2.1 (Ajustada para a tabela dim_tags_anuncio)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import get_engine, recalcular_curva_abc


# ============================================================
# FUNÇÕES AUXILIARES (CONECTADAS À dim_tags_anuncio)
# ============================================================

def _buscar_tags(engine, marketplace=None, curva=None, status=None):
    """Busca anúncios tagueados da dim_tags_anuncio."""
    query = "SELECT * FROM dim_tags_anuncio WHERE 1=1"
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
    """Busca anúncios órfãos (sem status atribuído)."""
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
        LEFT JOIN dim_tags_anuncio c 
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
          AND v.codigo_anuncio IS NOT NULL
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
    """Faz o UPSERT do status na dim_tags_anuncio."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dim_tags_anuncio (marketplace, codigo_anuncio, tag_status, observacoes, data_atualizacao)
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
    """Resumo de performance cruzando vendas e tags."""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    query_curva = """
        SELECT
            COALESCE(c.tag_curva, 'Sem curva') as curva,
            COUNT(DISTINCT v.codigo_anuncio) as total_anuncios,
            COUNT(*) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total,
            AVG(v.margem_percentual) as margem_media
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_tags_anuncio c
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
        GROUP BY c.tag_curva
        ORDER BY receita_total DESC
    """

    query_status = """
        SELECT
            COALESCE(c.tag_status, 'Sem status') as status,
            COUNT(DISTINCT v.codigo_anuncio) as total_anuncios,
            COUNT(*) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total,
            AVG(v.margem_percentual) as margem_media
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_tags_anuncio c
            ON v.marketplace_origem = c.marketplace
            AND v.codigo_anuncio = c.codigo_anuncio
        WHERE v.data_venda >= %s
        GROUP BY c.tag_status
        ORDER BY receita_total DESC
    """

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query_curva, (data_corte,))
        df_curva = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
        cursor.execute(query_status, (data_corte,))
        df_status = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
        cursor.close()
        conn.close()
        return df_curva, df_status
    except Exception as e:
        st.error(f"Erro no resumo agregado: {e}")
        return pd.DataFrame(), pd.DataFrame()


# ============================================================
# INTERFACE (TABS)
# ============================================================

def tab_lista_tags(engine):
    st.subheader("📋 Catálogo de Anúncios Tagueados")
    col1, col2, col3, col4 = st.columns(4)

    mktp_filtro = col1.selectbox("Marketplace:", ["Todos", "Mercado Livre", "Shopee", "Amazon", "Shein", "Magalu"])
    curva_filtro = col2.selectbox("Curva:", ["Todas", "A", "B", "C"])
    status_filtro = col3.selectbox("Status:", ["Todos", "Novo", "Escalando", "Estável", "Descontinuado", "Sem status"])
    periodo_abc = col4.selectbox("Período ABC:", ["30 dias", "60 dias", "90 dias"])

    if st.button("🔄 Recalcular Curva ABC", use_container_width=True):
        with st.spinner("Processando Pareto..."):
            dias = int(periodo_abc.split()[0])
            resultado = recalcular_curva_abc(engine, dias=dias)
            st.success(f"✅ ABC Atualizado: {resultado.get('total_anuncios', 0)} anúncios processados.")
            st.rerun()

    mktp_param = mktp_filtro if mktp_filtro != "Todos" else None
    curva_param = curva_filtro if curva_filtro != "Todas" else None
    status_param = status_filtro if status_filtro not in ["Todos", "Sem status"] else None

    df_tags = _buscar_tags(engine, mktp_param, curva_param, status_param)
    if status_filtro == "Sem status" and not df_tags.empty:
        df_tags = df_tags[df_tags['tag_status'].isna() | (df_tags['tag_status'] == '')]

    if not df_tags.empty:
        df_exibir = df_tags[['marketplace', 'codigo_anuncio', 'sku', 'tag_curva', 'tag_status', 'data_atualizacao']].copy()
        df_exibir.columns = ['Marketplace', 'Cód. Anúncio', 'SKU', 'Curva', 'Status', 'Última Atualização']
        st.dataframe(df_exibir, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum anúncio encontrado.")

def tab_atribuir_status(engine):
    st.subheader("🏷️ Atribuição Manual de Status")
    df_sem = _buscar_anuncios_sem_status(engine)
    
    if df_sem.empty:
        st.success("✅ Tudo tagueado!")
        return

    st.dataframe(df_sem.style.format({'receita_total': 'R$ {:,.2f}', 'margem_media': '{:.2%}'}), use_container_width=True)

    with st.form("atribuir_lote"):
        col1, col2 = st.columns(2)
        anuncios = df_sem.apply(lambda r: f"{r['marketplace']} | {r['codigo_anuncio']}", axis=1).tolist()
        selecionados = col1.multiselect("Selecionar Anúncios:", anuncios)
        novo_status = col2.selectbox("Status:", ["Novo", "Escalando", "Estável", "Descontinuado"])
        obs = st.text_input("Observações")
        
        if st.form_submit_button("Gravar Alterações"):
            for sel in selecionados:
                m, c = sel.split(" | ")
                _atualizar_status(engine, m, c, novo_status, obs)
            st.success("Atualizado!")
            st.rerun()

def tab_visao_geral(engine):
    st.subheader("📊 Performance por Classificação")
    df_curva, df_status = _buscar_resumo_agregado(engine)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Resumo por Curva**")
        st.dataframe(df_curva, hide_index=True)
    with col2:
        st.markdown("**Resumo por Status**")
        st.dataframe(df_status, hide_index=True)

def main():
    st.title("🏷️ Gestão de Tags Nala")
    engine = get_engine()
    t1, t2, t3 = st.tabs(["📋 Lista", "🏷️ Atribuir Status", "📊 Visão Geral"])
    with t1: tab_lista_tags(engine)
    with t2: tab_atribuir_status(engine)
    with t3: tab_visao_geral(engine)

if __name__ == "__main__":
    main()
