import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import get_engine, recalcular_curva_abc

def _buscar_tags_com_vendas(engine, dias=30, marketplace=None, curva=None):
    """Busca tags da dim_tags_anuncio cruzando com volume de vendas real."""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
    query = f"""
        SELECT 
            t.marketplace, t.codigo_anuncio, t.sku, t.tag_curva, t.tag_status,
            COUNT(v.id) as total_vendas,
            SUM(v.valor_venda_efetivo) as receita_total
        FROM dim_tags_anuncio t
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem 
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= '{data_corte}'
        WHERE 1=1
    """
    if marketplace and marketplace != "Todos": query += f" AND t.marketplace = '{marketplace}'"
    if curva and curva != "Todas": query += f" AND t.tag_curva = '{curva}'"
    
    query += " GROUP BY t.marketplace, t.codigo_anuncio, t.sku, t.tag_curva, t.tag_status"
    return pd.read_sql(query, engine)

def _atualizar_status(engine, mkt, cod, status, obs):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE dim_tags_anuncio SET tag_status = :s, observacoes = :o, data_atualizacao = NOW()
                WHERE marketplace = :m AND codigo_anuncio = :c
            """), {"s": status, "o": obs, "m": mkt, "c": cod})
            conn.commit()
        return True
    except: return False

def tab_lista_tags(engine):
    st.subheader("📋 Catálogo e Edição de Tags")
    
    # --- FILTROS E EDIDOR NO TOPO ---
    col1, col2, col3 = st.columns([1, 1, 1.5])
    dias = col1.selectbox("Vendas (Período):", [30, 60, 90], key="dias_lista")
    mkt = col2.selectbox("Marketplace:", ["Todos", "MERCADO LIVRE", "SHOPEE", "AMAZON"], key="mkt_lista")
    
    with col3.expander("✏️ Alterar Status Manual", expanded=False):
        df_edit = _buscar_tags_com_vendas(engine, dias=dias, marketplace=mkt)
        if not df_edit.empty:
            sel = st.selectbox("Anúncio:", df_edit.apply(lambda r: f"{r['marketplace']} | {r['codigo_anuncio']}", axis=1))
            st_novo = st.selectbox("Novo Status:", ["Novo", "Escalando", "Estável", "Descontinuado"])
            if st.button("💾 Salvar Alteração"):
                m, c = sel.split(" | ")
                if _atualizar_status(engine, m, c, st_novo, ""):
                    st.success("Atualizado!"); st.rerun()

    st.divider()
    df = _buscar_tags_com_vendas(engine, dias=dias, marketplace=mkt)
    
    if not df.empty:
        df_show = df.copy()
        df_show['receita_total'] = df_show['receita_total'].apply(formatar_valor)
        df_show['total_vendas'] = df_show['total_vendas'].apply(formatar_quantidade)
        df_show.columns = ['Marketplace', 'Cód. Anúncio', 'SKU', 'Curva', 'Status', 'Qtd Vendas', 'Receita']
        st.dataframe(df_show, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado encontrado para o período.")

def tab_atribuir_status(engine):
    st.subheader("🏷️ Anúncios sem Status Manual")
    # Busca anúncios que têm curva (ABC calculado) mas o status está vazio
    query = """
        SELECT marketplace, codigo_anuncio, sku, tag_curva 
        FROM dim_tags_anuncio 
        WHERE tag_status IS NULL OR tag_status = '' OR tag_status = 'None'
    """
    df_sem = pd.read_sql(query, engine)
    
    if df_sem.empty:
        st.success("✅ Todos os anúncios ativos já possuem status manual."); return

    st.warning(f"Existem {len(df_sem)} anúncios aguardando classificação manual.")
    st.dataframe(df_sem, use_container_width=True, hide_index=True)

    with st.form("atrib_manual"):
        anuncios = df_sem.apply(lambda r: f"{r['marketplace']} | {r['codigo_anuncio']}", axis=1).tolist()
        selecionados = st.multiselect("Selecionar Anúncios:", anuncios)
        status = st.selectbox("Status:", ["Novo", "Escalando", "Estável", "Descontinuado"])
        if st.form_submit_button("Gravar em Lote"):
            for s in selecionados:
                m, c = s.split(" | ")
                _atualizar_status(engine, m, c, status, "Atribuição em lote")
            st.rerun()

def tab_visao_geral(engine):
    st.subheader("📊 Performance por Classificação")
    dias = st.selectbox("Análise de:", [30, 60, 90], key="dias_visao")
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
    
    # Query robusta para Visão Geral
    q = f"""
        SELECT 
            COALESCE(t.tag_curva, 'Sem Curva') as curva,
            COALESCE(t.tag_status, 'Sem Status') as status,
            SUM(v.valor_venda_efetivo) as receita,
            COUNT(v.id) as vendas
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_tags_anuncio t ON v.marketplace_origem = t.marketplace AND v.codigo_anuncio = t.codigo_anuncio
        WHERE v.data_venda >= '{data_corte}'
        GROUP BY t.tag_curva, t.tag_status
    """
    df = pd.read_sql(q, engine)
    
    if not df.empty:
        c1, c2 = st.columns(2)
        c1.markdown("**Resumo por Curva**")
        c1.dataframe(df.groupby('curva')[['vendas', 'receita']].sum().sort_values('receita', ascending=False))
        c2.markdown("**Resumo por Status**")
        c2.dataframe(df.groupby('status')[['vendas', 'receita']].sum().sort_values('receita', ascending=False))
    else:
        st.warning("Sem dados de vendas no período selecionado.")

def main():
    st.title("🏷️ Gestão de Tags Nala")
    engine = get_engine()
    t1, t2, t3 = st.tabs(["📋 Lista Tagueada", "🏷️ Atribuir Status", "📊 Visão Geral"])
    with t1: tab_lista_tags(engine)
    with t2: tab_atribuir_status(engine)
    with t3: tab_visao_geral(engine)

from sqlalchemy import text
if __name__ == "__main__": main()
