import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from formatadores import formatar_valor, formatar_quantidade
from database_utils import get_engine, recalcular_curva_abc

# ============================================================
# FUNÇÕES DE DADOS (COM JOIN DIM_PRODUTOS)
# ============================================================

def _buscar_tags_completo(engine, dias=30, sem_status=False):
    """Busca tags cruzando com vendas e nomes de produtos."""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
    
    status_filter = "AND (t.tag_status IS NULL OR t.tag_status = '' OR t.tag_status = 'None')" if sem_status else ""
    
    query = f"""
        SELECT 
            t.marketplace as "Marketplace",
            t.codigo_anuncio as "Cód. Anúncio",
            t.sku as "SKU",
            p.nome as "Produto",
            t.tag_curva as "Curva",
            t.tag_status as "Status",
            COUNT(v.id) as "Vendas",
            SUM(v.valor_venda_efetivo) as "Receita"
        FROM dim_tags_anuncio t
        LEFT JOIN dim_produtos p ON t.sku = p.sku
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem 
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= '{data_corte}'
        WHERE 1=1 {status_filter}
        GROUP BY t.marketplace, t.codigo_anuncio, t.sku, p.nome, t.tag_curva, t.tag_status
        ORDER BY "Receita" DESC NULLS LAST
    """
    return pd.read_sql(query, engine)

def _salvar_edicoes_lote(engine, df_editado):
    """Salva as alterações feitas no st.data_editor de volta ao banco."""
    try:
        with engine.connect() as conn:
            for _, row in df_editado.iterrows():
                conn.execute(text("""
                    UPDATE dim_tags_anuncio 
                    SET tag_status = :s, data_atualizacao = NOW()
                    WHERE marketplace = :m AND codigo_anuncio = :c
                """), {"s": str(row["Status"]), "m": row["Marketplace"], "c": row["Cód. Anúncio"]})
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ============================================================
# INTERFACE
# ============================================================

def tab_lista_tags(engine):
    st.subheader("📋 Catálogo Geral de Anúncios")
    
    col1, col2 = st.columns([1, 2])
    dias = col1.selectbox("Vendas (Últimos):", [30, 60, 90], format_func=lambda x: f"{x} dias")
    busca = col2.text_input("🔍 Busca Inteligente (SKU ou Nome do Produto):", placeholder="Ex: L-0321 ou Jogo de escovas...")

    df = _buscar_tags_completo(engine, dias=dias)
    
    if busca:
        df = df[df['SKU'].str.contains(busca, case=False, na=False) | 
                df['Produto'].str.contains(busca, case=False, na=False)]

    if not df.empty:
        # Configuração do editor
        df_editado = st.data_editor(
            df,
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Status Manual",
                    options=["Novo", "Escalando", "Estável", "Descontinuado"],
                    required=True,
                ),
                "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
                "Vendas": st.column_config.NumberColumn(format="%d"),
            },
            disabled=["Marketplace", "Cód. Anúncio", "SKU", "Produto", "Curva", "Vendas", "Receita"],
            hide_index=True,
            use_container_width=True,
            key="editor_lista"
        )
        
        if st.button("💾 Gravar Alterações na Linha", type="primary", use_container_width=True):
            if _salvar_edicoes_lote(engine, df_editado):
                st.success("✅ Tags atualizadas com sucesso!"); st.rerun()
    else:
        st.info("Nenhum anúncio encontrado.")

def tab_atribuir_status(engine):
    st.subheader("🏷️ Anúncios Pendentes de Classificação")
    st.caption("Filtre e defina o status dos anúncios que ainda não foram tagueados manualmente.")
    
    busca = st.text_input("🔍 Filtrar Pendentes (SKU ou Nome):", key="busca_pend")
    df_sem = _buscar_tags_completo(engine, dias=90, sem_status=True)
    
    if busca:
        df_sem = df_sem[df_sem['SKU'].str.contains(busca, case=False, na=False) | 
                        df_sem['Produto'].str.contains(busca, case=False, na=False)]

    if df_sem.empty:
        st.success("✅ Todos os anúncios ativos já possuem status manual."); return

    df_editado = st.data_editor(
        df_sem,
        column_config={
            "Status": st.column_config.SelectboxColumn(
                options=["Novo", "Escalando", "Estável", "Descontinuado"],
            ),
            "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
        },
        disabled=["Marketplace", "Cód. Anúncio", "SKU", "Produto", "Curva", "Vendas", "Receita"],
        hide_index=True,
        use_container_width=True,
        key="editor_atribuir"
    )

    if st.button("💾 Atribuir Status em Lote / Linha", type="primary", use_container_width=True):
        if _salvar_edicoes_lote(engine, df_editado):
            st.success("✅ Classificação salva!"); st.rerun()

def tab_visao_geral(engine):
    st.subheader("📊 Performance Agregada")
    dias = st.selectbox("Análise dos últimos:", [30, 60, 90], format_func=lambda x: f"{x} dias", key="dias_v")
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
    
    q = f"""
        SELECT 
            COALESCE(t.tag_curva, 'Sem Curva') as "Curva",
            COALESCE(t.tag_status, 'Sem Status') as "Status",
            SUM(v.valor_venda_efetivo) as "Receita",
            COUNT(v.id) as "Vendas"
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_tags_anuncio t ON v.marketplace_origem = t.marketplace AND v.codigo_anuncio = t.codigo_anuncio
        WHERE v.data_venda >= '{data_corte}'
        GROUP BY t.tag_curva, t.tag_status
    """
    df = pd.read_sql(q, engine)
    
    if not df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**💰 Receita por Curva**")
            res_curva = df.groupby("Curva")[["Vendas", "Receita"]].sum().sort_values("Receita", ascending=False)
            st.dataframe(res_curva.style.format({"Receita": "R$ {:,.2f}"}), use_container_width=True)
        with c2:
            st.markdown("**🏷️ Receita por Status**")
            res_status = df.groupby("Status")[["Vendas", "Receita"]].sum().sort_values("Receita", ascending=False)
            st.dataframe(res_status.style.format({"Receita": "R$ {:,.2f}"}), use_container_width=True)
    else:
        st.warning("Sem dados de vendas para este período.")

def main():
    st.title("🏷️ Gestão de Tags Nala")
    engine = get_engine()
    t1, t2, t3 = st.tabs(["📋 Lista Tagueada", "🏷️ Atribuir Status", "📊 Visão Geral"])
    with t1: tab_lista_tags(engine)
    with t2: tab_atribuir_status(engine)
    with t3: tab_visao_geral(engine)

if __name__ == "__main__": main()
