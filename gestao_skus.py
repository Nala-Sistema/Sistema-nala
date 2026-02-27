import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def main():
    st.header("📦 Gestão de SKUs Nala")
    engine = get_engine()
    
    user_role = st.session_state.get('perfil', 'Admin') 
    is_admin = user_role in ['Admin', 'Controladoria']

    t1, t2, t3 = st.tabs(["📋 Lista e Busca", "⚙️ Gerenciar SKU", "📥 Importação"])
    
    # --- TAB 1: LISTA E BUSCA INTELIGENTE ---
    with t1:
        c1, c2 = st.columns([2, 1])
        # A busca inteligente: filtra por SKU ou Nome
        busca = c1.text_input("🔍 Buscar por SKU ou Nome do Produto", placeholder="Digite parte do nome ou código...")
        
        if st.button("🔄 Atualizar Base"):
            st.rerun()

        try:
            query = """
                SELECT p.sku, p.nome, p.categoria, p.status, 
                       c.cod_fornecedor, c.preco_compra, c.embalagem, c.mdo, c.custo_ads, c.custo_final, 
                       p.preco_a_ser_considerado 
                FROM dim_produtos p
                LEFT JOIN dim_produtos_custos c ON p.sku = c.sku
                ORDER BY p.sku ASC
            """ if is_admin else "SELECT sku, nome, categoria, status, preco_a_ser_considerado FROM dim_produtos ORDER BY sku ASC"
            
            df = pd.read_sql(query, engine)
            
            if not df.empty:
                # Lógica de Filtro Inteligente (Autocomplete/Partial Match)
                if busca:
                    mask = df['sku'].str.contains(busca, case=False, na=False) | \
                           df['nome'].str.contains(busca, case=False, na=False)
                    df_filtrado = df[mask]
                else:
                    df_filtrado = df

                st.write(f"Mostrando {len(df_filtrado)} de {len(df)} itens.")
                
                # Exibição com formatação BR
                colunas_moeda = ['preco_compra', 'embalagem', 'mdo', 'custo_ads', 'custo_final', 'preco_a_ser_considerado']
                st.dataframe(
                    df_filtrado.style.format({col: "{:,.2f}" for col in colunas_moeda if col in df.columns}, decimal=',', thousands='.'),
                    use_container_width=True, hide_index=True
                )
            else:
                st.info("Base de dados vazia.")
        except Exception as e:
            st.error(f"Erro ao carregar: {e}")

    # --- TAB 2: GERENCIAR (NOVO / EDITAR / EXCLUIR) ---
    with t2:
        if not is_admin:
            st.warning("Acesso restrito.")
        else:
            # Sub-aba de Cadastro
            with st.expander("➕ Cadastrar / Editar SKU", expanded=True):
                with st.form("f_cadastro"):
                    c1, c2 = st.columns(2)
                    v_sku = c1.text_input("SKU (ID único)")
                    v_nome = c2.text_input("Nome")
                    v_preco = st.text_input("Preço Considerado (R$)", value="0,00")
                    if st.form_submit_button("Salvar Produto"):
                        # ... (Lógica de inserção que já temos)
                        st.success("Salvo!")

            # ZONA DE PERIGO: EXCLUSÃO (Para apagar o de teste, por exemplo)
            st.markdown("---")
            with st.expander("🗑️ ZONA DE PERIGO - Excluir SKU"):
                st.warning("A exclusão é permanente e removerá também os custos associados.")
                # Autocomplete para encontrar o SKU a ser deletado
                sku_para_deletar = st.selectbox("Selecione o SKU para remover:", 
                                               options=[None] + df['sku'].tolist() if not df.empty else [None],
                                               index=0)
                
                confirmar = st.checkbox(f"Confirmo que desejo excluir o SKU {sku_para_deletar}")
                
                if st.button("❌ EXCLUIR DEFINITIVAMENTE", type="primary"):
                    if sku_para_deletar and confirmar:
                        try:
                            with engine.connect() as conn:
                                # Deleta primeiro da tabela de custos (por causa da Foreign Key)
                                conn.execute(text("DELETE FROM dim_produtos_custos WHERE sku = :s"), {"s": sku_para_deletar})
                                # Depois deleta da tabela principal
                                conn.execute(text("DELETE FROM dim_produtos WHERE sku = :s"), {"s": sku_para_deletar})
                                conn.commit()
                            st.error(f"SKU {sku_para_deletar} removido com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir: {e}")
                    else:
                        st.info("Selecione o SKU e marque a confirmação para proceder.")

    # --- TAB 3: IMPORTAÇÃO ---
    with t3:
        st.subheader("Sincronização em Massa")
        # ... (Mantemos a lógica de importação que já funciona)