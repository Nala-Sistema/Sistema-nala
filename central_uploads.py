import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

# Conexão Direta
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def main():
    st.header("💰 Central de Vendas")
    engine = get_engine()
    
    # Substituindo o session_state pela busca no banco Neon
    try:
        df_f = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("⚠️ Erro: Tabela de lojas não encontrada no banco. Salve em Config primeiro.")
        return

    if df_f.empty:
        st.warning("⚠️ Nenhuma loja cadastrada no banco. Vá na aba Config e clique em Salvar.")
        return
    
    t1, t2 = st.tabs(["🚀 Processar Relatório", "🕒 Histórico"])
    
    with t1:
        c1, c2, c3 = st.columns(3)
        
        # Lista de Marketplaces vinda do banco
        mktp_list = sorted(df_f['marketplace'].unique().tolist())
        mktp = c1.selectbox("Marketplace:", mktp_list)
        
        # Filtra lojas baseada no banco
        lojas = df_f[df_f['marketplace'] == mktp]['loja'].tolist()
        
        if lojas:
            loja_sel = c2.selectbox("Loja:", lojas)
            taxa = df_f[df_f['loja'] == loja_sel]['imposto'].values[0]
            dt = c3.date_input("Data do Relatório")
            
            st.info(f"⚙️ Calculando margens para **{loja_sel}** com **{taxa}%** de imposto.")
            
            up = st.file_uploader("Relatório de Faturamento", type=['xlsx', 'csv'])
            
            if up and st.button("🚀 Processar Agora"):
                try:
                    # 1. Carrega o Relatório de Vendas
                    df_vendas = pd.read_excel(up) if up.name.endswith('xlsx') else pd.read_csv(up)
                    
                    # 2. Busca SKUs e Custos (Operacional + Estratégico)
                    # Só Admin/Controladoria deveriam ver custos, mas para o cálculo do sistema precisamos puxar
                    query_custos = """
                        SELECT p.sku, p.nome, p.preco_a_ser_considerado, c.custo_final 
                        FROM dim_produtos p
                        LEFT JOIN dim_produtos_custos c ON p.sku = c.sku
                    """
                    df_custos = pd.read_sql(query_custos, engine)
                    
                    # 3. Cruzamento (ETL): Une Vendas com Custos usando a coluna 'SKU'
                    # Nota: O relatório deve ter uma coluna chamada 'SKU' exatamente assim.
                    df_final = pd.merge(df_vendas, df_custos, left_on='SKU', right_on='sku', how='left')
                    
                    # 4. Feedback
                    st.success(f"Sucesso! {len(df_final)} linhas cruzadas com a base de dados.")
                    
                    # Identifica SKUs que não foram encontrados (Custo Zero/Vazio)
                    sem_custo = df_final[df_final['custo_final'].isna()]['SKU'].unique()
                    if len(sem_custo) > 0:
                        st.warning(f"⚠️ Atenção: {len(sem_custo)} SKUs no relatório não possuem custo cadastrado.")
                    
                    # Exibe prévia do resultado
                    st.dataframe(df_final.head(), use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Erro no processamento: {e}. Verifique se a coluna de SKU no arquivo se chama 'SKU'.")
        else:
            st.warning("Cadastre uma loja para este marketplace em Config.")

if __name__ == "__main__": 
    main()