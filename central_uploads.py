import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def converter_data_ml(data_str):
    if pd.isna(data_str):
        return None
    try:
        if isinstance(data_str, datetime):
            return data_str.strftime("%d/%m/%Y")
        meses = {'janeiro':'01','fevereiro':'02','março':'03','abril':'04','maio':'05','junho':'06',
                 'julho':'07','agosto':'08','setembro':'09','outubro':'10','novembro':'11','dezembro':'12'}
        partes = str(data_str).lower().split()
        return f"{partes[0].zfill(2)}/{meses.get(partes[2],'01')}/{partes[4]}"
    except:
        return str(data_str)

def limpar_numero(valor):
    if pd.isna(valor):
        return 0.0
    try:
        return float(str(valor).replace(',','.').replace('R$','').strip())
    except:
        return 0.0

def processar_ml(arquivo, loja, imposto, engine):
    # Detectar header
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    header_idx = 5
    for idx in range(20):
        if any('sku' in str(c).lower() for c in df_raw.iloc[idx]):
            header_idx = idx
            break
    
    df = pd.read_excel(arquivo, header=header_idx)
    st.info(f"📋 {len(df)} linhas | Header: linha {header_idx+1}")
    
    # Renomear colunas
    rename = {}
    for col in df.columns:
        c = str(col).lower().strip()
        if 'n.' in c and 'venda' in c: 
            rename[col] = 'pedido'
        elif 'data' in c and 'venda' in c: 
            rename[col] = 'data'
        elif c == 'sku': 
            rename[col] = 'sku'
        elif 'unidades' in c: 
            rename[col] = 'qtd'
        elif 'receita' in c and 'produtos' in c: 
            rename[col] = 'receita'
        elif 'tarifa' in c and 'venda' in c: 
            rename[col] = 'tarifa'
    
    df = df.rename(columns=rename)
    
    if not all(c in df.columns for c in ['sku','receita','tarifa']):
        st.error("❌ Colunas essenciais não encontradas")
        return None
    
    # Buscar custos - COLUNA CORRETA: custo_final
    query = """
        SELECT s.sku, COALESCE(c.custo_final, 0) as custo
        FROM dim_skus s
        LEFT JOIN dim_produtos_custos c ON s.sku = c.sku
        WHERE s.ativo = TRUE
    """
    df_custos = pd.read_sql(query, engine)
    custos_dict = df_custos.set_index('sku')['custo'].to_dict()
    
    # Processar
    vendas = []
    for _, row in df.iterrows():
        if pd.isna(row.get('sku')):
            continue
        
        sku = str(row['sku']).strip()
        receita = limpar_numero(row.get('receita', 0))
        tarifa = abs(limpar_numero(row.get('tarifa', 0)))
        qtd = int(row.get('qtd', 1)) if pd.notna(row.get('qtd')) else 1
        
        custo = custos_dict.get(sku, 0) * qtd
        imposto_val = receita * (imposto / 100)
        margem = receita - tarifa - imposto_val - custo
        margem_pct = (margem / receita * 100) if receita > 0 else 0
        
        vendas.append({
            'pedido': str(row.get('pedido', '')),
            'data': converter_data_ml(row.get('data')),
            'sku': sku,
            'qtd': qtd,
            'receita': round(receita, 2),
            'tarifa': round(tarifa, 2),
            'imposto': round(imposto_val, 2),
            'custo': round(custo, 2),
            'margem': round(margem, 2),
            'margem_%': round(margem_pct, 2)
        })
    
    if not vendas:
        st.error("❌ Nenhuma venda processada")
        return None
    
    df_result = pd.DataFrame(vendas)
    st.success(f"✅ {len(df_result)} vendas processadas!")
    
    col1, col2 = st.columns(2)
    col1.metric("Vendas", len(df_result))
    col2.metric("Receita Total", f"R$ {df_result['receita'].sum():,.2f}")
    
    return df_result

def main():
    st.header("💰 Central de Vendas")
    engine = get_engine()
    
    df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    if df_lojas.empty:
        st.warning("Configure lojas em Config")
        return
    
    tab1, tab2 = st.tabs(["🚀 Processar", "📊 Histórico"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
        lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
        loja = col2.selectbox("Loja:", lojas)
        imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
        data = col3.date_input("Data", format="DD/MM/YYYY")
        
        st.info(f"⚙️ {loja} | Imposto: {imposto}%")
        
        arquivo = st.file_uploader("📁 Upload XLSX", type=['xlsx'])
        
        if arquivo and st.button("🚀 Processar"):
            if 'MERCADO' in mktp.upper() and 'LIVRE' in mktp.upper():
                df = processar_ml(arquivo, loja, imposto, engine)
                if df is not None:
                    st.write("---")
                    st.subheader("📊 Preview (20 primeiras linhas)")
                    st.dataframe(df.head(20), use_container_width=True)
            else:
                st.error(f"{mktp} não implementado")
    
    with tab2:
        st.info("Histórico em desenvolvimento")

if __name__ == "__main__":
    main()
