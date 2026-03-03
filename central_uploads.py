import streamlit as st
import pandas as pd
import openpyxl
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def formatar_valor(valor):
    """R$ 1.234,56"""
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "R$ 0,00"

def formatar_percentual(valor):
    """18,50%"""
    try:
        return f"{float(valor):.2f}%".replace('.', ',')
    except:
        return "0,00%"

def formatar_quantidade(valor):
    """1.245"""
    try:
        return f"{int(valor):,}".replace(',', '.')
    except:
        return "0"

def converter_data_ml(data_str):
    if pd.isna(data_str):
        return None
    try:
        if isinstance(data_str, datetime):
            return data_str.strftime("%d/%m/%Y")
        meses = {
            'janeiro':'01','fevereiro':'02','março':'03','abril':'04',
            'maio':'05','junho':'06','julho':'07','agosto':'08',
            'setembro':'09','outubro':'10','novembro':'11','dezembro':'12'
        }
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

def processar_mercado_livre(arquivo, loja_sel, imposto, engine):
    # Detectar header
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    header_idx = 5
    for idx in range(20):
        if any('sku' in str(c).lower() for c in df_raw.iloc[idx]):
            header_idx = idx
            break
    
    df = pd.read_excel(arquivo, header=header_idx)
    
    # Renomear
    rename = {}
    for col in df.columns:
        c = str(col).lower().strip()
        if 'n.' in c and 'venda' in c: rename[col] = 'pedido'
        elif 'data' in c and 'venda' in c: rename[col] = 'data'
        elif c == 'sku': rename[col] = 'sku'
        elif 'unidades' in c: rename[col] = 'qtd'
        elif 'receita' in c and 'produtos' in c: rename[col] = 'receita'
        elif 'tarifa' in c and 'venda' in c: rename[col] = 'tarifa'
        elif '#' in c and 'anúncio' in c: rename[col] = 'mlb'
    
    df = df.rename(columns=rename)
    
    if not all(c in df.columns for c in ['sku','receita','tarifa']):
        return None, "❌ Colunas não encontradas"
    
    # Buscar custos
    query = """
        SELECT s.sku, COALESCE(c.custo_final, 0) as custo
        FROM dim_skus s
        LEFT JOIN dim_produtos_custos c ON s.sku = c.sku
        WHERE s.ativo = TRUE
    """
    df_custos = pd.read_sql(query, engine)
    custos_dict = df_custos.set_index('sku')['custo'].to_dict()
    
    # Detectar período
    datas = []
    for d in df['data'].dropna():
        try:
            dc = converter_data_ml(d)
            if dc:
                datas.append(datetime.strptime(dc, "%d/%m/%Y"))
        except:
            continue
    
    periodo_inicio = min(datas).strftime("%d/%m/%Y") if datas else None
    periodo_fim = max(datas).strftime("%d/%m/%Y") if datas else None
    
    # Processar
    vendas = []
    skus_sem_custo = set()
    
    for idx, row in df.iterrows():
        try:
            if pd.isna(row['sku']):
                continue
            
            sku = str(row['sku']).strip()
            receita = limpar_numero(row['receita'])
            tarifa = abs(limpar_numero(row['tarifa']))
            
            try:
                qtd_val = row['qtd'] if 'qtd' in df.columns else 1
                qtd = int(qtd_val) if not pd.isna(qtd_val) else 1
            except:
                qtd = 1
            
            custo_unit = custos_dict.get(sku, 0)
            if custo_unit == 0:
                skus_sem_custo.add(sku)
            
            custo_total = custo_unit * qtd
            imposto_val = receita * (imposto / 100)
            margem = receita - tarifa - imposto_val - custo_total
            margem_pct = (margem / receita * 100) if receita > 0 else 0
            
            vendas.append({
                'pedido': str(row.get('pedido', '')),
                'data': converter_data_ml(row.get('data')),
                'sku': sku,
                'mlb': str(row.get('mlb', '')) if 'mlb' in df.columns else '',
                'qtd': qtd,
                'receita': round(receita, 2),
                'tarifa': round(tarifa, 2),
                'imposto': round(imposto_val, 2),
                'custo': round(custo_total, 2),
                'margem': round(margem, 2),
                'margem_%': round(margem_pct, 2),
                'tem_custo': custo_unit > 0
            })
        except:
            continue
    
    if not vendas:
        return None, "❌ Nenhuma venda processada"
    
    df_result = pd.DataFrame(vendas)
    
    info = {
        'total_linhas': len(df_result),
        'periodo_inicio': periodo_inicio,
        'periodo_fim': periodo_fim,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name
    }
    
    return df_result, info

def tab_vendas_consolidadas(engine):
    st.subheader("📊 Vendas Consolidadas")
    
    col1, col2, col3, col4 = st.columns(4)
    
    periodo = col1.selectbox("Período:", 
        ["Hoje", "Ontem", "Últimos 7 dias", "Últimos 15 dias", "Últimos 30 dias", "Personalizado"])
    
    hoje = datetime.now().date()
    if periodo == "Hoje":
        data_ini = data_fim = hoje
    elif periodo == "Ontem":
        data_ini = data_fim = hoje - timedelta(days=1)
    elif periodo == "Últimos 7 dias":
        data_ini = hoje - timedelta(days=7)
        data_fim = hoje
    elif periodo == "Últimos 15 dias":
        data_ini = hoje - timedelta(days=15)
        data_fim = hoje
    elif periodo == "Últimos 30 dias":
        data_ini = hoje - timedelta(days=30)
        data_fim = hoje
    else:
        data_ini = col2.date_input("De:", hoje - timedelta(days=30))
        data_fim = col2.date_input("Até:", hoje)
    
    df_lojas = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)
    mktp_filtro = col3.selectbox("Marketplace:", ["Todos"] + df_lojas['marketplace'].tolist())
    
    if mktp_filtro != "Todos":
        df_lojas_filtradas = pd.read_sql(f"SELECT loja FROM dim_lojas WHERE marketplace = '{mktp_filtro}'", engine)
        loja_filtro = col4.selectbox("Loja:", ["Todas"] + df_lojas_filtradas['loja'].tolist())
    else:
        loja_filtro = "Todas"
    
    query = f"""
        SELECT * FROM fact_vendas_snapshot
        WHERE data_venda BETWEEN '{data_ini}' AND '{data_fim}'
    """
    if mktp_filtro != "Todos":
        query += f" AND marketplace_origem = '{mktp_filtro}'"
    if loja_filtro != "Todas":
        query += f" AND loja_origem = '{loja_filtro}'"
    
    try:
        df_vendas = pd.read_sql(query, engine)
    except:
        df_vendas = pd.DataFrame()
    
    if df_vendas.empty:
        st.warning("⚠️ Nenhuma venda encontrada")
        return
    
    df_com_custo = df_vendas[df_vendas['custo_total'] > 0]
    df_sem_custo = df_vendas[df_vendas['custo_total'] == 0]
    
    dias_diff = (data_fim - data_ini).days
    data_ini_ant = data_ini - timedelta(days=dias_diff + 1)
    data_fim_ant = data_fim - timedelta(days=dias_diff + 1)
    
    query_ant = query.replace(f"'{data_ini}' AND '{data_fim}'", f"'{data_ini_ant}' AND '{data_fim_ant}'")
    try:
        df_ant = pd.read_sql(query_ant, engine)
        df_ant_com_custo = df_ant[df_ant['custo_total'] > 0]
    except:
        df_ant_com_custo = pd.DataFrame()
    
    st.markdown("### 📈 Indicadores do Período")
    
    c1, c2, c3, c4 = st.columns(4)
    
    receita_atual = df_com_custo['valor_venda_efetivo'].sum()
    receita_ant = df_ant_com_custo['valor_venda_efetivo'].sum() if not df_ant_com_custo.empty else 0
    var_receita = ((receita_atual - receita_ant) / receita_ant * 100) if receita_ant > 0 else 0
    
    c1.metric("Receita Total", formatar_valor(receita_atual), 
              f"{formatar_percentual(var_receita)} vs período anterior")
    
    pedidos_atual = len(df_com_custo)
    pedidos_ant = len(df_ant_com_custo) if not df_ant_com_custo.empty else 0
    var_pedidos = ((pedidos_atual - pedidos_ant) / pedidos_ant * 100) if pedidos_ant > 0 else 0
    
    c2.metric("Pedidos", formatar_quantidade(pedidos_atual), formatar_percentual(var_pedidos))
    
    margem_atual = df_com_custo['margem_percentual'].mean()
    margem_ant = df_ant_com_custo['margem_percentual'].mean() if not df_ant_com_custo.empty else 0
    var_margem = margem_atual - margem_ant
    
    c3.metric("Margem Média", formatar_percentual(margem_atual), formatar_percentual(var_margem))
    
    c4.metric("⚠️ Pendentes", formatar_quantidade(len(df_sem_custo)),
              formatar_valor(df_sem_custo['valor_venda_efetivo'].sum()) + " não contabilizados",
              delta_color="off")
    
    st.write("---")
    st.subheader("📋 Detalhamento de Vendas")
    
    df_display = df_vendas.copy()
    df_display['data_venda'] = pd.to_datetime(df_display['data_venda']).dt.strftime('%d/%m/%Y')
    
    st.dataframe(df_display[['data_venda', 'numero_pedido', 'sku', 'quantidade', 
                             'valor_venda_efetivo', 'custo_total', 'margem_percentual']].head(50),
                use_container_width=True, height=400)
    
    if st.button("📊 Download Excel"):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_vendas.to_excel(writer, index=False, sheet_name='Vendas')
        st.download_button("⬇️ Baixar", buffer.getvalue(),
                          f"vendas_{data_ini}_{data_fim}.xlsx",
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def tab_uploads_historico(engine):
    st.subheader("📥 Histórico")
    
    try:
        df_log = pd.read_sql("SELECT * FROM log_uploads ORDER BY data_upload DESC LIMIT 50", engine)
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True)
        else:
            st.info("Nenhuma importação registrada")
    except:
        st.info("Histórico não disponível. Execute os scripts SQL.")

def main():
    st.header("💰 Central de Vendas")
    engine = get_engine()
    
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("⚠️ Configure lojas em Config")
        return
    
    if df_lojas.empty:
        st.warning("⚠️ Cadastre lojas")
        return
    
    tab1, tab2, tab3 = st.tabs(["🚀 Processar Upload", "📊 Vendas Consolidadas", "📥 Histórico"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        
        mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
        lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
        loja = col2.selectbox("Loja:", lojas)
        imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
        
        st.info(f"⚙️ {loja} | Imposto: {formatar_percentual(imposto)}")
        
        arquivo = st.file_uploader("📁 Upload XLSX", type=['xlsx'])
        
        if arquivo and st.button("🔍 Analisar"):
            with st.spinner("Processando..."):
                if 'MERCADO' in mktp.upper() and 'LIVRE' in mktp.upper():
                    df_proc, info = processar_mercado_livre(arquivo, loja, imposto, engine)
                    
                    if df_proc is not None:
                        st.success(f"✅ {info['total_linhas']} vendas processadas!")
                        
                        st.markdown("### 📊 CONFIRMAÇÃO DE IMPORTAÇÃO")
                        
                        col_a, col_b = st.columns(2)
                        col_a.info(f"**Arquivo:** {info['arquivo_nome']}")
                        col_a.info(f"**Período:** {info['periodo_inicio']} a {info['periodo_fim']}")
                        col_b.info(f"**Marketplace:** {mktp}")
                        col_b.info(f"**Loja:** {loja}")
                        
                        if info['skus_sem_custo'] > 0:
                            st.warning(f"⚠️ {info['skus_sem_custo']} SKUs sem custo")
                        
                        st.dataframe(df_proc.head(20), use_container_width=True)
                        
                        if st.button("✅ Confirmar (em desenvolvimento)", disabled=True):
                            st.info("Gravação será implementada em breve")
                    else:
                        st.error(info)
                else:
                    st.error(f"{mktp} não implementado")
    
    with tab2:
        tab_vendas_consolidadas(engine)
    
    with tab3:
        tab_uploads_historico(engine)

if __name__ == "__main__":
    main()
