import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def formatar_valor(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "R$ 0,00"

def formatar_percentual(valor):
    try:
        return f"{float(valor):.2f}%".replace('.', ',')
    except:
        return "0,00%"

def formatar_quantidade(valor):
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
            'janeiro':'01','fevereiro':'02','marco':'03','abril':'04',
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
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    header_idx = 5
    for idx in range(20):
        if any('sku' in str(c).lower() for c in df_raw.iloc[idx]):
            header_idx = idx
            break
    
    df = pd.read_excel(arquivo, header=header_idx)
    
    rename = {}
    for col in df.columns:
        c = str(col).lower().strip()
        if 'n.' in c and 'venda' in c:
            rename[col] = 'pedido'
        elif 'data' in c and 'venda' in c:
            rename[col] = 'data'
        elif c == 'sku':
            rename[col] = 'sku'
        elif 'estado' in c:
            rename[col] = 'status'
        elif 'unidades' in c:
            rename[col] = 'qtd'
        elif 'receita' in c and 'produtos' in c:
            rename[col] = 'receita'
        elif 'tarifa' in c and 'venda' in c:
            rename[col] = 'tarifa'
    
    df = df.rename(columns=rename)
    
    if not all(c in df.columns for c in ['sku','receita','tarifa']):
        return None, "Colunas nao encontradas"
    
    query = "SELECT s.sku, COALESCE(c.custo_final, 0) as custo FROM dim_skus s LEFT JOIN dim_produtos_custos c ON s.sku = c.sku WHERE s.ativo = TRUE"
    df_custos = pd.read_sql(query, engine)
    custos_dict = df_custos.set_index('sku')['custo'].to_dict()
    
    vendas = []
    skus_sem_custo = set()
    linhas_descartadas = 0
    
    for idx, row in df.iterrows():
        try:
            if pd.isna(row['sku']):
                continue
            
            if 'status' in df.columns:
                status = str(row['status']).lower()
                if 'cancelad' in status or 'devolv' in status or 'reembolso' in status:
                    linhas_descartadas += 1
                    continue
            
            sku = str(row['sku']).strip()
            receita = limpar_numero(row['receita'])
            
            if receita == 0:
                linhas_descartadas += 1
                continue
            
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
            
            data_venda = converter_data_ml(row.get('data'))
            
            vendas.append({
                'pedido': str(row.get('pedido', '')),
                'data': data_venda,
                'sku': sku,
                'qtd': qtd,
                'receita': receita,
                'tarifa': tarifa,
                'imposto': imposto_val,
                'custo': custo_total,
                'margem': margem,
                'margem_pct': margem_pct,
                'tem_custo': custo_unit > 0,
                '_data_obj': datetime.strptime(data_venda, "%d/%m/%Y") if data_venda else None
            })
        except:
            continue
    
    if not vendas:
        return None, f"Nenhuma venda valida ({linhas_descartadas} descartadas)"
    
    df_result = pd.DataFrame(vendas)
    
    datas_validas = [v['_data_obj'] for v in vendas if v['_data_obj']]
    periodo_inicio = min(datas_validas).strftime("%d/%m/%Y") if datas_validas else None
    periodo_fim = max(datas_validas).strftime("%d/%m/%Y") if datas_validas else None
    
    info = {
        'total_linhas': len(df_result),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': periodo_inicio,
        'periodo_fim': periodo_fim,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name
    }
    
    df_result = df_result.drop(columns=['_data_obj'])
    
    return df_result, info

def tab_vendas_consolidadas(engine):
    st.subheader("Vendas Consolidadas")
    
    col1, col2, col3, col4 = st.columns(4)
    
    periodo = col1.selectbox("Periodo:", ["Hoje", "Ontem", "Ultimos 7 dias", "Ultimos 15 dias", "Ultimos 30 dias", "Personalizado"])
    
    hoje = datetime.now().date()
    if periodo == "Hoje":
        data_ini = data_fim = hoje
    elif periodo == "Ontem":
        data_ini = data_fim = hoje - timedelta(days=1)
    elif "7" in periodo:
        data_ini = hoje - timedelta(days=7)
        data_fim = hoje
    elif "15" in periodo:
        data_ini = hoje - timedelta(days=15)
        data_fim = hoje
    elif "30" in periodo:
        data_ini = hoje - timedelta(days=30)
        data_fim = hoje
    else:
        data_ini = col2.date_input("De:", hoje - timedelta(days=30))
        data_fim = col2.date_input("Ate:", hoje)
    
    df_lojas = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)
    mktp_filtro = col3.selectbox("Marketplace:", ["Todos"] + df_lojas['marketplace'].tolist())
    
    if mktp_filtro != "Todos":
        df_lojas_filtradas = pd.read_sql(f"SELECT loja FROM dim_lojas WHERE marketplace = '{mktp_filtro}'", engine)
        loja_filtro = col4.selectbox("Loja:", ["Todas"] + df_lojas_filtradas['loja'].tolist())
    else:
        loja_filtro = "Todas"
    
    query = f"SELECT * FROM fact_vendas_snapshot WHERE data_venda BETWEEN '{data_ini}' AND '{data_fim}'"
    if mktp_filtro != "Todos":
        query += f" AND marketplace_origem = '{mktp_filtro}'"
    if loja_filtro != "Todas":
        query += f" AND loja_origem = '{loja_filtro}'"
    
    try:
        df_vendas = pd.read_sql(query, engine)
    except:
        df_vendas = pd.DataFrame()
    
    if df_vendas.empty:
        st.warning("Nenhuma venda encontrada")
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
    
    st.markdown("### Indicadores do Periodo")
    
    c1, c2, c3, c4 = st.columns(4)
    
    receita_atual = df_com_custo['valor_venda_efetivo'].sum()
    receita_ant = df_ant_com_custo['valor_venda_efetivo'].sum() if not df_ant_com_custo.empty else 0
    var_receita = ((receita_atual - receita_ant) / receita_ant * 100) if receita_ant > 0 else 0
    
    c1.metric("Receita Total", formatar_valor(receita_atual), f"{formatar_percentual(var_receita)} vs periodo anterior")
    
    pedidos_atual = len(df_com_custo)
    pedidos_ant = len(df_ant_com_custo) if not df_ant_com_custo.empty else 0
    var_pedidos = ((pedidos_atual - pedidos_ant) / pedidos_ant * 100) if pedidos_ant > 0 else 0
    
    c2.metric("Pedidos", formatar_quantidade(pedidos_atual), formatar_percentual(var_pedidos))
    
    margem_atual = df_com_custo['margem_percentual'].mean()
    margem_ant = df_ant_com_custo['margem_percentual'].mean() if not df_ant_com_custo.empty else 0
    var_margem = margem_atual - margem_ant
    
    c3.metric("Margem Media", formatar_percentual(margem_atual), formatar_percentual(var_margem))
    
    c4.metric("Pendentes", formatar_quantidade(len(df_sem_custo)), formatar_valor(df_sem_custo['valor_venda_efetivo'].sum()) + " nao contabilizados", delta_color="off")
    
    st.write("---")
    st.subheader("Detalhamento de Vendas")
    
    df_display = df_vendas.copy()
    df_display['data_venda'] = pd.to_datetime(df_display['data_venda']).dt.strftime('%d/%m/%Y')
    
    st.dataframe(df_display[['data_venda', 'numero_pedido', 'sku', 'quantidade', 'valor_venda_efetivo', 'custo_total', 'margem_percentual']].head(50), use_container_width=True, height=400)
    
    if st.button("Download Excel"):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_vendas.to_excel(writer, index=False, sheet_name='Vendas')
        st.download_button("Baixar", buffer.getvalue(), f"vendas_{data_ini}_{data_fim}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def tab_uploads_historico(engine):
    st.subheader("Historico")
    
    try:
        df_log = pd.read_sql("SELECT * FROM log_uploads ORDER BY data_upload DESC LIMIT 50", engine)
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True)
        else:
            st.info("Nenhuma importacao registrada")
    except:
        st.info("Historico nao disponivel")

def main():
    st.header("Central de Vendas")
    engine = get_engine()
    
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("Configure lojas em Config")
        return
    
    if df_lojas.empty:
        st.warning("Cadastre lojas")
        return
    
    tab1, tab2, tab3 = st.tabs(["Processar Upload", "Vendas Consolidadas", "Historico"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        
        mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
        lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
        loja = col2.selectbox("Loja:", lojas)
        imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
        
        st.info(f"{loja} | Imposto: {formatar_percentual(imposto)}")
        
        arquivo = st.file_uploader("Upload XLSX", type=['xlsx'])
        
        if arquivo and st.button("Analisar"):
            if 'MERCADO' in mktp.upper() and 'LIVRE' in mktp.upper():
                df_proc, info = processar_mercado_livre(arquivo, loja, imposto, engine)
                
                if df_proc is not None:
                    st.session_state['df_proc'] = df_proc
                    st.session_state['info'] = info
                    st.session_state['mktp'] = mktp
                    st.session_state['loja'] = loja
                    st.rerun()
        
        if 'df_proc' in st.session_state:
            df_proc = st.session_state['df_proc']
            info = st.session_state['info']
            mktp = st.session_state['mktp']
            loja = st.session_state['loja']
            
            st.success(f"{info['total_linhas']} vendas processadas!")
            
            col_a, col_b = st.columns(2)
            col_a.info(f"Periodo: {info['periodo_inicio']} a {info['periodo_fim']}")
            col_b.info(f"Loja: {loja}")
            
            if info.get('linhas_descartadas', 0) > 0:
                st.info(f"{info['linhas_descartadas']} linhas descartadas")
            
            df_preview = df_proc.copy()
            df_preview['receita'] = df_preview['receita'].apply(formatar_valor)
            df_preview['tarifa'] = df_preview['tarifa'].apply(formatar_valor)
            df_preview['custo'] = df_preview['custo'].apply(formatar_valor)
            df_preview['margem'] = df_preview['margem'].apply(formatar_valor)
            df_preview['margem_pct'] = df_preview['margem_pct'].apply(formatar_percentual)
            
            st.dataframe(df_preview.head(20), use_container_width=True)
            
            if st.button("GRAVAR NO BANCO", type="primary"):
                try:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    
                    registros = 0
                    erros = 0
                    
                    for _, row in df_proc.iterrows():
                        try:
                            data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
                            qtd = int(row['qtd'])
                            receita = float(row['receita'])
                            custo_total = float(row['custo'])
                            tarifa = float(row['tarifa'])
                            imposto = float(row['imposto'])
                            margem = float(row['margem'])
                            margem_pct = float(row['margem_pct'])
                            
                            preco_venda = receita / qtd if qtd > 0 else receita
                            custo_unit = custo_total / qtd if qtd > 0 else custo_total
                            valor_liquido = receita - tarifa - imposto
                            
                            sql = "INSERT INTO fact_vendas_snapshot (marketplace_origem, loja_origem, numero_pedido, data_venda, sku, codigo_anuncio, quantidade, preco_venda, desconto_parceiro, desconto_marketplace, valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido, margem_total, margem_percentual, data_processamento, arquivo_origem) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)"
                            # Validar SKU
                            if not row['sku'] or row['sku'].strip() == '':
                            continue
    
                            # Verificar se SKU existe
                            if row['sku'] not in custos_dict and row['sku'] not in st.session_state.get('skus_validados', set()):
                            continue
                            cursor.execute(sql, (mktp, loja, row['pedido'], data_venda, row['sku'], '', qtd, preco_venda, 0, 0, receita, custo_unit, custo_total, imposto, tarifa, 0, 0, 0, tarifa, valor_liquido, margem, margem_pct, info['arquivo_nome']))
                            
                            registros += 1
                        except Exception as e:
                            conn.rollback()
                            erros += 1
                            if erros == 1:
                                st.warning(f"Erro: {str(e)[:150]}")
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    st.success(f"{registros} vendas gravadas! ({erros} erros)")
                    del st.session_state['df_proc']
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"Erro: {e}")
    
    with tab2:
        tab_vendas_consolidadas(engine)
    
    with tab3:
        tab_uploads_historico(engine)

if __name__ == "__main__":
    main()

