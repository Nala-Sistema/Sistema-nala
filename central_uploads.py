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
        if 'n.' in c and 'venda' in c: rename[col] = 'pedido'
        elif 'data' in c and 'venda' in c: rename[col] = 'data'
        elif c == 'sku': rename[col] = 'sku'
        elif 'estado' in c: rename[col] = 'status'
        elif 'unidades' in c: rename[col] = 'qtd'
        elif 'receita' in c and 'produtos' in c: rename[col] = 'receita'
        elif 'tarifa' in c and 'venda' in c: rename[col] = 'tarifa'
        elif '#' in c and 'anúncio' in c: rename[col] = 'mlb'
    
    df = df.rename(columns=rename)
    
    if not all(c in df.columns for c in ['sku','receita','tarifa']):
        return None, "❌ Colunas não encontradas"
    
    query = """
        SELECT s.sku, COALESCE(c.custo_final, 0) as custo
        FROM dim_skus s
        LEFT JOIN dim_produtos_custos c ON s.sku = c.sku
        WHERE s.ativo = TRUE
    """
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
                'mlb': str(row.get('mlb', '')) if 'mlb' in df.columns else '',
                'qtd': qtd,
                'receita': receita,
                'tarifa': tarifa,
                'imposto': imposto_val,
                'custo': custo_total,
                'margem': margem,
                'margem_%': margem_pct,
                'tem_custo': custo_unit > 0,
                '_data_obj': datetime.strptime(data_venda, "%d/%m/%Y") if data_venda else None
            })
        except Exception as e:
            continue
    
    if not vendas:
        return None, f"❌ Nenhuma venda válida ({linhas_descartadas} linhas descartadas)"
    
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
                        
                        if info.get('linhas_descartadas', 0) > 0:
                            st.info(f"ℹ️ {info['linhas_descartadas']} linhas descartadas (cancelamentos/devoluções/trocas)")
                        
                        if info['skus_sem_custo'] > 0:
                            st.warning(f"⚠️ {info['skus_sem_custo']} SKUs sem custo")
                        
                        df_preview = df_proc.copy()
                        df_preview['receita'] = df_preview['receita'].apply(formatar_valor)
                        df_preview['tarifa'] = df_preview['tarifa'].apply(formatar_valor)
                        df_preview['imposto'] = df_preview['imposto'].apply(formatar_valor)
                        df_preview['custo'] = df_preview['custo'].apply(formatar_valor)
                        df_preview['margem'] = df_preview['margem'].apply(formatar_valor)
                        df_preview['margem_%'] = df_preview['margem_%'].apply(formatar_percentual)
                        
                        st.dataframe(df_preview.head(20), use_container_width=True)
                        
                        if st.button("✅ Confirmar e Gravar no Banco", type="primary"):
                            try:
                                with st.spinner("Gravando vendas no banco..."):
                                    st.write("🔍 Iniciando gravação...")
                                    
                                    erros_gravacao = []
                                    registros_inseridos = 0
                                    registros_duplicados = 0
                                    
                                    try:
                                        with engine.connect() as test_conn:
                                            result = test_conn.execute(text("SELECT 1"))
                                            st.write("✅ Conexão com banco OK")
                                    except Exception as e_conn:
                                        st.error(f"❌ Erro de conexão: {e_conn}")
                                        raise
                                    
                                    with engine.begin() as conn:
                                        for idx_row, row in df_proc.iterrows():
                                            try:
                                                data_venda_obj = datetime.strptime(row['data'], "%d/%m/%Y")
                                                
                                                query_insert = text("""
                                                    INSERT INTO fact_vendas_snapshot (
                                                        marketplace_origem, loja_origem, numero_pedido,
                                                        data_venda, sku, codigo_anuncio, quantidade,
                                                        preco_venda, valor_venda_efetivo, custo_unitario,
                                                        custo_total, imposto, comissao, total_tarifas,
                                                        margem_total, margem_percentual, data_processamento
                                                    ) VALUES (
                                                        :mktp, :loja, :pedido, :data, :sku, :mlb, :qtd,
                                                        :preco, :receita, :custo_unit, :custo_total,
                                                        :imposto, :tarifa, :tarifa, :margem, :margem_pct,
                                                        NOW()
                                                    )
                                                """)
                                                
                                                params = {
                                                    'mktp': mktp,
                                                    'loja': loja,
                                                    'pedido': row['pedido'],
                                                    'data': data_venda_obj.date(),
                                                    'sku': row['sku'],
                                                    'mlb': row['mlb'] if row['mlb'] else None,
                                                    'qtd': int(row['qtd']),
                                                    'preco': float(row['receita']) / int(row['qtd']),
                                                    'receita': float(row['receita']),
                                                    'custo_unit': float(row['custo']) / int(row['qtd']) if int(row['qtd']) > 0 else 0,
                                                    'custo_total': float(row['custo']),
                                                    'imposto': float(row['imposto']),
                                                    'tarifa': float(row['tarifa']),
                                                    'margem': float(row['margem']),
                                                    'margem_pct': float(row['margem_%'])
                                                }
                                                
                                                result = conn.execute(query_insert, params)
                                                
                                                if result.rowcount > 0:
                                                    registros_inseridos += 1
                                                    if idx_row < 3:
                                                        st.write(f"✅ Linha {idx_row}: {row['pedido']} - {row['sku']}")
                                                        
                                            except Exception as e_row:
                                                erro_msg = str(e_row)
                                                if 'duplicate key' in erro_msg.lower() or 'unique constraint' in erro_msg.lower():
                                                    registros_duplicados += 1
                                                else:
                                                    erros_gravacao.append(f"Linha {idx_row} ({row['pedido']}-{row['sku']}): {erro_msg[:200]}")
                                                    if idx_row < 3:
                                                        st.warning(f"⚠️ Linha {idx_row}: {erro_msg[:100]}")
                                        
                                        try:
                                            query_log = text("""
                                                INSERT INTO log_uploads (
                                                    usuario, marketplace, loja, arquivo_nome,
                                                    periodo_inicio, periodo_fim, total_linhas,
                                                    linhas_importadas, skus_sem_custo
                                                ) VALUES (
                                                    'Admin', :mktp, :loja, :arq, :inicio, :fim,
                                                    :total, :importadas, :sem_custo
                                                )
                                            """)
                                            
                                            conn.execute(query_log, {
                                                'mktp': mktp,
                                                'loja': loja,
                                                'arq': info['arquivo_nome'],
                                                'inicio': datetime.strptime(info['periodo_inicio'], "%d/%m/%Y").date(),
                                                'fim': datetime.strptime(info['periodo_fim'], "%d/%m/%Y").date(),
                                                'total': info['total_linhas'],
                                                'importadas': registros_inseridos,
                                                'sem_custo': info['skus_sem_custo']
                                            })
                                        except Exception as e_log:
                                            st.warning(f"Log não gravado: {e_log}")
                                    
                                    st.success(f"✅ {registros_inseridos} vendas NOVAS gravadas!")
                                    
                                    if registros_duplicados > 0:
                                        st.info(f"ℹ️ {registros_duplicados} registros já existiam")
                                    
                                    if erros_gravacao:
                                        with st.expander(f"⚠️ {len(erros_gravacao)} erros"):
                                            for erro in erros_gravacao[:10]:
                                                st.error(erro)
                                    
                                    if registros_inseridos > 0:
                                        st.balloons()
                                    
                            except Exception as e:
                                st.error(f"❌ ERRO GERAL: {str(e)}")
                                st.code(str(e))
                                import traceback
                                st.code(traceback.format_exc())
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
