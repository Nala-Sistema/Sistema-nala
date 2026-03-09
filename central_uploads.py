"""
CENTRAL DE UPLOADS - Sistema Nala
Interface principal para upload e processamento de vendas
VERSÃO FINAL: Todos bugs corrigidos
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

# Imports dos módulos criados
from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import get_engine, gravar_log_upload
from processar_ml import processar_arquivo_ml, gravar_vendas_ml


def tab_processar_upload(engine):
    """Tab 1: Upload e processamento de arquivos"""
    
    # 1. BUSCAR LOJAS DO BANCO
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("⚠️ Erro ao carregar lojas. Configure lojas em Config.")
        return
    
    if df_lojas.empty:
        st.warning("⚠️ Cadastre lojas no módulo Config primeiro.")
        return
    
    # 2. SELEÇÃO DE MARKETPLACE E LOJA
    col1, col2, col3 = st.columns(3)
    
    mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
    lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
    loja = col2.selectbox("Loja:", lojas)
    imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
    
    st.info(f"📍 {loja} | Imposto: {formatar_percentual(imposto)}")
    
    # 3. UPLOAD DE ARQUIVO
    arquivo = st.file_uploader("📂 Upload do arquivo de vendas (XLSX)", type=['xlsx'])
    
    # 4. BOTÃO ANALISAR
    if arquivo and st.button("🔍 ANALISAR ARQUIVO", type="primary"):
        with st.spinner("Processando arquivo..."):
            
            # Verificar qual marketplace
            if 'MERCADO' in mktp.upper() and 'LIVRE' in mktp.upper():
                df_proc, info = processar_arquivo_ml(arquivo, loja, imposto, engine)
            else:
                st.error(f"⚠️ Processador para {mktp} ainda não implementado.")
                return
            
            # Salvar no session_state
            if df_proc is not None:
                st.session_state['df_proc'] = df_proc
                st.session_state['info'] = info
                st.session_state['mktp'] = mktp
                st.session_state['loja'] = loja
                st.session_state['arquivo_nome'] = arquivo.name
                st.rerun()
            else:
                st.error(f"❌ {info}")
    
    # 5. PREVIEW (se já processou)
    if 'df_proc' in st.session_state:
        df_proc = st.session_state['df_proc']
        info = st.session_state['info']
        mktp = st.session_state['mktp']
        loja = st.session_state['loja']
        arquivo_nome = st.session_state['arquivo_nome']
        
        # Mensagem de sucesso
        st.success(f"✅ {info['total_linhas']} vendas processadas com sucesso!")
        
        # Informações do arquivo
        col_a, col_b, col_c = st.columns(3)
        col_a.info(f"📅 Período: {info['periodo_inicio']} a {info['periodo_fim']}")
        col_b.info(f"🏪 Loja: {loja}")
        col_c.info(f"📦 Arquivo: {arquivo_nome}")
        
        # Alertas
        if info.get('linhas_descartadas', 0) > 0:
            st.warning(f"⚠️ {info['linhas_descartadas']} linhas descartadas (canceladas, devolvidas ou receita = 0)")
        
        if info.get('skus_sem_custo', 0) > 0:
            st.warning(f"⚠️ {info['skus_sem_custo']} SKUs sem custo cadastrado (margem = 0)")
        
        # PREVIEW COM FORMATAÇÃO BR
        st.subheader("📋 Preview das Vendas (primeiras 20 linhas)")
        
        df_preview = df_proc.copy()
        
        # Aplicar formatação brasileira
        df_preview['receita'] = df_preview['receita'].apply(formatar_valor)
        df_preview['tarifa'] = df_preview['tarifa'].apply(formatar_valor)
        df_preview['imposto'] = df_preview['imposto'].apply(formatar_valor)
        df_preview['frete'] = df_preview['frete'].apply(formatar_valor)
        df_preview['custo'] = df_preview['custo'].apply(formatar_valor)
        df_preview['margem'] = df_preview['margem'].apply(formatar_valor)
        df_preview['margem_pct'] = df_preview['margem_pct'].apply(formatar_percentual)
        
        # Exibir tabela
        st.dataframe(
            df_preview.head(20),
            use_container_width=True,
            height=400
        )
        
        # 6. BOTÃO GRAVAR
        st.divider()
        
        col_btn1, col_btn2 = st.columns([1, 3])
        
        if col_btn1.button("💾 GRAVAR NO BANCO", type="primary", use_container_width=True):
            
            with st.spinner("Gravando vendas no banco..."):
                
                # Gravar vendas
                registros, erros, skus_invalidos = gravar_vendas_ml(
                    df_proc, mktp, loja, arquivo_nome, engine
                )
                
                # CORRIGIDO: Gravar log de forma correta
                try:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    
                    sql_log = """
                        INSERT INTO log_uploads (
                            data_upload, marketplace, loja, arquivo_nome,
                            periodo_inicio, periodo_fim, total_linhas,
                            linhas_importadas, linhas_erro, status
                        ) VALUES (
                            NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    
                    cursor.execute(sql_log, (
                        mktp, loja, arquivo_nome,
                        info['periodo_inicio'], info['periodo_fim'],
                        info['total_linhas'], registros, erros,
                        'SUCESSO' if registros > 0 else 'ERRO'
                    ))
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception as e:
                    st.warning(f"⚠️ Erro ao gravar log: {e}")
                
                # Mensagens
                if registros > 0:
                    st.success(f"✅ {registros} vendas gravadas com sucesso!")
                    st.balloons()
                
                if erros > 0:
                    st.warning(f"⚠️ {erros} vendas com erro (SKUs inválidos ou vazios)")
                
                if skus_invalidos:
                    lista_skus = ', '.join(list(skus_invalidos)[:10])
                    if len(skus_invalidos) > 10:
                        lista_skus += f" ... (+{len(skus_invalidos) - 10} SKUs)"
                    st.error(f"❌ SKUs não cadastrados no sistema: {lista_skus}")
                
                # Limpar session_state
                del st.session_state['df_proc']
                del st.session_state['info']
                del st.session_state['mktp']
                del st.session_state['loja']
                del st.session_state['arquivo_nome']


def tab_vendas_consolidadas(engine):
    """Tab 2: Visualização de vendas consolidadas"""
    
    st.subheader("📊 Vendas Consolidadas")
    
    # Filtros de período
    col1, col2, col3, col4 = st.columns(4)
    
    periodo = col1.selectbox(
        "Período:",
        ["Hoje", "Ontem", "Últimos 7 dias", "Últimos 15 dias", "Últimos 30 dias", "Personalizado"]
    )
    
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
        data_fim = col3.date_input("Até:", hoje)
    
    # Filtros de marketplace e loja
    df_lojas = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)
    mktp_filtro = col4.selectbox("Marketplace:", ["Todos"] + df_lojas['marketplace'].tolist())
    
    # Query de vendas
    query = f"SELECT * FROM fact_vendas_snapshot WHERE data_venda BETWEEN '{data_ini}' AND '{data_fim}'"
    if mktp_filtro != "Todos":
        query += f" AND marketplace_origem = '{mktp_filtro}'"
    
    try:
        df_vendas = pd.read_sql(query, engine)
    except:
        df_vendas = pd.DataFrame()
    
    if df_vendas.empty:
        st.warning("⚠️ Nenhuma venda encontrada no período selecionado.")
        return
    
    # Separar vendas com/sem custo
    df_com_custo = df_vendas[df_vendas['custo_total'] > 0]
    df_sem_custo = df_vendas[df_vendas['custo_total'] == 0]
    
    # Calcular período anterior (para comparação)
    dias_diff = (data_fim - data_ini).days
    data_ini_ant = data_ini - timedelta(days=dias_diff + 1)
    data_fim_ant = data_fim - timedelta(days=dias_diff + 1)
    
    query_ant = query.replace(
        f"'{data_ini}' AND '{data_fim}'",
        f"'{data_ini_ant}' AND '{data_fim_ant}'"
    )
    
    try:
        df_ant = pd.read_sql(query_ant, engine)
        df_ant_com_custo = df_ant[df_ant['custo_total'] > 0]
    except:
        df_ant_com_custo = pd.DataFrame()
    
    # INDICADORES
    st.markdown("### 📈 Indicadores do Período")
    
    c1, c2, c3, c4 = st.columns(4)
    
    # Receita
    receita_atual = df_com_custo['valor_venda_efetivo'].sum()
    receita_ant = df_ant_com_custo['valor_venda_efetivo'].sum() if not df_ant_com_custo.empty else 0
    var_receita = ((receita_atual - receita_ant) / receita_ant * 100) if receita_ant > 0 else 0
    
    c1.metric(
        "Receita Total",
        formatar_valor(receita_atual),
        f"{formatar_percentual(var_receita)} vs período anterior"
    )
    
    # Pedidos
    pedidos_atual = len(df_com_custo)
    pedidos_ant = len(df_ant_com_custo) if not df_ant_com_custo.empty else 0
    var_pedidos = ((pedidos_atual - pedidos_ant) / pedidos_ant * 100) if pedidos_ant > 0 else 0
    
    c2.metric(
        "Pedidos",
        formatar_quantidade(pedidos_atual),
        formatar_percentual(var_pedidos)
    )
    
    # Margem
    margem_atual = df_com_custo['margem_percentual'].mean()
    margem_ant = df_ant_com_custo['margem_percentual'].mean() if not df_ant_com_custo.empty else 0
    var_margem = margem_atual - margem_ant
    
    c3.metric(
        "Margem Média",
        formatar_percentual(margem_atual),
        formatar_percentual(var_margem)
    )
    
    # Pendentes
    c4.metric(
        "Pendentes",
        formatar_quantidade(len(df_sem_custo)),
        formatar_valor(df_sem_custo['valor_venda_efetivo'].sum()) + " não contabilizados",
        delta_color="off"
    )
    
    # TABELA DETALHADA
    st.divider()
    st.subheader("📋 Detalhamento de Vendas")
    
    df_display = df_vendas.copy()
    df_display['data_venda'] = pd.to_datetime(df_display['data_venda']).dt.strftime('%d/%m/%Y')
    
    # CORRIGIDO: Remover limite de 99 linhas
    st.dataframe(
        df_display[[
            'data_venda', 'numero_pedido', 'sku', 'codigo_anuncio', 'quantidade',
            'valor_venda_efetivo', 'custo_total', 'margem_percentual'
        ]],
        use_container_width=True,
        height=600
    )
    
    # CORRIGIDO: Download Excel com formatação BR
    if st.button("📥 Download Excel"):
        buffer = io.BytesIO()
        
        # Preparar DataFrame para Excel com formatação BR
        df_excel = df_vendas.copy()
        
        # CORRIGIDO: Formatar data como dd/mm/aaaa
        df_excel['data_venda'] = pd.to_datetime(df_excel['data_venda']).dt.strftime('%d/%m/%Y')
        
        # CORRIGIDO: Formatar valores com 2 casas decimais
        colunas_valor = [
            'preco_venda', 'valor_venda_efetivo', 'custo_unitario', 'custo_total',
            'imposto', 'comissao', 'frete', 'total_tarifas', 'valor_liquido', 'margem_total'
        ]
        
        for col in colunas_valor:
            if col in df_excel.columns:
                df_excel[col] = df_excel[col].apply(lambda x: f"{x:.2f}".replace('.', ','))
        
        # CORRIGIDO: Formatar percentual com 2 casas decimais
        if 'margem_percentual' in df_excel.columns:
            df_excel['margem_percentual'] = df_excel['margem_percentual'].apply(lambda x: f"{x:.2f}".replace('.', ','))
        
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_excel.to_excel(writer, index=False, sheet_name='Vendas')
        
        st.download_button(
            "⬇️ Baixar Relatório",
            buffer.getvalue(),
            f"vendas_{data_ini.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    # NOVO: Botão deletar vendas (apenas ADMIN)
    st.divider()
    with st.expander("🗑️ Deletar Vendas (ADMIN)", expanded=False):
        st.warning("⚠️ **ATENÇÃO:** Esta ação é irreversível!")
        
        col_del1, col_del2 = st.columns(2)
        
        confirm_delete = col_del1.text_input("Digite 'DELETAR' para confirmar:")
        marketplace_del = col_del2.selectbox("Marketplace a deletar:", [""] + df_lojas['marketplace'].tolist())
        
        if st.button("🗑️ DELETAR VENDAS", type="secondary"):
            if confirm_delete == "DELETAR" and marketplace_del:
                try:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    
                    # Deletar vendas
                    cursor.execute(
                        "DELETE FROM fact_vendas_snapshot WHERE marketplace_origem = %s",
                        (marketplace_del,)
                    )
                    
                    # Deletar logs
                    cursor.execute(
                        "DELETE FROM log_uploads WHERE marketplace = %s",
                        (marketplace_del,)
                    )
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    st.success(f"✅ Vendas de {marketplace_del} deletadas!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Erro ao deletar: {e}")
            else:
                st.warning("⚠️ Confirme digitando 'DELETAR' e selecionando marketplace.")


def tab_historico_uploads(engine):
    """Tab 3: Histórico de importações"""
    
    st.subheader("📚 Histórico de Importações")
    
    try:
        df_log = pd.read_sql(
            """SELECT 
                data_upload, marketplace, loja, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas,
                linhas_importadas, linhas_erro, status
            FROM log_uploads 
            ORDER BY data_upload DESC 
            LIMIT 200""",
            engine
        )
        
        if not df_log.empty:
            # Formatar data
            df_log['data_upload'] = pd.to_datetime(df_log['data_upload']).dt.strftime('%d/%m/%Y %H:%M')
            
            st.dataframe(df_log, use_container_width=True, height=600)
        else:
            st.info("ℹ️ Nenhuma importação registrada ainda.")
    
    except Exception as e:
        st.warning(f"⚠️ Histórico não disponível: {str(e)}")


def main():
    """Função principal do módulo"""
    
    st.title("💰 Central de Vendas")
    
    # Conexão com banco
    engine = get_engine()
    
    # Tabs
    tab1, tab2, tab3 = st.tabs([
        "📤 Processar Upload",
        "📊 Vendas Consolidadas",
        "📚 Histórico"
    ])
    
    with tab1:
        tab_processar_upload(engine)
    
    with tab2:
        tab_vendas_consolidadas(engine)
    
    with tab3:
        tab_historico_uploads(engine)


if __name__ == "__main__":
    main()
