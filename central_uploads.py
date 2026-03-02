import streamlit as st
import pandas as pd
import openpyxl
from sqlalchemy import create_engine, text
from datetime import datetime
import io

# ============================================================================
# CONFIGURAÇÃO DO BANCO DE DADOS
# ============================================================================
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def converter_data_ml(data_str):
    """
    Converte data do formato ML para dd/mm/aaaa
    Ex: "10 de fevereiro de 2026 10:47 hs." → "10/02/2026"
    """
    if pd.isna(data_str):
        return None
    
    try:
        # Se já for datetime, formatar
        if isinstance(data_str, datetime):
            return data_str.strftime("%d/%m/%Y")
        
        # Mapear meses em português
        meses = {
            'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
            'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
            'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
        }
        
        # Extrair partes da data
        partes = str(data_str).lower().split()
        dia = partes[0]
        mes = meses.get(partes[2], '01')
        ano = partes[4]
        
        return f"{dia.zfill(2)}/{mes}/{ano}"
    except:
        return None

def detectar_header(df_raw):
    """
    Detecta automaticamente a linha do header procurando por "N.º de venda"
    """
    for idx in range(min(20, len(df_raw))):
        row = df_raw.iloc[idx]
        if any('venda' in str(cell).lower() for cell in row):
            return idx
    return 5  # Padrão: linha 6 (índice 5)

def limpar_valor_numerico(valor):
    """
    Converte valores para float, tratando vírgulas e valores vazios
    """
    if pd.isna(valor):
        return 0.0
    try:
        return float(str(valor).replace(',', '.').replace('R$', '').strip())
    except:
        return 0.0

def identificar_cor_linha(wb, sheet_name, row_num):
    """
    Identifica a cor de fundo de uma linha específica
    """
    try:
        ws = wb[sheet_name]
        cell = ws.cell(row=row_num, column=1)
        
        if cell.fill and cell.fill.fgColor:
            if isinstance(cell.fill.fgColor.rgb, str):
                return cell.fill.fgColor.rgb
        return None
    except:
        return None

# ============================================================================
# PROCESSAMENTO MERCADO LIVRE
# ============================================================================

def processar_mercado_livre(arquivo, loja, imposto_percentual, engine):
    """
    Processa planilha do Mercado Livre com suporte a carrinhos compostos
    """
    
    # 1. CARREGAR PLANILHA (detectar header automaticamente)
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    header_row = detectar_header(df_raw)
    
    df = pd.read_excel(arquivo, header=header_row)
    
    st.info(f"📋 Planilha carregada: {len(df)} linhas | Header detectado na linha {header_row + 1}")
    
    # 2. CARREGAR CORES (para identificar carrinhos)
    wb = openpyxl.load_workbook(arquivo)
    ws = wb.active
    
    cores = []
    for idx in range(len(df)):
        row_excel = idx + header_row + 2  # +2 porque Excel começa em 1 e header já foi pulado
        cor = identificar_cor_linha(wb, ws.title, row_excel)
        cores.append(cor)
    
    df['_cor_fundo'] = cores
    
    # 3. NORMALIZAR NOMES DE COLUNAS
    colunas_esperadas = {
        'N.º de venda': 'numero_pedido',
        'Data da venda': 'data_venda',
        'Estado': 'status',
        '# de anúncio': 'mlb_produto',
        'SKU': 'sku',
        'Título do anúncio': 'titulo',
        'Unidades': 'quantidade',
        'Receita por produtos (BRL)': 'receita',
        'Tarifa de venda e impostos (BRL)': 'tarifa',
        'Preço unitário de venda do anúncio (BRL)': 'preco_unitario',
        'Total (BRL)': 'total_liquido',
        'Receita por envio (BRL)': 'credito_frete',
        'Tarifas de envio (BRL)': 'debito_frete',
        'Forma de entrega': 'forma_entrega',
        'Pacote de diversos produtos': 'pacote'
    }
    
    df = df.rename(columns=colunas_esperadas)
    
    # 4. IDENTIFICAR TIPOS DE LINHA
    df['_eh_totalizadora'] = (df['_cor_fundo'] == 'FFD9D9D9') & (df['sku'].isna())
    df['_eh_item_carrinho'] = (df['_cor_fundo'] == 'FFF3F3F3') & (df['pacote'] == 'Sim')
    df['_eh_venda_simples'] = (~df['_eh_totalizadora']) & (~df['_eh_item_carrinho']) & (df['sku'].notna())
    
    # 5. BUSCAR BASE DE ANÚNCIOS
    try:
        query = """
            SELECT id_plataforma as mlb, sku, comissao_percentual, frete_estimado
            FROM dim_config_marketplace 
            WHERE marketplace = 'MERCADO_LIVRE'
        """
        df_base_anuncios = pd.read_sql(query, engine)
        base_anuncios_dict = df_base_anuncios.set_index('mlb').to_dict('index')
    except:
        st.error("⚠️ Base de anúncios ML não encontrada! Configure em 'Config → Mercado Livre'")
        return None
    
    # 6. BUSCAR CUSTOS DE PRODUTOS
    try:
        query_custos = """
            SELECT sku, preco_a_ser_considerado
            FROM dim_skus
            WHERE ativo = TRUE
        """
        df_custos = pd.read_sql(query_custos, engine)
        custos_dict = df_custos.set_index('sku').to_dict('index')
    except:
        st.error("⚠️ Tabela de SKUs não encontrada!")
        return None
    
    # 7. PROCESSAR VENDAS
    vendas_processadas = []
    erros = []
    alertas_frete = []
    alertas_sku = []
    alertas_mlb = []
    
    i = 0
    while i < len(df):
        linha = df.iloc[i]
        
        # CASO 1: LINHA TOTALIZADORA (início de carrinho composto)
        if linha['_eh_totalizadora']:
            numero_pedido = linha['numero_pedido']
            receita_total = limpar_valor_numerico(linha['receita'])
            tarifa_total = abs(limpar_valor_numerico(linha['tarifa']))
            total_liquido = limpar_valor_numerico(linha['total_liquido'])
            data_venda = converter_data_ml(linha['data_venda'])
            
            # Coletar itens do carrinho (linhas seguintes)
            itens = []
            j = i + 1
            while j < len(df) and df.iloc[j]['_eh_item_carrinho']:
                itens.append(df.iloc[j])
                j += 1
            
            if len(itens) == 0:
                erros.append(f"Carrinho {numero_pedido}: totalizadora sem itens")
                i += 1
                continue
            
            # Calcular preço total dos itens (para proporção)
            precos_itens = []
            for item in itens:
                preco = limpar_valor_numerico(item['preco_unitario'])
                if preco == 0:
                    # Tentar buscar na base de anúncios
                    mlb = str(item['mlb_produto'])
                    if mlb in base_anuncios_dict:
                        preco = limpar_valor_numerico(base_anuncios_dict[mlb].get('preco_unitario', 0))
                precos_itens.append(preco)
            
            total_precos = sum(precos_itens)
            
            if total_precos == 0:
                erros.append(f"Carrinho {numero_pedido}: não foi possível determinar preços dos itens")
                i = j
                continue
            
            # Distribuir receita e tarifa proporcionalmente
            for idx_item, item in enumerate(itens):
                proporcao = precos_itens[idx_item] / total_precos
                
                receita_item = receita_total * proporcao
                tarifa_item = tarifa_total * proporcao
                
                sku = str(item['sku']).strip()
                mlb = str(item['mlb_produto'])
                
                # Validar MLB na base
                if mlb not in base_anuncios_dict:
                    alertas_mlb.append(f"MLB {mlb} não encontrado na base de anúncios")
                
                # Buscar custo
                custo_unitario = 0
                if sku in custos_dict:
                    custo_unitario = custos_dict[sku]['preco_a_ser_considerado'] or 0
                else:
                    alertas_sku.append(f"SKU {sku} não encontrado na base")
                
                quantidade = int(item['quantidade']) if pd.notna(item['quantidade']) else 1
                custo_total = custo_unitario * quantidade
                
                # Calcular imposto
                imposto = receita_item * (imposto_percentual / 100)
                
                # Margem
                margem = receita_item - tarifa_item - imposto - custo_total
                margem_percentual = (margem / receita_item * 100) if receita_item > 0 else 0
                
                # Detectar FLEX
                eh_flex = 'flex' in str(item['forma_entrega']).lower()
                
                vendas_processadas.append({
                    'numero_pedido': numero_pedido,
                    'data_venda': data_venda,
                    'marketplace': 'MERCADO_LIVRE',
                    'loja': loja,
                    'sku': sku,
                    'mlb_produto': mlb,
                    'titulo': item['titulo'],
                    'quantidade': quantidade,
                    'receita': round(receita_item, 2),
                    'tarifa': round(tarifa_item, 2),
                    'imposto': round(imposto, 2),
                    'custo_unitario': round(custo_unitario, 2),
                    'custo_total': round(custo_total, 2),
                    'margem': round(margem, 2),
                    'margem_percentual': round(margem_percentual, 2),
                    'total_liquido': round(total_liquido * proporcao, 2),
                    'eh_flex': eh_flex,
                    'eh_carrinho_composto': True
                })
            
            i = j  # Pular para próxima linha após itens
        
        # CASO 2: VENDA SIMPLES
        elif linha['_eh_venda_simples']:
            numero_pedido = linha['numero_pedido']
            sku = str(linha['sku']).strip()
            mlb = str(linha['mlb_produto'])
            
            receita = limpar_valor_numerico(linha['receita'])
            tarifa = abs(limpar_valor_numerico(linha['tarifa']))
            total_liquido = limpar_valor_numerico(linha['total_liquido'])
            data_venda = converter_data_ml(linha['data_venda'])
            
            # Validar MLB
            if mlb not in base_anuncios_dict:
                alertas_mlb.append(f"MLB {mlb} não encontrado na base")
            
            # Buscar custo
            custo_unitario = 0
            if sku in custos_dict:
                custo_unitario = custos_dict[sku]['preco_a_ser_considerado'] or 0
            else:
                alertas_sku.append(f"SKU {sku} não encontrado")
            
            quantidade = int(linha['quantidade']) if pd.notna(linha['quantidade']) else 1
            custo_total = custo_unitario * quantidade
            
            # Calcular imposto
            imposto = receita * (imposto_percentual / 100)
            
            # Margem
            margem = receita - tarifa - imposto - custo_total
            margem_percentual = (margem / receita * 100) if receita > 0 else 0
            
            # Detectar FLEX
            eh_flex = 'flex' in str(linha['forma_entrega']).lower()
            
            # Validar frete (crédito vs débito)
            credito_frete = limpar_valor_numerico(linha['credito_frete'])
            debito_frete = limpar_valor_numerico(linha['debito_frete'])
            
            if not eh_flex and credito_frete > 0 and abs(credito_frete - debito_frete) > 0.5:
                alertas_frete.append(f"Pedido {numero_pedido}: Crédito frete ({credito_frete:.2f}) ≠ Débito ({debito_frete:.2f})")
            
            vendas_processadas.append({
                'numero_pedido': numero_pedido,
                'data_venda': data_venda,
                'marketplace': 'MERCADO_LIVRE',
                'loja': loja,
                'sku': sku,
                'mlb_produto': mlb,
                'titulo': linha['titulo'],
                'quantidade': quantidade,
                'receita': round(receita, 2),
                'tarifa': round(tarifa, 2),
                'imposto': round(imposto, 2),
                'custo_unitario': round(custo_unitario, 2),
                'custo_total': round(custo_total, 2),
                'margem': round(margem, 2),
                'margem_percentual': round(margem_percentual, 2),
                'total_liquido': round(total_liquido, 2),
                'eh_flex': eh_flex,
                'eh_carrinho_composto': False
            })
            
            i += 1
        else:
            i += 1
    
    # 8. CRIAR DATAFRAME FINAL
    df_processado = pd.DataFrame(vendas_processadas)
    
    # 9. EXIBIR ESTATÍSTICAS E ALERTAS
    st.success(f"✅ {len(df_processado)} vendas processadas com sucesso!")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Vendas", len(df_processado))
    col2.metric("Carrinhos Compostos", df_processado['eh_carrinho_composto'].sum())
    col3.metric("Vendas FLEX", df_processado['eh_flex'].sum())
    col4.metric("Receita Total", f"R$ {df_processado['receita'].sum():,.2f}")
    
    # Alertas
    if alertas_sku:
        with st.expander(f"⚠️ {len(set(alertas_sku))} SKUs não encontrados", expanded=False):
            for alerta in set(alertas_sku):
                st.warning(alerta)
    
    if alertas_mlb:
        with st.expander(f"⚠️ {len(set(alertas_mlb))} MLBs não cadastrados na base", expanded=False):
            for alerta in set(alertas_mlb):
                st.warning(alerta)
    
    if alertas_frete:
        with st.expander(f"⚠️ {len(alertas_frete)} inconsistências de frete", expanded=False):
            for alerta in alertas_frete:
                st.warning(alerta)
    
    if erros:
        with st.expander(f"❌ {len(erros)} erros no processamento", expanded=False):
            for erro in erros:
                st.error(erro)
    
    return df_processado

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    st.header("💰 Central de Vendas")
    engine = get_engine()
    
    # Buscar lojas cadastradas
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("⚠️ Erro: Tabela de lojas não encontrada. Configure em 'Config → Impostos & Lojas'")
        return
    
    if df_lojas.empty:
        st.warning("⚠️ Nenhuma loja cadastrada. Vá em 'Config → Impostos & Lojas' e salve a estrutura.")
        return
    
    # TABS
    tab1, tab2 = st.tabs(["🚀 Processar Vendas", "📊 Histórico"])
    
    # ========================================================================
    # TAB 1: PROCESSAR VENDAS
    # ========================================================================
    with tab1:
        col1, col2, col3 = st.columns(3)
        
        # Seleção de Marketplace
        marketplaces = sorted(df_lojas['marketplace'].unique().tolist())
        marketplace_sel = col1.selectbox("Marketplace:", marketplaces)
        
        # Seleção de Loja (filtrada por marketplace)
        lojas_filtradas = df_lojas[df_lojas['marketplace'] == marketplace_sel]['loja'].tolist()
        
        if not lojas_filtradas:
            st.warning(f"Nenhuma loja cadastrada para {marketplace_sel}. Configure em 'Config'")
            return
        
        loja_sel = col2.selectbox("Loja:", lojas_filtradas)
        
        # Buscar imposto da loja
        imposto = df_lojas[df_lojas['loja'] == loja_sel]['imposto'].values[0]
        
        # Data do relatório
        data_relatorio = col3.date_input("Data do Relatório")
        
        st.info(f"⚙️ Loja: **{loja_sel}** | Imposto: **{imposto}%**")
        
        # Upload do arquivo
        arquivo = st.file_uploader("📁 Upload do Relatório de Vendas", type=['xlsx'])
        
        if arquivo:
            st.write("---")
            
            # Botão processar
            if st.button("🚀 Processar Relatório", type="primary"):
                with st.spinner("Processando..."):
                    # Processar conforme marketplace
                    if marketplace_sel == 'MERCADO_LIVRE':
                        df_processado = processar_mercado_livre(arquivo, loja_sel, imposto, engine)
                    else:
                        st.error(f"Marketplace {marketplace_sel} ainda não implementado. Aguarde próximas versões!")
                        df_processado = None
                    
                    if df_processado is not None and len(df_processado) > 0:
                        st.write("---")
                        st.subheader("📊 Preview dos Dados Processados")
                        
                        # Exibir preview (20 primeiras linhas)
                        colunas_preview = ['numero_pedido', 'data_venda', 'sku', 'quantidade', 
                                          'receita', 'tarifa', 'imposto', 'margem', 'margem_percentual']
                        st.dataframe(df_processado[colunas_preview].head(20), use_container_width=True)
                        
                        st.write("---")
                        
                        # Botão confirmar importação
                        if st.button("✅ Confirmar e Gravar no Banco", type="primary"):
                            try:
                                with st.spinner("Gravando no banco..."):
                                    # Gravar em fact_vendas_snapshot
                                    registros_inseridos = 0
                                    
                                    with engine.connect() as conn:
                                        for _, row in df_processado.iterrows():
                                            # ID único: MLB do produto (não do pedido)
                                            id_venda_marketplace = row['mlb_produto']
                                            
                                            query = text("""
                                                INSERT INTO fact_vendas_snapshot (
                                                    id_venda_marketplace, data_venda, marketplace, loja,
                                                    sku, nome_produto, quantidade, preco_venda,
                                                    receita_bruta, comissao_marketplace, imposto,
                                                    custo_produto_snapshot, margem_liquida, margem_percentual,
                                                    data_importacao
                                                ) VALUES (
                                                    :id_venda, :data, :mktp, :loja,
                                                    :sku, :nome, :qtd, :preco,
                                                    :receita, :comissao, :imposto,
                                                    :custo, :margem, :margem_perc,
                                                    NOW()
                                                )
                                                ON CONFLICT (id_venda_marketplace) DO NOTHING
                                            """)
                                            
                                            result = conn.execute(query, {
                                                'id_venda': id_venda_marketplace,
                                                'data': row['data_venda'],
                                                'mktp': row['marketplace'],
                                                'loja': row['loja'],
                                                'sku': row['sku'],
                                                'nome': row['titulo'],
                                                'qtd': row['quantidade'],
                                                'preco': row['receita'] / row['quantidade'],
                                                'receita': row['receita'],
                                                'comissao': row['tarifa'],
                                                'imposto': row['imposto'],
                                                'custo': row['custo_total'],
                                                'margem': row['margem'],
                                                'margem_perc': row['margem_percentual']
                                            })
                                            
                                            if result.rowcount > 0:
                                                registros_inseridos += 1
                                        
                                        conn.commit()
                                    
                                    st.success(f"✅ {registros_inseridos} vendas gravadas com sucesso!")
                                    
                                    # Log de auditoria
                                    try:
                                        with engine.connect() as conn:
                                            conn.execute(text("""
                                                INSERT INTO log_auditoria_uploads (
                                                    marketplace, loja, data_upload, total_linhas, 
                                                    linhas_importadas, usuario
                                                ) VALUES (
                                                    :mktp, :loja, NOW(), :total, :importadas, 'Admin'
                                                )
                                            """), {
                                                'mktp': marketplace_sel,
                                                'loja': loja_sel,
                                                'total': len(df_processado),
                                                'importadas': registros_inseridos
                                            })
                                            conn.commit()
                                    except:
                                        pass  # Tabela de log pode não existir
                                    
                                    st.balloons()
                                    
                            except Exception as e:
                                st.error(f"❌ Erro ao gravar: {str(e)}")
    
    # ========================================================================
    # TAB 2: HISTÓRICO
    # ========================================================================
    with tab2:
        st.subheader("📊 Histórico de Importações")
        
        try:
            df_historico = pd.read_sql("""
                SELECT marketplace, loja, data_upload, total_linhas, 
                       linhas_importadas, usuario
                FROM log_auditoria_uploads
                ORDER BY data_upload DESC
                LIMIT 50
            """, engine)
            
            if not df_historico.empty:
                st.dataframe(df_historico, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhuma importação registrada ainda.")
        except:
            st.info("Histórico de importações não disponível. Tabela de log não existe.")

if __name__ == "__main__":
    main()
