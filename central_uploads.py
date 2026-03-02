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
    """Converte data do formato ML para dd/mm/aaaa"""
    if pd.isna(data_str):
        return None
    
    try:
        if isinstance(data_str, datetime):
            return data_str.strftime("%d/%m/%Y")
        
        meses = {
            'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
            'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
            'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
        }
        
        partes = str(data_str).lower().split()
        dia = partes[0]
        mes = meses.get(partes[2], '01')
        ano = partes[4]
        
        return f"{dia.zfill(2)}/{mes}/{ano}"
    except:
        return str(data_str)

def detectar_header(df_raw):
    """Detecta linha do header procurando por 'N.º de venda' ou 'SKU'"""
    for idx in range(min(20, len(df_raw))):
        row = df_raw.iloc[idx].astype(str)
        # Procura por palavras-chave do header ML
        if any(('venda' in cell.lower() and 'n.' in cell.lower()) or 'sku' in cell.lower() for cell in row):
            return idx
    return 5

def limpar_valor_numerico(valor):
    """Converte valores para float"""
    if pd.isna(valor):
        return 0.0
    try:
        return float(str(valor).replace(',', '.').replace('R$', '').strip())
    except:
        return 0.0

def identificar_cor_linha(wb, sheet_name, row_num):
    """Identifica cor de fundo de uma linha"""
    try:
        ws = wb[sheet_name]
        cell = ws.cell(row=row_num, column=1)
        if cell.fill and cell.fill.fgColor and isinstance(cell.fill.fgColor.rgb, str):
            return cell.fill.fgColor.rgb
        return None
    except:
        return None

# ============================================================================
# PROCESSAMENTO MERCADO LIVRE
# ============================================================================

def processar_mercado_livre(arquivo, loja, imposto_percentual, engine):
    """Processa planilha do Mercado Livre"""
    
    # 1. DETECTAR HEADER
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    header_row = detectar_header(df_raw)
    
    df = pd.read_excel(arquivo, header=header_row)
    
    st.info(f"📋 {len(df)} linhas | Header: linha {header_row + 1}")
    
    # 2. NORMALIZAR NOMES DE COLUNAS (limpar espaços e caracteres especiais)
    df.columns = df.columns.str.strip()
    
    # 3. RENOMEAR COLUNAS (mapeamento flexível)
    rename_map = {}
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if 'n.' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'numero_pedido'
        elif 'data' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'data_venda'
        elif col_lower == 'estado':
            rename_map[col] = 'status'
        elif '#' in col_lower and 'anúncio' in col_lower:
            rename_map[col] = 'mlb_produto'
        elif col_lower == 'sku':
            rename_map[col] = 'sku'
        elif 'título' in col_lower and 'anúncio' in col_lower:
            rename_map[col] = 'titulo'
        elif col_lower == 'unidades':
            rename_map[col] = 'quantidade'
        elif 'receita' in col_lower and 'produtos' in col_lower:
            rename_map[col] = 'receita'
        elif 'tarifa' in col_lower and 'venda' in col_lower and 'impostos' in col_lower:
            rename_map[col] = 'tarifa'
        elif 'preço' in col_lower and 'unitário' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'preco_unitario'
        elif col_lower == 'total (brl)':
            rename_map[col] = 'total_liquido'
        elif 'receita' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'credito_frete'
        elif 'tarifas' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'debito_frete'
        elif 'forma' in col_lower and 'entrega' in col_lower:
            rename_map[col] = 'forma_entrega'
        elif 'pacote' in col_lower and 'diversos' in col_lower:
            rename_map[col] = 'pacote'
    
    df = df.rename(columns=rename_map)
    
    # 4. VERIFICAR SE COLUNAS ESSENCIAIS EXISTEM
    colunas_essenciais = ['numero_pedido', 'sku', 'receita', 'tarifa']
    faltando = [c for c in colunas_essenciais if c not in df.columns]
    if faltando:
        st.error(f"❌ Colunas não encontradas: {faltando}")
        st.write("Colunas disponíveis:", list(df.columns)[:20])
        return None
    
    # 5. CARREGAR CORES
    wb = openpyxl.load_workbook(arquivo)
    ws = wb.active
    cores = []
    for idx in range(len(df)):
        row_excel = idx + header_row + 2
        cor = identificar_cor_linha(wb, ws.title, row_excel)
        cores.append(cor)
    df['_cor_fundo'] = cores
    
    # 6. IDENTIFICAR TIPOS DE LINHA (com verificações)
    df['_eh_totalizadora'] = (df['_cor_fundo'] == 'FFD9D9D9') & (df['sku'].isna())
    
    tem_pacote = 'pacote' in df.columns
    if tem_pacote:
        df['_eh_item_carrinho'] = (df['_cor_fundo'] == 'FFF3F3F3') & (df['pacote'] == 'Sim')
    else:
        df['_eh_item_carrinho'] = False
    
    df['_eh_venda_simples'] = (~df['_eh_totalizadora']) & (~df['_eh_item_carrinho']) & (df['sku'].notna())
    
    # 7. BUSCAR CUSTOS
    try:
        df_custos = pd.read_sql("SELECT sku, preco_a_ser_considerado FROM dim_skus WHERE ativo = TRUE", engine)
        custos_dict = df_custos.set_index('sku').to_dict('index')
    except Exception as e:
        st.error(f"❌ Erro ao buscar custos: {e}")
        return None
    
    # 8. PROCESSAR VENDAS
    vendas = []
    alertas_sku = set()
    
    for idx, linha in df.iterrows():
        if not linha['_eh_venda_simples']:
            continue
        
        try:
            sku = str(linha['sku']).strip()
            receita = limpar_valor_numerico(linha['receita'])
            tarifa = abs(limpar_valor_numerico(linha['tarifa']))
            quantidade = int(linha['quantidade']) if pd.notna(linha.get('quantidade')) else 1
            
            # Buscar custo
            custo_unit = custos_dict.get(sku, {}).get('preco_a_ser_considerado', 0) or 0
            if custo_unit == 0:
                alertas_sku.add(sku)
            
            custo_total = custo_unit * quantidade
            imposto = receita * (imposto_percentual / 100)
            margem = receita - tarifa - imposto - custo_total
            margem_perc = (margem / receita * 100) if receita > 0 else 0
            
            # Formatar data
            data_venda = converter_data_ml(linha.get('data_venda'))
            
            vendas.append({
                'numero_pedido': str(linha.get('numero_pedido', '')),
                'data_venda': data_venda,
                'sku': sku,
                'quantidade': quantidade,
                'receita': round(receita, 2),
                'tarifa': round(tarifa, 2),
                'imposto': round(imposto, 2),
                'custo_total': round(custo_total, 2),
                'margem': round(margem, 2),
                'margem_percentual': round(margem_perc, 2)
            })
        except Exception as e:
            continue
    
    if not vendas:
        st.error("❌ Nenhuma venda válida encontrada!")
        return None
    
    df_proc = pd.DataFrame(vendas)
    
    # 9. ESTATÍSTICAS
    st.success(f"✅ {len(df_proc)} vendas processadas!")
    
    col1, col2 = st.columns(2)
    col1.metric("Vendas", len(df_proc))
    col2.metric("Receita", f"R$ {df_proc['receita'].sum():,.2f}")
    
    if alertas_sku:
        st.warning(f"⚠️ {len(alertas_sku)} SKUs sem custo: {', '.join(list(alertas_sku)[:5])}")
    
    return df_proc

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    st.header("💰 Central de Vendas")
    engine = get_engine()
    
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except:
        st.error("⚠️ Configure lojas em 'Config'")
        return
    
    if df_lojas.empty:
        st.warning("⚠️ Cadastre lojas em 'Config'")
        return
    
    tab1, tab2 = st.tabs(["🚀 Processar Vendas", "📊 Histórico"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        
        marketplaces = sorted(df_lojas['marketplace'].unique())
        mktp = col1.selectbox("Marketplace:", marketplaces)
        
        lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
        if not lojas:
            st.warning("Configure lojas")
            return
        
        loja = col2.selectbox("Loja:", lojas)
        imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
        data_rel = col3.date_input("Data", format="DD/MM/YYYY")
        
        st.info(f"⚙️ {loja} | Imposto: {imposto}%")
        
        arquivo = st.file_uploader("📁 Upload (XLSX)", type=['xlsx'])
        
        if arquivo and st.button("🚀 Processar", type="primary"):
            with st.spinner("Processando..."):
                mktp_norm = mktp.strip().upper()
                
                if 'MERCADO' in mktp_norm and 'LIVRE' in mktp_norm:
                    df_proc = processar_mercado_livre(arquivo, loja, imposto, engine)
                else:
                    st.error(f"{mktp} não implementado ainda")
                    df_proc = None
                
                if df_proc is not None and len(df_proc) > 0:
                    st.write("---")
                    st.subheader("📊 Preview (primeiras 20 linhas)")
                    st.dataframe(df_proc.head(20), use_container_width=True)
                    
                    # Botão confirmar (simplificado por ora)
                    if st.button("✅ Confirmar (em desenvolvimento)", disabled=True):
                        st.info("Gravação no banco será implementada em breve")
    
    with tab2:
        st.info("Histórico em desenvolvimento")

if __name__ == "__main__":
    main()
