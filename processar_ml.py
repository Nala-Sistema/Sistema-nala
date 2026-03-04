"""
PROCESSADOR MERCADO LIVRE - Sistema Nala
Processa arquivos de vendas do Mercado Livre
- Detecta header automaticamente
- Filtra vendas canceladas/devolvidas
- Valida SKUs antes de gravar
- Calcula margem
- Grava no banco com barra de progresso
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import converter_data_ml, limpar_numero
from database_utils import buscar_custos_skus, buscar_skus_validos


def detectar_header_ml(arquivo):
    """
    Detecta em qual linha está o header do arquivo ML.
    Procura por 'sku' nas primeiras 20 linhas.
    """
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)
    
    for idx in range(20):
        linha = df_raw.iloc[idx]
        # Procura por 'sku' em qualquer coluna
        if any('sku' in str(c).lower() for c in linha):
            return idx
    
    # Se não encontrar, assume linha 5 (padrão ML)
    return 5


def renomear_colunas_ml(df):
    """
    Renomeia colunas do ML para nomes padronizados.
    """
    rename_map = {}
    
    for col in df.columns:
        col_lower = str(col).lower().strip()
        
        if 'n.' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'pedido'
        elif 'data' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'data'
        elif col_lower == 'sku':
            rename_map[col] = 'sku'
        elif 'estado' in col_lower and 'estado.' not in col_lower:
            rename_map[col] = 'status'
        elif 'unidades' in col_lower and 'unidades.' not in col_lower:
            rename_map[col] = 'qtd'
        elif 'receita' in col_lower and 'produtos' in col_lower:
            rename_map[col] = 'receita'
        elif 'tarifa' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'tarifa'
    
    return df.rename(columns=rename_map)


def processar_arquivo_ml(arquivo, loja, imposto, engine):
    """
    Processa arquivo Excel do Mercado Livre.
    
    Retorna:
        df_processado, info_dict ou None, erro_msg
    """
    
    # 1. DETECTAR HEADER
    header_idx = detectar_header_ml(arquivo)
    
    # 2. LER ARQUIVO
    try:
        df = pd.read_excel(arquivo, header=header_idx)
    except Exception as e:
        return None, f"Erro ao ler arquivo: {str(e)}"
    
    # 3. RENOMEAR COLUNAS
    df = renomear_colunas_ml(df)
    
    # 4. VALIDAR COLUNAS ESSENCIAIS
    colunas_obrigatorias = ['sku', 'receita', 'tarifa']
    if not all(col in df.columns for col in colunas_obrigatorias):
        return None, f"Colunas obrigatórias não encontradas: {colunas_obrigatorias}"
    
    # 5. BUSCAR CUSTOS DO BANCO
    custos_dict = buscar_custos_skus(engine)
    
    # 6. PROCESSAR VENDAS
    vendas = []
    skus_sem_custo = set()
    linhas_descartadas = 0
    
    for idx, row in df.iterrows():
        try:
            # Validar SKU não vazio
            if pd.isna(row.get('sku')) or str(row.get('sku')).strip() == '':
                linhas_descartadas += 1
                continue
            
            sku = str(row['sku']).strip()
            
            # Filtrar por status
            if 'status' in df.columns:
                status = str(row['status']).lower()
                if any(palavra in status for palavra in ['cancelad', 'devolv', 'reembolso']):
                    linhas_descartadas += 1
                    continue
            
            # Validar receita
            receita = limpar_numero(row['receita'])
            if receita <= 0:
                linhas_descartadas += 1
                continue
            
            # Tarifa (valor absoluto)
            tarifa = abs(limpar_numero(row['tarifa']))
            
            # Quantidade
            try:
                qtd = int(row.get('qtd', 1)) if not pd.isna(row.get('qtd')) else 1
                if qtd <= 0:
                    qtd = 1
            except:
                qtd = 1
            
            # Buscar custo
            custo_unit = custos_dict.get(sku, 0)
            if custo_unit == 0:
                skus_sem_custo.add(sku)
            
            # Calcular valores
            custo_total = custo_unit * qtd
            imposto_val = receita * (imposto / 100)
            margem = receita - tarifa - imposto_val - custo_total
            margem_pct = (margem / receita * 100) if receita > 0 else 0
            
            # Data
            data_venda = converter_data_ml(row.get('data'))
            
            # Montar registro
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
                '_custo_unit': custo_unit,
                '_data_obj': datetime.strptime(data_venda, "%d/%m/%Y") if data_venda else None
            })
            
        except Exception as e:
            linhas_descartadas += 1
            continue
    
    # 7. VALIDAR SE TEM VENDAS
    if not vendas:
        return None, f"Nenhuma venda válida encontrada ({linhas_descartadas} linhas descartadas)"
    
    # 8. CRIAR DATAFRAME
    df_result = pd.DataFrame(vendas)
    
    # 9. CALCULAR PERÍODO
    datas_validas = [v['_data_obj'] for v in vendas if v['_data_obj']]
    periodo_inicio = min(datas_validas).strftime("%d/%m/%Y") if datas_validas else None
    periodo_fim = max(datas_validas).strftime("%d/%m/%Y") if datas_validas else None
    
    # 10. CRIAR INFO
    info = {
        'total_linhas': len(df_result),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': periodo_inicio,
        'periodo_fim': periodo_fim,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name
    }
    
    # 11. LIMPAR COLUNAS TEMPORÁRIAS
    df_result = df_result.drop(columns=['_data_obj', '_custo_unit'])
    
    return df_result, info


def gravar_vendas_ml(df_vendas, marketplace, loja, arquivo_nome, engine):
    """
    Grava vendas do ML no banco com validação de SKU e barra de progresso.
    
    Retorna:
        registros_gravados, erros, skus_invalidos
    """
    
    # 1. BUSCAR SKUs VÁLIDOS
    skus_validos = buscar_skus_validos(engine)
    
    # 2. PREPARAR GRAVAÇÃO
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    registros = 0
    erros = 0
    skus_invalidos = set()
    
    # 3. BARRA DE PROGRESSO
    total = len(df_vendas)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 4. PROCESSAR CADA VENDA
    for idx, row in df_vendas.iterrows():
        try:
            # Atualizar progresso
            progresso = (idx + 1) / total
            progress_bar.progress(progresso)
            status_text.text(f"Gravando venda {idx + 1} de {total}...")
            
            # Validar SKU
            sku = row['sku']
            if not sku or sku.strip() == '':
                erros += 1
                continue
            
            if sku not in skus_validos:
                skus_invalidos.add(sku)
                erros += 1
                continue
            
            # Preparar dados
            data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
            qtd = int(row['qtd'])
            receita = float(row['receita'])
            custo_total = float(row['custo'])
            tarifa = float(row['tarifa'])
            imposto = float(row['imposto'])
            margem = float(row['margem'])
            margem_pct = float(row['margem_pct'])
            
            # Calcular valores derivados
            preco_venda = receita / qtd if qtd > 0 else receita
            custo_unit = custo_total / qtd if qtd > 0 else custo_total
            valor_liquido = receita - tarifa - imposto
            
            # SQL INSERT
            sql = """
                INSERT INTO fact_vendas_snapshot (
                    marketplace_origem, loja_origem, numero_pedido, data_venda, sku,
                    codigo_anuncio, quantidade, preco_venda, desconto_parceiro, desconto_marketplace,
                    valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao,
                    frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido,
                    margem_total, margem_percentual, data_processamento, arquivo_origem
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, NOW(), %s
                )
            """
            
            cursor.execute(sql, (
                marketplace, loja, row['pedido'], data_venda, sku,
                '', qtd, preco_venda, 0, 0,
                receita, custo_unit, custo_total, imposto, tarifa,
                0, 0, 0, tarifa, valor_liquido,
                margem, margem_pct, arquivo_nome
            ))
            
            registros += 1
            
        except Exception as e:
            conn.rollback()
            erros += 1
            if erros == 1:
                st.warning(f"Primeiro erro: {str(e)[:200]}")
    
    # 5. COMMIT FINAL
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no commit final: {e}")
    
    # 6. FECHAR CONEXÃO
    cursor.close()
    conn.close()
    
    # 7. LIMPAR BARRA
    progress_bar.empty()
    status_text.empty()
    
    return registros, erros, skus_invalidos
