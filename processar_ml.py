"""
PROCESSADOR MERCADO LIVRE - Sistema Nala
Processa arquivos de vendas do Mercado Livre
- Detecta header automaticamente
- Filtra vendas canceladas/devolvidas/mediações
- Valida SKUs antes de gravar
- Calcula margem CORRETA (com frete e FLEX)
- Grava no banco com barra de progresso
- VERSÃO SEGURA: Apenas correções essenciais
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import converter_data_ml, limpar_numero
from database_utils import buscar_custos_skus, buscar_skus_validos

# CONFIGURAÇÃO FLEX (editável)
CUSTO_FLEX_ML = 12.90  # Custo fixo transportadora FLEX


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
        
        # Colunas principais
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
        
        # Receitas e tarifas
        elif 'receita' in col_lower and 'produtos' in col_lower:
            rename_map[col] = 'receita'
        elif 'tarifa' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'tarifa'
        elif 'receita' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'receita_envio'
        elif 'tarifas' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'tarifa_envio'
        
        # Total e forma de entrega
        elif col_lower == 'total (brl)':
            rename_map[col] = 'total_brl'
        elif 'forma' in col_lower and 'entrega' in col_lower:
            rename_map[col] = 'forma_entrega'
    
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
    avisos_divergencia = []
    
    for idx, row in df.iterrows():
        try:
            # Validar SKU não vazio
            if pd.isna(row.get('sku')) or str(row.get('sku')).strip() == '':
                linhas_descartadas += 1
                continue
            
            sku = str(row['sku']).strip()
            
            # CORRIGIDO: Filtrar por status (incluindo mediação)
            if 'status' in df.columns:
                status = str(row['status']).lower()
                if any(palavra in status for palavra in ['cancelad', 'devolv', 'reembolso', 'mediação', 'mediacao']):
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
            
            # FRETE - Colunas opcionais
            receita_envio = abs(limpar_numero(row.get('receita_envio', 0)))
            tarifa_envio = abs(limpar_numero(row.get('tarifa_envio', 0)))
            forma_entrega = str(row.get('forma_entrega', '')).lower()
            total_brl = limpar_numero(row.get('total_brl', 0))
            
            # DETECTAR FLEX
            is_flex = 'flex' in forma_entrega
            
            # CORRIGIDO: CALCULAR FRETE E IMPOSTO
            if is_flex:
                # FLEX: Custo líquido (transportadora - cliente pagou)
                custo_frete = CUSTO_FLEX_ML - receita_envio
                imposto_val = 0.0  # SEM imposto no FLEX
            else:
                # NORMAL: Frete líquido
                custo_frete = tarifa_envio - receita_envio
                imposto_val = receita * (imposto / 100)
            
            # Buscar custo produto
            custo_unit = custos_dict.get(sku, 0)
            if custo_unit == 0:
                skus_sem_custo.add(sku)
            
            # Calcular custo total
            custo_total = custo_unit * qtd
            
            # MARGEM = receita - tarifa - imposto - frete - custo
            margem = receita - tarifa - imposto_val - custo_frete - custo_total
            margem_pct = (margem / receita * 100) if receita > 0 else 0
            
            # VALIDAÇÃO contra Total (BRL)
            if total_brl > 0:
                valor_calculado = receita - tarifa - imposto_val - custo_frete
                divergencia = abs(valor_calculado - total_brl)
                
                if divergencia > 5.00:
                    avisos_divergencia.append({
                        'pedido': str(row.get('pedido', '')),
                        'calculado': valor_calculado,
                        'total_brl': total_brl,
                        'diferenca': divergencia
                    })
            
            # Data
            data_venda = converter_data_ml(row.get('data'))
            
            # Montar registro (MANTÉM ESTRUTURA ORIGINAL - compatível com central_uploads.py)
            vendas.append({
                'pedido': str(row.get('pedido', '')),
                'data': data_venda,
                'sku': sku,
                'qtd': qtd,
                'receita': receita,
                'tarifa': tarifa,
                'imposto': imposto_val,
                'frete': custo_frete,
                'custo': custo_total,
                'margem': margem,
                'margem_pct': margem_pct,
                'tem_custo': custo_unit > 0,
                'is_flex': is_flex,
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
        'arquivo_nome': arquivo.name,
        'divergencias': avisos_divergencia
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
            frete = float(row['frete'])
            margem = float(row['margem'])
            margem_pct = float(row['margem_pct'])
            
            # Calcular valores derivados
            preco_venda = receita / qtd if qtd > 0 else receita
            custo_unit = custo_total / qtd if qtd > 0 else custo_total
            total_tarifas = tarifa + frete
            valor_liquido = receita - total_tarifas - imposto
            
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
                '',  # codigo_anuncio vazio por enquanto (evita erros)
                qtd, preco_venda, 0, 0,
                receita, custo_unit, custo_total, imposto, tarifa,
                frete, 0, 0, total_tarifas, valor_liquido,
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
