"""
PROCESSADOR AMAZON - Sistema Nala v1.0
- Processa Business Report (CSV) com datas acumuladas
- Lógica de ID Sintético para evitar duplicatas por período
- Taxas excludentes (Frete vs Taxa Fixa)
- De-Para de SKUs automático com limpeza de sufixos
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import limpar_numero
from database_utils import (
    buscar_custos_skus,
    buscar_skus_validos,
    gravar_venda_pendente,
    buscar_custo_flex
)

def processar_arquivo_amazon(arquivo, loja, imposto, engine, data_ini, data_fim):
    """Lê o Business Report e prepara os dados para gravação"""
    try:
        # Tenta ler o CSV (padrão Amazon Business Report)
        df = pd.read_csv(arquivo)
    except Exception as e:
        return None, f"Erro ao ler CSV: {e}"

    # 1. MAPEAMENTO DE COLUNAS (conforme Business Report bruto)
    col_map = {
        'Código SKU': 'sku_amz',
        'Unidades pedidas': 'qtd',
        'Vendas de produtos pedidos': 'receita_bruta',
        'ASIN (child)': 'asin'
    }
    df = df.rename(columns=col_map)

    if 'sku_amz' not in df.columns:
        return None, "Coluna 'Código SKU' não encontrada no arquivo."

    # 2. BUSCAR CONFIGURAÇÕES (Taxas e De-Para)
    try:
        query_config = "SELECT id_plataforma as asin, sku as sku_original, comissao_percentual, taxa_fixa, frete_estimado FROM dim_config_marketplace WHERE marketplace = 'AMAZON'"
        df_config = pd.read_sql(query_config, engine)
        config_dict = df_config.set_index('asin').to_dict('index')
    except:
        config_dict = {}

    custos_dict = buscar_custos_skus(engine)
    
    vendas = []
    skus_sem_mapeamento = set()

    for _, row in df.iterrows():
        try:
            sku_amz = str(row['sku_amz']).strip()
            asin = str(row.get('asin', '')).strip()
            receita = limpar_numero(row['receita_bruta'])
            qtd = int(row['qtd'])
            
            if qtd <= 0 or receita <= 0: continue

            # Lógica De-Para: Busca por ASIN ou tenta limpar SKU
            conf = config_dict.get(asin, {})
            sku_original = conf.get('sku_original')
            
            if not sku_original:
                # Tenta limpar sufixos (-FBA, -DBA, -PR)
                sku_original = sku_amz.split('-FBA')[0].split('-DBA')[0].split('-PR')[0]
            
            # Cálculo de Taxas Excludentes
            comissao_pct = float(conf.get('comissao_percentual', 15.0))
            taxa_fixa = float(conf.get('taxa_fixa', 0.0))
            frete_est = float(conf.get('frete_estimado', 0.0))

            v_comissao = receita * (comissao_pct / 100)
            
            # Se tem frete, zera a taxa fixa. Se não tem frete, usa taxa fixa.
            if frete_est > 0:
                v_frete = frete_est * qtd
                v_taxa_fixa = 0.0
            else:
                v_frete = 0.0
                v_taxa_fixa = taxa_fixa * qtd

            imposto_val = receita * (imposto / 100)
            custo_un = custos_dict.get(sku_original, 0.0)
            custo_total = custo_un * qtd

            margem = receita - v_comissao - v_taxa_fixa - v_frete - imposto_val - custo_total

            vendas.append({
                'pedido': f"AMZ_{loja}_{data_ini.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}_{sku_amz}",
                'data': data_ini.strftime("%d/%m/%Y"), # Data início como referência
                'sku': sku_original,
                'sku_amz': sku_amz,
                'asin': asin,
                'qtd': qtd,
                'receita': receita,
                'comissao': v_comissao,
                'taxa_fixa': v_taxa_fixa,
                'frete': v_frete,
                'imposto': imposto_val,
                'custo': custo_total,
                'margem': margem,
                'margem_pct': (margem / receita * 100) if receita > 0 else 0,
                'tem_mapeamento': sku_original in custos_dict
            })
        except: continue

    return pd.DataFrame(vendas), {"total_linhas": len(vendas), "arquivo": arquivo.name}

def gravar_vendas_amazon(df, marketplace, loja, arq_nome, engine, data_ini, data_fim):
    """Grava com Delete-Before-Insert para evitar duplicatas de período"""
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    reg = err = inv = dups = pend = desc = atualiz = 0
    skus_cadastrados = buscar_skus_validos(engine)

    try:
        # 1. DELETE BEFORE INSERT (Limpeza do Período para esta Loja)
        cursor.execute("DELETE FROM fact_vendas_snapshot WHERE loja_origem = %s AND data_venda BETWEEN %s AND %s", 
                       (loja, data_ini, data_fim))
        atualiz = cursor.rowcount # Consideramos como 'atualizados' os registros substituídos
        
        for _, row in df.iterrows():
            sku = row['sku']
            if sku not in skus_cadastrados:
                # Gravar como Pendente (SKU Amazon sem correspondente Original)
                d_p = {
                    'marketplace_origem': marketplace, 'loja_origem': loja, 'numero_pedido': row['pedido'],
                    'data_venda': data_ini, 'sku': sku, 'codigo_anuncio': row['asin'], 'quantidade': row['qtd'],
                    'preco_venda': row['receita']/row['qtd'], 'valor_venda_efetivo': row['receita'],
                    'imposto': row['imposto'], 'comissao': row['comissao'], 'frete': row['frete'],
                    'total_tarifas': row['comissao'] + row['frete'] + row['taxa_fixa'],
                    'valor_liquido': row['receita'] - (row['comissao'] + row['frete'] + row['taxa_fixa'] + row['imposto']),
                    'arquivo_origem': arq_nome, 'motivo': 'SKU Amazon não mapeado'
                }
                if gravar_venda_pendente(cursor, d_p): pend += 1
                else: err += 1
                continue

            # Gravação Normal
            sql = """INSERT INTO fact_vendas_snapshot (marketplace_origem, loja_origem, numero_pedido, data_venda, sku, codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao, frete, tarifa_fixa, total_tarifas, valor_liquido, margem_total, margem_percentual, data_processamento, arquivo_origem)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)"""
            
            cursor.execute(sql, (
                marketplace, loja, row['pedido'], data_ini, sku, row['asin'], row['qtd'], 
                row['receita']/row['qtd'], row['receita'], row['custo']/row['qtd'], row['custo'],
                row['imposto'], row['comissao'], row['frete'], row['taxa_fixa'],
                row['comissao']+row['frete']+row['taxa_fixa'], row['receita']-(row['comissao']+row['frete']+row['taxa_fixa']+row['imposto']),
                row['margem'], row['margem_pct'], arq_nome
            ))
            reg += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro na gravação Amazon: {e}")
        err = len(df)
    finally:
        cursor.close()
        conn.close()

    return reg, err, inv, dups, pend, desc, atualiz