"""
DATABASE UTILS - Sistema Nala
Versão: 2.3 (11/03/2026) - RESTAURAÇÃO DE FUNÇÕES PENDENTES
"""

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

# URL do banco Neon
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    """Retorna engine do SQLAlchemy"""
    return create_engine(DB_URL)

# ============================================================
# CONVERSORES E BUSCAS BÁSICAS
# ============================================================

def _converter_data_br_para_banco(data_str):
    """Converte dd/mm/aaaa para aaaa-mm-dd"""
    if not data_str or str(data_str).strip() == '':
        return None
    try:
        return datetime.strptime(str(data_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return str(data_str).strip()

def buscar_custos_skus(engine, force_refresh=None):
    """Busca custos priorizando dim_produtos.preco_a_ser_considerado"""
    query = """
        SELECT p.sku,
            COALESCE(
                NULLIF(p.preco_a_ser_considerado, 0),
                NULLIF(pc.preco_compra + pc.embalagem + pc.mdo + pc.custo_ads, 0),
                pc.preco_compra, 0
            ) as custo
        FROM dim_produtos p
        LEFT JOIN dim_produtos_custos pc ON p.sku = pc.sku
        WHERE p.status = 'Ativo'
    """
    try:
        df = pd.read_sql(query, engine)
        return {row['sku']: float(row['custo']) for _, row in df.iterrows()}
    except Exception as e:
        st.error(f"Erro ao buscar custos: {e}")
        return {}

def buscar_skus_validos(engine):
    """Busca set de SKUs ativos em dim_produtos"""
    query = "SELECT sku FROM dim_produtos WHERE status = 'Ativo'"
    try:
        df = pd.read_sql(query, engine)
        return set(df['sku'].tolist())
    except Exception as e:
        st.error(f"Erro ao buscar SKUs: {e}")
        return set()

# ============================================================
# LOGS E DUPLICATAS
# ============================================================

def gravar_log_upload(engine, info):
    """Grava log de upload no banco"""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO log_uploads (data_upload, marketplace, loja, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            info.get('marketplace'), info.get('loja'), info.get('arquivo_nome'),
            _converter_data_br_para_banco(info.get('periodo_inicio')),
            _converter_data_br_para_banco(info.get('periodo_fim')),
            info.get('total_linhas'), info.get('linhas_importadas'),
            info.get('linhas_erro'), 'SUCESSO' if info.get('linhas_importadas', 0) > 0 else 'ERRO'
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Erro ao gravar log: {e}")

def buscar_duplicatas_loja(engine, loja):
    """Carrega set de (pedido, sku) existentes para evitar duplicatas"""
    query = "SELECT numero_pedido, sku FROM fact_vendas_snapshot WHERE loja_origem = %s"
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (loja,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {(str(r[0]), str(r[1])) for r in rows}
    except Exception:
        return set()

# ============================================================
# VENDAS PENDENTES (RESTAURADO)
# ============================================================

def gravar_venda_pendente(cursor, dados):
    """Grava venda com SKU não cadastrado na fact_vendas_pendentes"""
    sql = """
        INSERT INTO fact_vendas_pendentes (
            marketplace_origem, loja_origem, numero_pedido, data_venda, sku, 
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo,
            imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas,
            valor_liquido, arquivo_origem, data_processamento, status, motivo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'Pendente', 'SKU não cadastrado')
        ON CONFLICT (numero_pedido, sku, loja_origem) DO NOTHING
    """
    try:
        cursor.execute(sql, (
            dados['marketplace_origem'], dados['loja_origem'], dados['numero_pedido'],
            dados['data_venda'], dados['sku'], dados.get('codigo_anuncio', ''),
            dados.get('quantidade', 1), dados.get('preco_venda', 0), dados.get('valor_venda_efetivo', 0),
            dados.get('imposto', 0), dados.get('comissao', 0), dados.get('frete', 0),
            dados.get('tarifa_fixa', 0), dados.get('outros_custos', 0), dados.get('total_tarifas', 0),
            dados.get('valor_liquido', 0), dados.get('arquivo_origem', '')
        ))
        return True
    except Exception:
        return False

def buscar_pendentes(engine, sku=None, marketplace=None, status='Pendente'):
    """Lista vendas pendentes do banco"""
    query = "SELECT * FROM fact_vendas_pendentes WHERE 1=1"
    params = []
    if status != 'Todos':
        query += " AND status = %s"; params.append(status)
    if sku:
        query += " AND sku = %s"; params.append(sku)
    if marketplace:
        query += " AND marketplace_origem = %s"; params.append(marketplace)
    query += " ORDER BY data_processamento DESC"
    try:
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        st.error(f"Erro ao buscar pendentes: {e}")
        return pd.DataFrame()

def buscar_pendentes_resumo(engine):
    """Retorna resumo de pendentes por SKU"""
    query = """
        SELECT sku, COUNT(*) as total_vendas, SUM(valor_venda_efetivo) as receita_total,
               STRING_AGG(DISTINCT marketplace_origem, ', ') as marketplaces,
               MIN(data_venda) as primeira_venda
        FROM fact_vendas_pendentes WHERE status = 'Pendente'
        GROUP BY sku ORDER BY total_vendas DESC
    """
    try: return pd.read_sql(query, engine)
    except Exception: return pd.DataFrame()

def reprocessar_pendentes_por_sku(engine, sku):
    """Reprocessa vendas pendentes após cadastro de SKU"""
    skus_validos = buscar_skus_validos(engine)
    if sku not in skus_validos: return {'sucesso': 0, 'erros': 0, 'mensagem': "SKU não cadastrado."}
    
    custo_unit = buscar_custos_skus(engine).get(sku, 0)
    df_pendentes = buscar_pendentes(engine, sku=sku, status='Pendente')
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    sucesso, erros, ids_repro = 0, 0, []

    sql_ins = """
        INSERT INTO fact_vendas_snapshot (marketplace_origem, loja_origem, numero_pedido, data_venda, sku,
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo, custo_unitario, custo_total,
            imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido,
            margem_total, margem_percentual, data_processamento, arquivo_origem)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
    """
    for _, row in df_pendentes.iterrows():
        try:
            cursor.execute(f"SAVEPOINT repro_{row['id']}")
            receita = float(row['valor_venda_efetivo'])
            custo_total = custo_unit * int(row['quantidade'])
            margem_total = float(row['valor_liquido']) - custo_total
            margem_pct = (margem_total / receita * 100) if receita > 0 else 0
            
            cursor.execute(sql_ins, (
                row['marketplace_origem'], row['loja_origem'], row['numero_pedido'], row['data_venda'], sku,
                row['codigo_anuncio'], row['quantidade'], row['preco_venda'], receita, custo_unit, custo_total,
                row['imposto'], row['comissao'], row['frete'], row['tarifa_fixa'], row['outros_custos'],
                row['total_tarifas'], row['valor_liquido'], margem_total, margem_pct, row['arquivo_origem']
            ))
            ids_repro.append(int(row['id'])); sucesso += 1
        except Exception:
            cursor.execute(f"ROLLBACK TO SAVEPOINT repro_{row['id']}"); erros += 1

    if ids_repro:
        cursor.execute(f"UPDATE fact_vendas_pendentes SET status = 'Reprocessado' WHERE id IN ({','.join(['%s']*len(ids_repro))})", ids_repro)
    
    conn.commit()
    cursor.close(); conn.close()
    return {'sucesso': sucesso, 'erros': erros, 'mensagem': f"Reprocessado: {sucesso} sucessos."}

# ============================================================
# CURVA ABC (PARETO) - NOVO MODELO dim_tags_anuncio
# ============================================================

def recalcular_curva_abc(engine, dias=30):
    """Recalcula Curva ABC dos anúncios na tabela dim_tags_anuncio"""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
    query = """
        SELECT marketplace_origem, codigo_anuncio, MAX(sku) as sku, SUM(valor_venda_efetivo) as receita_total
        FROM fact_vendas_snapshot WHERE data_venda >= %s AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
        GROUP BY marketplace_origem, codigo_anuncio ORDER BY receita_total DESC
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (data_corte,))
        df = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
        if df.empty: return {'total_anuncios': 0}

        receita_total = df['receita_total'].sum()
        df['pct_acumulado'] = df['receita_total'].cumsum() / receita_total * 100
        df['curva'] = 'C'
        df.loc[df['pct_acumulado'] <= 80, 'curva'] = 'A'
        df.loc[(df['pct_acumulado'] > 80) & (df['pct_acumulado'] <= 95), 'curva'] = 'B'

        sql_upsert = """
            INSERT INTO dim_tags_anuncio (marketplace, codigo_anuncio, sku, tag_curva, data_atualizacao)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (marketplace, codigo_anuncio)
            DO UPDATE SET tag_curva = EXCLUDED.tag_curva, 
                          sku = COALESCE(EXCLUDED.sku, dim_tags_anuncio.sku), data_atualizacao = NOW()
        """
        for _, row in df.iterrows():
            cursor.execute(sql_upsert, (row['marketplace_origem'], row['codigo_anuncio'], row['sku'], row['curva']))
        conn.commit()
        cursor.close(); conn.close()
        return {'total_anuncios': len(df)}
    except Exception: return {'total_anuncios': 0}

# ============================================================
# CONFIGURAÇÃO FLEX
# ============================================================

def buscar_custo_flex(engine, loja):
    """Busca custo_flex configurado na dim_lojas"""
    try:
        with engine.connect() as conn:
            query = text("SELECT custo_flex FROM dim_lojas WHERE loja = :loja")
            res = conn.execute(query, {"loja": loja}).fetchone()
            return float(res[0]) if res and res[0] is not None else None
    except Exception: return None
