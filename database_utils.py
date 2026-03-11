"""
DATABASE UTILS - Sistema Nala
Versão: 2.2 (11/03/2026)
Ajustada para a nova tabela dim_tags_anuncio e regras de preço estratégico.
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
# FUNÇÕES DE CONVERSÃO E BUSCA BÁSICA
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
    """
    Busca custos dos SKUs priorizando dim_produtos.preco_a_ser_considerado.
    """
    query = """
        SELECT 
            p.sku,
            COALESCE(
                NULLIF(p.preco_a_ser_considerado, 0),
                NULLIF(pc.preco_compra + pc.embalagem + pc.mdo + pc.custo_ads, 0),
                pc.preco_compra,
                0
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
    """Busca lista de SKUs ativos em dim_produtos"""
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
    """Grava log de upload com conversão de data BR"""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO log_uploads (
                data_upload, marketplace, loja, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas,
                linhas_importadas, linhas_erro, status
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    """Verifica pedidos já existentes para evitar reimportação"""
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
# CURVA ABC (RECALCULO) - MIGRADO PARA dim_tags_anuncio
# ============================================================

def recalcular_curva_abc(engine, dias=30):
    """
    Recalcula Curva ABC dos anúncios via Pareto.
    Agora grava na tabela dedicada: dim_tags_anuncio.
    """
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    # Agregação incluindo o SKU para enriquecer a tabela de tags
    query = """
        SELECT
            marketplace_origem,
            codigo_anuncio,
            MAX(sku) as sku,
            SUM(valor_venda_efetivo) as receita_total
        FROM fact_vendas_snapshot
        WHERE data_venda >= %s
          AND codigo_anuncio IS NOT NULL
          AND TRIM(codigo_anuncio) != ''
        GROUP BY marketplace_origem, codigo_anuncio
        ORDER BY receita_total DESC
    """

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (data_corte,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        if not rows:
            cursor.close()
            conn.close()
            return {'total_anuncios': 0, 'a': 0, 'b': 0, 'c': 0}

        df = pd.DataFrame(rows, columns=colunas)
        receita_total = df['receita_total'].sum()
        
        if receita_total <= 0:
            cursor.close()
            conn.close()
            return {'total_anuncios': 0}

        # Pareto
        df['pct_acumulado'] = df['receita_total'].cumsum() / receita_total * 100
        df['curva'] = 'C'
        df.loc[df['pct_acumulado'] <= 80, 'curva'] = 'A'
        df.loc[(df['pct_acumulado'] > 80) & (df['pct_acumulado'] <= 95), 'curva'] = 'B'

        # UPSERT na nova tabela dim_tags_anuncio
        sql_upsert = """
            INSERT INTO dim_tags_anuncio (marketplace, codigo_anuncio, sku, tag_curva, data_atualizacao)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (marketplace, codigo_anuncio)
            DO UPDATE SET tag_curva = EXCLUDED.tag_curva, 
                          sku = COALESCE(EXCLUDED.sku, dim_tags_anuncio.sku),
                          data_atualizacao = NOW()
        """

        for _, row in df.iterrows():
            cursor.execute(sql_upsert, (
                row['marketplace_origem'], row['codigo_anuncio'], 
                row['sku'], row['curva']
            ))

        conn.commit()
        cursor.close()
        conn.close()

        contagem = df['curva'].value_counts().to_dict()
        return {
            'total_anuncios': len(df),
            'a': contagem.get('A', 0),
            'b': contagem.get('B', 0),
            'c': contagem.get('C', 0),
        }

    except Exception as e:
        st.warning(f"Erro no recálculo ABC: {e}")
        return {'total_anuncios': 0}

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
    except Exception:
        return None
