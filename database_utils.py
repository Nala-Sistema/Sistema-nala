"""
DATABASE UTILS - Sistema Nala
Funções para conexão e queries no banco de dados
VERSÃO ATUALIZADA: Com force_refresh para evitar cache
"""

from sqlalchemy import create_engine
import pandas as pd
import streamlit as st

# URL do banco Neon
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"


def get_engine():
    """Retorna engine do SQLAlchemy"""
    return create_engine(DB_URL)


def buscar_custos_skus(engine, force_refresh=None):
    """
    Busca custos de SKUs do banco.
    
    Args:
        engine: SQLAlchemy engine
        force_refresh: Timestamp para forçar refresh (evita cache Streamlit)
    
    Retorna:
        dict {sku: preco_a_ser_considerado}
    """
    # O parâmetro force_refresh força query nova (evita cache)
    query = """
        SELECT 
            p.sku,
            COALESCE(pc.preco_compra, 0) as preco_compra,
            COALESCE(pc.embalagem, 0) as embalagem,
            COALESCE(pc.mdo, 0) as mdo,
            COALESCE(pc.custo_ads, 0) as custo_ads,
            COALESCE(pc.custo_final, 0) as custo_final
        FROM dim_produtos p
        LEFT JOIN dim_produtos_custos pc ON p.sku = pc.sku
        WHERE p.ativo = TRUE
    """
    
    try:
        df = pd.read_sql(query, engine)
        
        # Retornar dict {sku: custo_final}
        # Se custo_final = 0, usar preco_compra
        custos_dict = {}
        for _, row in df.iterrows():
            custo = row['custo_final'] if row['custo_final'] > 0 else row['preco_compra']
            custos_dict[row['sku']] = custo
        
        return custos_dict
        
    except Exception as e:
        st.error(f"Erro ao buscar custos: {e}")
        return {}


def buscar_skus_validos(engine):
    """
    Busca lista de SKUs válidos do banco.
    
    Retorna:
        set de SKUs ativos
    """
    query = "SELECT sku FROM dim_produtos WHERE ativo = TRUE"
    
    try:
        df = pd.read_sql(query, engine)
        return set(df['sku'].tolist())
    except Exception as e:
        st.error(f"Erro ao buscar SKUs: {e}")
        return set()


def gravar_log_upload(engine, info):
    """
    Grava log de upload no banco.
    
    Args:
        engine: SQLAlchemy engine
        info: dict com dados do upload
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        sql = """
            INSERT INTO log_uploads (
                data_upload, marketplace, loja, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas,
                linhas_importadas, linhas_erro, status
            ) VALUES (
                NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        cursor.execute(sql, (
            info.get('marketplace'),
            info.get('loja'),
            info.get('arquivo_nome'),
            info.get('periodo_inicio'),
            info.get('periodo_fim'),
            info.get('total_linhas'),
            info.get('linhas_importadas'),
            info.get('linhas_erro'),
            'SUCESSO' if info.get('linhas_importadas', 0) > 0 else 'ERRO'
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.warning(f"Erro ao gravar log: {e}")
