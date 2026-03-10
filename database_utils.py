"""
DATABASE UTILS - Sistema Nala
Funções para conexão e queries no banco de dados
VERSÃO FINAL: Com estrutura REAL do banco
CORREÇÃO 09/03/2026: gravar_log_upload converte datas dd/mm/aaaa para aaaa-mm-dd
CORREÇÃO 10/03/2026: buscar_custos_skus agora lê de dim_produtos.preco_a_ser_considerado
CORREÇÃO 10/03/2026: buscar_skus_validos agora lê de dim_produtos (onde gestao_skus cadastra)
"""

from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import streamlit as st

# URL do banco Neon
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"


def get_engine():
    """Retorna engine do SQLAlchemy"""
    return create_engine(DB_URL)


def _converter_data_br_para_banco(data_str):
    """
    Converte data do formato brasileiro (dd/mm/aaaa) para formato banco (aaaa-mm-dd).
    Se receber None ou string vazia, retorna None.
    """
    if not data_str or str(data_str).strip() == '':
        return None
    try:
        return datetime.strptime(str(data_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return str(data_str).strip()


def buscar_custos_skus(engine, force_refresh=None):
    """
    Busca custos de SKUs do banco.
    
    FONTE PRINCIPAL: dim_produtos.preco_a_ser_considerado
    (atualizado pelo módulo de compras e gestão de SKUs)
    
    FALLBACK: dim_produtos_custos (soma dos componentes de custo)
    Só usado se preco_a_ser_considerado estiver zerado ou nulo.

    Args:
        engine: SQLAlchemy engine
        force_refresh: Timestamp para forçar refresh (evita cache Streamlit)

    Retorna:
        dict {sku: custo}
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

        custos_dict = {}
        for _, row in df.iterrows():
            custos_dict[row['sku']] = float(row['custo'])

        return custos_dict

    except Exception as e:
        st.error(f"Erro ao buscar custos: {e}")
        return {}


def buscar_skus_validos(engine):
    """
    Busca lista de SKUs válidos do banco.
    CORREÇÃO: Busca de dim_produtos (onde gestao_skus.py cadastra)
    em vez de dim_skus.

    Retorna:
        set de SKUs ativos
    """
    query = "SELECT sku FROM dim_produtos WHERE status = 'Ativo'"

    try:
        df = pd.read_sql(query, engine)
        return set(df['sku'].tolist())
    except Exception as e:
        st.error(f"Erro ao buscar SKUs: {e}")
        return set()


def gravar_log_upload(engine, info):
    """
    Grava log de upload no banco.
    Converte datas de dd/mm/aaaa para aaaa-mm-dd antes de gravar.

    Args:
        engine: SQLAlchemy engine
        info: dict com dados do upload
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Converter datas do formato BR para formato banco
        periodo_inicio = _converter_data_br_para_banco(info.get('periodo_inicio'))
        periodo_fim = _converter_data_br_para_banco(info.get('periodo_fim'))

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
            periodo_inicio,
            periodo_fim,
            info.get('total_linhas'),
            info.get('linhas_importadas'),
            info.get('linhas_erro'),
            'SUCESSO' if info.get('linhas_importadas', 0) > 0 else 'ERRO'
        ))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"Erro ao gravar log de importação: {e}")
