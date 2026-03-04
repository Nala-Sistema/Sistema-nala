"""
DATABASE UTILS - Sistema Nala
Funções para conexão e queries no PostgreSQL (Neon)
"""

from sqlalchemy import create_engine

# URL do banco Neon
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"


def get_engine():
    """
    Retorna engine SQLAlchemy conectada ao banco Neon.
    """
    return create_engine(DB_URL)


def buscar_custos_skus(engine):
    """
    Busca custos de todos os SKUs ativos.
    Retorna dicionário: {sku: custo_final}
    """
    import pandas as pd
    
    query = """
        SELECT s.sku, COALESCE(c.custo_final, 0) as custo 
        FROM dim_skus s 
        LEFT JOIN dim_produtos_custos c ON s.sku = c.sku 
        WHERE s.ativo = TRUE
    """
    
    df_custos = pd.read_sql(query, engine)
    return df_custos.set_index('sku')['custo'].to_dict()


def buscar_skus_validos(engine):
    """
    Retorna set de SKUs válidos (ativos no sistema).
    """
    import pandas as pd
    
    df_skus = pd.read_sql("SELECT sku FROM dim_skus WHERE ativo = TRUE", engine)
    return set(df_skus['sku'].tolist())


def verificar_duplicatas(engine, marketplace, loja, numero_pedido, sku):
    """
    Verifica se venda já existe no banco.
    Retorna True se for duplicata.
    """
    import pandas as pd
    
    query = f"""
        SELECT COUNT(*) as total 
        FROM fact_vendas_snapshot 
        WHERE marketplace_origem = '{marketplace}' 
        AND loja_origem = '{loja}'
        AND numero_pedido = '{numero_pedido}'
        AND sku = '{sku}'
    """
    
    resultado = pd.read_sql(query, engine)
    return resultado['total'].iloc[0] > 0


def gravar_log_upload(engine, info_dict):
    """
    Grava registro no log_uploads.
    info_dict deve conter: usuario, marketplace, loja, arquivo_nome, 
    periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro
    """
    from datetime import datetime
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    try:
        sql = """
            INSERT INTO log_uploads 
            (data_upload, usuario, marketplace, loja, arquivo_nome, 
             periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            info_dict.get('usuario', 'Sistema'),
            info_dict.get('marketplace'),
            info_dict.get('loja'),
            info_dict.get('arquivo_nome'),
            info_dict.get('periodo_inicio'),
            info_dict.get('periodo_fim'),
            info_dict.get('total_linhas', 0),
            info_dict.get('linhas_importadas', 0),
            info_dict.get('linhas_erro', 0),
            'Concluído'
        ))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Erro ao gravar log: {e}")
        return False
    finally:
        cursor.close()
        conn.close()
