"""
DATABASE UTILS - Sistema Nala
Funções para conexão e queries no banco de dados

VERSÃO FINAL: Com estrutura REAL do banco
CORREÇÃO 09/03/2026: gravar_log_upload converte datas dd/mm/aaaa para aaaa-mm-dd
CORREÇÃO 10/03/2026: buscar_custos_skus agora lê de dim_produtos.preco_a_ser_considerado
CORREÇÃO 10/03/2026: buscar_skus_validos agora lê de dim_produtos (onde gestao_skus cadastra)

VERSÃO 2.0 (10/03/2026):
  - buscar_duplicatas_loja(): pré-carrega pedidos existentes para evitar reimportação
  - gravar_venda_pendente(): salva vendas com SKU não cadastrado
  - buscar_pendentes(): lista vendas pendentes para reprocessamento
  - reprocessar_pendentes_por_sku(): reprocessa pendentes após cadastro de SKU
  - recalcular_curva_abc(): calcula Curva ABC (Pareto) dos anúncios
  - buscar_custo_flex(): busca custo FLEX da dim_lojas (antes hardcoded)
"""

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

# URL do banco Neon
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"


def get_engine():
    """Retorna engine do SQLAlchemy"""
    return create_engine(DB_URL)


# ============================================================
# FUNÇÕES ORIGINAIS (preservadas)
# ============================================================

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


# ============================================================
# NOVAS FUNÇÕES v2.0 - PROTEÇÃO DUPLICATAS
# ============================================================

def buscar_duplicatas_loja(engine, loja):
    """
    Carrega set de (numero_pedido, sku) já existentes para uma loja.
    Usado para pré-verificação antes do loop de gravação.

    Args:
        engine: SQLAlchemy engine
        loja: nome da loja (ex: 'ML-Nala')

    Retorna:
        set de tuplas (numero_pedido, sku)
    """
    query = """
        SELECT numero_pedido, sku
        FROM fact_vendas_snapshot
        WHERE loja_origem = %s
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (loja,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {(str(r[0]), str(r[1])) for r in rows}
    except Exception as e:
        st.warning(f"Aviso: não foi possível carregar duplicatas existentes: {e}")
        return set()


# ============================================================
# NOVAS FUNÇÕES v2.0 - VENDAS PENDENTES
# ============================================================

def gravar_venda_pendente(cursor, dados):
    """
    Grava uma venda pendente (SKU não cadastrado) no banco.
    Usa SAVEPOINT para não comprometer outras gravações.

    Args:
        cursor: cursor do banco (já dentro de uma transação aberta)
        dados: dict com os campos da venda

    Retorna:
        True se gravou, False se erro (duplicata ou outro)
    """
    sql = """
        INSERT INTO fact_vendas_pendentes (
            marketplace_origem, loja_origem, numero_pedido, data_venda,
            sku, codigo_anuncio, quantidade, preco_venda,
            desconto_parceiro, desconto_marketplace, valor_venda_efetivo,
            imposto, comissao, frete, tarifa_fixa, outros_custos,
            total_tarifas, valor_liquido, arquivo_origem,
            data_processamento, status, motivo
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, NOW(), 'Pendente', 'SKU não cadastrado'
        )
        ON CONFLICT (numero_pedido, sku, loja_origem) DO NOTHING
    """
    try:
        cursor.execute(sql, (
            dados['marketplace_origem'],
            dados['loja_origem'],
            dados['numero_pedido'],
            dados['data_venda'],
            dados['sku'],
            dados.get('codigo_anuncio', ''),
            dados.get('quantidade', 1),
            dados.get('preco_venda', 0),
            dados.get('desconto_parceiro', 0),
            dados.get('desconto_marketplace', 0),
            dados.get('valor_venda_efetivo', 0),
            dados.get('imposto', 0),
            dados.get('comissao', 0),
            dados.get('frete', 0),
            dados.get('tarifa_fixa', 0),
            dados.get('outros_custos', 0),
            dados.get('total_tarifas', 0),
            dados.get('valor_liquido', 0),
            dados.get('arquivo_origem', ''),
        ))
        return True
    except Exception:
        return False


def buscar_pendentes(engine, sku=None, marketplace=None, status='Pendente'):
    """
    Lista vendas pendentes do banco.

    Args:
        engine: SQLAlchemy engine
        sku: filtrar por SKU específico (opcional)
        marketplace: filtrar por marketplace (opcional)
        status: 'Pendente', 'Reprocessado' ou 'Todos'

    Retorna:
        DataFrame com as vendas pendentes
    """
    query = "SELECT * FROM fact_vendas_pendentes WHERE 1=1"
    params = []

    if status != 'Todos':
        query += " AND status = %s"
        params.append(status)

    if sku:
        query += " AND sku = %s"
        params.append(sku)

    if marketplace:
        query += " AND marketplace_origem = %s"
        params.append(marketplace)

    query += " ORDER BY data_processamento DESC"

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao buscar vendas pendentes: {e}")
        return pd.DataFrame()


def buscar_pendentes_resumo(engine):
    """
    Retorna resumo das vendas pendentes agrupadas por SKU.
    Mostra: sku, total de vendas, receita total, marketplaces, primeiro arquivo.

    Retorna:
        DataFrame com resumo por SKU
    """
    query = """
        SELECT
            sku,
            COUNT(*) as total_vendas,
            SUM(valor_venda_efetivo) as receita_total,
            STRING_AGG(DISTINCT marketplace_origem, ', ') as marketplaces,
            STRING_AGG(DISTINCT loja_origem, ', ') as lojas,
            MIN(data_venda) as primeira_venda,
            MAX(data_venda) as ultima_venda
        FROM fact_vendas_pendentes
        WHERE status = 'Pendente'
        GROUP BY sku
        ORDER BY total_vendas DESC
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar resumo de pendentes: {e}")
        return pd.DataFrame()


def reprocessar_pendentes_por_sku(engine, sku):
    """
    Reprocessa vendas pendentes de um SKU específico.
    1. Verifica se SKU existe em dim_produtos
    2. Busca custo de dim_produtos.preco_a_ser_considerado
    3. Calcula campos de custo e margem
    4. Insere em fact_vendas_snapshot (com proteção de duplicata)
    5. Marca pendentes como 'Reprocessado'

    Args:
        engine: SQLAlchemy engine
        sku: SKU a reprocessar

    Retorna:
        dict com resultado: {'sucesso': int, 'erros': int, 'mensagem': str}
    """
    # 1. Verificar se SKU existe
    skus_validos = buscar_skus_validos(engine)
    if sku not in skus_validos:
        return {
            'sucesso': 0,
            'erros': 0,
            'mensagem': f"SKU '{sku}' ainda não cadastrado em dim_produtos."
        }

    # 2. Buscar custo
    custos = buscar_custos_skus(engine)
    custo_unit = custos.get(sku, 0)

    # 3. Buscar vendas pendentes deste SKU
    df_pendentes = buscar_pendentes(engine, sku=sku, status='Pendente')
    if df_pendentes.empty:
        return {
            'sucesso': 0,
            'erros': 0,
            'mensagem': f"Nenhuma venda pendente encontrada para SKU '{sku}'."
        }

    # 4. Inserir no fact_vendas_snapshot
    conn = engine.raw_connection()
    cursor = conn.cursor()

    sucesso = 0
    erros = 0
    ids_reprocessados = []

    sql_insert = """
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

    for _, row in df_pendentes.iterrows():
        try:
            qtd = int(row['quantidade'])
            custo_total = custo_unit * qtd
            valor_liq = float(row['valor_liquido'])
            receita = float(row['valor_venda_efetivo'])

            # Margem = valor_liquido - custo_total
            margem_total = valor_liq - custo_total
            margem_pct = (margem_total / receita * 100) if receita > 0 else 0

            preco_venda = float(row['preco_venda'])

            cursor.execute(f"SAVEPOINT sp_repro_{row['id']}")

            cursor.execute(sql_insert, (
                row['marketplace_origem'],
                row['loja_origem'],
                row['numero_pedido'],
                row['data_venda'],
                sku,
                row.get('codigo_anuncio', ''),
                qtd,
                preco_venda,
                float(row.get('desconto_parceiro', 0)),
                float(row.get('desconto_marketplace', 0)),
                receita,
                custo_unit,
                custo_total,
                float(row['imposto']),
                float(row['comissao']),
                float(row['frete']),
                float(row.get('tarifa_fixa', 0)),
                float(row.get('outros_custos', 0)),
                float(row['total_tarifas']),
                valor_liq,
                margem_total,
                margem_pct,
                row.get('arquivo_origem', ''),
            ))

            cursor.execute(f"RELEASE SAVEPOINT sp_repro_{row['id']}")
            ids_reprocessados.append(int(row['id']))
            sucesso += 1

        except Exception as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT sp_repro_{row['id']}")
            erros += 1

    # 5. Marcar como reprocessado
    if ids_reprocessados:
        placeholders = ','.join(['%s'] * len(ids_reprocessados))
        cursor.execute(
            f"UPDATE fact_vendas_pendentes SET status = 'Reprocessado' WHERE id IN ({placeholders})",
            ids_reprocessados
        )

    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return {
            'sucesso': 0,
            'erros': len(df_pendentes),
            'mensagem': f"Erro no commit: {e}"
        }

    cursor.close()
    conn.close()

    msg = f"SKU '{sku}': {sucesso} venda(s) gravada(s) com sucesso"
    if erros > 0:
        msg += f", {erros} erro(s) (possíveis duplicatas)"

    return {
        'sucesso': sucesso,
        'erros': erros,
        'mensagem': msg
    }


# ============================================================
# NOVAS FUNÇÕES v2.0 - CURVA ABC
# ============================================================

def recalcular_curva_abc(engine, dias=30):
    """
    Recalcula Curva ABC dos anúncios com base nas vendas dos últimos N dias.
    Pareto clássico: A = 80% da receita, B = próximos 15%, C = últimos 5%.

    Atualiza dim_config_marketplace.tag_curva via UPSERT.
    Não altera tag_status (é manual).

    Args:
        engine: SQLAlchemy engine
        dias: janela de cálculo (padrão 30)

    Retorna:
        dict com resultado: {'total_anuncios': int, 'a': int, 'b': int, 'c': int}
    """
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    # 1. Agregar receita por anúncio
    query = """
        SELECT
            marketplace_origem,
            codigo_anuncio,
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

        # 2. Calcular percentual acumulado
        receita_total = df['receita_total'].sum()
        if receita_total <= 0:
            cursor.close()
            conn.close()
            return {'total_anuncios': 0, 'a': 0, 'b': 0, 'c': 0}

        df['pct_acumulado'] = df['receita_total'].cumsum() / receita_total * 100

        # 3. Classificar ABC
        df['curva'] = 'C'
        df.loc[df['pct_acumulado'] <= 80, 'curva'] = 'A'
        df.loc[(df['pct_acumulado'] > 80) & (df['pct_acumulado'] <= 95), 'curva'] = 'B'

        # 4. UPSERT no banco (preserva tag_status e observacoes existentes)
        sql_upsert = """
            INSERT INTO dim_config_marketplace (marketplace, codigo_anuncio, tag_curva, data_atualizacao)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (marketplace, codigo_anuncio)
            DO UPDATE SET tag_curva = EXCLUDED.tag_curva, data_atualizacao = NOW()
        """

        for _, row in df.iterrows():
            cursor.execute(sql_upsert, (
                row['marketplace_origem'],
                row['codigo_anuncio'],
                row['curva']
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
        st.warning(f"Aviso: erro ao recalcular Curva ABC: {e}")
        return {'total_anuncios': 0, 'a': 0, 'b': 0, 'c': 0}


# ============================================================
# NOVAS FUNÇÕES v2.0 - CONFIGURAÇÃO FLEX
# ============================================================

def buscar_custo_flex(engine, loja):
    """
    Busca custo FLEX configurado para a loja na dim_lojas.
    Retorna None se não houver config (processador deve usar fallback).

    Args:
        engine: SQLAlchemy engine
        loja: nome da loja

    Retorna:
        float ou None
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT custo_flex FROM dim_lojas WHERE loja = %s",
            (loja,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row and row[0] is not None:
            return float(row[0])
        return None

    except Exception:
        return None
