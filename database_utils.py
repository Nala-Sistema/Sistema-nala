"""
DATABASE UTILS - Sistema Nala
Versão: 3.2 (16/03/2026)

CHANGELOG v3.2:
  - FIX: buscar_pendentes_por_tipo() — filtro agora usa ILIKE para pegar variações
         de motivo relacionadas a SKU (ex: 'SKU não cadastrado', 'SKU Amazon não mapeado')
         Isso corrige a tabela vazia na tela de Vendas Pendentes para Amazon.
  - FIX: buscar_pendentes_revisados() — nova função que retorna list of dicts
         corrigindo o erro "List argument must consist only of tuples or Dictionaries"
  - MELHORIA: buscar_pendentes() agora aceita motivo_like para buscas flexíveis

CHANGELOG v3.1:
  - NOVO: buscar_mapeamento_skus() — carrega tabela de→para de SKUs
  - NOVO: gravar_mapeamento_sku() — salva correção de SKU para imports futuros
  - NOVO: buscar_pendentes_por_tipo() — filtra pendentes por SKU ou Divergência
  - NOVO: reprocessar_pendentes_manual() — reprocessa pendentes editados manualmente

CHANGELOG v3.0:
  - NOVO: gravar_venda_descartada() — grava em fact_vendas_descartadas
  - NOVO: deletar_venda_snapshot() — remove venda (para mudança de status na reimportação)
  - AJUSTE: gravar_venda_pendente() — motivo agora é dinâmico (via dados['motivo'])
  - Todas as funções anteriores mantidas intactas
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
# VENDAS PENDENTES
# ============================================================

def gravar_venda_pendente(cursor, dados):
    """
    Grava venda com SKU não cadastrado ou divergência na fact_vendas_pendentes.

    VERSÃO 3.0: motivo agora é dinâmico — lê de dados['motivo'].
    Se não informado, usa 'SKU não cadastrado' (compatível com chamadas existentes).
    """
    # Motivo dinâmico: permite 'SKU não cadastrado', 'Divergência financeira', etc.
    motivo = dados.get('motivo', 'SKU não cadastrado')

    sql = """
        INSERT INTO fact_vendas_pendentes (
            marketplace_origem, loja_origem, numero_pedido, data_venda, sku, 
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo,
            imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas,
            valor_liquido, arquivo_origem, data_processamento, status, motivo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'Pendente', %s)
        ON CONFLICT (numero_pedido, sku, loja_origem) DO NOTHING
    """
    try:
        cursor.execute(sql, (
            dados['marketplace_origem'], dados['loja_origem'], dados['numero_pedido'],
            dados['data_venda'], dados['sku'], dados.get('codigo_anuncio', ''),
            dados.get('quantidade', 1), dados.get('preco_venda', 0), dados.get('valor_venda_efetivo', 0),
            dados.get('imposto', 0), dados.get('comissao', 0), dados.get('frete', 0),
            dados.get('tarifa_fixa', 0), dados.get('outros_custos', 0), dados.get('total_tarifas', 0),
            dados.get('valor_liquido', 0), dados.get('arquivo_origem', ''),
            motivo
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
    """
    Retorna resumo de pendentes por SKU.
    CORREÇÃO: Incluída a agregação de lojas para evitar KeyError.
    """
    query = """
        SELECT 
            sku, 
            COUNT(*) as total_vendas, 
            SUM(valor_venda_efetivo) as receita_total,
            STRING_AGG(DISTINCT marketplace_origem, ', ') as marketplaces,
            STRING_AGG(DISTINCT loja_origem, ', ') as lojas,
            MIN(data_venda) as primeira_venda
        FROM fact_vendas_pendentes 
        WHERE status = 'Pendente'
        GROUP BY sku 
        ORDER BY total_vendas DESC
    """
    try: 
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Erro ao buscar resumo de pendentes: {e}")
        return pd.DataFrame()

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
# VENDAS DESCARTADAS (NOVO v3.0)
# ============================================================

def gravar_venda_descartada(cursor, dados):
    """
    Grava venda descartada (cancelada/devolvida/mediação) em fact_vendas_descartadas.
    Usa cursor já aberto (dentro da transação existente).
    """
    sql = """
        INSERT INTO fact_vendas_descartadas (
            marketplace, loja, numero_pedido, status_original,
            motivo_descarte, receita_estimada, tarifa_venda_estimada,
            tarifa_envio_estimada, arquivo_origem
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(sql, (
            dados.get('marketplace', ''),
            dados.get('loja', ''),
            dados.get('numero_pedido', ''),
            dados.get('status_original', ''),
            dados.get('motivo_descarte', ''),
            dados.get('receita_estimada', 0),
            dados.get('tarifa_venda_estimada', 0),
            dados.get('tarifa_envio_estimada', 0),
            dados.get('arquivo_origem', '')
        ))
        return True
    except Exception:
        return False


def deletar_venda_snapshot(cursor, pedido, sku, loja):
    """
    Remove venda de fact_vendas_snapshot.
    Usado quando status muda na reimportação (ex: 'Entregue' → 'Devolvido').
    """
    try:
        cursor.execute(
            "DELETE FROM fact_vendas_snapshot WHERE numero_pedido = %s AND sku = %s AND loja_origem = %s",
            (pedido, sku, loja)
        )
        return cursor.rowcount > 0
    except Exception:
        return False

# ============================================================
# MAPEAMENTO DE SKUs (NOVO v3.1)
# ============================================================

def buscar_mapeamento_skus(engine):
    """
    Carrega tabela de mapeamento de SKUs (de→para).
    Usado no processamento para corrigir SKUs automaticamente.
    Retorna dict: {sku_errado: sku_correto}
    """
    try:
        df = pd.read_sql("SELECT sku_errado, sku_correto FROM dim_sku_mapeamento", engine)
        return {row['sku_errado']: row['sku_correto'] for _, row in df.iterrows()}
    except Exception:
        # Tabela pode não existir ainda
        return {}


def gravar_mapeamento_sku(engine, sku_errado, sku_correto):
    """
    Salva mapeamento de SKU (de→para) para correção automática em imports futuros.
    Usa UPSERT: se já existir, atualiza o correto.
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO dim_sku_mapeamento (sku_errado, sku_correto)
            VALUES (%s, %s)
            ON CONFLICT (sku_errado) 
            DO UPDATE SET sku_correto = EXCLUDED.sku_correto, data_criacao = NOW()
        """
        cursor.execute(sql, (sku_errado.strip(), sku_correto.strip()))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao gravar mapeamento de SKU: {e}")
        return False


def buscar_pendentes_por_tipo(engine, tipo='sku'):
    """
    Busca vendas pendentes filtradas por tipo de motivo.
    
    VERSÃO 3.2: Usa ILIKE com padrões amplos para não perder vendas
    de marketplaces que usam motivos ligeiramente diferentes.
    
    Args:
        tipo: 'sku' → motivos relacionados a SKU (não cadastrado, não mapeado, etc.)
              'divergencia' → motivo LIKE 'Divergência%'
              'todos' → sem filtro de motivo
    """
    query = "SELECT * FROM fact_vendas_pendentes WHERE status = 'Pendente'"
    params = []

    if tipo == 'sku':
        # FIX v3.2: Pega TODAS as variações de motivo relacionadas a SKU/ASIN
        # Inclui: 'SKU não cadastrado', 'SKU Amazon não mapeado', 'ASIN não configurado'
        query += " AND (motivo ILIKE %s OR motivo ILIKE %s OR motivo ILIKE %s)"
        params.append('%SKU%')
        params.append('%não cadastrado%')
        params.append('%ASIN%')
    elif tipo == 'divergencia':
        query += " AND motivo ILIKE %s"
        params.append('%Divergência%')

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
        st.error(f"Erro ao buscar pendentes por tipo: {e}")
        return pd.DataFrame()


def buscar_pendentes_revisados(engine, limit=50):
    """
    NOVO v3.2: Busca histórico de vendas reprocessadas/revisadas.
    Retorna DataFrame (não lista crua) — corrige o erro:
    "List argument must consist only of tuples or Dictionaries"
    
    Esse erro ocorria quando a página de Vendas Pendentes tentava
    exibir o histórico usando dados em formato incompatível.
    """
    query = """
        SELECT id, marketplace_origem, loja_origem, numero_pedido, 
               data_venda, sku, quantidade, valor_venda_efetivo,
               status, motivo, data_processamento
        FROM fact_vendas_pendentes 
        WHERE status IN ('Reprocessado', 'Revisado manualmente')
        ORDER BY data_processamento DESC
        LIMIT %s
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (limit,))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao buscar pendentes revisados: {e}")
        return pd.DataFrame()


def reprocessar_pendentes_manual(engine, ids_e_dados):
    """
    Reprocessa vendas pendentes com dados editados manualmente.
    Cada item em ids_e_dados é um dict com:
        id, sku (possivelmente corrigido), valor_venda_efetivo, comissao (tarifa),
        imposto, frete, quantidade, sku_original (para mapeamento)
    
    Busca custo de dim_produtos.preco_a_ser_considerado.
    Grava no fact_vendas_snapshot e marca pendente como 'Revisado manualmente'.
    
    Retorna: {'sucesso': int, 'erros': int, 'mapeados': int, 'mensagem': str}
    """
    skus_validos = buscar_skus_validos(engine)
    custos_dict = buscar_custos_skus(engine)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    sucesso, erros, mapeados = 0, 0, 0
    ids_processados = []

    sql_ins = """
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

    for item in ids_e_dados:
        try:
            id_pendente = int(item['id'])
            sku = str(item['sku']).strip()
            sku_original = str(item.get('sku_original', sku)).strip()

            # Validar SKU (possivelmente corrigido)
            if sku not in skus_validos:
                erros += 1
                continue

            # Buscar custo
            custo_unit = custos_dict.get(sku, 0)

            # Dados financeiros (podem ter sido editados)
            receita = float(item.get('valor_venda_efetivo', 0))
            tarifa = float(item.get('comissao', 0))
            imposto_val = float(item.get('imposto', 0))
            frete = float(item.get('frete', 0))
            qtd = int(item.get('quantidade', 1))

            preco_venda = receita / qtd if qtd > 0 else receita
            custo_total = custo_unit * qtd
            total_tarifas = tarifa + frete
            valor_liquido = receita - total_tarifas - imposto_val
            margem_total = valor_liquido - custo_total
            margem_pct = (margem_total / receita * 100) if receita > 0 else 0

            cursor.execute(f"SAVEPOINT manual_{id_pendente}")

            cursor.execute(sql_ins, (
                item.get('marketplace_origem', ''),
                item.get('loja_origem', ''),
                item.get('numero_pedido', ''),
                item.get('data_venda'),
                sku,
                item.get('codigo_anuncio', ''),
                qtd, preco_venda, 0, 0,
                receita, custo_unit, custo_total, imposto_val, tarifa,
                frete, 0, 0, total_tarifas, valor_liquido,
                margem_total, margem_pct,
                item.get('arquivo_origem', '')
            ))

            cursor.execute(f"RELEASE SAVEPOINT manual_{id_pendente}")
            ids_processados.append(id_pendente)
            sucesso += 1

            # Gravar mapeamento se SKU foi corrigido
            if sku != sku_original and sku_original:
                try:
                    cursor.execute("""
                        INSERT INTO dim_sku_mapeamento (sku_errado, sku_correto)
                        VALUES (%s, %s)
                        ON CONFLICT (sku_errado) 
                        DO UPDATE SET sku_correto = EXCLUDED.sku_correto, data_criacao = NOW()
                    """, (sku_original, sku))
                    mapeados += 1
                except Exception:
                    pass  # Não bloquear por falha no mapeamento

        except Exception as e:
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT manual_{id_pendente}")
            except:
                pass
            erros += 1

    # Marcar como revisados
    if ids_processados:
        placeholders = ','.join(['%s'] * len(ids_processados))
        cursor.execute(
            f"UPDATE fact_vendas_pendentes SET status = 'Revisado manualmente' WHERE id IN ({placeholders})",
            ids_processados
        )

    conn.commit()
    cursor.close()
    conn.close()

    return {
        'sucesso': sucesso,
        'erros': erros,
        'mapeados': mapeados,
        'mensagem': f"Reprocessado: {sucesso} sucesso(s), {erros} erro(s), {mapeados} mapeamento(s) salvo(s)."
    }


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
