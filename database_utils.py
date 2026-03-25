"""
DATABASE UTILS - Sistema Nala
Versão: 3.4 (25/03/2026)

CHANGELOG v3.4:
  - NOVO: excluir_pendentes_por_ids() — deleta permanentemente vendas pendentes por lista de IDs

CHANGELOG v3.3:
  - FIX CRÍTICO: reprocessar_pendentes_por_sku() agora RECALCULA taxas a partir de
         dim_config_marketplace em vez de copiar taxas zeradas do pendente.
         Isso corrige vendas reprocessadas ficando com comissao=0, tarifa_fixa=0.
  - FIX CRÍTICO: reprocessar_pendentes_manual() — mesma correção de recálculo de taxas.
  - NOVO: buscar_produtos_autocomplete() — busca SKU ou nome em dim_produtos (para campo inteligente)
  - NOVO: buscar_config_amazon_por_asin() — busca config por ASIN+logística (para reprocessamento)
  - NOVO: deletar_config_amazon() — exclui anúncio da dim_config_marketplace
  - NOVO: buscar_configs_amazon_lista() — lista configs Amazon para edição
  - MELHORIA: Todas funções de gravação agora incluem coluna 'logistica' no INSERT
  - MELHORIA: gravar_venda_pendente() aceita dados['logistica']

CHANGELOG v3.2:
  - FIX: buscar_pendentes_por_tipo() — filtro agora usa ILIKE para pegar variações
  - FIX: buscar_pendentes_revisados() — retorna DataFrame corrigindo erro de tipo
  - MELHORIA: buscar_pendentes() agora aceita motivo_like para buscas flexíveis

CHANGELOG v3.1:
  - NOVO: buscar_mapeamento_skus(), gravar_mapeamento_sku()
  - NOVO: buscar_pendentes_por_tipo(), reprocessar_pendentes_manual()

CHANGELOG v3.0:
  - NOVO: gravar_venda_descartada(), deletar_venda_snapshot()
  - AJUSTE: gravar_venda_pendente() — motivo dinâmico
"""

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

# v3.5: Código agnóstico ao banco — lê APENAS do Secrets do Streamlit.
# Cada app (Produção e Dev) tem seu próprio Secret com a URL correta.
# Isso garante isolamento total: Dev nunca acessa banco de Produção.

def get_engine():
    """Retorna engine do SQLAlchemy lendo DB_URL de forma segura."""
    try:
        # O uso do .get evita o erro de interrupção imediata (KeyError)
        db_url = st.secrets.get("DB_URL")
        
        if not db_url:
            st.error("❌ A variável 'DB_URL' não foi encontrada no Streamlit Cloud.")
            st.info("Acesse: Settings -> Secrets e cole a URL do banco de dados.")
            st.stop() # Para o app aqui em vez de dar erro de tela vermelha
            
        return create_engine(db_url)
    except Exception as e:
        st.error(f"⚠️ Falha ao criar a conexão com o banco: {e}")
        st.stop()

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
# BUSCA DE PRODUTOS — AUTOCOMPLETE INTELIGENTE (NOVO v3.3)
# ============================================================

def buscar_produtos_autocomplete(engine, termo, limit=15):
    """
    Busca produtos em dim_produtos por SKU ou nome.
    Usado no campo inteligente de Vincular Manual.
    
    O termo é buscado tanto no campo 'sku' quanto no campo 'nome'
    usando ILIKE (case insensitive, match parcial).
    
    Retorna lista de dicts: [{'sku': '...', 'nome': '...'}, ...]
    """
    if not termo or len(termo.strip()) < 2:
        return []
    
    query = """
        SELECT sku, nome 
        FROM dim_produtos 
        WHERE status = 'Ativo' 
          AND (sku ILIKE %s OR nome ILIKE %s)
        ORDER BY sku ASC
        LIMIT %s
    """
    padrao = f"%{termo.strip()}%"
    
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (padrao, padrao, limit))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(zip(colunas, row)) for row in rows]
    except Exception:
        return []


# ============================================================
# CONFIG AMAZON — BUSCA PARA REPROCESSAMENTO (NOVO v3.3)
# ============================================================

def buscar_config_amazon_por_asin(engine, asin, logistica=None):
    """
    Busca configuração de um ASIN na dim_config_marketplace.
    
    Usado pelo reprocessamento para recalcular taxas corretamente.
    
    Args:
        asin: código ASIN
        logistica: 'FBA', 'DBA' ou None (tenta match exato, depois fallback)
    
    Retorna: dict com {comissao_percentual, taxa_fixa, frete_estimado, logistica} ou None
    """
    if not asin or str(asin).strip() == '':
        return None
    
    asin = str(asin).strip()
    
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT logistica, comissao_percentual, taxa_fixa, frete_estimado
            FROM dim_config_marketplace 
            WHERE marketplace = 'AMAZON' AND asin = %s AND ativo = true
            ORDER BY logistica
        """, (asin,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            return None
        
        configs = []
        for row in rows:
            configs.append({
                'logistica': str(row[0] or '').strip(),
                'comissao_percentual': float(row[1] or 0),
                'taxa_fixa': float(row[2] or 0),
                'frete_estimado': float(row[3] or 0),
            })
        
        # 1. Match exato por logística
        if logistica:
            for c in configs:
                if c['logistica'] == logistica:
                    return c
            # Match parcial (ex: logistica='FBA', config='FBA')
            for c in configs:
                if logistica in c['logistica']:
                    return c
        
        # 2. Fallback: se só tem uma config, usa ela
        if len(configs) == 1:
            return configs[0]
        
        # 3. Se tem múltiplas, prefere DBA (mais comum sem sufixo)
        for c in configs:
            if 'DBA' in c['logistica'] and 'PF' not in c['logistica']:
                return c
        
        return configs[0]
    
    except Exception:
        return None


def _detectar_logistica_do_pedido(numero_pedido):
    """
    Detecta logística a partir do numero_pedido Amazon.
    Formato: AMZ_{loja}_{data_ini}_{data_fim}_{sku_amz}
    Se sku_amz contém '-FBA' → FBA, senão → DBA
    """
    pedido = str(numero_pedido or '').upper()
    if '-FBA' in pedido:
        return 'FBA'
    return 'DBA'


# ============================================================
# CONFIG AMAZON — CRUD (NOVO v3.3)
# ============================================================

def buscar_configs_amazon_lista(engine):
    """
    Lista todas as configurações Amazon para exibição e edição.
    Retorna DataFrame com id, asin, sku, logistica, taxas.
    """
    query = """
        SELECT id, asin, sku, logistica, comissao_percentual, taxa_fixa, frete_estimado
        FROM dim_config_marketplace 
        WHERE marketplace = 'AMAZON' AND ativo = true
        ORDER BY asin, logistica
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception:
        return pd.DataFrame()


def deletar_config_amazon(engine, asin, logistica):
    """
    Exclui um anúncio Amazon da dim_config_marketplace.
    Deleta por ASIN + logística (chave composta).
    Retorna True se deletou, False se erro.
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM dim_config_marketplace WHERE asin = %s AND marketplace = 'AMAZON' AND logistica = %s",
            (str(asin).strip(), str(logistica).strip())
        )
        deletados = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return deletados > 0
    except Exception as e:
        st.error(f"Erro ao excluir anúncio: {e}")
        return False


def salvar_config_amazon(engine, asin, sku, logistica, comissao_pct, taxa_fixa, frete_est):
    """
    Salva (insert/update) configuração de anúncio Amazon.
    Usa DELETE+INSERT para UPSERT por (asin, logistica).
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM dim_config_marketplace WHERE asin = %s AND marketplace = 'AMAZON' AND logistica = %s",
            (str(asin).strip(), str(logistica).strip())
        )
        cursor.execute("""
            INSERT INTO dim_config_marketplace 
                (asin, sku, marketplace, loja, logistica, 
                 comissao_percentual, taxa_fixa, frete_estimado, ativo, data_vigencia)
            VALUES (%s, %s, 'AMAZON', 'AMAZON', %s, %s, %s, %s, TRUE, CURRENT_DATE)
        """, (
            str(asin).strip(), str(sku).strip(), str(logistica).strip(),
            float(comissao_pct), float(taxa_fixa), float(frete_est)
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar config Amazon: {e}")
        return False


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

    VERSÃO 3.3: Inclui coluna logistica.
    VERSÃO 3.0: motivo agora é dinâmico — lê de dados['motivo'].
    """
    motivo = dados.get('motivo', 'SKU não cadastrado')
    logistica = dados.get('logistica', None)

    sql = """
        INSERT INTO fact_vendas_pendentes (
            marketplace_origem, loja_origem, numero_pedido, data_venda, sku, 
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo,
            imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas,
            valor_liquido, arquivo_origem, data_processamento, status, motivo, logistica
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'Pendente', %s, %s)
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
            motivo, logistica
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
    """Retorna resumo de pendentes por SKU."""
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
    """
    Reprocessa vendas pendentes após cadastro de SKU.
    
    VERSÃO 3.3 - FIX CRÍTICO:
        Agora RECALCULA taxas a partir da dim_config_marketplace para vendas Amazon.
        Antes copiava as taxas do pendente (que eram zero quando ASIN não tinha config).
        
        Para cada pendente Amazon:
        1. Pega o ASIN do campo codigo_anuncio
        2. Detecta logística (FBA/DBA) do numero_pedido
        3. Busca config atualizada em dim_config_marketplace
        4. Recalcula: comissão, taxa_fixa, frete, total_tarifas, valor_liquido, margem
    """
    skus_validos = buscar_skus_validos(engine)
    if sku not in skus_validos:
        return {'sucesso': 0, 'erros': 0, 'mensagem': "SKU não cadastrado."}
    
    custo_unit = buscar_custos_skus(engine).get(sku, 0)
    df_pendentes = buscar_pendentes(engine, sku=sku, status='Pendente')
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    sucesso, erros, sem_config = 0, 0, 0
    ids_repro = []

    sql_ins = """
        INSERT INTO fact_vendas_snapshot (marketplace_origem, loja_origem, numero_pedido, data_venda, sku,
            codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo, custo_unitario, custo_total,
            imposto, comissao, frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido,
            margem_total, margem_percentual, data_processamento, arquivo_origem, logistica)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
    """
    for _, row in df_pendentes.iterrows():
        try:
            cursor.execute(f"SAVEPOINT repro_{row['id']}")
            
            receita = float(row['valor_venda_efetivo'])
            qtd = int(row['quantidade'])
            custo_total = custo_unit * qtd
            imposto_val = float(row['imposto'])
            marketplace = str(row.get('marketplace_origem', '')).upper()
            
            # ============================================================
            # v3.3: RECALCULAR TAXAS para Amazon
            # ============================================================
            if 'AMAZON' in marketplace:
                asin = str(row.get('codigo_anuncio', '')).strip()
                logistica_det = _detectar_logistica_do_pedido(row.get('numero_pedido', ''))
                
                conf = buscar_config_amazon_por_asin(engine, asin, logistica_det)
                
                if conf:
                    comissao_pct = conf['comissao_percentual']
                    taxa_fixa_unit = conf['taxa_fixa']
                    frete_est = conf['frete_estimado']
                    logistica_final = conf['logistica'] or logistica_det
                    
                    v_comissao = receita * (comissao_pct / 100)
                    if frete_est > 0:
                        v_frete = frete_est * qtd
                        v_taxa_fixa = 0.0
                    else:
                        v_frete = 0.0
                        v_taxa_fixa = taxa_fixa_unit * qtd
                else:
                    # ASIN sem config — não reprocessar com taxa zero!
                    sem_config += 1
                    cursor.execute(f"ROLLBACK TO SAVEPOINT repro_{row['id']}")
                    continue
            else:
                # Outros marketplaces: manter taxas do pendente (Shopee, ML, etc.)
                v_comissao = float(row['comissao'])
                v_taxa_fixa = float(row['tarifa_fixa'])
                v_frete = float(row['frete'])
                logistica_final = row.get('logistica', None)
            
            total_tarifas = v_comissao + v_taxa_fixa + v_frete
            valor_liquido = receita - total_tarifas - imposto_val
            margem_total = valor_liquido - custo_total
            margem_pct = (margem_total / receita * 100) if receita > 0 else 0
            preco_venda = receita / qtd if qtd > 0 else receita
            
            cursor.execute(sql_ins, (
                row['marketplace_origem'], row['loja_origem'], row['numero_pedido'], row['data_venda'], sku,
                row['codigo_anuncio'], qtd, preco_venda, receita, custo_unit, custo_total,
                imposto_val, v_comissao, v_frete, v_taxa_fixa, 0, total_tarifas, valor_liquido,
                margem_total, margem_pct, row['arquivo_origem'], logistica_final
            ))
            ids_repro.append(int(row['id']))
            sucesso += 1
        except Exception:
            cursor.execute(f"ROLLBACK TO SAVEPOINT repro_{row['id']}")
            erros += 1

    if ids_repro:
        cursor.execute(
            f"UPDATE fact_vendas_pendentes SET status = 'Reprocessado' WHERE id IN ({','.join(['%s']*len(ids_repro))})",
            ids_repro
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    msg = f"Reprocessado: {sucesso} sucesso(s), {erros} erro(s)."
    if sem_config > 0:
        msg += f" ⚠️ {sem_config} venda(s) ignorada(s) — ASIN sem config cadastrada."
    
    return {'sucesso': sucesso, 'erros': erros, 'sem_config': sem_config, 'mensagem': msg}

# ============================================================
# VENDAS DESCARTADAS (v3.0)
# ============================================================

def gravar_venda_descartada(cursor, dados):
    """Grava venda descartada em fact_vendas_descartadas."""
    sql = """
        INSERT INTO fact_vendas_descartadas (
            marketplace, loja, numero_pedido, status_original,
            motivo_descarte, receita_estimada, tarifa_venda_estimada,
            tarifa_envio_estimada, arquivo_origem, logistica
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            dados.get('arquivo_origem', ''),
            dados.get('logistica', None)
        ))
        return True
    except Exception:
        return False


def deletar_venda_snapshot(cursor, pedido, sku, loja):
    """Remove venda de fact_vendas_snapshot."""
    try:
        cursor.execute(
            "DELETE FROM fact_vendas_snapshot WHERE numero_pedido = %s AND sku = %s AND loja_origem = %s",
            (pedido, sku, loja)
        )
        return cursor.rowcount > 0
    except Exception:
        return False

# ============================================================
# MAPEAMENTO DE SKUs (v3.1)
# ============================================================

def buscar_mapeamento_skus(engine):
    """Carrega tabela de mapeamento de SKUs (de→para)."""
    try:
        df = pd.read_sql("SELECT sku_errado, sku_correto FROM dim_sku_mapeamento", engine)
        return {row['sku_errado']: row['sku_correto'] for _, row in df.iterrows()}
    except Exception:
        return {}


def gravar_mapeamento_sku(engine, sku_errado, sku_correto):
    """Salva mapeamento de SKU (de→para) com UPSERT."""
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
    """Busca vendas pendentes filtradas por tipo de motivo (v3.2)."""
    query = "SELECT * FROM fact_vendas_pendentes WHERE status = 'Pendente'"
    params = []

    if tipo == 'sku':
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
    """Busca histórico de vendas reprocessadas/revisadas (v3.2)."""
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


def excluir_pendentes_por_ids(engine, ids):
    """
    Exclui permanentemente vendas pendentes por lista de IDs.
    Usado quando o usuário identifica registros como lixo ou duplicata.
    
    VERSÃO 3.4: Nova função.
    
    Args:
        engine: SQLAlchemy engine
        ids: lista de IDs (int) de fact_vendas_pendentes
    
    Returns: dict com {excluidos, erros, mensagem}
    """
    if not ids:
        return {'excluidos': 0, 'erros': 0, 'mensagem': 'Nenhum ID fornecido.'}
    
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(ids))
        cursor.execute(
            f"DELETE FROM fact_vendas_pendentes WHERE id IN ({placeholders})",
            [int(i) for i in ids]
        )
        excluidos = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return {
            'excluidos': excluidos,
            'erros': 0,
            'mensagem': f'{excluidos} pendente(s) excluída(s) permanentemente.'
        }
    except Exception as e:
        return {
            'excluidos': 0,
            'erros': 1,
            'mensagem': f'Erro ao excluir pendentes: {e}'
        }


def reprocessar_pendentes_manual(engine, ids_e_dados):
    """
    Reprocessa vendas pendentes com dados editados manualmente.
    
    VERSÃO 3.3 - FIX CRÍTICO:
        Para vendas Amazon, recalcula taxas a partir da dim_config_marketplace
        em vez de usar os valores (possivelmente zerados) do pendente.
    """
    skus_validos = buscar_skus_validos(engine)
    custos_dict = buscar_custos_skus(engine)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    sucesso, erros, mapeados, sem_config = 0, 0, 0, 0
    ids_processados = []

    sql_ins = """
        INSERT INTO fact_vendas_snapshot (
            marketplace_origem, loja_origem, numero_pedido, data_venda, sku,
            codigo_anuncio, quantidade, preco_venda, desconto_parceiro, desconto_marketplace,
            valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao,
            frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido,
            margem_total, margem_percentual, data_processamento, arquivo_origem, logistica
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, NOW(), %s, %s
        )
    """

    for item in ids_e_dados:
        try:
            id_pendente = int(item['id'])
            sku = str(item['sku']).strip()
            sku_original = str(item.get('sku_original', sku)).strip()

            if sku not in skus_validos:
                erros += 1
                continue

            custo_unit = custos_dict.get(sku, 0)
            receita = float(item.get('valor_venda_efetivo', 0))
            qtd = int(item.get('quantidade', 1))
            imposto_val = float(item.get('imposto', 0))
            marketplace = str(item.get('marketplace_origem', '')).upper()

            # ============================================================
            # v3.3: RECALCULAR TAXAS para Amazon
            # ============================================================
            if 'AMAZON' in marketplace:
                asin = str(item.get('codigo_anuncio', '')).strip()
                logistica_det = _detectar_logistica_do_pedido(item.get('numero_pedido', ''))
                
                conf = buscar_config_amazon_por_asin(engine, asin, logistica_det)
                
                if conf:
                    comissao_pct = conf['comissao_percentual']
                    taxa_fixa_unit = conf['taxa_fixa']
                    frete_est = conf['frete_estimado']
                    logistica_final = conf['logistica'] or logistica_det
                    
                    v_comissao = receita * (comissao_pct / 100)
                    if frete_est > 0:
                        v_frete = frete_est * qtd
                        v_taxa_fixa = 0.0
                    else:
                        v_frete = 0.0
                        v_taxa_fixa = taxa_fixa_unit * qtd
                else:
                    # Sem config — não gravar com taxa zero
                    sem_config += 1
                    erros += 1
                    continue
            else:
                v_comissao = float(item.get('comissao', 0))
                v_taxa_fixa = float(item.get('tarifa_fixa', 0))
                v_frete = float(item.get('frete', 0))
                logistica_final = item.get('logistica', None)

            preco_venda = receita / qtd if qtd > 0 else receita
            custo_total = custo_unit * qtd
            total_tarifas = v_comissao + v_taxa_fixa + v_frete
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
                receita, custo_unit, custo_total, imposto_val, v_comissao,
                v_frete, v_taxa_fixa, 0, total_tarifas, valor_liquido,
                margem_total, margem_pct,
                item.get('arquivo_origem', ''),
                logistica_final
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
                    pass

        except Exception as e:
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT manual_{id_pendente}")
            except:
                pass
            erros += 1

    if ids_processados:
        placeholders = ','.join(['%s'] * len(ids_processados))
        cursor.execute(
            f"UPDATE fact_vendas_pendentes SET status = 'Revisado manualmente' WHERE id IN ({placeholders})",
            ids_processados
        )

    conn.commit()
    cursor.close()
    conn.close()

    msg = f"Reprocessado: {sucesso} sucesso(s), {erros} erro(s), {mapeados} mapeamento(s) salvo(s)."
    if sem_config > 0:
        msg += f" ⚠️ {sem_config} venda(s) sem config de ASIN — não gravadas."

    return {
        'sucesso': sucesso,
        'erros': erros,
        'mapeados': mapeados,
        'sem_config': sem_config,
        'mensagem': msg
    }


# ============================================================
# CURVA ABC (PARETO)
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
