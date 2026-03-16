"""
PROCESSADOR AMAZON - Sistema Nala
Processa Business Report (CSV) com datas acumuladas

VERSÃO 1.3 (16/03/2026):
  - FIX CRÍTICO: Lookup de config agora usa (asin, logistica) como chave
         Antes: set_index('asin') falhava com ASINs duplicados por logística,
         caindo silenciosamente no fallback de 15% sem taxa_fixa e sem frete.
  - FIX: Logística determinada pelo sufixo do SKU Amazon (-FBA → FBA, senão DBA)
  - FIX: ASIN sem config vai para pendentes com motivo 'ASIN não configurado'
         (antes usava fallback silencioso de 15% gerando margem errada)
  - FIX: Motivo de pendente padronizado para 'SKU não cadastrado'
  - FIX: Removido '-PR' da limpeza de sufixos (PR = cor preta, não é sufixo)
  - NOVO: Consulta dim_sku_mapeamento ANTES de classificar como pendente
  - NOVO: Rastreamento de ASINs sem config para relatório

VERSÃO 1.1 (11/03/2026):
  - FIX: INSERT com todas as colunas NOT NULL
  - FIX: SAVEPOINT por linha
  - NOVO: Barra de progresso, mapeamento SKUs, rastreamento descartes

VERSÃO 1.0 (Gemini):
  - Processamento inicial do Business Report
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import limpar_numero
from database_utils import (
    buscar_custos_skus,
    buscar_skus_validos,
    gravar_venda_pendente,
    buscar_custo_flex,
    buscar_mapeamento_skus,
)


def _detectar_logistica(sku_amz):
    """
    Detecta o tipo de logística pelo sufixo do SKU Amazon.
    - Contém '-FBA' ou termina com 'FBA' → 'FBA'
    - Caso contrário → 'DBA' (inclui DBA PF que é tratado como DBA)
    """
    sku_upper = str(sku_amz).upper().strip()
    if '-FBA' in sku_upper or sku_upper.endswith('FBA'):
        return 'FBA'
    return 'DBA'


def _buscar_config_amazon(engine):
    """
    Busca configurações de anúncios Amazon (dim_config_marketplace).
    
    Retorna dois dicts:
    - config_por_asin_logistica: {(asin, logistica_base): {comissao_percentual, taxa_fixa, frete_estimado, sku}}
    - config_por_asin_geral: {asin: {comissao_percentual, taxa_fixa, frete_estimado, sku}}
      (fallback: pega a primeira config do ASIN se não achar logística específica)
    """
    config_por_asin_logistica = {}
    config_por_asin_geral = {}
    
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT asin, sku, logistica,
                   comissao_percentual, taxa_fixa, frete_estimado 
            FROM dim_config_marketplace 
            WHERE marketplace = 'AMAZON' AND ativo = true
            ORDER BY asin, logistica
        """)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            r = dict(zip(colunas, row))
            asin = str(r.get('asin', '')).strip()
            logistica_raw = str(r.get('logistica', '')).strip().upper()
            
            # Normalizar logística: 'DBA PF', 'DBA' → 'DBA' | 'FBA' → 'FBA'
            logistica_base = 'FBA' if 'FBA' in logistica_raw else 'DBA'
            
            config_data = {
                'sku_original': str(r.get('sku', '')).strip(),
                'comissao_percentual': float(r.get('comissao_percentual', 0) or 0),
                'taxa_fixa': float(r.get('taxa_fixa', 0) or 0),
                'frete_estimado': float(r.get('frete_estimado', 0) or 0),
                'logistica': logistica_base,
            }
            
            # Dict principal: (asin, logistica_base)
            config_por_asin_logistica[(asin, logistica_base)] = config_data
            
            # Dict fallback: primeiro registro por ASIN
            if asin not in config_por_asin_geral:
                config_por_asin_geral[asin] = config_data

        return config_por_asin_logistica, config_por_asin_geral

    except Exception as e:
        st.warning(f"⚠️ Erro ao buscar config Amazon: {e}")
        return {}, {}


def processar_arquivo_amazon(arquivo, loja, imposto, engine, data_ini, data_fim):
    """
    Lê o Business Report da Amazon e prepara os dados para gravação.

    VERSÃO 1.3: Config por (asin, logistica), ASIN sem config → pendente.

    Args:
        arquivo: arquivo CSV (UploadedFile)
        loja: nome da loja (str)
        imposto: percentual de imposto (float)
        engine: SQLAlchemy engine
        data_ini: data início do período (date)
        data_fim: data fim do período (date)

    Retorna:
        (df_processado, info_dict) ou (None, erro_msg)
    """
    try:
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

    # 2. BUSCAR CONFIGURAÇÕES (v1.3: por asin + logística)
    config_por_asin_logistica, config_por_asin_geral = _buscar_config_amazon(engine)

    if not config_por_asin_logistica and not config_por_asin_geral:
        st.warning("⚠️ Nenhuma configuração de anúncios Amazon encontrada. Todas as vendas irão para pendentes.")

    # 3. BUSCAR CUSTOS E MAPEAMENTO DE SKUs
    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    skus_cadastrados = buscar_skus_validos(engine)
    mapeamento_skus = buscar_mapeamento_skus(engine)

    # 4. PROCESSAR LINHAS
    vendas = []
    descartes = []
    skus_sem_custo = set()
    asins_sem_config = set()
    skus_corrigidos = 0
    linhas_descartadas = 0

    for _, row in df.iterrows():
        try:
            sku_amz = str(row['sku_amz']).strip()
            asin = str(row.get('asin', '')).strip()

            # Validar receita e quantidade
            receita = limpar_numero(row.get('receita_bruta', 0))
            try:
                qtd = int(row.get('qtd', 0))
            except (ValueError, TypeError):
                qtd = 0

            if qtd <= 0 or receita <= 0:
                if sku_amz:
                    descartes.append({
                        'numero_pedido': f"AMZ_{loja}_{sku_amz}",
                        'sku': sku_amz,
                        'status_original': 'Sem quantidade/receita',
                        'motivo_descarte': f"qtd={qtd}, receita={receita}",
                        'receita_estimada': max(receita, 0),
                        'tarifa_venda_estimada': 0,
                        'tarifa_envio_estimada': 0,
                    })
                linhas_descartadas += 1
                continue

            # ============================================================
            # RESOLUÇÃO DE CONFIG POR (ASIN + LOGÍSTICA) — v1.3
            # ============================================================
            logistica_detectada = _detectar_logistica(sku_amz)
            
            # 1º: Tenta (asin, logistica) exata
            conf = config_por_asin_logistica.get((asin, logistica_detectada))
            
            # 2º: Fallback — qualquer config desse ASIN
            if not conf:
                conf = config_por_asin_geral.get(asin)
            
            # 3º: ASIN sem config → vai para pendentes (NÃO usa fallback 15%)
            if not conf:
                asins_sem_config.add(asin)
                
                # Ainda assim tenta resolver o SKU para o registro pendente
                sku_limpo = sku_amz.split('-FBA')[0].split('-DBA')[0].strip()
                if sku_limpo in mapeamento_skus:
                    sku_limpo = mapeamento_skus[sku_limpo]
                elif sku_amz in mapeamento_skus:
                    sku_limpo = mapeamento_skus[sku_amz]

                vendas.append({
                    'pedido': f"AMZ_{loja}_{data_ini.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}_{sku_amz}",
                    'data': data_ini.strftime("%d/%m/%Y"),
                    'sku': sku_limpo,
                    'sku_amz': sku_amz,
                    'asin': asin,
                    'qtd': qtd,
                    'receita': receita,
                    'comissao': 0,
                    'taxa_fixa': 0,
                    'frete': 0,
                    'imposto': 0,
                    'custo': 0,
                    'margem': 0,
                    'margem_pct': 0,
                    'tem_custo': False,
                    '_custo_unit': 0,
                    '_pendente_asin': True,
                    '_motivo_pendente': 'ASIN não configurado',
                })
                continue

            # ============================================================
            # RESOLUÇÃO DE SKU
            # ============================================================
            sku_original = conf.get('sku_original', '')
            
            if not sku_original:
                # Limpa sufixos conhecidos (FBA, DBA) — NÃO remove -PR (cor preta)
                sku_original = sku_amz.split('-FBA')[0].split('-DBA')[0].strip()

            # Consulta mapeamento (dim_sku_mapeamento) — correções manuais
            if sku_original in mapeamento_skus:
                sku_original = mapeamento_skus[sku_original]
                skus_corrigidos += 1
            elif sku_amz in mapeamento_skus:
                sku_original = mapeamento_skus[sku_amz]
                skus_corrigidos += 1

            # ============================================================
            # CÁLCULO DE TAXAS (da config real, não mais fallback 15%)
            # ============================================================
            comissao_pct = conf.get('comissao_percentual', 0)
            taxa_fixa = conf.get('taxa_fixa', 0)
            frete_est = conf.get('frete_estimado', 0)

            v_comissao = receita * (comissao_pct / 100)

            # Taxas excludentes: se tem frete, zera taxa fixa e vice-versa
            if frete_est > 0:
                v_frete = frete_est * qtd
                v_taxa_fixa = 0.0
            else:
                v_frete = 0.0
                v_taxa_fixa = taxa_fixa * qtd

            imposto_val = receita * (imposto / 100)
            custo_un = custos_dict.get(sku_original, 0.0)
            custo_total = custo_un * qtd

            if custo_un == 0:
                skus_sem_custo.add(sku_original)

            total_tarifas = v_comissao + v_taxa_fixa + v_frete
            valor_liquido = receita - total_tarifas - imposto_val
            margem = valor_liquido - custo_total
            margem_pct = (margem / receita * 100) if receita > 0 else 0

            vendas.append({
                'pedido': f"AMZ_{loja}_{data_ini.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}_{sku_amz}",
                'data': data_ini.strftime("%d/%m/%Y"),
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
                'margem_pct': margem_pct,
                'tem_custo': custo_un > 0,
                '_custo_unit': custo_un,
                '_pendente_asin': False,
                '_motivo_pendente': None,
            })

        except Exception as e:
            linhas_descartadas += 1
            continue

    # 5. VALIDAR SE TEM VENDAS
    if not vendas and not descartes:
        return None, f"Nenhuma venda válida encontrada ({linhas_descartadas} linhas descartadas)"

    # 6. CRIAR DATAFRAME
    df_result = pd.DataFrame(vendas) if vendas else pd.DataFrame()

    # 7. CRIAR INFO (formato compatível com central_uploads)
    info = {
        'total_linhas': len(vendas),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': data_ini.strftime("%d/%m/%Y"),
        'periodo_fim': data_fim.strftime("%d/%m/%Y"),
        'skus_sem_custo': len(skus_sem_custo),
        'asins_sem_config': len(asins_sem_config),
        'arquivo_nome': arquivo.name,
        'divergencias': [],
        'carrinhos_encontrados': 0,
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
    }

    # 8. LIMPAR COLUNAS TEMPORÁRIAS (preserva _pendente_asin para gravação)
    if not df_result.empty:
        colunas_remover = ['_custo_unit']
        colunas_existentes = [c for c in colunas_remover if c in df_result.columns]
        if colunas_existentes:
            df_result = df_result.drop(columns=colunas_existentes)

    return df_result, info


def gravar_vendas_amazon(df, marketplace, loja, arq_nome, engine, data_ini, data_fim,
                         descartes=None, pendentes_carrinho=None):
    """
    Grava vendas da Amazon com Delete-Before-Insert para evitar duplicatas de período.

    VERSÃO 1.3:
    - NOVO: Suporte a _pendente_asin (ASIN não configurado → pendente)
    - FIX: Motivo padronizado para 'SKU não cadastrado'

    Retorna:
        (registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados)
    """
    if descartes is None:
        descartes = []
    if pendentes_carrinho is None:
        pendentes_carrinho = []

    conn = engine.raw_connection()
    cursor = conn.cursor()

    reg = 0
    err = 0
    skus_invalidos = set()
    dups = 0
    pend = 0
    desc_count = 0
    atualiz = 0

    skus_cadastrados = buscar_skus_validos(engine)

    # Barra de progresso
    total_itens = len(df) + len(descartes)
    if total_itens == 0:
        total_itens = 1
    progress_bar = st.progress(0)
    status_text = st.empty()
    item_atual = 0

    try:
        # 1. DELETE BEFORE INSERT (Limpeza do Período para esta Loja)
        cursor.execute(
            "DELETE FROM fact_vendas_snapshot WHERE loja_origem = %s AND data_venda BETWEEN %s AND %s",
            (loja, data_ini, data_fim)
        )
        atualiz = cursor.rowcount

        # 2. PROCESSAR DESCARTES
        from database_utils import gravar_venda_descartada

        for descarte in descartes:
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Processando descartes... {item_atual} de {total_itens}")

                descarte['marketplace'] = marketplace
                descarte['loja'] = loja
                descarte['arquivo_origem'] = arq_nome

                cursor.execute(f"SAVEPOINT desc_amz_{item_atual}")
                if gravar_venda_descartada(cursor, descarte):
                    desc_count += 1
                cursor.execute(f"RELEASE SAVEPOINT desc_amz_{item_atual}")
            except Exception:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT desc_amz_{item_atual}")
                except:
                    pass
                err += 1

        # 3. GRAVAR VENDAS
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

        for idx, row in df.iterrows():
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Gravando venda {item_atual} de {total_itens}...")

                sku = str(row['sku']).strip()
                is_pendente_asin = bool(row.get('_pendente_asin', False))
                motivo_pendente = row.get('_motivo_pendente', None)

                # ============================================================
                # CASO 1: ASIN não configurado → pendente
                # ============================================================
                if is_pendente_asin:
                    data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
                    receita = float(row['receita'])
                    qtd = int(row['qtd'])

                    dados_pendente = {
                        'marketplace_origem': marketplace,
                        'loja_origem': loja,
                        'numero_pedido': str(row['pedido']),
                        'data_venda': data_venda,
                        'sku': sku,
                        'codigo_anuncio': str(row.get('asin', '')),
                        'quantidade': qtd,
                        'preco_venda': receita / qtd if qtd > 0 else receita,
                        'valor_venda_efetivo': receita,
                        'imposto': 0,
                        'comissao': 0,
                        'frete': 0,
                        'tarifa_fixa': 0,
                        'outros_custos': 0,
                        'total_tarifas': 0,
                        'valor_liquido': receita,
                        'arquivo_origem': arq_nome,
                        'motivo': motivo_pendente or 'ASIN não configurado',
                    }

                    cursor.execute(f"SAVEPOINT pend_asin_{idx}")
                    try:
                        if gravar_venda_pendente(cursor, dados_pendente):
                            pend += 1
                        else:
                            err += 1
                        cursor.execute(f"RELEASE SAVEPOINT pend_asin_{idx}")
                    except Exception:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT pend_asin_{idx}")
                        err += 1
                    continue

                # ============================================================
                # CASO 2: SKU não cadastrado → pendente
                # ============================================================
                if sku not in skus_cadastrados:
                    skus_invalidos.add(sku)

                    data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
                    receita = float(row['receita'])
                    qtd = int(row['qtd'])
                    comissao = float(row['comissao'])
                    taxa_fixa_val = float(row['taxa_fixa'])
                    frete = float(row['frete'])
                    imposto_val = float(row['imposto'])
                    preco_venda = receita / qtd if qtd > 0 else receita
                    total_tarifas = comissao + frete + taxa_fixa_val
                    valor_liquido = receita - total_tarifas - imposto_val

                    dados_pendente = {
                        'marketplace_origem': marketplace,
                        'loja_origem': loja,
                        'numero_pedido': str(row['pedido']),
                        'data_venda': data_venda,
                        'sku': sku,
                        'codigo_anuncio': str(row.get('asin', '')),
                        'quantidade': qtd,
                        'preco_venda': preco_venda,
                        'valor_venda_efetivo': receita,
                        'imposto': imposto_val,
                        'comissao': comissao,
                        'frete': frete,
                        'tarifa_fixa': taxa_fixa_val,
                        'outros_custos': 0,
                        'total_tarifas': total_tarifas,
                        'valor_liquido': valor_liquido,
                        'arquivo_origem': arq_nome,
                        'motivo': 'SKU não cadastrado',
                    }

                    cursor.execute(f"SAVEPOINT pend_amz_{idx}")
                    try:
                        if gravar_venda_pendente(cursor, dados_pendente):
                            pend += 1
                        else:
                            err += 1
                        cursor.execute(f"RELEASE SAVEPOINT pend_amz_{idx}")
                    except Exception:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT pend_amz_{idx}")
                        err += 1
                    continue

                # ============================================================
                # CASO 3: Gravação normal
                # ============================================================
                data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
                qtd = int(row['qtd'])
                receita = float(row['receita'])
                custo_total = float(row['custo'])
                comissao = float(row['comissao'])
                taxa_fixa_val = float(row['taxa_fixa'])
                frete = float(row['frete'])
                imposto_val = float(row['imposto'])
                margem = float(row['margem'])
                margem_pct = float(row['margem_pct'])

                preco_venda = receita / qtd if qtd > 0 else receita
                custo_unit = custo_total / qtd if qtd > 0 else custo_total
                total_tarifas = comissao + frete + taxa_fixa_val
                valor_liquido = receita - total_tarifas - imposto_val

                cursor.execute(f"SAVEPOINT venda_amz_{idx}")

                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), data_venda, sku,
                    str(row.get('asin', '')),
                    qtd, preco_venda, 0, 0,
                    receita, custo_unit, custo_total, imposto_val, comissao,
                    frete, taxa_fixa_val, 0, total_tarifas, valor_liquido,
                    margem, margem_pct, arq_nome
                ))

                cursor.execute(f"RELEASE SAVEPOINT venda_amz_{idx}")
                reg += 1

            except Exception as e:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT venda_amz_{idx}")
                except:
                    pass
                err += 1
                if err == 1:
                    st.warning(f"Primeiro erro Amazon: {str(e)[:200]}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.error(f"Erro crítico na gravação Amazon: {e}")
        err = len(df)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz
