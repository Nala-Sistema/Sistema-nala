"""
PROCESSADOR AMAZON - Sistema Nala
Processa Business Report (CSV) com datas acumuladas

VERSÃO 1.5 (18/03/2026):
  - FIX CRÍTICO: _resolver_config NÃO faz mais fallback para logística errada.
         Se ASIN tem config DBA mas venda é FBA → vai para pendentes (antes usava taxa DBA)
  - FIX: logistica_final sempre vem do sufixo do SKU (detectada), não da config
  - FIX: Motivo de pendente inclui tipo de logística (ex: "ASIN não configurado para FBA")
  - Mantido: Toda lógica v1.4 intacta

VERSÃO 1.3 (16/03/2026):
  - FIX CRÍTICO: Lookup de config agora usa (asin + logística) para pegar taxas corretas
  - FIX: ASINs sem config vão para pendentes (antes usava fallback silencioso de 15%)
  - FIX: Detecção de logística pelo sufixo do SKU (-FBA, -DBA)
  - FIX: Motivo padronizado para 'SKU não cadastrado'
  - FIX: Removido '-PR' da limpeza de sufixos (PR = cor preta)
  - NOVO: Consulta dim_sku_mapeamento ANTES de classificar como pendente
  - NOVO: Rastreamento de ASINs sem config (info['asins_sem_config'])

VERSÃO 1.1 (11/03/2026):
  - FIX: INSERT com todas as colunas NOT NULL
  - FIX: SAVEPOINT por linha
  - NOVO: Barra de progresso, mapeamento automático de SKUs, rastreamento de descartes

VERSÃO 1.0 (Gemini):
  - Processa Business Report (CSV) com datas acumuladas
  - Lógica de ID Sintético para evitar duplicatas por período
  - Taxas excludentes (Frete vs Taxa Fixa)
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
    Detecta o tipo de logística pelo sufixo do SKU da Amazon.
    
    Regra: se contém '-FBA' → FBA, senão → DBA (sem sufixo = DBA).
    
    Exemplos:
        'L-0152-FBA' → 'FBA'
        'L-0152-DBA' → 'DBA'
        'L-0152'     → 'DBA'  (v1.4: agora retorna DBA em vez de None)
    
    Retorna: 'FBA' ou 'DBA'
    """
    sku_upper = str(sku_amz).upper().strip()
    if '-FBA' in sku_upper:
        return 'FBA'
    elif '-DBA' in sku_upper:
        return 'DBA'
    return 'DBA'  # v1.4: sem sufixo = DBA (antes retornava None)


def _buscar_config_amazon(engine):
    """
    Busca TODAS as configurações de anúncios Amazon.
    
    Retorna dois dicts:
        config_por_asin_logistica: {(asin, logistica): {sku, comissao_percentual, taxa_fixa, frete_estimado, logistica}}
        config_por_asin: {asin: [{sku, logistica, comissao_percentual, taxa_fixa, frete_estimado}, ...]}
    """
    config_por_asin_logistica = {}
    config_por_asin = {}
    
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT asin, sku, logistica,
                   comissao_percentual, taxa_fixa, frete_estimado 
            FROM dim_config_marketplace 
            WHERE marketplace = 'AMAZON' AND ativo = true
        """)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        for row in rows:
            dados = dict(zip(colunas, row))
            asin = str(dados.get('asin', '')).strip()
            logistica = str(dados.get('logistica', '')).strip()
            
            if not asin:
                continue
            
            config_item = {
                'sku_original': str(dados.get('sku', '')).strip(),
                'logistica': logistica,
                'comissao_percentual': float(dados.get('comissao_percentual', 0) or 0),
                'taxa_fixa': float(dados.get('taxa_fixa', 0) or 0),
                'frete_estimado': float(dados.get('frete_estimado', 0) or 0),
            }
            
            config_por_asin_logistica[(asin, logistica)] = config_item
            
            if asin not in config_por_asin:
                config_por_asin[asin] = []
            config_por_asin[asin].append(config_item)
        
    except Exception as e:
        st.warning(f"Aviso: Não foi possível carregar config Amazon: {e}")
    
    return config_por_asin_logistica, config_por_asin


def _resolver_config(asin, sku_amz, config_por_asin_logistica, config_por_asin):
    """
    Resolve a configuração correta para um ASIN + SKU da Amazon.
    
    VERSÃO 1.5 — SEM FALLBACK para logística errada:
        1. Match exato: (asin, logística detectada do SKU)
        2. Match parcial por logística (ex: 'DBA PF' quando detectou 'DBA')
        3. None: ASIN sem config para a logística detectada → vai para pendentes
    
    IMPORTANTE: Se o ASIN tem config DBA mas a venda é FBA, retorna None.
    Isso evita gravar vendas com taxa errada. O usuário precisa cadastrar
    a config para a logística que falta.
    
    Retorna: config_item dict ou None
    """
    logistica_detectada = _detectar_logistica(sku_amz)
    
    # 1. Match exato (asin, logistica)
    conf = config_por_asin_logistica.get((asin, logistica_detectada))
    if conf:
        return conf
    
    # 2. Match parcial (ex: 'DBA PF' quando detectou 'DBA')
    configs_asin = config_por_asin.get(asin, [])
    for c in configs_asin:
        if logistica_detectada in c['logistica']:
            return c
    
    # 3. ASIN não encontrado OU sem config para essa logística
    # NÃO faz fallback para outra logística — taxa errada é pior que pendente
    return None


def processar_arquivo_amazon(arquivo, loja, imposto, engine, data_ini, data_fim):
    """
    Lê o Business Report da Amazon e prepara os dados para gravação.

    VERSÃO 1.4: Inclui campo 'logistica' em cada venda.
    """
    try:
        df = pd.read_csv(arquivo)
    except Exception as e:
        return None, f"Erro ao ler CSV: {e}"

    # 1. MAPEAMENTO DE COLUNAS
    col_map = {
        'Código SKU': 'sku_amz',
        'Unidades pedidas': 'qtd',
        'Vendas de produtos pedidos': 'receita_bruta',
        'ASIN (child)': 'asin'
    }
    df = df.rename(columns=col_map)

    if 'sku_amz' not in df.columns:
        return None, "Coluna 'Código SKU' não encontrada no arquivo."

    # 2. BUSCAR CONFIGURAÇÕES (v1.3: com logística)
    config_por_asin_logistica, config_por_asin = _buscar_config_amazon(engine)
    
    total_configs = len(config_por_asin_logistica)
    if total_configs == 0:
        st.warning("⚠️ Nenhuma configuração Amazon encontrada em dim_config_marketplace. "
                    "Todas as vendas irão para Pendentes.")

    # 3. BUSCAR CUSTOS E MAPEAMENTO DE SKUs
    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    skus_cadastrados = buscar_skus_validos(engine)
    mapeamento_skus = buscar_mapeamento_skus(engine)

    # 4. PROCESSAR LINHAS
    vendas = []
    descartes = []
    skus_sem_custo = set()
    skus_corrigidos = 0
    linhas_descartadas = 0
    asins_sem_config = set()

    for _, row in df.iterrows():
        try:
            sku_amz = str(row['sku_amz']).strip()
            asin = str(row.get('asin', '')).strip()

            receita = limpar_numero(row.get('receita_bruta', 0))
            try:
                qtd = int(row.get('qtd', 0))
            except (ValueError, TypeError):
                qtd = 0

            # v1.4: Detectar logística para todos os caminhos
            logistica_detectada = _detectar_logistica(sku_amz)

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
                        'logistica': logistica_detectada,  # v1.4
                    })
                linhas_descartadas += 1
                continue

            # ============================================================
            # RESOLUÇÃO DE CONFIG (v1.3 - por ASIN + logística)
            # ============================================================
            conf = _resolver_config(asin, sku_amz, config_por_asin_logistica, config_por_asin)
            
            asin_configurado = conf is not None
            if not asin_configurado:
                asins_sem_config.add(asin)

            # v1.4: Pegar logística da config (mais confiável) ou usar a detectada
            logistica_final = conf.get('logistica', logistica_detectada) if conf else logistica_detectada

            # ============================================================
            # RESOLUÇÃO DE SKU
            # ============================================================
            sku_original = conf.get('sku_original', '') if conf else ''

            if not sku_original:
                sku_original = sku_amz.split('-FBA')[0].split('-DBA')[0].strip()

            if sku_original in mapeamento_skus:
                sku_original = mapeamento_skus[sku_original]
                skus_corrigidos += 1
            elif sku_amz in mapeamento_skus:
                sku_original = mapeamento_skus[sku_amz]
                skus_corrigidos += 1

            # ============================================================
            # CÁLCULO DE TAXAS (v1.3 - da config, não mais default)
            # ============================================================
            if asin_configurado:
                comissao_pct = conf['comissao_percentual']
                taxa_fixa = conf['taxa_fixa']
                frete_est = conf['frete_estimado']
            else:
                comissao_pct = 0
                taxa_fixa = 0
                frete_est = 0

            v_comissao = receita * (comissao_pct / 100)

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
                'data': data_fim.strftime("%d/%m/%Y"),
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
                'logistica': logistica_final,  # v1.4: NOVO campo
                '_custo_unit': custo_un,
                '_asin_configurado': asin_configurado,
            })

        except Exception as e:
            linhas_descartadas += 1
            continue

    # 5. VALIDAR SE TEM VENDAS
    if not vendas and not descartes:
        return None, f"Nenhuma venda válida encontrada ({linhas_descartadas} linhas descartadas)"

    # 6. CRIAR DATAFRAME
    df_result = pd.DataFrame(vendas) if vendas else pd.DataFrame()

    # 7. CRIAR INFO
    info = {
        'total_linhas': len(vendas),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': data_ini.strftime("%d/%m/%Y"),
        'periodo_fim': data_fim.strftime("%d/%m/%Y"),
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name,
        'divergencias': [],
        'carrinhos_encontrados': 0,
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
        'asins_sem_config': list(asins_sem_config),
        'total_configs_carregadas': total_configs,
    }

    # 8. LIMPAR COLUNAS TEMPORÁRIAS
    if not df_result.empty:
        colunas_temp = ['_custo_unit', '_asin_configurado']
        colunas_existentes = [c for c in colunas_temp if c in df_result.columns]
        if colunas_existentes:
            df_result = df_result.drop(columns=colunas_existentes)

    return df_result, info


def gravar_vendas_amazon(df, marketplace, loja, arq_nome, engine, data_ini, data_fim,
                         descartes=None, pendentes_carrinho=None):
    """
    Grava vendas da Amazon com Delete-Before-Insert para evitar duplicatas de período.

    VERSÃO 1.4:
    - NOVO: Salva coluna 'logistica' no fact_vendas_snapshot e fact_vendas_pendentes
    - FIX v1.3 mantido: ASINs sem config → pendentes com motivo 'ASIN não configurado'
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

    total_itens = len(df) + len(descartes)
    if total_itens == 0:
        total_itens = 1
    progress_bar = st.progress(0)
    status_text = st.empty()
    item_atual = 0

    try:
        # 1. DELETE BEFORE INSERT
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
        # v1.4: Adicionada coluna 'logistica' ao INSERT
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

        for idx, row in df.iterrows():
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Gravando venda {item_atual} de {total_itens}...")

                sku = str(row['sku']).strip()
                asin = str(row.get('asin', '')).strip()
                logistica = str(row.get('logistica', 'DBA')).strip()  # v1.4

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

                # ============================================================
                # VERIFICAR SE DEVE IR PARA PENDENTES
                # ============================================================
                asin_sem_config = (comissao == 0 and taxa_fixa_val == 0 and frete == 0)
                sku_nao_cadastrado = (sku not in skus_cadastrados)

                if asin_sem_config or sku_nao_cadastrado:
                    if asin_sem_config:
                        # v1.5: motivo inclui logística para facilitar diagnóstico
                        motivo = f'ASIN não configurado para {logistica}'
                    else:
                        motivo = 'SKU não cadastrado'

                    if sku_nao_cadastrado:
                        skus_invalidos.add(sku)

                    dados_pendente = {
                        'marketplace_origem': marketplace,
                        'loja_origem': loja,
                        'numero_pedido': str(row['pedido']),
                        'data_venda': data_venda,
                        'sku': sku,
                        'codigo_anuncio': asin,
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
                        'motivo': motivo,
                        'logistica': logistica,  # v1.4: NOVO
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
                # GRAVAÇÃO NORMAL
                # ============================================================
                custo_total = float(row['custo'])
                margem = float(row['margem'])
                margem_pct = float(row['margem_pct'])
                custo_unit = custo_total / qtd if qtd > 0 else custo_total

                cursor.execute(f"SAVEPOINT venda_amz_{idx}")

                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), data_venda, sku,
                    asin,
                    qtd, preco_venda, 0, 0,
                    receita, custo_unit, custo_total, imposto_val, comissao,
                    frete, taxa_fixa_val, 0, total_tarifas, valor_liquido,
                    margem, margem_pct, arq_nome, logistica  # v1.4: NOVO parâmetro
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
