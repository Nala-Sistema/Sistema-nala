"""
PROCESSADOR MAGALU - Sistema Nala
Processa Relatórios de Pedidos + Pacotes Magalu (CSV)

VERSÃO 1.0 (17/03/2026):
  - Construído do zero com base nas planilhas reais da Magalu
  - Dois arquivos: pedidos (financeiro) + pacotes (status/logística)
  - Carrinhos compostos: rateio proporcional por preço de cada item
  - Campanha subsidiada: desconto à vista parceiro + Magalu
  - Comissão: 14,8% sobre (valor pago + subsídio Magalu) + R$5 fixo por item
  - Desconto à vista oculto: diferença entre esperado e recebido → rateio proporcional
  - SKUs Magalu sem hífen → mapeamento automático (L0155 → L-0155)
  - Cancelados vindos do relatório de pacotes → fact_vendas_descartadas
  - Padrão Nala: SAVEPOINT, mapeamento SKU, barra de progresso, 7 retornos
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import limpar_numero
from database_utils import (
    buscar_custos_skus,
    buscar_skus_validos,
    gravar_venda_pendente,
    buscar_mapeamento_skus,
    gravar_venda_descartada,
)


def _limpar_valor_magalu(valor):
    """
    Limpa valores monetários da Magalu: 'R$ -5.49' → -5.49, 'R$ 39.9' → 39.9
    Trata 'Não se aplica' e variantes como 0.
    """
    if pd.isna(valor) or str(valor).strip() in ('', 'Não se aplica', 'nan'):
        return 0.0
    s = str(valor).replace('R$', '').replace(' ', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _normalizar_sku_magalu(sku_magalu):
    """
    Tenta normalizar SKU da Magalu para o formato Nala.
    Magalu não aceita hífens, então L0155 pode ser L-0155.
    
    Regras:
    - Se já tem hífen, retorna como está
    - Se começa com L seguido de dígitos: L0155 → L-0155
    - Se começa com K seguido de dígitos: K0287 → K-0287
    - Se começa com LMC seguido de dígitos: LMC0407 → LMC-0407
    - Se começa com LKE seguido de dígitos: LKE9945 → LKE-9945
    - Prefixos com hífen mantidos: LWI-DTD11026 fica como está
    """
    sku = str(sku_magalu).strip()
    if not sku or sku == 'nan':
        return sku
    
    # Se já tem hífen na posição esperada, retorna como está
    if '-' in sku:
        return sku
    
    # Tenta inserir hífen nos padrões conhecidos
    import re
    
    # Padrões: prefixo letras + números (sem hífen)
    # LMC0407 → LMC-0407, LKE9945 → LKE-9945, L0155 → L-0155, K0287 → K-0287
    match = re.match(r'^([A-Za-z]+)(\d.*)$', sku)
    if match:
        prefixo = match.group(1)
        resto = match.group(2)
        return f"{prefixo}-{resto}"
    
    return sku


def processar_arquivo_magalu(arquivo_pedidos, arquivo_pacotes, loja, imposto_pct, engine):
    """
    Processa os dois relatórios da Magalu (pedidos + pacotes).

    Args:
        arquivo_pedidos: CSV de pedidos (financeiro)
        arquivo_pacotes: CSV de pacotes (status/logística)
        loja: nome da loja (str)
        imposto_pct: percentual de imposto (float)
        engine: SQLAlchemy engine

    Retorna:
        (df_processado, info_dict) ou (None, erro_msg)
    """
    # 1. LER ARQUIVOS
    try:
        df_ped = pd.read_csv(arquivo_pedidos)
    except Exception as e:
        return None, f"Erro ao ler arquivo de pedidos Magalu: {e}"

    try:
        df_pac = pd.read_csv(arquivo_pacotes)
    except Exception as e:
        return None, f"Erro ao ler arquivo de pacotes Magalu: {e}"

    # Validar colunas essenciais
    cols_ped = ['Número do pedido', 'Codigo SKU seller', 'Valor Total do Item', 'Valor líquido estimado a receber']
    faltando = [c for c in cols_ped if c not in df_ped.columns]
    if faltando:
        return None, f"Colunas não encontradas em pedidos: {', '.join(faltando)}"

    # 2. MONTAR DICT DE STATUS/LOGÍSTICA A PARTIR DE PACOTES
    status_dict = {}  # {numero_pedido: {status, modalidade}}
    for _, row in df_pac.iterrows():
        pedido = str(row.get('Número do pedido', '')).strip()
        if pedido and pedido != 'nan':
            status_dict[pedido] = {
                'status': str(row.get('Status pacote no momento que o relatório foi solicitado', '')).strip(),
                'modalidade': str(row.get('Modalidade de entrega', '')).strip(),
                'data_entrega': str(row.get('Entregue em', '')).strip(),
            }

    # 3. BUSCAR CUSTOS E MAPEAMENTO
    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    mapeamento_skus = buscar_mapeamento_skus(engine)
    
    # Status de descarte
    STATUS_DESCARTE = ['Pedido cancelado', 'Cancelado', 'Devolvido', 'Reembolsado']

    vendas = []
    descartes = []
    skus_sem_custo = set()
    skus_corrigidos = 0
    linhas_descartadas = 0

    # 4. AGRUPAR PEDIDOS PARA TRATAR CARRINHOS
    # Primeiro, identificar quais pedidos têm múltiplos itens
    pedido_grupos = df_ped.groupby('Número do pedido')

    for pedido_id, grupo in pedido_grupos:
        try:
            pedido_id = str(pedido_id).strip()
            
            # Buscar status no dict de pacotes
            info_pacote = status_dict.get(pedido_id, {})
            status = info_pacote.get('status', '')
            modalidade = info_pacote.get('modalidade', '')
            
            # Data do pedido (primeira linha do grupo)
            primeira_linha = grupo.iloc[0]
            data_str = str(primeira_linha.get('Data do Pedido', '')).strip()
            try:
                data_venda = datetime.strptime(data_str[:10], "%d/%m/%Y").date()
            except Exception:
                try:
                    data_venda = datetime.strptime(data_str[:10], "%Y-%m-%d").date()
                except Exception:
                    data_venda = None

            # Verificar cancelamento
            if status in STATUS_DESCARTE:
                for _, item_row in grupo.iterrows():
                    sku_raw = str(item_row.get('Codigo SKU seller', '')).strip()
                    preco = _limpar_valor_magalu(item_row.get('Valor Total do Item', 0))
                    
                    descartes.append({
                        'numero_pedido': pedido_id,
                        'sku': sku_raw,
                        'status_original': status,
                        'motivo_descarte': f"Status pacote: {status}",
                        'receita_estimada': preco,
                        'tarifa_venda_estimada': 0,
                        'tarifa_envio_estimada': 0,
                    })
                linhas_descartadas += len(grupo)
                continue

            # ============================================================
            # PROCESSAR ITENS DO PEDIDO (com rateio para carrinhos)
            # ============================================================
            n_itens = len(grupo)
            
            # Valores TOTAIS do pedido (repetem em cada linha)
            valor_bruto_pedido = _limpar_valor_magalu(primeira_linha.get('Valor bruto do pedido', 0))
            valor_liquido_pedido = _limpar_valor_magalu(primeira_linha.get('Valor líquido estimado a receber', 0))
            tarifa_fixa_pedido = abs(_limpar_valor_magalu(primeira_linha.get('Tarifa fixa', 0)))
            comissao_mkt_pedido = abs(_limpar_valor_magalu(primeira_linha.get('Serviços do marketplace (1+2+3)', 0)))
            copart_frete = abs(_limpar_valor_magalu(primeira_linha.get('Coparticipação de Fretes estimada', 0)))
            
            # Descontos campanha (podem ser por item)
            # Calcular soma de preços dos itens para rateio proporcional
            itens_precos = []
            for _, item_row in grupo.iterrows():
                preco_item = _limpar_valor_magalu(item_row.get('Valor Total do Item', 0))
                qtd_item = int(item_row.get('Quantidade de itens', 1) or 1)
                itens_precos.append({
                    'row': item_row,
                    'preco': preco_item,
                    'qtd': qtd_item,
                })
            
            soma_precos = sum(i['preco'] * i['qtd'] for i in itens_precos)
            if soma_precos <= 0:
                soma_precos = 1  # Evitar divisão por zero
            
            for item_info in itens_precos:
                item_row = item_info['row']
                preco_item = item_info['preco']
                qtd = item_info['qtd']
                sku_raw = str(item_row.get('Codigo SKU seller', '')).strip()

                # Proporção deste item no total do pedido
                proporcao = (preco_item * qtd) / soma_precos

                # Rateio proporcional de valores do pedido
                receita_liquida_item = valor_liquido_pedido * proporcao
                tarifa_fixa_item = 5.0 * qtd  # R$5,00 fixo por unidade
                comissao_item = comissao_mkt_pedido * proporcao
                frete_item = copart_frete * proporcao

                # Descontos do parceiro e Magalu
                desc_parceiro_vista = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Parceiro (Coparticipação de Desconto à Vista)', 0)))
                desc_magalu_vista = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Magalu (Coparticipação de Desconto à Vista)', 0)))
                desc_parceiro_promo = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Parceiro (Coparticipação de Preço Promocional)', 0)))
                desc_magalu_promo = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Magalu (Coparticipação de Preço Promocional)', 0)))
                desc_parceiro_cupom = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Parceiro (Valor subsídio Cupom)', 0)))
                desc_magalu_cupom = abs(_limpar_valor_magalu(
                    item_row.get('Pago pelo Magalu (Valor subsídio Cupom)', 0)))

                # Total desconto do parceiro (nosso)
                desconto_parceiro = desc_parceiro_vista + desc_parceiro_promo + desc_parceiro_cupom
                # Total desconto do marketplace
                desconto_marketplace = desc_magalu_vista + desc_magalu_promo + desc_magalu_cupom

                # Valor pago pelo cliente (unitário)
                # = preço do item - descontos totais (parceiro + magalu)
                receita_efetiva = (preco_item * qtd) - desconto_parceiro - desconto_marketplace

                # Imposto sobre o valor pago pelo cliente
                imposto_val = receita_efetiva * (imposto_pct / 100)

                # ============================================================
                # RESOLUÇÃO DE SKU
                # ============================================================
                # 1º: Tentar mapeamento direto
                sku_final = sku_raw
                if sku_raw in mapeamento_skus:
                    sku_final = mapeamento_skus[sku_raw]
                    skus_corrigidos += 1
                else:
                    # 2º: Normalizar (inserir hífen) e tentar mapeamento
                    sku_normalizado = _normalizar_sku_magalu(sku_raw)
                    if sku_normalizado in mapeamento_skus:
                        sku_final = mapeamento_skus[sku_normalizado]
                        skus_corrigidos += 1
                    elif sku_normalizado != sku_raw:
                        # Usar o normalizado como candidato
                        sku_final = sku_normalizado

                # Custo
                custo_un = custos_dict.get(sku_final, 0.0)
                custo_total = custo_un * qtd
                if custo_un == 0:
                    skus_sem_custo.add(sku_final)

                # Total tarifas = comissão marketplace + tarifa fixa + frete
                total_tarifas = comissao_item + tarifa_fixa_item + frete_item

                # Margem = receita líquida - custo - imposto - desconto parceiro
                # Receita líquida do Magalu já desconta comissão e tarifa fixa
                valor_liquido_item = receita_liquida_item
                margem = valor_liquido_item - custo_total - imposto_val - desconto_parceiro
                margem_pct = (margem / receita_efetiva * 100) if receita_efetiva > 0 else 0

                # ID sintético
                id_sintetico = f"MGLU_{loja}_{pedido_id}_{sku_raw}"

                for unidade in range(qtd):
                    # Se qtd > 1, gerar IDs únicos por unidade
                    id_un = f"{id_sintetico}_{unidade+1}" if qtd > 1 else id_sintetico
                    
                    vendas.append({
                        'pedido': id_un,
                        'pedido_original': pedido_id,
                        'data': data_venda,
                        'sku': sku_final,
                        'sku_magalu': sku_raw,
                        'modo_envio': modalidade,
                        'qtd': 1,  # Já estamos explodindo por unidade
                        'preco_venda': preco_item,
                        'receita': receita_efetiva / qtd,  # Por unidade
                        'desconto_parceiro': desconto_parceiro / qtd,
                        'desconto_marketplace': desconto_marketplace / qtd,
                        'comissao': comissao_item / qtd,
                        'tarifa_fixa': 5.0,  # Sempre R$5,00 por unidade
                        'frete': frete_item / qtd,
                        'imposto': imposto_val / qtd,
                        'custo': custo_un,
                        'total_tarifas': total_tarifas / qtd,
                        'valor_liquido': valor_liquido_item / qtd,
                        'margem': margem / qtd,
                        'margem_pct': margem_pct,
                        'tem_custo': custo_un > 0,
                        'status': status,
                    })

        except Exception:
            linhas_descartadas += 1
            continue

    # VALIDAR
    if not vendas and not descartes:
        return None, f"Nenhuma venda válida ({linhas_descartadas} linhas descartadas)"

    df_result = pd.DataFrame(vendas) if vendas else pd.DataFrame()

    info = {
        'total_linhas': len(vendas),
        'linhas_descartadas': linhas_descartadas,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo_pedidos.name,
        'divergencias': [],
        'carrinhos_encontrados': sum(1 for _, g in pedido_grupos if len(g) > 1),
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
    }

    return df_result, info


def gravar_vendas_magalu(df, marketplace, loja, arq_nome, engine, data_ini, data_fim,
                         descartes=None, pendentes_carrinho=None):
    """
    Grava vendas da Magalu com Delete-Before-Insert.
    Padrão Nala: SAVEPOINT, barra de progresso, 7 retornos.

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

        # 2. DESCARTES
        for descarte in descartes:
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Processando descartes... {item_atual} de {total_itens}")

                descarte['marketplace'] = marketplace
                descarte['loja'] = loja
                descarte['arquivo_origem'] = arq_nome

                cursor.execute(f"SAVEPOINT desc_mglu_{item_atual}")
                if gravar_venda_descartada(cursor, descarte):
                    desc_count += 1
                cursor.execute(f"RELEASE SAVEPOINT desc_mglu_{item_atual}")
            except Exception:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT desc_mglu_{item_atual}")
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
                data_venda = row['data']
                if data_venda is None:
                    data_venda = data_ini

                receita = float(row['receita'])
                preco_venda = float(row['preco_venda'])
                comissao = float(row['comissao'])
                tarifa_fixa = float(row['tarifa_fixa'])
                frete = float(row['frete'])
                imposto_val = float(row['imposto'])
                desconto_parceiro = float(row.get('desconto_parceiro', 0))
                desconto_marketplace = float(row.get('desconto_marketplace', 0))
                total_tarifas = float(row['total_tarifas'])
                valor_liquido = float(row['valor_liquido'])
                custo_un = float(row['custo'])
                margem = float(row['margem'])
                margem_pct = float(row['margem_pct'])

                # SKU não cadastrado → pendente
                if sku not in skus_cadastrados:
                    skus_invalidos.add(sku)

                    dados_pendente = {
                        'marketplace_origem': marketplace,
                        'loja_origem': loja,
                        'numero_pedido': str(row['pedido']),
                        'data_venda': data_venda,
                        'sku': sku,
                        'codigo_anuncio': '',
                        'quantidade': 1,
                        'preco_venda': preco_venda,
                        'valor_venda_efetivo': receita,
                        'imposto': imposto_val,
                        'comissao': comissao,
                        'frete': frete,
                        'tarifa_fixa': tarifa_fixa,
                        'outros_custos': 0,
                        'total_tarifas': total_tarifas,
                        'valor_liquido': valor_liquido,
                        'arquivo_origem': arq_nome,
                        'motivo': 'SKU não cadastrado',
                    }

                    cursor.execute(f"SAVEPOINT pend_mglu_{idx}")
                    try:
                        if gravar_venda_pendente(cursor, dados_pendente):
                            pend += 1
                        else:
                            err += 1
                        cursor.execute(f"RELEASE SAVEPOINT pend_mglu_{idx}")
                    except Exception:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT pend_mglu_{idx}")
                        err += 1
                    continue

                # Gravação normal
                cursor.execute(f"SAVEPOINT venda_mglu_{idx}")

                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), data_venda, sku,
                    '',                           # codigo_anuncio (Magalu não tem)
                    1,                            # quantidade (já explodido por unidade)
                    preco_venda,
                    desconto_parceiro,
                    desconto_marketplace,
                    receita,                      # valor_venda_efetivo
                    custo_un,
                    custo_un,                     # custo_total (qtd=1 por linha)
                    imposto_val,
                    comissao,
                    frete,
                    tarifa_fixa,
                    0,                            # outros_custos
                    total_tarifas,
                    valor_liquido,
                    margem,
                    margem_pct,
                    arq_nome
                ))

                cursor.execute(f"RELEASE SAVEPOINT venda_mglu_{idx}")
                reg += 1

            except Exception as e:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT venda_mglu_{idx}")
                except:
                    pass
                err += 1
                if err == 1:
                    st.warning(f"Primeiro erro Magalu: {str(e)[:200]}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.error(f"Erro crítico na gravação Magalu: {e}")
        err = len(df)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz
