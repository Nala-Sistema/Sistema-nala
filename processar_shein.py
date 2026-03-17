"""
PROCESSADOR SHEIN - Sistema Nala
Processa Relatório de Pedidos Shein (XLSX)

VERSÃO 1.0 (17/03/2026):
  - Construído do zero com base na planilha real da Shein
  - Header na linha 1 (linha 0 é agrupador de categorias)
  - Cada linha = 1 unidade (mesmo pedido repete = múltiplas unidades)
  - Receita líquida Shein = Preço - Comissão - Frete (já vem calculada)
  - Considera: cupom, desconto campanha, taxa de estocagem
  - Data em formato PT-BR ("31 janeiro 2026 00:23") com parser especial
  - SKC salvo como código de anúncio
  - Modo de envio salvo como logística (para dashboards futuros)
  - Descartes: Cancelado, Reembolsado → fact_vendas_descartadas
  - Padrão Nala: SAVEPOINT por linha, mapeamento SKU, barra de progresso
  - Imposto como parâmetro (vem da config da loja), não fixo
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

# Meses em português para parser de data
MESES_PT = {
    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12',
}


def _parse_data_shein(data_str):
    """
    Converte data Shein "31 janeiro 2026 00:23" → date object.
    Retorna None se não conseguir.
    """
    if not data_str or str(data_str).strip() in ('', 'nan', 'NaT'):
        return None
    try:
        partes = str(data_str).strip().split()
        if len(partes) >= 3:
            dia = partes[0].zfill(2)
            mes = MESES_PT.get(partes[1].lower(), '01')
            ano = partes[2]
            return datetime.strptime(f"{dia}/{mes}/{ano}", "%d/%m/%Y").date()
    except Exception:
        pass
    # Fallback: tentar formatos comuns
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(data_str).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


def processar_arquivo_shein(arquivo, loja, imposto_pct, engine):
    """
    Lê o relatório da Shein (XLSX) e prepara os dados para gravação.

    Colunas relevantes da Shein:
        - 'Número do pedido': ID do pedido (pode repetir = múltiplas unidades)
        - 'SKU do vendedor': SKU usado pelo vendedor (nosso SKU)
        - 'SKC': código de variação do anúncio Shein
        - 'ID do item': ID único por linha (usado no ID sintético)
        - 'Status do pedido': Entregue, Enviado, Reembolsado, etc.
        - 'Modo de envio': tipo de logística
        - 'Preço do produto': preço bruto unitário
        - 'Valor do cupom': desconto via cupom
        - 'Desconto de campanha da loja': desconto campanha
        - 'Comissão': comissão Shein
        - 'Taxa de intermediação de frete': frete cobrado pela Shein
        - 'Taxa de operação de estocagem': taxa de armazém (se houver)
        - 'Receita estimada de mercadorias': receita líquida calculada pela Shein
        - 'Data e hora de criação do pedido': data em formato PT-BR

    Args:
        arquivo: arquivo XLSX ou CSV (UploadedFile)
        loja: nome da loja (str)
        imposto_pct: percentual de imposto (float) — parâmetro da config
        engine: SQLAlchemy engine

    Retorna:
        (df_processado, info_dict) ou (None, erro_msg)
    """
    try:
        # Header na linha 1 (linha 0 é agrupador "Solicite informações básicas")
        if arquivo.name.endswith('.csv'):
            df = pd.read_csv(arquivo, header=1)
        else:
            df = pd.read_excel(arquivo, header=1)
    except Exception as e:
        return None, f"Erro ao ler arquivo Shein: {e}"

    # Validar colunas essenciais
    colunas_necessarias = ['Número do pedido', 'SKU do vendedor', 'Preço do produto']
    faltando = [c for c in colunas_necessarias if c not in df.columns]
    if faltando:
        return None, f"Colunas não encontradas: {', '.join(faltando)}"

    # Buscar custos e mapeamento
    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    mapeamento_skus = buscar_mapeamento_skus(engine)

    # Status que NÃO são vendas válidas → vão para descartes
    STATUS_DESCARTE = [
        'Cancelado', 'Reembolsado por cliente', 'Reembolsado',
        'Em devolução', 'Devolvido', 'Cancelado pelo sistema',
    ]

    vendas = []
    descartes = []
    skus_sem_custo = set()
    skus_corrigidos = 0
    linhas_descartadas = 0

    for idx, row in df.iterrows():
        try:
            pedido = str(row.get('Número do pedido', '')).strip()
            sku_raw = str(row.get('SKU do vendedor', '')).strip()
            status = str(row.get('Status do pedido', '')).strip()
            id_item = str(row.get('ID do item', '')).strip()
            skc = str(row.get('SKC', '')).strip()
            modo_envio = str(row.get('Modo de envio', '')).strip()
            data_str = str(row.get('Data e hora de criação do pedido', '')).strip()

            # Validação básica
            if not pedido or pedido == 'nan':
                linhas_descartadas += 1
                continue

            # Parse de data
            data_venda = _parse_data_shein(data_str)

            # Status de descarte → contabilizar para dashboards futuros
            if status in STATUS_DESCARTE:
                preco = limpar_numero(row.get('Preço do produto', 0))
                comissao = limpar_numero(row.get('Comissão', 0))
                frete = limpar_numero(row.get('Taxa de intermediação de frete', 0))

                descartes.append({
                    'numero_pedido': pedido,
                    'sku': sku_raw,
                    'status_original': status,
                    'motivo_descarte': f"Status: {status}",
                    'receita_estimada': preco,
                    'tarifa_venda_estimada': comissao,
                    'tarifa_envio_estimada': frete,
                })
                linhas_descartadas += 1
                continue

            # Limpeza de valores financeiros
            preco_venda = limpar_numero(row.get('Preço do produto', 0))
            cupom = limpar_numero(row.get('Valor do cupom', 0))
            desconto_campanha = limpar_numero(row.get('Desconto de campanha da loja', 0))
            comissao = limpar_numero(row.get('Comissão', 0))
            frete = limpar_numero(row.get('Taxa de intermediação de frete', 0))
            taxa_estocagem = limpar_numero(row.get('Taxa de operação de estocagem', 0))
            receita_shein = limpar_numero(row.get('Receita estimada de mercadorias', 0))

            # Descartar linhas sem valor
            if preco_venda <= 0:
                linhas_descartadas += 1
                continue

            # Receita efetiva = preço - cupom - desconto campanha
            receita_efetiva = preco_venda - cupom - desconto_campanha

            # Mapeamento de SKU
            sku_final = sku_raw
            if sku_raw in mapeamento_skus:
                sku_final = mapeamento_skus[sku_raw]
                skus_corrigidos += 1

            # Busca de custo
            custo_un = custos_dict.get(sku_final, 0.0)
            if custo_un == 0:
                skus_sem_custo.add(sku_final)

            # Cálculos
            imposto_val = receita_efetiva * (imposto_pct / 100)
            total_tarifas = comissao + frete + taxa_estocagem
            valor_liquido = receita_efetiva - total_tarifas - imposto_val
            margem = valor_liquido - custo_un
            margem_pct = (margem / receita_efetiva * 100) if receita_efetiva > 0 else 0

            # ID sintético único por linha (pedido + id_item garante unicidade)
            if id_item and id_item != 'nan':
                id_sintetico = f"SHEIN_{loja}_{pedido}_{id_item}"
            else:
                id_sintetico = f"SHEIN_{loja}_{pedido}_{sku_raw}_{idx}"

            vendas.append({
                'pedido': id_sintetico,
                'pedido_original': pedido,
                'data': data_venda,
                'sku': sku_final,
                'sku_original': sku_raw,
                'skc': skc if skc != 'nan' else '',
                'modo_envio': modo_envio if modo_envio != 'nan' else '',
                'qtd': 1,  # Shein: 1 linha = 1 unidade sempre
                'preco_venda': preco_venda,
                'receita': receita_efetiva,
                'desconto_parceiro': cupom,
                'desconto_marketplace': desconto_campanha,
                'comissao': comissao,
                'frete': frete,
                'taxa_estocagem': taxa_estocagem,
                'imposto': imposto_val,
                'custo': custo_un,
                'total_tarifas': total_tarifas,
                'valor_liquido': valor_liquido,
                'margem': margem,
                'margem_pct': margem_pct,
                'tem_custo': custo_un > 0,
                'status': status,
            })

        except Exception:
            linhas_descartadas += 1
            continue

    # Validar se tem vendas
    if not vendas and not descartes:
        return None, f"Nenhuma venda válida encontrada ({linhas_descartadas} linhas descartadas)"

    df_result = pd.DataFrame(vendas) if vendas else pd.DataFrame()

    # Info compatível com central_uploads
    info = {
        'total_linhas': len(vendas),
        'linhas_descartadas': linhas_descartadas,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name,
        'divergencias': [],
        'carrinhos_encontrados': 0,
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
    }

    return df_result, info


def gravar_vendas_shein(df, marketplace, loja, arq_nome, engine, data_ini, data_fim,
                        descartes=None, pendentes_carrinho=None):
    """
    Grava vendas da Shein com Delete-Before-Insert para evitar duplicatas.

    Padrão Nala: SAVEPOINT por linha, barra de progresso, pendentes padronizados.

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
        for descarte in descartes:
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Processando descartes... {item_atual} de {total_itens}")

                descarte['marketplace'] = marketplace
                descarte['loja'] = loja
                descarte['arquivo_origem'] = arq_nome

                cursor.execute(f"SAVEPOINT desc_shein_{item_atual}")
                if gravar_venda_descartada(cursor, descarte):
                    desc_count += 1
                cursor.execute(f"RELEASE SAVEPOINT desc_shein_{item_atual}")
            except Exception:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT desc_shein_{item_atual}")
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

                # Se data não parseou, usa data_ini do período
                if data_venda is None:
                    data_venda = data_ini

                receita = float(row['receita'])
                preco_venda = float(row['preco_venda'])
                comissao = float(row['comissao'])
                frete = float(row['frete'])
                taxa_estocagem = float(row.get('taxa_estocagem', 0))
                imposto_val = float(row['imposto'])
                desconto_parceiro = float(row.get('desconto_parceiro', 0))
                desconto_marketplace = float(row.get('desconto_marketplace', 0))
                total_tarifas = float(row['total_tarifas'])
                valor_liquido = float(row['valor_liquido'])
                custo_un = float(row['custo'])
                margem = float(row['margem'])
                margem_pct = float(row['margem_pct'])
                skc = str(row.get('skc', ''))

                # SKU não cadastrado → pendente
                if sku not in skus_cadastrados:
                    skus_invalidos.add(sku)

                    dados_pendente = {
                        'marketplace_origem': marketplace,
                        'loja_origem': loja,
                        'numero_pedido': str(row['pedido']),
                        'data_venda': data_venda,
                        'sku': sku,
                        'codigo_anuncio': skc,
                        'quantidade': 1,
                        'preco_venda': preco_venda,
                        'valor_venda_efetivo': receita,
                        'imposto': imposto_val,
                        'comissao': comissao,
                        'frete': frete,
                        'tarifa_fixa': 0,
                        'outros_custos': taxa_estocagem,
                        'total_tarifas': total_tarifas,
                        'valor_liquido': valor_liquido,
                        'arquivo_origem': arq_nome,
                        'motivo': 'SKU não cadastrado',
                    }

                    cursor.execute(f"SAVEPOINT pend_shein_{idx}")
                    try:
                        if gravar_venda_pendente(cursor, dados_pendente):
                            pend += 1
                        else:
                            err += 1
                        cursor.execute(f"RELEASE SAVEPOINT pend_shein_{idx}")
                    except Exception:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT pend_shein_{idx}")
                        err += 1
                    continue

                # Gravação normal
                cursor.execute(f"SAVEPOINT venda_shein_{idx}")

                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), data_venda, sku,
                    skc,                          # codigo_anuncio = SKC
                    1,                            # quantidade (sempre 1 na Shein)
                    preco_venda,                  # preco_venda
                    desconto_parceiro,            # desconto_parceiro = cupom
                    desconto_marketplace,         # desconto_marketplace = desconto campanha
                    receita,                      # valor_venda_efetivo
                    custo_un,                     # custo_unitario
                    custo_un,                     # custo_total (qtd=1, então igual)
                    imposto_val,                  # imposto
                    comissao,                     # comissao
                    frete,                        # frete
                    0,                            # tarifa_fixa (Shein não tem)
                    taxa_estocagem,               # outros_custos = taxa estocagem
                    total_tarifas,                # total_tarifas
                    valor_liquido,                # valor_liquido
                    margem,                       # margem_total
                    margem_pct,                   # margem_percentual
                    arq_nome                      # arquivo_origem
                ))

                cursor.execute(f"RELEASE SAVEPOINT venda_shein_{idx}")
                reg += 1

            except Exception as e:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT venda_shein_{idx}")
                except:
                    pass
                err += 1
                if err == 1:
                    st.warning(f"Primeiro erro Shein: {str(e)[:200]}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.error(f"Erro crítico na gravação Shein: {e}")
        err = len(df)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz
