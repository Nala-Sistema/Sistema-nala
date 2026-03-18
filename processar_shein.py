"""
PROCESSADOR SHEIN - Sistema Nala
VERSAO 1.1 (17/03/2026):
  - FIX: Periodo extraido automaticamente dos dados (nao precisa informar datas)
  - FIX: periodo_inicio e periodo_fim no info dict
  - NOVO: Salva pedido_original no banco (rastreabilidade)
  - Mantido: Parser data PT-BR, SKC, modo envio, descartes, SAVEPOINT
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

MESES_PT = {
    'janeiro': '01', 'fevereiro': '02', 'marco': '03', 'abril': '04',
    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12',
    'março': '03',
}


def _parse_data_shein(data_str):
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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(data_str).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


def processar_arquivo_shein(arquivo, loja, imposto_pct, engine):
    try:
        if arquivo.name.endswith('.csv'):
            df = pd.read_csv(arquivo, header=1)
        else:
            df = pd.read_excel(arquivo, header=1)
    except Exception as e:
        return None, f"Erro ao ler arquivo Shein: {e}"

    colunas_necessarias = ['Número do pedido', 'SKU do vendedor', 'Preço do produto']
    faltando = [c for c in colunas_necessarias if c not in df.columns]
    if faltando:
        return None, f"Colunas nao encontradas: {', '.join(faltando)}"

    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    mapeamento_skus = buscar_mapeamento_skus(engine)

    STATUS_DESCARTE = [
        'Cancelado', 'Reembolsado por cliente', 'Reembolsado',
        'Em devolução', 'Devolvido', 'Cancelado pelo sistema',
    ]

    vendas = []
    descartes = []
    skus_sem_custo = set()
    skus_corrigidos = 0
    linhas_descartadas = 0
    datas_encontradas = []

    for idx, row in df.iterrows():
        try:
            pedido = str(row.get('Número do pedido', '')).strip()
            sku_raw = str(row.get('SKU do vendedor', '')).strip()
            status = str(row.get('Status do pedido', '')).strip()
            id_item = str(row.get('ID do item', '')).strip()
            skc = str(row.get('SKC', '')).strip()
            modo_envio = str(row.get('Modo de envio', '')).strip()
            data_str = str(row.get('Data e hora de criação do pedido', '')).strip()

            if not pedido or pedido == 'nan':
                linhas_descartadas += 1
                continue

            data_venda = _parse_data_shein(data_str)
            if data_venda:
                datas_encontradas.append(data_venda)

            if status in STATUS_DESCARTE:
                preco = limpar_numero(row.get('Preço do produto', 0))
                comissao = limpar_numero(row.get('Comissão', 0))
                frete = limpar_numero(row.get('Taxa de intermediação de frete', 0))
                descartes.append({
                    'numero_pedido': pedido, 'sku': sku_raw,
                    'status_original': status, 'motivo_descarte': f"Status: {status}",
                    'receita_estimada': preco, 'tarifa_venda_estimada': comissao,
                    'tarifa_envio_estimada': frete,
                })
                linhas_descartadas += 1
                continue

            preco_venda = limpar_numero(row.get('Preço do produto', 0))
            cupom = limpar_numero(row.get('Valor do cupom', 0))
            desconto_campanha = limpar_numero(row.get('Desconto de campanha da loja', 0))
            comissao = limpar_numero(row.get('Comissão', 0))
            frete = limpar_numero(row.get('Taxa de intermediação de frete', 0))
            taxa_estocagem = limpar_numero(row.get('Taxa de operação de estocagem', 0))

            if preco_venda <= 0:
                linhas_descartadas += 1
                continue

            receita_efetiva = preco_venda - cupom - desconto_campanha

            sku_final = sku_raw
            if sku_raw in mapeamento_skus:
                sku_final = mapeamento_skus[sku_raw]
                skus_corrigidos += 1

            custo_un = custos_dict.get(sku_final, 0.0)
            if custo_un == 0:
                skus_sem_custo.add(sku_final)

            imposto_val = receita_efetiva * (imposto_pct / 100)
            total_tarifas = comissao + frete + taxa_estocagem
            valor_liquido = receita_efetiva - total_tarifas - imposto_val
            margem = valor_liquido - custo_un
            margem_pct = (margem / receita_efetiva * 100) if receita_efetiva > 0 else 0

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
                'qtd': 1,
                'preco_venda': round(preco_venda, 2),
                'receita': round(receita_efetiva, 2),
                'desconto_parceiro': round(cupom, 2),
                'desconto_marketplace': round(desconto_campanha, 2),
                'comissao': round(comissao, 2),
                'frete': round(frete, 2),
                'taxa_estocagem': round(taxa_estocagem, 2),
                'imposto': round(imposto_val, 2),
                'custo': round(custo_un, 2),
                'total_tarifas': round(total_tarifas, 2),
                'valor_liquido': round(valor_liquido, 2),
                'margem': round(margem, 2),
                'margem_pct': round(margem_pct, 2),
                'tem_custo': custo_un > 0,
                'status': status,
            })

        except Exception:
            linhas_descartadas += 1
            continue

    if not vendas and not descartes:
        return None, f"Nenhuma venda valida ({linhas_descartadas} linhas descartadas)"

    df_result = pd.DataFrame(vendas) if vendas else pd.DataFrame()

    # Periodo extraido dos dados
    if datas_encontradas:
        periodo_ini = min(datas_encontradas).strftime("%d/%m/%Y")
        periodo_fim = max(datas_encontradas).strftime("%d/%m/%Y")
    else:
        periodo_ini = '-'
        periodo_fim = '-'

    info = {
        'total_linhas': len(vendas),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': periodo_ini,
        'periodo_fim': periodo_fim,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name,
        'divergencias': [],
        'carrinhos_encontrados': 0,
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
    }

    return df_result, info


def gravar_vendas_shein(df, marketplace, loja, arq_nome, engine, data_ini=None, data_fim=None,
                        descartes=None, pendentes_carrinho=None):
    if descartes is None:
        descartes = []
    if pendentes_carrinho is None:
        pendentes_carrinho = []

    # Auto-detectar periodo se nao informado
    if (data_ini is None or data_fim is None) and not df.empty and 'data' in df.columns:
        datas_validas = df['data'].dropna()
        if not datas_validas.empty:
            data_ini = datas_validas.min()
            data_fim = datas_validas.max()

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
        # DELETE BEFORE INSERT (se temos periodo)
        if data_ini and data_fim:
            cursor.execute(
                "DELETE FROM fact_vendas_snapshot WHERE loja_origem = %s AND data_venda BETWEEN %s AND %s",
                (loja, data_ini, data_fim)
            )
            atualiz = cursor.rowcount

        # DESCARTES
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

        # GRAVAR VENDAS
        sql_ins = """
            INSERT INTO fact_vendas_snapshot (
                marketplace_origem, loja_origem, numero_pedido, pedido_original, data_venda, sku,
                codigo_anuncio, quantidade, preco_venda, desconto_parceiro, desconto_marketplace,
                valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao,
                frete, tarifa_fixa, outros_custos, total_tarifas, valor_liquido,
                margem_total, margem_percentual, data_processamento, arquivo_origem
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                pedido_original = str(row.get('pedido_original', ''))

                if sku not in skus_cadastrados:
                    skus_invalidos.add(sku)
                    dados_pendente = {
                        'marketplace_origem': marketplace, 'loja_origem': loja,
                        'numero_pedido': str(row['pedido']), 'data_venda': data_venda,
                        'sku': sku, 'codigo_anuncio': skc, 'quantidade': 1,
                        'preco_venda': preco_venda, 'valor_venda_efetivo': receita,
                        'imposto': imposto_val, 'comissao': comissao, 'frete': frete,
                        'tarifa_fixa': 0, 'outros_custos': taxa_estocagem,
                        'total_tarifas': total_tarifas, 'valor_liquido': valor_liquido,
                        'arquivo_origem': arq_nome, 'motivo': 'SKU não cadastrado',
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

                cursor.execute(f"SAVEPOINT venda_shein_{idx}")
                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), pedido_original, data_venda, sku,
                    skc, 1, preco_venda, desconto_parceiro, desconto_marketplace,
                    receita, custo_un, custo_un, imposto_val, comissao,
                    frete, 0, taxa_estocagem, total_tarifas, valor_liquido,
                    margem, margem_pct, arq_nome
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
        st.error(f"Erro critico na gravacao Shein: {e}")
        err = len(df)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz
