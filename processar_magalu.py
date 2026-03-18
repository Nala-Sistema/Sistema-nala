"""
PROCESSADOR MAGALU - Sistema Nala
VERSAO 1.1 (17/03/2026):
  - FIX: Periodo extraido automaticamente dos dados (nao precisa informar datas)
  - FIX: Valores arredondados com 2 casas decimais
  - NOVO: Salva pedido_original no banco (rastreabilidade)
  - Mantido: 2 CSVs, rateio carrinhos, normalizacao SKU, SAVEPOINT
"""

import pandas as pd
import streamlit as st
import re
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
    if pd.isna(valor) or str(valor).strip() in ('', 'Não se aplica', 'nan'):
        return 0.0
    s = str(valor).replace('R$', '').replace(' ', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _normalizar_sku_magalu(sku_magalu):
    sku = str(sku_magalu).strip()
    if not sku or sku == 'nan':
        return sku
    if '-' in sku:
        return sku
    match = re.match(r'^([A-Za-z]+)(\d.*)$', sku)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return sku


def processar_arquivo_magalu(arquivo_pedidos, arquivo_pacotes, loja, imposto_pct, engine):
    try:
        df_ped = pd.read_csv(arquivo_pedidos)
    except Exception as e:
        return None, f"Erro ao ler pedidos Magalu: {e}"
    try:
        df_pac = pd.read_csv(arquivo_pacotes)
    except Exception as e:
        return None, f"Erro ao ler pacotes Magalu: {e}"

    cols_ped = ['Número do pedido', 'Codigo SKU seller', 'Valor Total do Item', 'Valor líquido estimado a receber']
    faltando = [c for c in cols_ped if c not in df_ped.columns]
    if faltando:
        return None, f"Colunas nao encontradas em pedidos: {', '.join(faltando)}"

    # Status/logistica dos pacotes
    status_dict = {}
    for _, row in df_pac.iterrows():
        pedido = str(row.get('Número do pedido', '')).strip()
        if pedido and pedido != 'nan':
            status_dict[pedido] = {
                'status': str(row.get('Status pacote no momento que o relatório foi solicitado', '')).strip(),
                'modalidade': str(row.get('Modalidade de entrega', '')).strip(),
            }

    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)
    mapeamento_skus = buscar_mapeamento_skus(engine)

    STATUS_DESCARTE = ['Pedido cancelado', 'Cancelado', 'Devolvido', 'Reembolsado']

    vendas = []
    descartes = []
    skus_sem_custo = set()
    skus_corrigidos = 0
    linhas_descartadas = 0
    datas_encontradas = []
    carrinhos_count = 0

    pedido_grupos = df_ped.groupby('Número do pedido')

    for pedido_id, grupo in pedido_grupos:
        try:
            pedido_id = str(pedido_id).strip()
            info_pacote = status_dict.get(pedido_id, {})
            status = info_pacote.get('status', '')
            modalidade = info_pacote.get('modalidade', '')

            primeira_linha = grupo.iloc[0]
            data_str = str(primeira_linha.get('Data do Pedido', '')).strip()
            try:
                data_venda = datetime.strptime(data_str[:10], "%d/%m/%Y").date()
            except Exception:
                try:
                    data_venda = datetime.strptime(data_str[:10], "%Y-%m-%d").date()
                except Exception:
                    data_venda = None

            if data_venda:
                datas_encontradas.append(data_venda)

            if status in STATUS_DESCARTE:
                for _, item_row in grupo.iterrows():
                    sku_raw = str(item_row.get('Codigo SKU seller', '')).strip()
                    preco = _limpar_valor_magalu(item_row.get('Valor Total do Item', 0))
                    descartes.append({
                        'numero_pedido': pedido_id, 'sku': sku_raw,
                        'status_original': status, 'motivo_descarte': f"Status pacote: {status}",
                        'receita_estimada': preco, 'tarifa_venda_estimada': 0, 'tarifa_envio_estimada': 0,
                    })
                linhas_descartadas += len(grupo)
                continue

            n_itens = len(grupo)
            if n_itens > 1:
                carrinhos_count += 1

            valor_bruto_pedido = _limpar_valor_magalu(primeira_linha.get('Valor bruto do pedido', 0))
            valor_liquido_pedido = _limpar_valor_magalu(primeira_linha.get('Valor líquido estimado a receber', 0))
            tarifa_fixa_pedido = abs(_limpar_valor_magalu(primeira_linha.get('Tarifa fixa', 0)))
            comissao_mkt_pedido = abs(_limpar_valor_magalu(primeira_linha.get('Serviços do marketplace (1+2+3)', 0)))
            copart_frete = abs(_limpar_valor_magalu(primeira_linha.get('Coparticipação de Fretes estimada', 0)))

            itens_precos = []
            for _, item_row in grupo.iterrows():
                preco_item = _limpar_valor_magalu(item_row.get('Valor Total do Item', 0))
                qtd_item = int(item_row.get('Quantidade de itens', 1) or 1)
                itens_precos.append({'row': item_row, 'preco': preco_item, 'qtd': qtd_item})

            soma_precos = sum(i['preco'] * i['qtd'] for i in itens_precos)
            if soma_precos <= 0:
                soma_precos = 1

            for item_info in itens_precos:
                item_row = item_info['row']
                preco_item = item_info['preco']
                qtd = item_info['qtd']
                sku_raw = str(item_row.get('Codigo SKU seller', '')).strip()

                proporcao = (preco_item * qtd) / soma_precos

                receita_liquida_item = valor_liquido_pedido * proporcao
                tarifa_fixa_item = 5.0 * qtd
                comissao_item = comissao_mkt_pedido * proporcao
                frete_item = copart_frete * proporcao

                desc_parceiro_vista = abs(_limpar_valor_magalu(item_row.get('Pago pelo Parceiro (Coparticipação de Desconto à Vista)', 0)))
                desc_magalu_vista = abs(_limpar_valor_magalu(item_row.get('Pago pelo Magalu (Coparticipação de Desconto à Vista)', 0)))
                desc_parceiro_promo = abs(_limpar_valor_magalu(item_row.get('Pago pelo Parceiro (Coparticipação de Preço Promocional)', 0)))
                desc_magalu_promo = abs(_limpar_valor_magalu(item_row.get('Pago pelo Magalu (Coparticipação de Preço Promocional)', 0)))
                desc_parceiro_cupom = abs(_limpar_valor_magalu(item_row.get('Pago pelo Parceiro (Valor subsídio Cupom)', 0)))
                desc_magalu_cupom = abs(_limpar_valor_magalu(item_row.get('Pago pelo Magalu (Valor subsídio Cupom)', 0)))

                desconto_parceiro = desc_parceiro_vista + desc_parceiro_promo + desc_parceiro_cupom
                desconto_marketplace = desc_magalu_vista + desc_magalu_promo + desc_magalu_cupom

                receita_efetiva = (preco_item * qtd) - desconto_parceiro - desconto_marketplace
                imposto_val = receita_efetiva * (imposto_pct / 100)

                sku_final = sku_raw
                if sku_raw in mapeamento_skus:
                    sku_final = mapeamento_skus[sku_raw]
                    skus_corrigidos += 1
                else:
                    sku_normalizado = _normalizar_sku_magalu(sku_raw)
                    if sku_normalizado in mapeamento_skus:
                        sku_final = mapeamento_skus[sku_normalizado]
                        skus_corrigidos += 1
                    elif sku_normalizado != sku_raw:
                        sku_final = sku_normalizado

                custo_un = custos_dict.get(sku_final, 0.0)
                custo_total = custo_un * qtd
                if custo_un == 0:
                    skus_sem_custo.add(sku_final)

                total_tarifas = comissao_item + tarifa_fixa_item + frete_item
                valor_liquido_item = receita_liquida_item
                margem = valor_liquido_item - custo_total - imposto_val - desconto_parceiro
                margem_pct = (margem / receita_efetiva * 100) if receita_efetiva > 0 else 0

                id_sintetico = f"MGLU_{loja}_{pedido_id}_{sku_raw}"

                for unidade in range(qtd):
                    id_un = f"{id_sintetico}_{unidade+1}" if qtd > 1 else id_sintetico

                    vendas.append({
                        'pedido': id_un,
                        'pedido_original': pedido_id,
                        'data': data_venda,
                        'sku': sku_final,
                        'sku_magalu': sku_raw,
                        'modo_envio': modalidade,
                        'qtd': 1,
                        'preco_venda': round(preco_item, 2),
                        'receita': round(receita_efetiva / qtd, 2),
                        'desconto_parceiro': round(desconto_parceiro / qtd, 2),
                        'desconto_marketplace': round(desconto_marketplace / qtd, 2),
                        'comissao': round(comissao_item / qtd, 2),
                        'tarifa_fixa': 5.0,
                        'frete': round(frete_item / qtd, 2),
                        'imposto': round(imposto_val / qtd, 2),
                        'custo': round(custo_un, 2),
                        'total_tarifas': round(total_tarifas / qtd, 2),
                        'valor_liquido': round(valor_liquido_item / qtd, 2),
                        'margem': round(margem / qtd, 2),
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
        'arquivo_nome': arquivo_pedidos.name,
        'divergencias': [],
        'carrinhos_encontrados': carrinhos_count,
        'skus_corrigidos': skus_corrigidos,
        'descartes': descartes,
        'pendentes_carrinho': [],
    }

    return df_result, info


def gravar_vendas_magalu(df, marketplace, loja, arq_nome, engine, data_ini=None, data_fim=None,
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
        if data_ini and data_fim:
            cursor.execute(
                "DELETE FROM fact_vendas_snapshot WHERE loja_origem = %s AND data_venda BETWEEN %s AND %s",
                (loja, data_ini, data_fim)
            )
            atualiz = cursor.rowcount

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
                pedido_original = str(row.get('pedido_original', ''))

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

                if sku not in skus_cadastrados:
                    skus_invalidos.add(sku)
                    dados_pendente = {
                        'marketplace_origem': marketplace, 'loja_origem': loja,
                        'numero_pedido': str(row['pedido']), 'data_venda': data_venda,
                        'sku': sku, 'codigo_anuncio': '', 'quantidade': 1,
                        'preco_venda': preco_venda, 'valor_venda_efetivo': receita,
                        'imposto': imposto_val, 'comissao': comissao, 'frete': frete,
                        'tarifa_fixa': tarifa_fixa, 'outros_custos': 0,
                        'total_tarifas': total_tarifas, 'valor_liquido': valor_liquido,
                        'arquivo_origem': arq_nome, 'motivo': 'SKU não cadastrado',
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

                cursor.execute(f"SAVEPOINT venda_mglu_{idx}")
                cursor.execute(sql_ins, (
                    marketplace, loja, str(row['pedido']), pedido_original, data_venda, sku,
                    '', 1, preco_venda, desconto_parceiro, desconto_marketplace,
                    receita, custo_un, custo_un, imposto_val, comissao,
                    frete, tarifa_fixa, 0, total_tarifas, valor_liquido,
                    margem, margem_pct, arq_nome
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
        st.error(f"Erro critico na gravacao Magalu: {e}")
        err = len(df)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz
