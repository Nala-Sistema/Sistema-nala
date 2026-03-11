"""
PROCESSADOR MERCADO LIVRE - Sistema Nala
Processa arquivos de vendas do Mercado Livre

VERSÃO 3.0 (11/03/2026):
  - NOVO: Lógica de carrinho (parent-child) com distribuição proporcional
  - NOVO: Rastreamento de descartes em fact_vendas_descartadas
  - NOVO: Divergência financeira em carrinhos → pendentes com motivo
  - NOVO: Reimportação inteligente — atualiza status (ex: Entregue → Devolvido)
  - FLEX dinâmico: custo lido de dim_lojas.custo_flex (fallback R$ 12,90)
  - Retorno expandido: +descartadas_count, +atualizados_count (7 valores)

VERSÃO 2.0 (10/03/2026):
  - Proteção contra duplicatas: pré-carrega (pedido, sku) existentes
  - Vendas pendentes: SKU não cadastrado vai para fact_vendas_pendentes
  - FLEX dinâmico: custo lido de dim_lojas.custo_flex (fallback R$ 12,90)
  - Retorno expandido: (registros, erros, skus_invalidos, duplicatas, pendentes)

CORREÇÕES ANTERIORES:
  - 09/03/2026: codigo_anuncio agora mapeia com e sem acento
  - 10/03/2026: rollback individual com SAVEPOINT (não perde vendas já gravadas)
"""

import re
import pandas as pd
import streamlit as st
from datetime import datetime
from formatadores import converter_data_ml, limpar_numero
from database_utils import (
    buscar_custos_skus,
    buscar_skus_validos,
    buscar_duplicatas_loja,
    gravar_venda_pendente,
    buscar_custo_flex,
    gravar_venda_descartada,
    deletar_venda_snapshot,
)

# CONFIGURAÇÃO FLEX (fallback caso dim_lojas.custo_flex esteja vazio)
CUSTO_FLEX_ML_PADRAO = 12.90

# Status que indicam venda inválida (normalizado, sem acentos)
PALAVRAS_DESCARTE = ['cancelad', 'devolv', 'devoluc', 'reembolso', 'mediacao']

# Tolerância para divergência financeira em carrinhos (R$)
TOLERANCIA_DIVERGENCIA = 5.00


def detectar_header_ml(arquivo):
    """
    Detecta em qual linha está o header do arquivo ML.
    Procura por 'sku' nas primeiras 20 linhas.
    """
    df_raw = pd.read_excel(arquivo, header=None, nrows=20)

    for idx in range(20):
        linha = df_raw.iloc[idx]
        # Procura por 'sku' em qualquer coluna
        if any('sku' in str(c).lower() for c in linha):
            return idx

    # Se não encontrar, assume linha 5 (padrão ML)
    return 5


def _normalizar_texto(texto):
    """
    Remove acentos e caracteres especiais para comparação segura.
    Exemplo: 'anúncio' -> 'anuncio', 'mediação' -> 'mediacao'
    """
    substituicoes = {
        'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i',
        'ó': 'o', 'ò': 'o', 'õ': 'o', 'ô': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u',
        'ç': 'c', 'ñ': 'n',
    }
    resultado = texto.lower()
    for acentuado, sem_acento in substituicoes.items():
        resultado = resultado.replace(acentuado, sem_acento)
    return resultado


def _eh_status_descarte(status_str):
    """
    Verifica se o status indica venda inválida (cancelada/devolvida/mediação).
    Retorna (True/False, motivo_descarte).
    """
    if not status_str or str(status_str).strip() == '':
        return False, ''

    status_normalizado = _normalizar_texto(str(status_str))

    for palavra in PALAVRAS_DESCARTE:
        if palavra in status_normalizado:
            return True, str(status_str).strip()

    return False, ''


def _eh_mestra_carrinho(status_str):
    """
    Verifica se a linha é uma mestra de carrinho.
    Detecta pelo campo Estado contendo 'Pacote de N produtos'.
    Retorna (True/False, numero_de_filhas).
    """
    if not status_str or str(status_str).strip() == '':
        return False, 0

    status_normalizado = _normalizar_texto(str(status_str))

    # Padrão: "pacote de 2 produtos", "pacote de 3 produtos", etc.
    match = re.search(r'pacote de (\d+) produto', status_normalizado)
    if match:
        n_filhas = int(match.group(1))
        return True, n_filhas

    return False, 0


def renomear_colunas_ml(df):
    """
    Renomeia colunas do ML para nomes padronizados.
    Usa normalização de acentos para evitar falhas de mapeamento.
    
    VERSÃO 3.0: Adicionado mapeamento de preco_unit_anuncio.
    """
    rename_map = {}

    for col in df.columns:
        col_lower = str(col).lower().strip()
        # Versão sem acentos para comparação segura
        col_norm = _normalizar_texto(str(col).strip())

        # Colunas principais
        if 'n.' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'pedido'
        elif 'data' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'data'
        elif col_lower == 'sku':
            rename_map[col] = 'sku'
        elif 'estado' in col_lower and 'estado.' not in col_lower:
            rename_map[col] = 'status'
        elif 'unidades' in col_lower and 'unidades.' not in col_lower:
            rename_map[col] = 'qtd'

        # Receitas e tarifas
        elif 'receita' in col_lower and 'produtos' in col_lower:
            rename_map[col] = 'receita'
        elif 'tarifa' in col_lower and 'venda' in col_lower:
            rename_map[col] = 'tarifa'
        elif 'receita' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'receita_envio'
        elif 'tarifas' in col_lower and 'envio' in col_lower:
            rename_map[col] = 'tarifa_envio'

        # Total e forma de entrega
        elif col_lower == 'total (brl)':
            rename_map[col] = 'total_brl'
        elif 'forma' in col_lower and 'entrega' in col_lower:
            rename_map[col] = 'forma_entrega'

        # CORREÇÃO 09/03: Mapear código do anúncio (com e sem acento)
        elif '#' in col_norm and 'anuncio' in col_norm:
            rename_map[col] = 'codigo_anuncio'

        # NOVO v3.0: Preço unitário de venda do anúncio (para distribuição de carrinho)
        elif 'preco' in col_norm and 'unitario' in col_norm and 'anuncio' in col_norm:
            rename_map[col] = 'preco_unit_anuncio'

    return df.rename(columns=rename_map)


def _processar_carrinhos(df):
    """
    Pré-processa pedidos de carrinho (pacotes com múltiplos produtos).

    Lógica:
    1. Identifica linhas mestra pelo campo status contendo 'Pacote de N produtos'
    2. As N linhas subsequentes são as filhas (têm SKU, preço unitário, mas sem financeiros)
    3. Distribui receita/tarifas da mestra proporcionalmente pelo peso de cada filha
    4. Peso = preco_unit_anuncio × quantidade
    5. Copia forma_entrega da mestra para as filhas
    6. Verifica divergência financeira por grupo (tolerância R$ 5,00)

    Retorna:
        df modificado com colunas auxiliares:
            _is_mestra (bool) — True para linhas mestra (serão ignoradas no loop)
            _carrinho_grupo (str) — pedido da mestra para agrupar filhas
            _total_brl_carrinho (float) — total_brl da mestra para check de divergência
    """

    # Inicializar colunas auxiliares
    df['_is_mestra'] = False
    df['_carrinho_grupo'] = ''
    df['_total_brl_carrinho'] = 0.0

    idx = 0
    carrinhos_encontrados = 0

    while idx < len(df):
        row = df.iloc[idx]
        status_str = str(row.get('status', ''))

        # Verificar se é mestra de carrinho
        is_mestra, n_filhas = _eh_mestra_carrinho(status_str)

        if not is_mestra:
            idx += 1
            continue

        # ---- MESTRA ENCONTRADA ----
        carrinhos_encontrados += 1
        df_idx = df.index[idx]
        df.at[df_idx, '_is_mestra'] = True

        # Extrair financeiros da mestra
        receita_mestra = abs(limpar_numero(row.get('receita', 0)))
        tarifa_mestra = abs(limpar_numero(row.get('tarifa', 0)))
        rec_envio_mestra = abs(limpar_numero(row.get('receita_envio', 0)))
        tar_envio_mestra = abs(limpar_numero(row.get('tarifa_envio', 0)))
        total_brl_mestra = limpar_numero(row.get('total_brl', 0))
        forma_entrega_mestra = str(row.get('forma_entrega', ''))
        pedido_mestra = str(row.get('pedido', ''))

        # Coletar filhas (as N linhas seguintes)
        filhas_info = []
        filhas_reais = 0

        for f in range(1, n_filhas + 1):
            filha_pos = idx + f
            if filha_pos >= len(df):
                break

            filha = df.iloc[filha_pos]
            filha_sku = str(filha.get('sku', '')).strip()

            # Validar que é realmente uma filha (tem SKU e receita vazia/zero)
            receita_filha = limpar_numero(filha.get('receita', 0))
            if filha_sku == '' or receita_filha > 0:
                # Não é uma filha válida — pode ser outro pedido
                break

            # Calcular peso para distribuição proporcional
            preco_unit = limpar_numero(filha.get('preco_unit_anuncio', 0))
            try:
                qtd_filha = int(filha.get('qtd', 1)) if not pd.isna(filha.get('qtd')) else 1
                if qtd_filha <= 0:
                    qtd_filha = 1
            except (ValueError, TypeError):
                qtd_filha = 1

            peso = preco_unit * qtd_filha

            filhas_info.append({
                'df_index': df.index[filha_pos],
                'peso': peso,
            })
            filhas_reais += 1

        # Calcular peso total
        peso_total = sum(f['peso'] for f in filhas_info)

        # Se peso_total = 0 (sem preço unitário), distribuir igualmente
        if peso_total <= 0 and filhas_reais > 0:
            for f in filhas_info:
                f['peso'] = 1.0
            peso_total = float(filhas_reais)

        # Distribuir financeiros proporcionalmente para cada filha
        if peso_total > 0 and filhas_reais > 0:
            for f in filhas_info:
                proporcao = f['peso'] / peso_total
                fidx = f['df_index']

                df.at[fidx, 'receita'] = receita_mestra * proporcao
                df.at[fidx, 'tarifa'] = -(tarifa_mestra * proporcao)  # Manter sinal negativo como no original
                df.at[fidx, 'receita_envio'] = rec_envio_mestra * proporcao
                df.at[fidx, 'tarifa_envio'] = tar_envio_mestra * proporcao
                df.at[fidx, 'forma_entrega'] = forma_entrega_mestra
                df.at[fidx, '_carrinho_grupo'] = pedido_mestra
                df.at[fidx, '_total_brl_carrinho'] = total_brl_mestra

        # Avançar além das filhas
        idx += 1 + filhas_reais
        continue

    return df, carrinhos_encontrados


def processar_arquivo_ml(arquivo, loja, imposto, engine):
    """
    Processa arquivo Excel do Mercado Livre.

    VERSÃO 3.0:
    - Pré-processamento de carrinhos (parent-child)
    - Coleta de descartes para rastreamento
    - FLEX dinâmico (custo de dim_lojas.custo_flex)
    - Verificação de divergência financeira em carrinhos

    Retorna:
        (df_processado, info_dict) ou (None, erro_msg)

    info_dict agora inclui:
        - descartes: lista de dicts com dados das vendas descartadas
        - carrinhos_encontrados: quantidade de carrinhos processados
        - pendentes_carrinho: lista de vendas com divergência financeira
    """

    # 1. DETECTAR HEADER
    header_idx = detectar_header_ml(arquivo)

    # 2. LER ARQUIVO
    try:
        df = pd.read_excel(arquivo, header=header_idx)
    except Exception as e:
        return None, f"Erro ao ler arquivo: {str(e)}"

    # 3. RENOMEAR COLUNAS
    df = renomear_colunas_ml(df)

    # 4. VALIDAR COLUNAS ESSENCIAIS
    colunas_obrigatorias = ['sku', 'receita', 'tarifa']
    if not all(col in df.columns for col in colunas_obrigatorias):
        return None, f"Colunas obrigatórias não encontradas: {colunas_obrigatorias}"

    # 5. PRÉ-PROCESSAMENTO DE CARRINHOS (NOVO v3.0)
    df, carrinhos_encontrados = _processar_carrinhos(df)

    # 6. CORRIGIDO: FORÇAR REFRESH DE CUSTOS (evitar cache antigo)
    timestamp = datetime.now().timestamp()
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)

    # 6b. BUSCAR CUSTO FLEX DINÂMICO
    custo_flex = buscar_custo_flex(engine, loja)
    if custo_flex is None:
        custo_flex = CUSTO_FLEX_ML_PADRAO

    # 7. PROCESSAR VENDAS
    vendas = []
    descartes = []
    pendentes_carrinho = []
    skus_sem_custo = set()
    linhas_descartadas = 0
    avisos_divergencia = []

    # Acumulador para divergência de carrinho: {grupo: [lista_vendas]}
    carrinho_vendas_temp = {}

    for idx, row in df.iterrows():
        try:
            # ---- PULAR LINHAS MESTRA (já processadas na pré-passada) ----
            if row.get('_is_mestra', False):
                continue

            # ---- VERIFICAR STATUS DE DESCARTE ----
            status_str = str(row.get('status', ''))
            eh_descarte, motivo_descarte = _eh_status_descarte(status_str)

            if eh_descarte:
                # Coletar dados para rastreamento (NOVO v3.0)
                sku_descarte = str(row.get('sku', '')).strip()
                descartes.append({
                    'numero_pedido': str(row.get('pedido', '')),
                    'sku': sku_descarte,
                    'status_original': status_str,
                    'motivo_descarte': motivo_descarte,
                    'receita_estimada': abs(limpar_numero(row.get('receita', 0))),
                    'tarifa_venda_estimada': abs(limpar_numero(row.get('tarifa', 0))),
                    'tarifa_envio_estimada': abs(limpar_numero(row.get('tarifa_envio', 0))),
                })
                linhas_descartadas += 1
                continue

            # ---- VALIDAR SKU NÃO VAZIO ----
            if pd.isna(row.get('sku')) or str(row.get('sku')).strip() == '':
                linhas_descartadas += 1
                continue

            sku = str(row['sku']).strip()

            # ---- VALIDAR RECEITA ----
            receita = limpar_numero(row['receita'])
            if receita <= 0:
                linhas_descartadas += 1
                continue

            # ---- TARIFA (valor absoluto) ----
            tarifa = abs(limpar_numero(row['tarifa']))

            # ---- QUANTIDADE ----
            try:
                qtd = int(row.get('qtd', 1)) if not pd.isna(row.get('qtd')) else 1
                if qtd <= 0:
                    qtd = 1
            except (ValueError, TypeError):
                qtd = 1

            # ---- FRETE - Colunas opcionais ----
            receita_envio = abs(limpar_numero(row.get('receita_envio', 0)))
            tarifa_envio = abs(limpar_numero(row.get('tarifa_envio', 0)))
            forma_entrega = str(row.get('forma_entrega', '')).lower()
            total_brl = limpar_numero(row.get('total_brl', 0))

            # ---- CAPTURAR CÓDIGO ANÚNCIO ----
            codigo_anuncio = str(row.get('codigo_anuncio', '')).strip()

            # ---- DETECTAR FLEX ----
            is_flex = 'flex' in forma_entrega

            # ---- CALCULAR FRETE E IMPOSTO ----
            if is_flex:
                # FLEX: Custo líquido (transportadora - cliente pagou)
                custo_frete = custo_flex - receita_envio
                imposto_val = 0.0  # SEM imposto no FLEX
            else:
                # NORMAL: Frete líquido
                custo_frete = tarifa_envio - receita_envio
                imposto_val = receita * (imposto / 100)

            # ---- BUSCAR CUSTO PRODUTO ----
            custo_unit = custos_dict.get(sku, 0)
            if custo_unit == 0:
                skus_sem_custo.add(sku)

            # ---- CALCULAR CUSTO TOTAL ----
            custo_total = custo_unit * qtd

            # ---- MARGEM = receita - tarifa - imposto - frete - custo ----
            margem = receita - tarifa - imposto_val - custo_frete - custo_total
            margem_pct = (margem / receita * 100) if receita > 0 else 0

            # ---- VALIDAÇÃO contra Total (BRL) — vendas normais (sem carrinho) ----
            carrinho_grupo = str(row.get('_carrinho_grupo', ''))
            total_brl_carrinho = float(row.get('_total_brl_carrinho', 0))

            if carrinho_grupo == '' and total_brl > 0:
                # Venda simples: validar individualmente
                # Total (BRL) do ML = receita - tarifa - frete (NÃO inclui imposto)
                valor_calculado = receita - tarifa - custo_frete
                divergencia = abs(valor_calculado - total_brl)

                if divergencia > TOLERANCIA_DIVERGENCIA:
                    avisos_divergencia.append({
                        'pedido': str(row.get('pedido', '')),
                        'calculado': valor_calculado,
                        'total_brl': total_brl,
                        'diferenca': divergencia
                    })

            # ---- DATA ----
            data_venda = converter_data_ml(row.get('data'))

            # ---- MONTAR REGISTRO ----
            venda = {
                'pedido': str(row.get('pedido', '')),
                'data': data_venda,
                'sku': sku,
                'codigo_anuncio': codigo_anuncio,
                'qtd': qtd,
                'receita': receita,
                'tarifa': tarifa,
                'imposto': imposto_val,
                'frete': custo_frete,
                'custo': custo_total,
                'margem': margem,
                'margem_pct': margem_pct,
                'tem_custo': custo_unit > 0,
                'is_flex': is_flex,
                '_custo_unit': custo_unit,
                '_data_obj': datetime.strptime(data_venda, "%d/%m/%Y") if data_venda else None,
                '_carrinho_grupo': carrinho_grupo,
            }

            # ---- AGRUPAR VENDAS DE CARRINHO PARA CHECK DE DIVERGÊNCIA ----
            if carrinho_grupo != '':
                if carrinho_grupo not in carrinho_vendas_temp:
                    carrinho_vendas_temp[carrinho_grupo] = {
                        'vendas': [],
                        'total_brl_mestra': total_brl_carrinho,
                    }
                carrinho_vendas_temp[carrinho_grupo]['vendas'].append(venda)
            else:
                # Venda simples: adicionar direto
                vendas.append(venda)

        except Exception as e:
            linhas_descartadas += 1
            continue

    # 8. VERIFICAR DIVERGÊNCIA FINANCEIRA DOS CARRINHOS
    for grupo, dados_grupo in carrinho_vendas_temp.items():
        vendas_grupo = dados_grupo['vendas']
        total_brl_mestra = dados_grupo['total_brl_mestra']

        # Somar valor líquido calculado de todas as filhas
        # NOTA: Total (BRL) do ML = receita - tarifa - frete (NÃO inclui imposto)
        # Imposto é cálculo interno nosso, não entra na comparação
        soma_liquido = sum(
            v['receita'] - v['tarifa'] - v['frete']
            for v in vendas_grupo
        )

        # Verificar divergência contra total_brl da mestra
        if total_brl_mestra != 0:
            divergencia = abs(soma_liquido - total_brl_mestra)
        else:
            divergencia = 0  # Sem total para comparar, aceitar

        if divergencia > TOLERANCIA_DIVERGENCIA:
            # Divergência: todas as filhas vão para pendentes com motivo
            motivo = f"Divergência financeira - carrinho {grupo} (diff R$ {divergencia:.2f})"
            for v in vendas_grupo:
                v['_motivo_pendente'] = motivo
            pendentes_carrinho.extend(vendas_grupo)
        else:
            # Sem divergência: adicionar normalmente
            vendas.extend(vendas_grupo)

    # 9. VALIDAR SE TEM VENDAS (ou pelo menos descartes/pendentes para processar)
    if not vendas and not descartes and not pendentes_carrinho:
        return None, f"Nenhuma venda válida encontrada ({linhas_descartadas} linhas descartadas)"

    # 10. CRIAR DATAFRAME (mesmo que vazio, para manter interface)
    if vendas:
        df_result = pd.DataFrame(vendas)
    else:
        df_result = pd.DataFrame()

    # 11. CALCULAR PERÍODO
    todas_vendas = vendas + pendentes_carrinho
    datas_validas = [v['_data_obj'] for v in todas_vendas if v.get('_data_obj')]
    periodo_inicio = min(datas_validas).strftime("%d/%m/%Y") if datas_validas else None
    periodo_fim = max(datas_validas).strftime("%d/%m/%Y") if datas_validas else None

    # 12. CRIAR INFO
    info = {
        'total_linhas': len(df_result) + len(pendentes_carrinho),
        'linhas_descartadas': linhas_descartadas,
        'periodo_inicio': periodo_inicio,
        'periodo_fim': periodo_fim,
        'skus_sem_custo': len(skus_sem_custo),
        'arquivo_nome': arquivo.name,
        'divergencias': avisos_divergencia,
        'carrinhos_encontrados': carrinhos_encontrados,
        'descartes': descartes,
        'pendentes_carrinho': pendentes_carrinho,
    }

    # 13. LIMPAR COLUNAS TEMPORÁRIAS
    if not df_result.empty:
        colunas_temp = ['_data_obj', '_custo_unit', '_carrinho_grupo']
        colunas_existentes = [c for c in colunas_temp if c in df_result.columns]
        df_result = df_result.drop(columns=colunas_existentes)

    return df_result, info


def gravar_vendas_ml(df_vendas, marketplace, loja, arquivo_nome, engine,
                     descartes=None, pendentes_carrinho=None):
    """
    Grava vendas do ML no banco com validação de SKU e barra de progresso.

    VERSÃO 3.0:
    - Novo param descartes: lista de vendas descartadas para rastreamento
    - Novo param pendentes_carrinho: vendas com divergência financeira
    - Reimportação inteligente: se duplicata tem status de descarte, move de snapshot para descartadas
    - Retorno expandido: 7 valores

    CORREÇÃO 10/03/2026: Usa SAVEPOINT para rollback individual.
    Um erro em uma venda NÃO apaga as vendas já gravadas.

    Retorna:
        (registros_gravados, erros, skus_invalidos, duplicatas_count,
         pendentes_count, descartadas_count, atualizados_count)
    """

    if descartes is None:
        descartes = []
    if pendentes_carrinho is None:
        pendentes_carrinho = []

    # 1. BUSCAR SKUs VÁLIDOS
    skus_validos = buscar_skus_validos(engine)

    # 2. CARREGAR DUPLICATAS EXISTENTES (proteção contra reimportação)
    duplicatas_existentes = buscar_duplicatas_loja(engine, loja)

    # 3. PREPARAR GRAVAÇÃO
    conn = engine.raw_connection()
    cursor = conn.cursor()

    registros = 0
    erros = 0
    skus_invalidos = set()
    duplicatas_count = 0
    pendentes_count = 0
    descartadas_count = 0
    atualizados_count = 0

    # 4. BARRA DE PROGRESSO
    total_itens = len(df_vendas) + len(descartes) + len(pendentes_carrinho)
    if total_itens == 0:
        total_itens = 1  # Evitar divisão por zero
    progress_bar = st.progress(0)
    status_text = st.empty()
    item_atual = 0

    # ============================================================
    # 5A. PROCESSAR DESCARTES (NOVO v3.0)
    # ============================================================
    for descarte in descartes:
        try:
            item_atual += 1
            progress_bar.progress(min(item_atual / total_itens, 1.0))
            status_text.text(f"Processando descartes... {item_atual} de {total_itens}")

            pedido_desc = str(descarte.get('numero_pedido', ''))
            sku_desc = str(descarte.get('sku', '')).strip()

            # Preencher campos de contexto
            descarte['marketplace'] = marketplace
            descarte['loja'] = loja
            descarte['arquivo_origem'] = arquivo_nome

            # Verificar se esta venda existe no snapshot (reimportação com mudança de status)
            if sku_desc and pedido_desc:
                chave = (pedido_desc, sku_desc)
                if chave in duplicatas_existentes:
                    # Status mudou! Mover de snapshot para descartadas
                    descarte['motivo_descarte'] = f"Status atualizado: {descarte.get('status_original', '')}"

                    cursor.execute(f"SAVEPOINT desc_{item_atual}")
                    try:
                        deletar_venda_snapshot(cursor, pedido_desc, sku_desc, loja)
                        gravar_venda_descartada(cursor, descarte)
                        cursor.execute(f"RELEASE SAVEPOINT desc_{item_atual}")
                        atualizados_count += 1
                        duplicatas_existentes.discard(chave)
                    except Exception:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT desc_{item_atual}")
                        erros += 1
                    continue

            # Descarte novo (primeira vez vendo esta venda cancelada/devolvida)
            cursor.execute(f"SAVEPOINT desc_{item_atual}")
            try:
                gravar_venda_descartada(cursor, descarte)
                cursor.execute(f"RELEASE SAVEPOINT desc_{item_atual}")
                descartadas_count += 1
            except Exception:
                cursor.execute(f"ROLLBACK TO SAVEPOINT desc_{item_atual}")
                erros += 1

        except Exception:
            erros += 1

    # ============================================================
    # 5B. PROCESSAR PENDENTES DE CARRINHO (NOVO v3.0)
    # ============================================================
    for pend in pendentes_carrinho:
        try:
            item_atual += 1
            progress_bar.progress(min(item_atual / total_itens, 1.0))
            status_text.text(f"Processando pendentes carrinho... {item_atual} de {total_itens}")

            sku = str(pend.get('sku', '')).strip()
            pedido = str(pend.get('pedido', '')).strip()
            motivo = pend.get('_motivo_pendente', 'Divergência financeira')

            # Converter data
            data_venda = None
            if pend.get('data'):
                try:
                    data_venda = datetime.strptime(pend['data'], "%d/%m/%Y").date()
                except (ValueError, TypeError):
                    data_venda = None

            receita = float(pend.get('receita', 0))
            tarifa = float(pend.get('tarifa', 0))
            imposto_val = float(pend.get('imposto', 0))
            frete = float(pend.get('frete', 0))
            qtd = int(pend.get('qtd', 1))
            preco_venda = receita / qtd if qtd > 0 else receita
            total_tarifas = tarifa + frete
            valor_liquido = receita - total_tarifas - imposto_val
            codigo_anuncio = str(pend.get('codigo_anuncio', '')).strip()

            dados_pendente = {
                'marketplace_origem': marketplace,
                'loja_origem': loja,
                'numero_pedido': pedido,
                'data_venda': data_venda,
                'sku': sku,
                'codigo_anuncio': codigo_anuncio,
                'quantidade': qtd,
                'preco_venda': preco_venda,
                'desconto_parceiro': 0,
                'desconto_marketplace': 0,
                'valor_venda_efetivo': receita,
                'imposto': imposto_val,
                'comissao': tarifa,
                'frete': frete,
                'tarifa_fixa': 0,
                'outros_custos': 0,
                'total_tarifas': total_tarifas,
                'valor_liquido': valor_liquido,
                'arquivo_origem': arquivo_nome,
                'motivo': motivo,
            }

            cursor.execute(f"SAVEPOINT pend_carr_{item_atual}")
            try:
                if gravar_venda_pendente(cursor, dados_pendente):
                    pendentes_count += 1
                else:
                    erros += 1
                cursor.execute(f"RELEASE SAVEPOINT pend_carr_{item_atual}")
            except Exception:
                cursor.execute(f"ROLLBACK TO SAVEPOINT pend_carr_{item_atual}")
                erros += 1

        except Exception:
            erros += 1

    # ============================================================
    # 5C. PROCESSAR VENDAS NORMAIS (loop existente mantido)
    # ============================================================
    for idx, row in df_vendas.iterrows():
        try:
            # Atualizar progresso
            item_atual += 1
            progress_bar.progress(min(item_atual / total_itens, 1.0))
            status_text.text(f"Gravando venda {item_atual} de {total_itens}...")

            # Validar SKU
            sku = row['sku']
            if not sku or str(sku).strip() == '':
                erros += 1
                continue

            sku = str(sku).strip()
            pedido = str(row['pedido']).strip()

            # ---- PROTEÇÃO DUPLICATA ----
            chave = (pedido, sku)
            if chave in duplicatas_existentes:
                duplicatas_count += 1
                continue

            # ---- SKU NÃO CADASTRADO → SALVAR COMO PENDENTE ----
            if sku not in skus_validos:
                skus_invalidos.add(sku)

                # Preparar dados para venda pendente
                data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
                receita = float(row['receita'])
                tarifa = float(row['tarifa'])
                imposto_val = float(row['imposto'])
                frete = float(row['frete'])
                qtd = int(row['qtd'])
                preco_venda = receita / qtd if qtd > 0 else receita
                total_tarifas = tarifa + frete
                valor_liquido = receita - total_tarifas - imposto_val
                codigo_anuncio = str(row.get('codigo_anuncio', '')).strip()

                dados_pendente = {
                    'marketplace_origem': marketplace,
                    'loja_origem': loja,
                    'numero_pedido': pedido,
                    'data_venda': data_venda,
                    'sku': sku,
                    'codigo_anuncio': codigo_anuncio,
                    'quantidade': qtd,
                    'preco_venda': preco_venda,
                    'desconto_parceiro': 0,
                    'desconto_marketplace': 0,
                    'valor_venda_efetivo': receita,
                    'imposto': imposto_val,
                    'comissao': tarifa,
                    'frete': frete,
                    'tarifa_fixa': 0,
                    'outros_custos': 0,
                    'total_tarifas': total_tarifas,
                    'valor_liquido': valor_liquido,
                    'arquivo_origem': arquivo_nome,
                    # motivo não informado → default 'SKU não cadastrado'
                }

                if gravar_venda_pendente(cursor, dados_pendente):
                    pendentes_count += 1
                else:
                    erros += 1

                continue

            # ---- GRAVAÇÃO NORMAL ----

            # Preparar dados
            data_venda = datetime.strptime(row['data'], "%d/%m/%Y").date()
            qtd = int(row['qtd'])
            receita = float(row['receita'])
            custo_total = float(row['custo'])
            tarifa = float(row['tarifa'])
            imposto_val = float(row['imposto'])
            frete = float(row['frete'])
            margem = float(row['margem'])
            margem_pct = float(row['margem_pct'])

            # Pegar código anúncio do row
            codigo_anuncio = str(row.get('codigo_anuncio', '')).strip()

            # Calcular valores derivados
            preco_venda = receita / qtd if qtd > 0 else receita
            custo_unit = custo_total / qtd if qtd > 0 else custo_total
            total_tarifas = tarifa + frete
            valor_liquido = receita - total_tarifas - imposto_val

            # SAVEPOINT: se der erro nessa venda, só desfaz ela
            cursor.execute(f"SAVEPOINT venda_{idx}")

            # SQL INSERT
            sql = """
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

            cursor.execute(sql, (
                marketplace, loja, pedido, data_venda, sku,
                codigo_anuncio,
                qtd, preco_venda, 0, 0,
                receita, custo_unit, custo_total, imposto_val, tarifa,
                frete, 0, 0, total_tarifas, valor_liquido,
                margem, margem_pct, arquivo_nome
            ))

            # Liberar SAVEPOINT (sucesso)
            cursor.execute(f"RELEASE SAVEPOINT venda_{idx}")
            registros += 1

            # Adicionar ao set de duplicatas (evita duplicata intra-arquivo)
            duplicatas_existentes.add(chave)

        except Exception as e:
            # ROLLBACK só desta venda, não de todas
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT venda_{idx}")
            except:
                pass
            erros += 1
            if erros == 1:
                st.warning(f"Primeiro erro: {str(e)[:200]}")

    # 6. COMMIT FINAL (todas as vendas + pendentes + descartes que deram certo)
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no commit final: {e}")

    # 7. FECHAR CONEXÃO
    cursor.close()
    conn.close()

    # 8. LIMPAR BARRA
    progress_bar.empty()
    status_text.empty()

    return (registros, erros, skus_invalidos, duplicatas_count,
            pendentes_count, descartadas_count, atualizados_count)
