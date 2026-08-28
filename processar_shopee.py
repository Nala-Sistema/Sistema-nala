"""
PROCESSADOR SHOPEE - Sistema Nala
Processa arquivos de vendas da Shopee (.xlsx exportado do painel)

VERSÃO 2.4 (18/08/2026) — auditoria do subsídio PIX:
  - FIX (A): carrinho não soma mais o ajuste uma vez por linha. O arquivo repete os
         valores do PEDIDO em cada linha (validado em 8 carrinhos de 4 arquivos —
         só 'Subtotal do produto' é por linha). Agora os valores do pedido são
         rateados entre as linhas na proporção do subtotal.
  - FIX (B): imposto passa a incidir sobre o valor da NOTA, não sobre o preço de
         tabela. Confirmado por 3 documentos fiscais (DANFE 4833/7, DANFE 4141/7,
         NF-e 43085/6): a nota sai sempre pelo valor com desconto.
  - FIX (C): margem % passa a ser calculada sobre a receita de NF, não sobre o
         subtotal bruto. É o mesmo efeito que a Shopee mostra (81% cartão vs 88% PIX).
  - FIX (E2): o ajuste é DERIVADO de (bruta − líquida) em vez de lido da coluna.
         Pedidos chegaram com 'Ajuste por ação comercial' zerado enquanto a comissão
         já vinha abatida — o sistema contava o subsídio da Shopee como lucro nosso.
  - NOVO: alerta no upload quando a comissão bruta vem abaixo da tabela oficial
         (rede de segurança caso bruta e líquida cheguem ambas já abatidas).
  - NOVO: lê 'Incentivo de cupom' — é CRÉDITO da Shopee, não custo. Entra na renda
         e fica fora da base de imposto.
  - Fórmula validada contra o relatório de transações da Shopee:
         485 de 517 pedidos fecharam no centavo (os 32 restantes são cobrança de
         frete por divergência de peso, que não existe no export de pedidos).

VERSÃO 2.7 (28/08/2026) — layout novo do export:
  - FIX: a Shopee acrescentou 'Taxa de Serviço Instantâneo pago pelo comprador'
         (presente no export de 25/08, ausente no de 09/08). O matcher genérico
         de 'taxa de serviço' capturava essa coluna e a renomeava para
         'Taxa de serviço líquida', criando duas colunas com o mesmo nome — o
         upload morria com 'cannot reindex on an axis with duplicate labels'.
         Agora taxas cobradas DO COMPRADOR são ignoradas: não são custo nosso.
         Cuidado ao reabrir isso: a coluna nova vem ANTES da verdadeira no
         arquivo, então "ficar com a primeira" pegaria a taxa errada (zerada em
         30/30 linhas) e descartaria R$ 243,71 de taxa de serviço real,
         inflando a margem sem nenhum erro na tela.
  - NOVO: trava de colisão no rename — um nome-destino só pode ser reivindicado
         por uma coluna, e quem já chega com o nome certo tem prioridade.
         Vale também para 'coin cashback' e 'FBS', que têm matcher solto.
  - NOVO: se ainda assim sobrar nome duplicado, o upload para dizendo QUAIS
         colunas colidiram, em vez do erro ilegível do pandas.

VERSÃO 2.3 (08/04/2026):
  - FIX: Inclui coluna "Ajuste por participação em ação comercial" no cálculo de margem
         Essa coluna captura descontos PIX e outros ajustes promocionais que a Shopee aplica.
         Sem ela, margem ficava inflada no valor exato do ajuste.
         Validado em 4 arquivos (2 lojas, 3 meses, 929 pedidos, 98.9%+ match).
  - Novo campo ajuste_comercial gravado em desconto_parceiro no banco.

VERSÃO 2.2 (06/04/2026):
  - FIX: renomear_colunas_shopee agora exclui colunas 'bruta' (Shopee adicionou
         'Taxa de comissão bruta' e 'Taxa de serviço bruta' ao export, causando
         duplicata de coluna e KeyError: 0)
  - FIX: Cashback rename restrito a 'coin cashback' (evita duplicata com 'Compensar Moedas')

VERSÃO 2.1 (18/03/2026):
  - NOVO: Salva pedido_original no banco (pedido real da Shopee)
  - FIX: Barra de progresso agora mostra texto com contagem de pedidos
  - Mantido: Toda lógica v2.0 intacta

REGRAS DE NEGÓCIO (v2.4):

    ajuste      = (comissão bruta − líquida) + (serviço bruta − líquida)
    receita_nf  = subtotal − ajuste − incentivo_cupom − cupom_vendedor
    renda       = subtotal − ajuste − cupom_vendedor − taxas_líquidas
    imposto     = receita_nf × alíquota da loja (dim_lojas)
    margem      = renda − imposto − custo
    margem_%    = margem ÷ receita_nf

- valor_venda_efetivo continua sendo o SUBTOTAL BRUTO, por compatibilidade com os
  demais marketplaces e com os 139 pontos do sistema que leem esse campo.
  Quem precisar do faturamento real usa: valor_venda_efetivo − desconto_parceiro.
- O subsídio da Shopee (PIX + cupom dela) é NEUTRO: ela desconta do comprador e
  devolve ao vendedor abatendo a comissão. Por isso entra dos dois lados.
- Incentivo de cupom: CRÉDITO da Shopee. Já está embutido na renda (porque o
  ajuste vem líquido dele) e é excluído da base de imposto.
- Carrinhos: valores do pedido rateados por linha na proporção do subtotal.
- Frete: IGNORADO. Existe cobrança por divergência de peso (medida em R$ 79,13
  em 29 de 517 pedidos), mas ela não aparece no export de pedidos — só no
  relatório de transações. Tratada fora do sistema por enquanto.
- Verificação de comissão: alerta quando o valor do arquivo diverge da tabela vigente

TABELA DE COMISSÕES (válida a partir de 01/03/2026, aplicada POR ITEM):
- Até R$ 79,99:         20% + R$ 4,00
- R$ 80,00 a R$ 99,99:  14% + R$ 16,00
- R$ 100,00 a R$ 199,99: 14% + R$ 20,00
- R$ 200,00 a R$ 499,99: 14% + R$ 26,00
- Acima de R$ 500,00:   14% + R$ 26,00

CARRINHOS COMPOSTOS:
- Detectados quando o mesmo ID do pedido aparece em múltiplas linhas
- O arquivo Shopee repete as comissões do pedido inteiro em cada linha (não divide)
- Para carrinhos: comissão calculada pela tabela por item (não usa o valor do arquivo)

CORREÇÃO 10/03/2026:
- _buscar_custos_skus agora lê de dim_produtos.preco_a_ser_considerado (não dim_produtos_custos)
- _buscar_skus_validos agora lê de dim_produtos (não dim_skus)

VERSÃO 2.0 (10/03/2026):
- Proteção contra duplicatas: pré-carrega (pedido, sku) existentes
- Vendas pendentes: SKU não cadastrado vai para fact_vendas_pendentes (não descarta)
- Retorno expandido: (registros, erros, skus_invalidos, duplicatas, pendentes)
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime

from formatadores import formatar_valor, formatar_percentual
from database_utils import (
    get_engine,
    gravar_venda_pendente,
    gravar_venda_descartada,
    buscar_mapeamento_skus,
)


# ============================================================
# TABELA OFICIAL DE COMISSÕES SHOPEE
# Válida a partir de 01/03/2026
# Aplicada POR ITEM: (preco_unitario × taxa + fixo) × quantidade
# ============================================================
TABELA_COMISSAO_SHOPEE = [
    (79.99,       0.20, 4.00),
    (99.99,       0.14, 16.00),
    (199.99,      0.14, 20.00),
    (499.99,      0.14, 26.00),
    (float('inf'), 0.14, 26.00),
]


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def calcular_comissao_shopee(preco_unitario: float, quantidade: int) -> float:
    """
    Calcula comissão esperada pela tabela oficial da Shopee.
    Regra aplicada por item (preço unitário), multiplicada pela quantidade.
    A parte fixa (R$4, R$16, etc.) também é multiplicada pela quantidade.
    """
    for limite, taxa, fixo in TABELA_COMISSAO_SHOPEE:
        if preco_unitario <= limite:
            return round((preco_unitario * taxa + fixo) * quantidade, 2)
    # fallback (não deve ocorrer com float('inf') na tabela)
    return round((preco_unitario * 0.14 + 26.00) * quantidade, 2)


def _limpar_numero(valor) -> float:
    """Converte valor para float de forma segura."""
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return 0.0
    try:
        s = str(valor).strip()
        if s in ('', 'nan', 'NaN', 'None', '-'):
            return 0.0
        return float(s.replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0


def _buscar_custos_skus(skus: list, engine) -> dict:
    """
    Busca custo dos SKUs no banco.
    CORREÇÃO: Fonte principal é dim_produtos.preco_a_ser_considerado
    (onde gestao_skus.py e app_compras.py atualizam).
    Fallback para soma dos componentes em dim_produtos_custos.

    Retorna dict {sku: custo}.
    """
    if not skus:
        return {}
    try:
        placeholders = ','.join(['%s'] * len(skus))
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT 
                    p.sku,
                    COALESCE(
                        NULLIF(p.preco_a_ser_considerado, 0),
                        NULLIF(pc.preco_compra + pc.embalagem + pc.mdo + pc.custo_ads, 0),
                        pc.preco_compra,
                        0
                    ) as custo
                FROM dim_produtos p
                LEFT JOIN dim_produtos_custos pc ON p.sku = pc.sku
                WHERE p.sku IN ({placeholders})
                  AND p.status = 'Ativo'""",
            skus
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0]: float(row[1]) if row[1] is not None else 0.0 for row in rows}
    except Exception:
        return {}


def _buscar_skus_validos(skus: list, engine) -> set:
    """
    Retorna conjunto de SKUs cadastrados.
    CORREÇÃO: Busca de dim_produtos (onde gestao_skus.py cadastra)
    em vez de dim_skus.
    """
    if not skus:
        return set()
    try:
        placeholders = ','.join(['%s'] * len(skus))
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT sku FROM dim_produtos WHERE sku IN ({placeholders}) AND status = 'Ativo'",
            skus
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0] for row in rows}
    except Exception:
        return set(skus)


# ============================================================
# DETECÇÃO DE HEADER
# ============================================================

def detectar_header_shopee(arquivo) -> int:
    """
    Detecta a linha do header no arquivo Shopee.
    Procura pela coluna 'ID do pedido'.
    Retorna o índice da linha (0-based) a ser usada como skiprows.
    Retorna 0 como padrão caso não encontre.
    """
    try:
        df_raw = pd.read_excel(arquivo, header=None, nrows=15)
        for i, row in df_raw.iterrows():
            for val in row.values:
                if 'ID do pedido' in str(val):
                    return i
    except Exception:
        pass
    return 0


# ============================================================
# PADRONIZAÇÃO DE COLUNAS
# ============================================================

_VALORES_SIM_FBS = ('sim', 'yes', 'y', 's', 'true', '1', 'fbs')


def classificar_logistica_shopee(row) -> str | None:
    """
    Deriva o valor da coluna `logistica` para a Shopee.

    Mesmo vocabulário fechado usado no ML, para permitir gráfico consistente
    entre marketplaces:

        FULL   — FBS (Fulfillment by Shopee): estoque no CD da Shopee, onde
                 incidem armazenagem e coleta
        ENVIOS — todo o resto (Shopee Xpress, Retirada, Postagem...)

    Não há FLEX na Shopee hoje. As modalidades de envio são agrupadas em
    ENVIOS porque o que muda decisão é onde o estoque está, não como o pedido
    sai.

    A flag de Full não aparece em todos os exports — na inspeção do
    `Order.all` de 27/07 a 02/08/2026 (Litstore Yanni) havia apenas
    'Opção de envio' e 'Método de envio', sem nenhuma coluna de FBS. Por isso
    a detecção é defensiva e olha os dois lugares.

    Devolve None quando não há informação nenhuma — não inventa categoria.
    """
    valor_fbs = str(row.get('Pedido FBS', '') or '').strip().lower()
    if valor_fbs and valor_fbs not in ('nan', 'none'):
        return 'FULL' if valor_fbs in _VALORES_SIM_FBS else 'ENVIOS'

    metodo = str(row.get('Método de envio', '') or '').strip()
    if metodo and metodo.lower() not in ('nan', 'none'):
        # Rede de segurança: se a Shopee passar a sinalizar Full pelo método
        # de envio em vez de uma coluna própria, ainda assim é capturado.
        minusculo = metodo.lower()
        if 'full' in minusculo or 'fbs' in minusculo:
            return 'FULL'
        return 'ENVIOS'

    return None


# Nomes que o resto do módulo espera encontrar. Servem de trava: se o arquivo
# já traz uma coluna com esse nome exato, nenhuma outra pode roubá-lo.
_DESTINOS_SHOPEE = (
    'ID do pedido',
    'Taxa de comissão bruta',
    'Net Commission Fee',
    'Taxa de serviço bruta',
    'Taxa de serviço líquida',
    'Incentivo de cupom',
    'Seller Absorbed Coin Cashback',
    'Pedido FBS',
)


def _classificar_taxa_servico(col_norm: str) -> str | None:
    """
    Diz se a coluna é uma taxa de serviço NOSSA e, se for, qual delas.

    v2.7: a Shopee passou a exportar 'Taxa de Serviço Instantâneo pago pelo
    comprador'. O nome contém 'taxa de serviço', então o matcher antigo a
    tratava como a nossa taxa líquida. Taxa paga pelo comprador não é custo
    nosso e não entra em cálculo nenhum — por isso o corte por 'comprador'.

    Devolve None quando a coluna não é taxa de serviço nossa.
    """
    if 'taxa de serviço' not in col_norm and 'taxa de servico' not in col_norm:
        return None
    if 'comprador' in col_norm:
        return None
    if 'bruta' in col_norm:
        return 'Taxa de serviço bruta'
    if 'líquida' in col_norm or 'liquida' in col_norm:
        return 'Taxa de serviço líquida'
    if col_norm in ('taxa de serviço', 'taxa de servico'):
        # Layout antigo, anterior à separação bruta/líquida.
        return 'Taxa de serviço líquida'
    return None


def renomear_colunas_shopee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes de colunas do arquivo Shopee (v2.7).
    Suporta variações em Inglês e Português para evitar erros de mapeamento.

    v2.2 FIX: Shopee passou a exportar colunas 'bruta' (Taxa de comissão bruta,
    Taxa de serviço bruta). A condição genérica com 'in' renomeava tanto bruta
    quanto líquida para o mesmo nome, gerando duplicata e KeyError: 0.
    Agora exclui explicitamente colunas contendo 'bruta'.

    v2.7 FIX: o mesmo tipo de colisão voltou por outra porta ('Taxa de Serviço
    Instantâneo pago pelo comprador'). Além do corte específico, agora existe
    trava geral: cada nome-destino é reivindicado por UMA coluna só, e quem já
    chega no arquivo com o nome certo tem prioridade sobre quem casaria por
    pedaço do nome. Sem isso, a coluna nova (que vem antes no arquivo) ficava
    com o nome da verdadeira e o valor real era descartado em silêncio.
    """
    rename_map = {}

    # Quem já vem com o nome certo do arquivo não precisa de rename e trava o
    # destino contra candidatas parecidas.
    reivindicados = {str(c) for c in df.columns if str(c) in _DESTINOS_SHOPEE}

    def reivindicar(coluna, destino):
        if destino in reivindicados:
            return
        rename_map[coluna] = destino
        reivindicados.add(destino)

    for col in df.columns:
        col_norm = str(col).lower().strip()

        # Taxas de serviço: bruta, líquida, ou nenhuma das duas (v2.7)
        destino_servico = _classificar_taxa_servico(col_norm)
        if destino_servico:
            reivindicar(col, destino_servico)
            continue
        
        # v2.4: Comissão BRUTA — base para derivar o ajuste comercial.
        # Precisa vir antes da líquida para não ser capturada pela condição abaixo.
        if col_norm in ['taxa de comissão bruta', 'taxa de comissao bruta']:
            reivindicar(col, 'Taxa de comissão bruta')

        # Mapeamento: Net Commission Fee / Taxa de comissão líquida
        elif col_norm in ['net commission fee', 'taxa de comissão líquida', 'taxa de comissao liquida']:
            reivindicar(col, 'Net Commission Fee')

        # v2.4: parte do cupom da Shopee paga como crédito direto em vez de
        # abatimento de comissão. No extrato aparece com sinal POSITIVO.
        elif col_norm in ['incentivo de cupom', 'incentivo do cupom']:
            reivindicar(col, 'Incentivo de cupom')

        # Mapeamento: ID do Pedido (Garante que espaços não quebrem a detecção)
        elif col_norm == 'id do pedido':
            reivindicar(col, 'ID do pedido')

        # Mapeamento: Cashback — só 'Coin Cashback' (Shopee adicionou 'Compensar Moedas' separado)
        elif 'coin cashback' in col_norm:
            reivindicar(col, 'Seller Absorbed Coin Cashback')

        # Mapeamento: flag de FBS (Fulfillment by Shopee) — o nome varia entre
        # exports e a coluna nem sempre está presente. Normaliza para que
        # classificar_logistica_shopee() tenha um nome único para procurar.
        elif col_norm == 'fbs' or ('fbs' in col_norm and 'pedido' in col_norm):
            reivindicar(col, 'Pedido FBS')

    return df.rename(columns=rename_map)


# ============================================================
# PROCESSADOR PRINCIPAL
# ============================================================

def processar_arquivo_shopee(arquivo, loja: str, imposto: float, engine):
    """
    Processa arquivo de vendas da Shopee.

    Parâmetros:
        arquivo   : arquivo XLSX enviado via st.file_uploader
        loja      : nome da loja (ex: 'Shopee Litstore')
        imposto   : alíquota de imposto da loja em % (ex: 10.0)
        engine    : conexão SQLAlchemy

    Retorna:
        (df_processado, info_dict)  → sucesso
        (None, mensagem_erro)       → falha
    """
    try:
        # --------------------------------------------------
        # 1. LER ARQUIVO
        # --------------------------------------------------
        arquivo.seek(0)
        skiprows = detectar_header_shopee(arquivo)
        arquivo.seek(0)

        df = pd.read_excel(arquivo, skiprows=skiprows)
        df = renomear_colunas_shopee(df)

        # v2.7: rede de segurança. Se a Shopee mexer no layout de novo e duas
        # colunas terminarem com o mesmo nome, o pandas quebra lá na frente com
        # 'cannot reindex on an axis with duplicate labels' — mensagem que não
        # diz nada para quem está subindo o arquivo. Melhor parar aqui dizendo
        # quais colunas colidiram.
        _duplicadas = df.columns[df.columns.duplicated()].unique().tolist()
        if _duplicadas:
            return None, (
                "Colunas duplicadas após padronização: "
                + ", ".join(str(c) for c in _duplicadas)
                + ". Provável mudança no layout do export da Shopee — "
                  "envie o arquivo para o time técnico."
            )

        if df.empty:
            return None, "Arquivo vazio ou sem dados válidos."

        # --------------------------------------------------
        # 2. VERIFICAR COLUNAS OBRIGATÓRIAS
        # --------------------------------------------------
        colunas_obrigatorias = [
            'ID do pedido',
            'Status do pedido',
            'Número de referência SKU',
            'Preço acordado',
            'Quantidade',
            'Subtotal do produto',
            'Net Commission Fee',
        ]
        faltando = [c for c in colunas_obrigatorias if c not in df.columns]
        if faltando:
            return None, f"Colunas obrigatórias não encontradas: {', '.join(faltando)}"

        # --------------------------------------------------
        # 3. CONVERTER TIPOS
        # --------------------------------------------------
        df['Preço acordado']        = df['Preço acordado'].apply(_limpar_numero)
        df['Quantidade']            = df['Quantidade'].apply(_limpar_numero).astype(int)
        df['Subtotal do produto']   = df['Subtotal do produto'].apply(_limpar_numero)
        df['Net Commission Fee']    = df['Net Commission Fee'].apply(_limpar_numero)
        df['Taxa de serviço líquida'] = df['Taxa de serviço líquida'].apply(_limpar_numero)
        df['Total global']          = df['Total global'].apply(_limpar_numero) \
                                        if 'Total global' in df.columns \
                                        else pd.Series(1.0, index=df.index)

        # Cupom do vendedor (opcional)
        if 'Cupom do vendedor' in df.columns:
            df['Cupom do vendedor'] = df['Cupom do vendedor'].apply(_limpar_numero)
        else:
            df['Cupom do vendedor'] = 0.0

        # v2.3: Ajuste por participação em ação comercial (PIX, promos, etc.)
        if 'Ajuste por participação em ação comercial' in df.columns:
            df['Ajuste por participação em ação comercial'] = df['Ajuste por participação em ação comercial'].apply(_limpar_numero)
        else:
            df['Ajuste por participação em ação comercial'] = 0.0

        # v2.4: Incentivo de cupom — parte do cupom que a Shopee paga como crédito
        # direto. É receita, não custo, e fica fora da base de imposto.
        if 'Incentivo de cupom' in df.columns:
            df['Incentivo de cupom'] = df['Incentivo de cupom'].apply(_limpar_numero)
        else:
            df['Incentivo de cupom'] = 0.0

        # v2.4: taxas BRUTAS — usadas para derivar o ajuste
        for _col_bruta in ('Taxa de comissão bruta', 'Taxa de serviço bruta'):
            if _col_bruta in df.columns:
                df[_col_bruta] = df[_col_bruta].apply(_limpar_numero)
            else:
                df[_col_bruta] = 0.0

        # v2.4 FIX (E2): o ajuste é DERIVADO da diferença bruta − líquida, não lido
        # da coluna. Houve pedidos com a coluna zerada enquanto a comissão já vinha
        # abatida — o sistema lia "comissão baixa, desconto nenhum" e contava o
        # subsídio da Shopee como lucro nosso.
        # A identidade fechou em 1.433 de 1.433 linhas em 4 arquivos.
        tem_brutas = (df['Taxa de comissão bruta'].abs().sum() > 0
                      or df['Taxa de serviço bruta'].abs().sum() > 0)

        if tem_brutas:
            df['_ajuste_derivado'] = (
                (df['Taxa de comissão bruta'] - df['Net Commission Fee'])
                + (df['Taxa de serviço bruta'] - df['Taxa de serviço líquida'])
            ).clip(lower=0).round(2)
        else:
            # Layout antigo, sem colunas brutas: só resta o valor declarado.
            df['_ajuste_derivado'] = df['Ajuste por participação em ação comercial']

        # v2.6: mapeamento de SKU (de→para). A Shopee era o único módulo que
        # não consultava dim_sku_mapeamento — ML, Amazon, Magalu, Shein e
        # TikTok já faziam. Sem isso, um SKU já mapeado ia para pendentes a
        # cada upload e a venda nunca chegava no snapshot: o anúncio da LPT
        # exporta 'K-10-LKE-3104-4030' e o cadastro tem 'K10-LKE-3104-4030'.
        # Carregado antes dos filtros porque a lista de descartes também
        # precisa do SKU corrigido — é por (pedido, sku) que a venda cancelada
        # é localizada e removida do snapshot.
        mapeamento_skus = buscar_mapeamento_skus(engine)
        skus_corrigidos = 0

        # --------------------------------------------------
        # 4. FILTRAR REGISTROS INVÁLIDOS
        # --------------------------------------------------
        total_original = len(df)

        # Cancelados
        mask_cancelado = df['Status do pedido'].astype(str).str.contains(
            'cancelad', case=False, na=False
        )

        # Devoluções / reembolsos
        if 'Status da Devolução / Reembolso' in df.columns:
            mask_devolucao = (
                df['Status da Devolução / Reembolso'].notna()
                & (df['Status da Devolução / Reembolso'].astype(str).str.strip() != '')
                & (~df['Status da Devolução / Reembolso'].astype(str).str.lower().isin(
                    ['nan', 'none', '-', '']
                ))
            )
        else:
            mask_devolucao = pd.Series(False, index=df.index)

        # Sem receita (Total global = 0 — pedidos sem pagamento real)
        mask_sem_receita = df['Total global'] == 0.0

        mask_descartar = mask_cancelado | mask_devolucao | mask_sem_receita
        df_valido = df[~mask_descartar].copy()
        linhas_descartadas = total_original - len(df_valido)

        # v2.5: guardar QUAIS pedidos foram descartados, não só quantos.
        #
        # Antes essa informação morria aqui. Um pedido gravado como válido na
        # semana e cancelado depois continuava no banco para sempre — subir o
        # arquivo do mês fechado não removia, porque a linha cancelada era
        # jogada fora antes de chegar na gravação. Agora a lista segue adiante
        # e gravar_vendas_shopee apaga essas vendas do snapshot.
        df_descartes = df[mask_descartar].copy()
        descartes = []
        for _, _row in df_descartes.iterrows():
            _sku = str(_row.get('Número de referência SKU', '') or '').strip()
            _ped = str(_row.get('ID do pedido', '') or '').strip()
            if not _ped or _sku.lower() in ('nan', 'none', ''):
                continue
            # mesmo mapeamento das vendas válidas — o snapshot guarda o SKU
            # corrigido, então a remoção precisa procurar por ele
            _sku = mapeamento_skus.get(_sku, _sku)
            if mask_cancelado.get(_row.name, False):
                _motivo = 'Pedido cancelado'
            elif mask_devolucao.get(_row.name, False):
                _motivo = 'Devolução / reembolso'
            else:
                _motivo = 'Sem receita (Total global = 0)'
            descartes.append({
                'pedido':          _ped,
                'sku':             _sku,
                'status_original': str(_row.get('Status do pedido', '') or '').strip(),
                'motivo':          _motivo,
                'receita':         _limpar_numero(_row.get('Subtotal do produto', 0)),
            })

        if df_valido.empty:
            return None, "Nenhuma venda válida encontrada após filtros."

        # --------------------------------------------------
        # 5. CARRINHOS: RATEAR OS VALORES DO PEDIDO ENTRE AS LINHAS
        #
        # v2.4 FIX (A): o arquivo repete os valores do PEDIDO INTEIRO em cada
        # linha — só 'Subtotal do produto' é por linha. Validado em 8 carrinhos
        # de 4 arquivos: ajuste, taxas líquidas e cupom do vendedor vieram
        # idênticos em 8/8; o subtotal variou em 8/8.
        #
        # Antes, cada linha subtraía o ajuste do pedido inteiro. Um pedido de
        # R$ 69,98 com ajuste de R$ 20,60 tinha R$ 41,20 descontados e virava
        # prejuízo de R$ 34,72 quando o lucro real era R$ 8,39.
        # --------------------------------------------------
        contagem_pedidos = df_valido['ID do pedido'].value_counts()
        ids_carrinho = set(contagem_pedidos[contagem_pedidos > 1].index)

        _grupo = df_valido.groupby('ID do pedido')
        _subtotal_pedido = _grupo['Subtotal do produto'].transform('sum')

        # Proporção da linha dentro do pedido. Pedido de item único → 1,0.
        df_valido['_proporcao'] = np.where(
            _subtotal_pedido > 0,
            df_valido['Subtotal do produto'] / _subtotal_pedido,
            1.0 / _grupo['Subtotal do produto'].transform('size'),
        )

        df_valido['_taxas_liquidas_linha'] = (
            df_valido['Net Commission Fee'] + df_valido['Taxa de serviço líquida']
        )

        # Valores do pedido: pega UMA vez (não soma) e rateia.
        _rateio = {
            '_ajuste':     '_ajuste_derivado',
            '_taxas_liq':  '_taxas_liquidas_linha',
            '_cupom_vend': 'Cupom do vendedor',
            '_inc_cupom':  'Incentivo de cupom',
        }

        for destino, origem in _rateio.items():
            df_valido[destino] = (
                df_valido.groupby('ID do pedido')[origem].transform('first')
                * df_valido['_proporcao']
            ).round(2)

        # --------------------------------------------------
        # 6. PROCESSAR LINHA A LINHA
        # --------------------------------------------------
        resultados       = []
        alertas_comissao = []
        # v2.4: linhas em que a comissão cheia ficou abaixo da tabela oficial —
        # sinal de arquivo exportado antes da liquidação do ajuste.
        alertas_arquivo_incompleto = []

        # Identificar coluna de data disponível
        colunas_data_candidatas = [
            'Data de criação do pedido',
            'Hora do pagamento do pedido',
            'Data',
        ]

        for _, row in df_valido.iterrows():
            pedido_id  = str(row['ID do pedido']).strip()
            is_carrinho = pedido_id in ids_carrinho

            # SKU
            sku = str(row['Número de referência SKU']).strip()
            if not sku or sku.lower() in ('nan', 'none', ''):
                continue

            # v2.6: aplicar mapeamento antes de qualquer validação
            if sku in mapeamento_skus:
                sku = mapeamento_skus[sku]
                skus_corrigidos += 1

            # Código do anúncio (SKU pai / agrupador)
            codigo_anuncio = ''
            if 'Nº de referência do SKU principal' in df.columns:
                val_anuncio = str(row.get('Nº de referência do SKU principal', '')).strip()
                codigo_anuncio = '' if val_anuncio.lower() in ('nan', 'none', '') else val_anuncio
            if not codigo_anuncio:
                codigo_anuncio = sku

            # Valores financeiros — já rateados por linha (v2.4)
            preco_unitario  = _limpar_numero(row['Preço acordado'])
            quantidade      = int(_limpar_numero(row['Quantidade']))
            subtotal        = _limpar_numero(row['Subtotal do produto'])
            cupom_vendedor  = _limpar_numero(row.get('_cupom_vend', 0))
            ajuste_comercial = _limpar_numero(row.get('_ajuste', 0))
            incentivo_cupom = _limpar_numero(row.get('_inc_cupom', 0))

            if subtotal == 0 and preco_unitario > 0:
                subtotal = preco_unitario * quantidade

            # Data da venda
            data_venda = None
            for col_data in colunas_data_candidatas:
                if col_data in df_valido.columns:
                    val_data = row.get(col_data)
                    if pd.notna(val_data) and str(val_data).strip() not in ('', 'nan'):
                        try:
                            data_venda = pd.to_datetime(val_data).date()
                            break
                        except Exception:
                            continue
            if data_venda is None:
                data_venda = datetime.now().date()

            # --------------------------------------------------
            # 7. COMISSÃO
            #
            # v2.4: sempre o valor líquido do arquivo, rateado. Não existe mais
            # caminho separado para carrinho — o rateio já resolveu a repetição.
            # A tabela oficial serve só de conferência.
            # --------------------------------------------------
            comissao       = round(_limpar_numero(row.get('_taxas_liq', 0)), 2)
            fonte_comissao = 'arquivo'

            comissao_esperada = calcular_comissao_shopee(preco_unitario, quantidade)
            tolerancia        = max(0.50, comissao_esperada * 0.05)  # 5% ou R$0,50

            # A comissão CHEIA é a líquida somada ao subsídio devolvido. É ela que
            # deve bater com a tabela — comparar a líquida sozinha acusaria
            # divergência em todo pedido com desconto.
            comissao_cheia = comissao + ajuste_comercial
            divergencia    = comissao_cheia - comissao_esperada

            if abs(divergencia) > tolerancia:
                alertas_comissao.append({
                    'pedido':             pedido_id,
                    'sku':                sku,
                    'comissao_arquivo':   round(comissao_cheia, 2),
                    'comissao_esperada':  round(comissao_esperada, 2),
                    'divergencia':        round(divergencia, 2),
                })

            # v2.4: rede de segurança contra o Erro E2. Se a comissão CHEIA vier
            # bem abaixo da tabela, o arquivo provavelmente chegou com bruta e
            # líquida ambas já abatidas — e aí o ajuste derivado dá zero e o
            # subsídio da Shopee some do cálculo, inflando a margem.
            if comissao_cheia < comissao_esperada - tolerancia:
                alertas_arquivo_incompleto.append({
                    'pedido':            pedido_id,
                    'sku':               sku,
                    'comissao_no_arquivo': round(comissao_cheia, 2),
                    'comissao_tabela':   round(comissao_esperada, 2),
                    'diferenca':         round(comissao_esperada - comissao_cheia, 2),
                })

            # --------------------------------------------------
            # 8. RECEITA DE NF E IMPOSTO
            #
            # v2.4 FIX (B): a nota sai pelo valor com desconto — confirmado em
            # DANFE 4833/7, DANFE 4141/7 e NF-e 43085/6. O imposto acompanha.
            # O incentivo de cupom é crédito da Shopee: entra na renda, mas não
            # no faturamento, então sai da base.
            # --------------------------------------------------
            receita_nf    = round(subtotal - ajuste_comercial - incentivo_cupom - cupom_vendedor, 2)
            if receita_nf < 0:
                receita_nf = 0.0
            imposto_valor = round(receita_nf * (imposto / 100), 2)

            # Título do produto (Nome do Produto) — usado para o dicionário
            # título→SKU do módulo de Ads (match automático anúncio↔SKU).
            titulo_produto = str(row.get('Nome do Produto', '') or '').strip()
            if titulo_produto.lower() in ('nan', 'none'):
                titulo_produto = ''

            resultados.append({
                'pedido':            pedido_id,
                'pedido_original':   pedido_id,  # v2.1: pedido real da Shopee
                'data':              data_venda,
                'sku':               sku,
                'codigo_anuncio':    codigo_anuncio,
                'titulo':            titulo_produto,  # NOVO: p/ dicionário de Ads
                'logistica':         classificar_logistica_shopee(row),
                'qtd':               quantidade,
                'preco_unit':        preco_unitario,
                'receita':           subtotal,      # subtotal bruto (compat. c/ outros canais)
                'receita_nf':        receita_nf,    # v2.4: base do imposto e da margem %
                'tarifa':            round(comissao, 2),
                'imposto':           imposto_valor,
                'cupom_vendedor':    cupom_vendedor,
                'ajuste_comercial':  ajuste_comercial,  # v2.3: PIX, promos, etc.
                'incentivo_cupom':   incentivo_cupom,   # v2.4: crédito da Shopee
                'frete':             0.0,
                'custo':             0.0,       # preenchido após busca no banco
                'custo_unit':        0.0,
                'tem_custo':         False,
                'fonte_comissao':    fonte_comissao,
                'is_carrinho':       is_carrinho,
            })

        if not resultados:
            return None, "Nenhuma linha processada com sucesso."

        df_proc = pd.DataFrame(resultados)

        # --------------------------------------------------
        # 9. BUSCAR CUSTOS NO BANCO (snapshot)
        # --------------------------------------------------
        skus_unicos = df_proc['sku'].unique().tolist()
        custos_db   = _buscar_custos_skus(skus_unicos, engine)

        df_proc['custo_unit'] = df_proc['sku'].map(custos_db).fillna(0.0)
        df_proc['custo']      = (df_proc['custo_unit'] * df_proc['qtd']).round(2)
        df_proc['tem_custo']  = df_proc['custo'] > 0

        # --------------------------------------------------
        # 10. CALCULAR MARGEM (v2.4)
        #
        #   renda    = subtotal − ajuste − cupom_vendedor − taxas_líquidas
        #   margem   = renda − imposto − custo
        #   margem_% = margem ÷ receita_nf        ← FIX (C)
        #
        # A renda reproduz o que a Shopee efetivamente deposita: validada contra
        # o relatório de transações em 485 de 517 pedidos, no centavo.
        # O incentivo de cupom não aparece aqui porque já está embutido — o
        # ajuste vem líquido dele, então não subtraí-lo equivale a somá-lo.
        # --------------------------------------------------
        df_proc['renda'] = (
            df_proc['receita']
            - df_proc['ajuste_comercial']
            - df_proc['cupom_vendedor']
            - df_proc['tarifa']
        ).round(2)

        df_proc['margem'] = (
            df_proc['renda']
            - df_proc['imposto']
            - df_proc['custo']
        ).round(2)

        # FIX (C): denominador é a receita de NF, não o preço de tabela.
        df_proc['margem_pct'] = df_proc.apply(
            lambda r: round((r['margem'] / r['receita_nf'] * 100), 2) if r['receita_nf'] > 0 else 0.0,
            axis=1
        )

        # --------------------------------------------------
        # 11. MONTAR INFO DICT
        # --------------------------------------------------
        skus_sem_custo = int((~df_proc['tem_custo']).sum())

        try:
            periodo_inicio = df_proc['data'].min().strftime('%d/%m/%Y')
            periodo_fim    = df_proc['data'].max().strftime('%d/%m/%Y')
        except Exception:
            hoje = datetime.now().strftime('%d/%m/%Y')
            periodo_inicio = periodo_fim = hoje

        info = {
            'total_linhas':       len(df_proc),
            'periodo_inicio':     periodo_inicio,
            'periodo_fim':        periodo_fim,
            'linhas_descartadas': linhas_descartadas,
            'descartes':          descartes,   # v2.5: pedidos a remover do snapshot
            'skus_corrigidos':    skus_corrigidos,  # v2.6: via dim_sku_mapeamento
            'skus_sem_custo':     skus_sem_custo,
            'carrinhos':          len(ids_carrinho),
            'alertas_comissao':   alertas_comissao,
            # v2.4: rede de segurança do Erro E2
            'alertas_arquivo_incompleto': alertas_arquivo_incompleto,
            'ajuste_derivado':    bool(tem_brutas),
            'total_ajuste':       round(float(df_proc['ajuste_comercial'].sum()), 2),
            'receita_bruta':      round(float(df_proc['receita'].sum()), 2),
            'receita_nf':         round(float(df_proc['receita_nf'].sum()), 2),
        }

        return df_proc, info

    except Exception as e:
        return None, f"Erro ao processar arquivo Shopee: {str(e)}"


# ============================================================
# GRAVAÇÃO NO BANCO
# ============================================================

def gravar_vendas_shopee(df_vendas: pd.DataFrame, marketplace: str, loja: str,
                          arquivo_nome: str, engine, descartes: list = None):
    """
    Grava vendas da Shopee na tabela fact_vendas_snapshot.

    VERSÃO 2.3:
    - NOVO: Grava ajuste_comercial em desconto_parceiro
    - Ajuste_comercial incluído em outros_custos e total_tarifas

    VERSÃO 2.1:
    - NOVO: Salva pedido_original no INSERT
    - FIX: Barra de progresso mostra texto com contagem
    - Mantido: Toda lógica v2.0 intacta

    VERSÃO 2.0:
    - Proteção contra duplicatas: pré-carrega (pedido, sku) existentes da loja
    - Vendas pendentes: SKU não cadastrado vai para fact_vendas_pendentes
    - Retorno expandido com contadores de duplicatas e pendentes

    Parâmetros:
        df_vendas    : DataFrame processado por processar_arquivo_shopee
        marketplace  : nome do marketplace (ex: 'Shopee')
        loja         : nome da loja (ex: 'Shopee Litstore')
        arquivo_nome : nome do arquivo original
        engine       : conexão SQLAlchemy

    Retorna:
        (registros, erros, skus_invalidos, duplicatas_count,
         pendentes_count, descartadas_count, atualizados_count)

        v2.5: assinatura alinhada com ML, Amazon, Shein, Magalu e TikTok.
        duplicatas_count fica sempre 0 — nao existe mais salto por duplicata,
        o upsert atualiza. O que era pulado agora aparece em atualizados_count.
    """
    registros      = 0
    erros          = 0
    skus_invalidos = set()
    pendentes_count  = 0
    duplicatas_count = 0   # sempre 0 na v2.5 — mantido pela assinatura comum

    if df_vendas.empty and not descartes:
        return 0, 0, set(), 0, 0, 0, 0

    # Verificar SKUs cadastrados em dim_produtos (CORRIGIDO)
    skus_todos   = df_vendas['sku'].unique().tolist()
    skus_validos = _buscar_skus_validos(skus_todos, engine)

    # v2.5: a proteção por duplicata deixou de existir. Ela comparava só
    # (pedido, sku) e pulava a linha — não sabia diferenciar "isso é repetido"
    # de "isso é a versão corrigida". Resultado: subir arquivo corrigido por
    # cima não corrigia nada. Agora o INSERT faz upsert, mesmo modelo já usado
    # no TikTok. Reimportar passou a ser o caminho normal de correção.
    atualizados_count = 0

    total    = len(df_vendas)
    progress = st.progress(0)
    status_text = st.empty()  # v2.1: texto de status

    # v2.1: INSERT agora inclui pedido_original
    sql_insert = """
        INSERT INTO fact_vendas_snapshot (
            marketplace_origem,
            loja_origem,
            numero_pedido,
            pedido_original,
            data_venda,
            sku,
            codigo_anuncio,
            quantidade,
            preco_venda,
            desconto_parceiro,
            desconto_marketplace,
            valor_venda_efetivo,
            custo_unitario,
            custo_total,
            imposto,
            comissao,
            frete,
            tarifa_fixa,
            outros_custos,
            total_tarifas,
            valor_liquido,
            margem_total,
            margem_percentual,
            data_processamento,
            arquivo_origem,
            logistica
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, NOW(), %s, %s
        )
        ON CONFLICT (numero_pedido, sku, loja_origem) DO UPDATE SET
            pedido_original      = EXCLUDED.pedido_original,
            data_venda           = EXCLUDED.data_venda,
            codigo_anuncio       = EXCLUDED.codigo_anuncio,
            quantidade           = EXCLUDED.quantidade,
            preco_venda          = EXCLUDED.preco_venda,
            desconto_parceiro    = EXCLUDED.desconto_parceiro,
            desconto_marketplace = EXCLUDED.desconto_marketplace,
            valor_venda_efetivo  = EXCLUDED.valor_venda_efetivo,
            custo_unitario       = EXCLUDED.custo_unitario,
            custo_total          = EXCLUDED.custo_total,
            imposto              = EXCLUDED.imposto,
            comissao             = EXCLUDED.comissao,
            frete                = EXCLUDED.frete,
            tarifa_fixa          = EXCLUDED.tarifa_fixa,
            outros_custos        = EXCLUDED.outros_custos,
            total_tarifas        = EXCLUDED.total_tarifas,
            valor_liquido        = EXCLUDED.valor_liquido,
            margem_total         = EXCLUDED.margem_total,
            margem_percentual    = EXCLUDED.margem_percentual,
            arquivo_origem       = EXCLUDED.arquivo_origem,
            logistica            = EXCLUDED.logistica,
            data_processamento   = NOW()
        RETURNING (xmax = 0) AS inseriu
    """

    conn   = engine.raw_connection()
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # v2.5: REMOVER DO SNAPSHOT O QUE VEIO CANCELADO / DEVOLVIDO
    #
    # Roda antes das gravações. É o que faz o upload do mês fechado limpar
    # os pedidos que foram cancelados depois do upload semanal — antes isso
    # não acontecia e a venda cancelada ficava no banco indefinidamente.
    # ------------------------------------------------------------------
    removidas_count = 0
    for d in (descartes or []):
        try:
            cursor.execute("SAVEPOINT sp_desc")
            cursor.execute(
                """DELETE FROM fact_vendas_snapshot
                    WHERE marketplace_origem = %s AND loja_origem = %s
                      AND numero_pedido = %s AND sku = %s""",
                (marketplace, loja, d['pedido'], d['sku'])
            )
            if cursor.rowcount:
                removidas_count += cursor.rowcount
                try:
                    gravar_venda_descartada(cursor, {
                        'marketplace':     marketplace,
                        'loja':            loja,
                        'numero_pedido':   d['pedido'],
                        'status_original': d.get('status_original', ''),
                        'motivo_descarte': d.get('motivo', ''),
                        'receita_estimada': d.get('receita', 0.0),
                        'arquivo_origem':  arquivo_nome,
                    })
                except Exception:
                    # Rastreio é desejável, mas não pode impedir a remoção.
                    pass
            cursor.execute("RELEASE SAVEPOINT sp_desc")
        except Exception:
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_desc")
            except Exception:
                pass

    for idx, (_, row) in enumerate(df_vendas.iterrows()):
        sku = row['sku']
        pedido = str(row['pedido']).strip()
        pedido_original = str(row.get('pedido_original', pedido)).strip()  # v2.1

        # v2.1: Atualizar texto do progresso
        progress.progress(min((idx + 1) / total, 1.0))
        status_text.text(f"Gravando venda {idx + 1} de {total}...")

        # v2.5: sem salto por duplicata — o upsert resolve. Contamos só para
        # o relatório de upload continuar informando o que foi atualizado.

        # ---- SKU NÃO CADASTRADO → SALVAR COMO PENDENTE ----
        if sku not in skus_validos:
            skus_invalidos.add(sku)

            # Preparar dados financeiros para pendente
            receita          = float(row['receita'])
            comissao         = float(row['tarifa'])
            imposto_val      = float(row['imposto'])
            cupom_vendedor   = float(row.get('cupom_vendedor', 0.0))
            ajuste_comercial = float(row.get('ajuste_comercial', 0.0))  # v2.3
            frete            = 0.0
            tarifa_fixa      = 0.0
            outros_custos    = cupom_vendedor + ajuste_comercial        # v2.3: inclui ajuste
            total_tarifas    = comissao + imposto_val + outros_custos
            valor_liquido    = round(receita - total_tarifas, 2)

            dados_pendente = {
                'marketplace_origem': marketplace,
                'loja_origem': loja,
                'numero_pedido': pedido,
                'data_venda': row['data'],
                'sku': sku,
                'codigo_anuncio': row.get('codigo_anuncio', ''),
                'quantidade': int(row['qtd']),
                'preco_venda': float(row.get('preco_unit', 0)),
                'desconto_parceiro': ajuste_comercial,                  # v2.3
                'desconto_marketplace': 0,
                'valor_venda_efetivo': receita,
                'imposto': imposto_val,
                'comissao': comissao,
                'frete': frete,
                'tarifa_fixa': tarifa_fixa,
                'outros_custos': outros_custos,
                'total_tarifas': total_tarifas,
                'valor_liquido': valor_liquido,
                'arquivo_origem': arquivo_nome,
                # Preserva a logística para não perder o dado quando a
                # pendente for reprocessada para o snapshot.
                'logistica': row.get('logistica') or None,
            }

            if gravar_venda_pendente(cursor, dados_pendente):
                pendentes_count += 1
            else:
                erros += 1

            continue

        # ---- GRAVAÇÃO NORMAL ----
        try:
            # Valores financeiros
            receita          = float(row['receita'])
            comissao         = float(row['tarifa'])
            imposto_val      = float(row['imposto'])
            cupom_vendedor   = float(row.get('cupom_vendedor', 0.0))
            ajuste_comercial = float(row.get('ajuste_comercial', 0.0))  # v2.3
            custo_unit       = float(row.get('custo_unit', 0.0))
            custo_total      = float(row['custo'])

            frete            = 0.0
            tarifa_fixa      = 0.0
            outros_custos    = cupom_vendedor + ajuste_comercial        # v2.3: inclui ajuste
            total_tarifas    = comissao + imposto_val + outros_custos
            valor_liquido    = round(receita - total_tarifas, 2)
            margem_total     = float(row['margem'])
            margem_pct       = float(row['margem_pct'])

            # Savepoint individual — rollback só desta linha em caso de erro
            cursor.execute(f"SAVEPOINT sp_shopee_{idx}")
            cursor.execute(sql_insert, (
                marketplace,
                loja,
                pedido,
                pedido_original,  # v2.1: NOVO parâmetro
                row['data'],
                sku,
                row['codigo_anuncio'],
                int(row['qtd']),
                float(row['preco_unit']),   # preco_venda = preço unitário acordado
                ajuste_comercial,            # v2.3: desconto_parceiro = ajuste comercial
                0.0,                         # desconto_marketplace (cupom Shopee — absorvido pela plataforma)
                receita,
                custo_unit,
                custo_total,
                imposto_val,
                comissao,
                frete,
                tarifa_fixa,
                outros_custos,
                total_tarifas,
                valor_liquido,
                margem_total,
                margem_pct,
                arquivo_nome,
                row.get('logistica') or None,
            ))
            # v2.5: o RETURNING devolve xmax = 0 quando foi INSERT e diferente
            # de zero quando foi UPDATE. rowcount não serve — o Postgres reporta
            # 1 nos dois casos. Serve para o relatório distinguir venda nova de
            # linha corrigida.
            _res = cursor.fetchone()
            if _res is not None and not _res[0]:
                atualizados_count += 1
            cursor.execute(f"RELEASE SAVEPOINT sp_shopee_{idx}")
            registros += 1

        except Exception:
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT sp_shopee_{idx}")
            except:
                pass
            erros += 1

    # Commit único no final (vendas + pendentes)
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"❌ Erro ao commitar vendas Shopee: {e}")

    cursor.close()
    conn.close()
    progress.empty()
    status_text.empty()  # v2.1: limpar texto

    # ---- NOVO: alimentar o dicionário título→SKU (módulo de Ads) ----
    # Isolado em try/except: falha aqui NUNCA compromete a gravação da venda.
    try:
        from matching_ads_titulos import atualizar_dicionario_de_vendas
        atualizar_dicionario_de_vendas(engine, loja, df_vendas)
    except Exception:
        pass

    return (registros, erros, skus_invalidos, duplicatas_count,
            pendentes_count, removidas_count, atualizados_count)
