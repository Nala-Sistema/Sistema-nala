"""
COBERTURA DE ESTOQUE EM PEÇA — ML + Shopee (frente [KITS], 2ª entrega, 01/10/2026;
Full da Shopee: frente [DADOS SHOPEE], parte (b), 06/10/2026)

Aba "📦 Cobertura em peça (ML + Shopee)" de Análise de Produtos. Substitui a
aba antiga, que lia dim_estoque (upload do UpSeller, parado desde 17/05).

FONTE ÚNICA: estoque da API do Mercado Livre e da foto diária da Shopee
(fact_estoque_diario + ponte dim_estoque_anuncio, por marketplace),
composição de dim_kit_composicao, venda de fact_vendas_snapshot. Só leitura,
sem chamada à API.

SHOPEE (decisões do Mestre B1–B3, 06/10/2026)
  - Full da Shopee SOMA com o do ML: é outro armazém físico. Entra em peça
    pela mesma composição, em coluna própria ("Full Shopee (peças)") e na
    cobertura TOTAL. O estoque_id da Shopee é por variação de cada anúncio
    (item:model), então somar por estoque_id não dobra nada.
  - "Cobertura só Full ML" continua Full ML ÷ venda: somar o Full de um
    marketplace com a venda do outro mistura canais. A cobertura do Full
    Shopee em dias é da frente "Cobertura do Full da Shopee".
  - Galpão: é o MESMO galpão físico (UpSeller) nos dois. Vale o do ML; a
    Shopee só AVISA quando publica diferente (mesma regra de ruído). Quando o
    ML não tem anúncio da própria peça, usa-se o galpão da Shopee, marcado
    "pela Shopee" (vem antes do piso pelo kit, que é estimativa); com
    anúncios Shopee publicando números diferentes, usa-se o MAIOR e avisa,
    como entre as lojas do ML (ressalva R1 do auditor à parte (b)). Galpão de
    anúncio de KIT da Shopee é ignorado (vem dividido).
  - Regra do Full (Thiago, 06/10/2026, ML e Shopee iguais): Full zerado ainda
    vende pelo galpão, mas MUITO menos — é urgente; Full + galpão zerados =
    para de vender. A cobertura total desta tela soma os dois.

REGRAS (plano aprovado pelo Mestre em 01/10/2026, medido no banco)
  - Foto: o último dia de CADA loja. Estoque que não aparece nele saiu do ML
    (o coletor grava todo estoque todo dia).
  - Full em peça = Σ Full do kit × qtd + Full da própria peça, somado por
    estoque_id DISTINTO: vários anúncios dividem o mesmo estoque, e somar por
    anúncio dobraria o Full.
  - Galpão em peça: SÓ de anúncio cuja SKU é a própria peça. O galpão do
    anúncio de kit já vem dividido pelo UpSeller (LKE-3104-4030 = 1.315 →
    K10 = 131); multiplicar de volta conta a peça várias vezes.
  - Galpão é um só: não soma entre lojas. Usa o MAIOR entre os anúncios da
    peça (o UpSeller pode publicar com limite numa loja) e avisa quando as
    lojas divergem. ML-YanniSP é prep center: coluna própria, fora do galpão.
  - Peça sem anúncio próprio: galpão "desconhecido", nunca zero. Piso
    "≥ N pelo kit" = galpão do anúncio de um kit de UMA peça só × qtd,
    sempre marcado como estimativa; a cobertura dela sai marcada "mínimo".
  - Venda em peça = venda do kit × qtd + venda da peça sozinha, em TODOS os
    marketplaces (o galpão abastece todos). Full da Amazon ainda não entra
    no estoque: a cobertura sai conservadora (dito na tela).
  - Kit sem composição (pendente) entra como ele mesmo, com aviso.
  - Nunca soma unidades de produtos diferentes. R$ em jogo = receita de 30
    dias dos SKUs (kits e a própria peça) que param se a peça acabar; o
    total do topo soma SKUs DISTINTOS. Aqui NÃO há rateio: kit sem uma peça
    para inteiro.

RECEITA E MARGEM POR PEÇA (Mais Vendidos "Produto (peça)"; regra do Thiago,
01/10/2026, Manual do Notion seção 4: "sempre que falar de peças, falar de
valores")
  - Venda da peça sozinha entra inteira.
  - Venda do kit é RATEADA entre as peças pelo PESO DO CUSTO:
    peso = custo da peça × qtd ÷ Σ(custo × qtd) do kit. A margem do kit (a
    margem real gravada na venda) é rateada pelo mesmo peso. O custo
    cadastrado do kit não entra: só os pesos das peças.
  - Custo da peça = o mesmo que a venda usa (coletor e upload):
    preco_a_ser_considerado, senão a soma preço de compra + embalagem + MDO +
    ads, senão o preço de compra. NÃO é dim_produtos_custos.custo_final, que
    em 01/10/2026 divergia dos componentes em 792 de 982 SKUs (L-0321: 1,88
    contra 7,20) e estava vazio em 190.
  - Peça sem custo, zero ou NaN (lembrar 13/08): aquele kit cai para rateio
    por QUANTIDADE e é listado em aviso. Nunca divide por zero.
  - Fecha ao centavo: soma das peças = soma dos kits + avulsas, em receita e
    em margem (a última peça de cada kit fica com o resto do arredondamento).
  - Kit sem composição entra inteiro como ele mesmo.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd

MARKETPLACE = 'MERCADO LIVRE'
MARKETPLACE_SHOPEE = 'SHOPEE'
LOJA_PREP_CENTER = 'ML-YanniSP'
PRAZO_REPOSICAO_PADRAO = 14


# ============================================================
# SQL — parâmetros ligados (psycopg2); sem schema de propósito: o teste usa
# tabelas TEMPORÁRIAS com os mesmos nomes.
# ============================================================

# Último dia de cada loja; um estoque por linha (ponte com DISTINCT: dois
# anúncios do mesmo estoque não duplicam o saldo).
SQL_ESTOQUE = """
    WITH ult AS (
        SELECT loja, max(data) AS data
          FROM fact_estoque_diario
         WHERE marketplace = %(marketplace)s
         GROUP BY loja),
    ponte AS (
        SELECT DISTINCT loja, estoque_id, sku
          FROM dim_estoque_anuncio
         WHERE marketplace = %(marketplace)s AND estoque_id IS NOT NULL
           AND sku IS NOT NULL)
    SELECT f.loja, f.estoque_id, p.sku, f.data,
           COALESCE(f.full_disponivel, 0)       AS full_disponivel,
           COALESCE(f.full_em_transferencia, 0) AS full_em_transferencia,
           f.galpao_disponivel
      FROM fact_estoque_diario f
      JOIN ult u ON u.loja = f.loja AND u.data = f.data
      JOIN ponte p ON p.loja = f.loja AND p.estoque_id = f.estoque_id
     WHERE f.marketplace = %(marketplace)s
"""

# Venda de todos os marketplaces, por SKU vendido.
SQL_VENDAS = """
    SELECT sku,
           SUM(quantidade) FILTER (WHERE data_venda >= %(ini_7)s)  AS qtd_7d,
           SUM(quantidade)                                         AS qtd_30d,
           SUM(valor_venda_efetivo)                                AS receita_30d
      FROM fact_vendas_snapshot
     WHERE data_venda >= %(ini_30)s AND data_venda <= %(fim)s
     GROUP BY sku
"""

# Quantos estoques cada loja tem no último dia e no anterior: foto parcial
# (coleta que parou no meio) some com estoque sem erro nenhum.
SQL_CONTAGEM_DIAS = """
    WITH d AS (
        SELECT loja, data, count(*) AS n
          FROM fact_estoque_diario
         WHERE marketplace = %(marketplace)s
         GROUP BY loja, data),
    r AS (
        SELECT loja, data, n,
               row_number() OVER (PARTITION BY loja ORDER BY data DESC) AS k
          FROM d)
    SELECT loja,
           max(n)    FILTER (WHERE k = 1) AS n_ultimo,
           max(data) FILTER (WHERE k = 1) AS ultimo,
           max(n)    FILTER (WHERE k = 2) AS n_anterior,
           max(data) FILTER (WHERE k = 2) AS anterior
      FROM r
     WHERE k <= 2
     GROUP BY loja
"""

SQL_COMPOSICAO = "SELECT kit_sku, peca_sku, quantidade FROM dim_kit_composicao"

SQL_KITS_PENDENTES = "SELECT DISTINCT kit_sku FROM dim_kit_composicao_pendente"

SQL_MAPEAMENTO = "SELECT sku_errado, sku_correto FROM dim_sku_mapeamento"

SQL_NOMES = "SELECT sku, nome FROM dim_produtos WHERE sku = ANY(%(skus)s)"

TODAS_AS_SQL = (SQL_ESTOQUE, SQL_VENDAS, SQL_COMPOSICAO, SQL_KITS_PENDENTES,
                SQL_MAPEAMENTO, SQL_NOMES, SQL_CONTAGEM_DIAS)

QUEDA_FOTO_PARCIAL = 0.20

# Mais Vendidos por peça (3ª entrega): o WHERE vem de
# analise_produtos._montar_where_filtros (período, loja com RBAC,
# marketplace), só com placeholders %s; os valores vão em params.
SQL_VENDAS_POR_SKU_MODELO = """
    SELECT f.sku, SUM(f.quantidade) AS qtd,
           SUM(f.valor_venda_efetivo) AS receita, SUM(f.margem_total) AS margem
      FROM fact_vendas_snapshot f
     WHERE {where}
     GROUP BY f.sku
"""

# Custo da peça para o PESO do rateio: a mesma regra de custo da venda
# (nala-coletor-ml/coletor/vendas_db.py e o upload). MAX porque dim_produtos
# já teve SKU em duplicidade.
SQL_CUSTOS = """
    SELECT p.sku,
           MAX(COALESCE(NULLIF(p.preco_a_ser_considerado, 0),
                        NULLIF(pc.preco_compra + pc.embalagem + pc.mdo + pc.custo_ads, 0),
                        pc.preco_compra)) AS custo
      FROM dim_produtos p
      LEFT JOIN dim_produtos_custos pc ON pc.sku = p.sku
     WHERE p.sku = ANY(%(skus)s)
     GROUP BY p.sku
"""
TODAS_AS_SQL += (SQL_VENDAS_POR_SKU_MODELO.format(where='TRUE'), SQL_CUSTOS)


def params_vendas(hoje):
    """30 dias fechados (até ontem): hoje ainda está vendendo."""
    fim = hoje - timedelta(days=1)
    return {'ini_30': fim - timedelta(days=29), 'ini_7': fim - timedelta(days=6), 'fim': fim}


def tolerancia_atraso(agora_sp):
    """Mesma regra de v_cobertura_full (copiada, não lida): depois das 10h de
    Brasília o dado de ontem já devia ter chegado (1 dia); antes, 2."""
    return 1 if agora_sp.hour >= 10 else 2


def ler(conn, hoje):
    """Lê tudo o que a conta precisa. Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_ESTOQUE, {'marketplace': MARKETPLACE})
        estoque = cur.fetchall()
        cur.execute(SQL_VENDAS, params_vendas(hoje))
        vendas = cur.fetchall()
        cur.execute(SQL_COMPOSICAO)
        composicao = cur.fetchall()
        cur.execute(SQL_KITS_PENDENTES)
        pendentes = {r[0] for r in cur.fetchall()}
        cur.execute(SQL_MAPEAMENTO)
        mapa = dict(cur.fetchall())
        return estoque, vendas, composicao, pendentes, mapa
    finally:
        cur.close()
        conn.rollback()


def ler_estoque_shopee(conn):
    """Última foto de cada loja Shopee, no mesmo formato de `ler` (o mesmo
    SQL_ESTOQUE com outro marketplace). Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_ESTOQUE, {'marketplace': MARKETPLACE_SHOPEE})
        return cur.fetchall()
    finally:
        cur.close()
        conn.rollback()


def ler_composicao(conn):
    """(composicao, kits_pendentes, mapa) para a conta em peça. Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_COMPOSICAO)
        composicao = cur.fetchall()
        cur.execute(SQL_KITS_PENDENTES)
        pendentes = {r[0] for r in cur.fetchall()}
        cur.execute(SQL_MAPEAMENTO)
        return composicao, pendentes, dict(cur.fetchall())
    finally:
        cur.close()
        conn.rollback()


def ler_custos(conn, skus):
    """{sku: custo} para o peso do rateio (sem custo -> não aparece)."""
    if not skus:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(SQL_CUSTOS, {'skus': sorted(skus)})
        return {s: c for s, c in cur.fetchall() if c is not None}
    finally:
        cur.close()
        conn.rollback()


def _custo_valido(c):
    """Decimal > 0, ou None (sem custo, zero, NaN)."""
    try:
        d = Decimal(str(c))
    except Exception:
        return None
    return d if d.is_finite() and d > 0 else None


def _ratear(valor, pesos):
    """valor (Decimal) em partes proporcionais aos pesos, ao centavo; a
    última parte fica com o resto, então a soma é EXATAMENTE o valor."""
    total = sum(pesos)
    partes, acumulado = [], Decimal(0)
    for w in pesos[:-1]:
        parte = (valor * w / total).quantize(Decimal('0.01'), ROUND_HALF_EVEN)
        partes.append(parte)
        acumulado += parte
    partes.append(valor - acumulado)
    return partes


def valor_em_peca(vendas_valor, comp, custos):
    """
    Receita e margem por peça (regra no docstring do módulo).

    vendas_valor: [(sku vendido, receita, margem)].
    custos: {peca: custo} (ler_custos).
    Devolve ({peca: {'receita_sozinha', 'receita_kit', 'margem_sozinha',
    'margem_kit'} em Decimal}, [kits rateados por QUANTIDADE por falta de
    custo de alguma peça]).
    """
    out, por_quantidade = {}, set()

    def _v(peca):
        return out.setdefault(peca, {'receita_sozinha': Decimal(0), 'receita_kit': Decimal(0),
                                     'margem_sozinha': Decimal(0), 'margem_kit': Decimal(0)})

    for sku, receita, margem in vendas_valor:
        receita = Decimal(str(receita or 0))
        margem = Decimal(str(margem or 0))
        if sku not in comp:            # avulsa (ou kit sem composição): inteira
            v = _v(sku)
            v['receita_sozinha'] += receita
            v['margem_sozinha'] += margem
            continue
        itens = sorted(comp[sku].items())
        custo = [_custo_valido(custos.get(p)) for p, _q in itens]
        if all(c is not None for c in custo):
            pesos = [c * q for c, (_p, q) in zip(custo, itens)]
        else:
            pesos = [Decimal(q) for _p, q in itens]
            por_quantidade.add(sku)
        for (peca, _q), r, m in zip(itens, _ratear(receita, pesos), _ratear(margem, pesos)):
            v = _v(peca)
            v['receita_kit'] += r
            v['margem_kit'] += m
    return out, sorted(por_quantidade)


def ler_vendas_por_sku(conn, where_sql, params):
    """[(sku, qtd, receita, margem)] do período/filtros da aba Mais Vendidos.
    Só leitura."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_VENDAS_POR_SKU_MODELO.format(where=where_sql), params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.rollback()


def tabela_por_peca(venda, limite=None, valores=None):
    """
    Saída de venda_em_peca (unidades) e, se vier, de valor_em_peca (R$) ->
    DataFrame, uma linha por peça, maior total de unidades primeiro.
    """
    linhas = [{
        'peca': peca,
        'sozinha': v['sozinha'],
        'em_kit': v['em_kit'],
        'total': v['total'],
        'pct_em_kit': v['em_kit'] / v['total'] if v['total'] else None,
        'qtd_kits': len(v['kits']),
        'kits': tuple(sorted(v['kits'])),
        'kit_sem_composicao': v['kit_sem_composicao'],
    } for peca, v in venda.items() if v['total']]
    df = pd.DataFrame(linhas, columns=['peca', 'sozinha', 'em_kit', 'total', 'pct_em_kit',
                                       'qtd_kits', 'kits', 'kit_sem_composicao'])
    if valores is not None:
        zero = {'receita_sozinha': Decimal(0), 'receita_kit': Decimal(0),
                'margem_sozinha': Decimal(0), 'margem_kit': Decimal(0)}
        val = df['peca'].map(lambda s: valores.get(s, zero))
        df['receita_sozinha'] = val.map(lambda v: v['receita_sozinha'])
        df['receita_kit'] = val.map(lambda v: v['receita_kit'])
        df['receita_total'] = df['receita_sozinha'] + df['receita_kit']
        df['margem'] = val.map(lambda v: v['margem_sozinha'] + v['margem_kit'])
        df['margem_pct'] = [m / r if r else None
                            for m, r in zip(df['margem'], df['receita_total'])]
    df = df.sort_values(['total', 'peca'], ascending=[False, True]).reset_index(drop=True)
    return df.head(limite) if limite else df


def ler_contagens(conn, marketplace=MARKETPLACE):
    """[(loja, n_ultimo, ultimo, n_anterior, anterior)]. Só leitura.
    Padrão ML: o Sinais do Dia chama sem o marketplace."""
    cur = conn.cursor()
    try:
        cur.execute(SQL_CONTAGEM_DIAS, {'marketplace': marketplace})
        return cur.fetchall()
    finally:
        cur.close()
        conn.rollback()


def fotos_parciais(contagens, queda=QUEDA_FOTO_PARCIAL):
    """Lojas cujo último dia tem bem menos estoques que o anterior (já houve:
    ML-Nala 25/09 com 15 linhas, ML-YanniRJ 25/09 com 2). A foto é usada
    assim mesmo; a tela avisa que pode faltar estoque."""
    return [(loja, n_ult, ult, n_ant, ant)
            for loja, n_ult, ult, n_ant, ant in contagens
            if n_ant and n_ult < (1 - queda) * n_ant]


def ler_nomes(conn, skus):
    if not skus:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(SQL_NOMES, {'skus': sorted(skus)})
        return dict(cur.fetchall())
    finally:
        cur.close()
        conn.rollback()


# ============================================================
# CONTA EM PEÇA (sem banco)
# ============================================================

def _diverge(valores):
    """Lojas publicando galpão diferente, fora o ruído da hora da coleta."""
    if len(valores) < 2:
        return False
    alto, baixo = max(valores), min(valores)
    return alto - baixo > max(2, 0.01 * alto)


def agrupar_composicao(composicao):
    """[(kit, peca, qtd)] -> {kit: {peca: qtd}}."""
    comp = {}
    for kit, peca, qtd in composicao:
        comp.setdefault(kit, {})[peca] = int(qtd)
    return comp


def kits_sem_composicao(kits_pendentes, mapa=None):
    """Kits pendentes, pelo SKU guardado e pelo da correção (a venda chega
    com o oficial)."""
    mapa = mapa or {}
    return {mapa.get(k, k) for k in kits_pendentes} | set(kits_pendentes)


def pecas_do_sku(sku, comp):
    """Peças de um SKU vendido: o kit vira suas peças; o resto é ele mesmo
    (inclusive kit sem composição)."""
    return comp[sku].items() if sku in comp else [(sku, 1)]


def venda_em_peca(vendas_qtd, comp, sem_comp=()):
    """
    A ÚNICA conta de venda em peça do sistema (cobertura e Mais Vendidos).

    vendas_qtd: [(sku vendido, quantidade)].
    Devolve {peca: {'sozinha', 'em_kit', 'total', 'kits': set, 'skus': set,
    'kit_sem_composicao': bool}}. Quantidade do kit × qtd da peça no kit.
    Kit sem composição entra como ele mesmo, marcado.
    """
    out = {}
    for sku, qtd in vendas_qtd:
        qtd = float(qtd or 0)
        for peca, q in pecas_do_sku(sku, comp):
            v = out.setdefault(peca, {'sozinha': 0.0, 'em_kit': 0.0, 'total': 0.0,
                                      'kits': set(), 'skus': set(),
                                      'kit_sem_composicao': False})
            if sku in comp:
                v['em_kit'] += qtd * q
                v['kits'].add(sku)
            else:
                v['sozinha'] += qtd
                v['kit_sem_composicao'] |= sku in sem_comp
            v['total'] += qtd * q
            v['skus'].add(sku)
    return out


def montar(estoque, vendas, composicao, kits_pendentes=(), mapa=None,
           prazo=PRAZO_REPOSICAO_PADRAO, estoque_shopee=()):
    """
    estoque: [(loja, estoque_id, sku, data, full, transferencia, galpao)] — ML
    estoque_shopee: o mesmo formato, da Shopee (Full soma; galpão só confere
      ou preenche a peça que o ML não publica — ver cabeçalho)
    vendas: [(sku, qtd_7d, qtd_30d, receita_30d)]
    composicao: [(kit, peca, qtd)]

    Devolve (pecas, kits_por_peca, avisos):
      pecas: DataFrame, uma linha por peça;
      kits_por_peca: {peca: [dict por kit que usa a peça]};
      avisos: dict de listas para a tela.
    """
    mapa = mapa or {}
    comp = agrupar_composicao(composicao)
    sem_comp = kits_sem_composicao(kits_pendentes, mapa)

    def _pecas_de(sku):
        return pecas_do_sku(sku, comp)

    p = {}

    def _p(peca):
        return p.setdefault(peca, {
            'full': 0, 'full_shopee': 0, 'galpao_shopee': set(),
            'transf': 0, 'galpao_lojas': {}, 'prep': None,
            'qtd_7d': 0.0, 'qtd_30d': 0.0, 'skus_dependentes': set(),
            'kits': set(), 'vem_de_kit_sem_comp': False})

    # ---- estoque ---------------------------------------------------------
    galpao_kit = {}       # kit -> maior galpão publicado (fora o prep center)
    datas_loja = {}
    # SKU do anúncio passa pela mesma correção das vendas (um salto só), e só
    # DEPOIS o estoque é contado uma vez: um anúncio com o SKU errado e outro
    # com o certo no mesmo estoque_id viram uma linha, não duas (Full dobrado).
    def _unicos(linhas):
        unicos = {}
        for l, e, s, d, f, t, g in linhas:
            unicos.setdefault((l, e, mapa.get(s, s)), (l, e, mapa.get(s, s), d, f, t, g))
        return list(unicos.values())
    estoque = _unicos(estoque)
    estoque_shopee = _unicos(estoque_shopee)
    for loja, _eid, sku, data, full, transf, galpao in estoque:
        datas_loja[loja] = data
        for peca, q in _pecas_de(sku):
            reg = _p(peca)
            reg['full'] += int(full) * q
            reg['transf'] += int(transf) * q
            if sku in comp:
                reg['kits'].add(sku)
        if galpao is None:
            continue
        if sku in comp:
            if loja != LOJA_PREP_CENTER:
                galpao_kit[sku] = max(galpao_kit.get(sku, 0), int(galpao))
            continue
        reg = _p(sku)
        if loja == LOJA_PREP_CENTER:
            reg['prep'] = max(reg['prep'] or 0, int(galpao))
        else:
            reg['galpao_lojas'].setdefault(loja, set()).add(int(galpao))

    # Shopee: Full soma (outro armazém); galpão só da própria peça, guardado à
    # parte (o do anúncio de kit vem dividido e é ignorado).
    for loja, _eid, sku, data, full, _transf, galpao in estoque_shopee:
        datas_loja[loja] = data
        for peca, q in _pecas_de(sku):
            reg = _p(peca)
            reg['full_shopee'] += int(full or 0) * q
            if sku in comp:
                reg['kits'].add(sku)
        if galpao is not None and sku not in comp:
            _p(sku)['galpao_shopee'].add(int(galpao))

    # ---- venda -----------------------------------------------------------
    receita_sku = {sku: float(rec or 0) for sku, _q7, _q30, rec in vendas}
    v7 = venda_em_peca([(s, q7) for s, q7, _q30, _r in vendas], comp, sem_comp)
    v30 = venda_em_peca([(s, q30) for s, _q7, q30, _r in vendas], comp, sem_comp)
    for peca, v in v30.items():
        reg = _p(peca)
        reg['qtd_30d'] += v['total']
        reg['qtd_7d'] += v7.get(peca, {}).get('total', 0.0)
        reg['skus_dependentes'] |= v['skus']
        reg['kits'] |= v['kits']
        reg['vem_de_kit_sem_comp'] |= v['kit_sem_composicao']
    for sku in {r[2] for r in list(estoque) + list(estoque_shopee)} & sem_comp:
        _p(sku)['vem_de_kit_sem_comp'] = True

    # Piso do galpão: kit de UMA peça só, galpão do kit × qtd (aprovado).
    piso = {}
    for kit, g in galpao_kit.items():
        if len(comp[kit]) == 1:
            (peca, q), = comp[kit].items()
            piso[peca] = max(piso.get(peca, 0), g * q)

    # ---- linha por peça --------------------------------------------------
    linhas, divergentes, divergentes_shopee = [], [], []
    for peca, reg in p.items():
        valores = [v for vs in reg['galpao_lojas'].values() for v in vs]
        shopee = max(reg['galpao_shopee']) if reg['galpao_shopee'] else None
        galpao_shopee_diverge = None
        if valores:
            galpao, galpao_tipo = max(valores), 'conhecido'
            if _diverge(valores):
                divergentes.append(peca)
            if shopee is not None and _diverge([galpao, shopee]):
                galpao_shopee_diverge = shopee
                divergentes_shopee.append((peca, galpao, shopee))
        elif shopee is not None:
            # O maior, como entre as lojas do ML (o UpSeller pode publicar com
            # limite num anúncio/loja), e avisa quando os anúncios divergem.
            galpao, galpao_tipo = shopee, 'shopee'
            if _diverge(list(reg['galpao_shopee'])):
                divergentes.append(peca)
        elif peca in piso:
            galpao, galpao_tipo = piso[peca], 'piso'
        else:
            galpao, galpao_tipo = None, 'desconhecido'
        dia_30 = reg['qtd_30d'] / 30.0
        dia_7 = reg['qtd_7d'] / 7.0
        estoque_total = (galpao or 0) + reg['full'] + reg['full_shopee'] + (reg['prep'] or 0)
        cobertura = estoque_total / dia_30 if dia_30 > 0 else None
        minimo = galpao_tipo not in ('conhecido', 'shopee')
        linhas.append({
            'peca': peca,
            'galpao': galpao,
            'galpao_tipo': galpao_tipo,
            'galpao_diverge': peca in divergentes,
            'galpao_shopee_diverge': galpao_shopee_diverge,
            'full': reg['full'],
            'full_shopee': reg['full_shopee'],
            'transferencia': reg['transf'],
            'prep_center': reg['prep'],
            'venda_dia_7d': dia_7,
            'venda_dia_30d': dia_30,
            'cobertura_dias': cobertura,
            'cobertura_minima': minimo,
            'cobertura_full_dias': reg['full'] / dia_30 if dia_30 > 0 else None,
            'em_jogo_30d': sum(receita_sku.get(s, 0.0) for s in reg['skus_dependentes']),
            'skus_dependentes': tuple(sorted(reg['skus_dependentes'])),
            'qtd_kits': len(reg['kits']),
            'kit_sem_composicao': reg['vem_de_kit_sem_comp'],
            'ruptura': cobertura is not None and cobertura < prazo,
        })
    pecas = pd.DataFrame(linhas)
    if not pecas.empty:
        pecas = pecas.sort_values(['ruptura', 'em_jogo_30d', 'peca'],
                                  ascending=[False, False, True]).reset_index(drop=True)

    # ---- detalhe: kits que usam cada peça --------------------------------
    full_kit, full_kit_shopee = {}, {}
    for _loja, _eid, sku, _d, full, _t, _g in estoque:
        if sku in comp:
            full_kit[sku] = full_kit.get(sku, 0) + int(full)
    for _loja, _eid, sku, _d, full, _t, _g in estoque_shopee:
        if sku in comp:
            full_kit_shopee[sku] = full_kit_shopee.get(sku, 0) + int(full or 0)
    venda_kit = {s: (q30, rec) for s, _q7, q30, rec in vendas}
    kits_por_peca = {}
    for kit, c in comp.items():
        for peca, q in c.items():
            if peca not in p or kit not in p[peca]['kits']:
                continue
            q30, rec = venda_kit.get(kit, (0, 0))
            kits_por_peca.setdefault(peca, []).append({
                'Kit': kit, 'Qtd da peça no kit': q,
                'Full ML do kit (kits)': full_kit.get(kit, 0),
                'Full Shopee do kit (kits)': full_kit_shopee.get(kit, 0),
                'Galpão publicado do kit': galpao_kit.get(kit),
                'Kits vendidos 30d': int(q30 or 0),
                'Receita 30d do kit': float(rec or 0)})

    avisos = {
        'datas_loja': datas_loja,
        'galpao_divergente': sorted(divergentes),
        'galpao_shopee_divergente': sorted(divergentes_shopee),
        'kits_sem_composicao': sorted(pecas.loc[pecas['kit_sem_composicao'], 'peca'])
        if not pecas.empty else [],
    }
    return pecas, kits_por_peca, avisos


def skus_em_jogo(pecas, mascara):
    """SKUs DISTINTOS que dependem das peças marcadas (kit misto não
    conta duas vezes)."""
    skus = set()
    for t in pecas.loc[mascara, 'skus_dependentes']:
        skus |= set(t)
    return skus


def lojas_atrasadas(datas_loja, hoje, agora_sp):
    tol = tolerancia_atraso(agora_sp)
    return sorted(l for l, d in datas_loja.items()
                  if (hoje - pd.Timestamp(d).date()).days > tol)


# ============================================================
# TELA
# ============================================================

def _int(v):
    return f'{int(round(v)):,}'.replace(',', '.')


def _brl(v):
    return 'R$ ' + f'{v:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def texto_galpao(linha):
    if linha['galpao_tipo'] == 'conhecido':
        texto = _int(linha['galpao']) + (' ⚠ publicado diferente entre lojas'
                                         if linha['galpao_diverge'] else '')
        shopee = linha.get('galpao_shopee_diverge')
        if shopee is not None and not pd.isna(shopee):
            texto += f' ⚠ Shopee publica {_int(shopee)}'
        return texto
    if linha['galpao_tipo'] == 'shopee':
        return f"{_int(linha['galpao'])} (pela Shopee)" + (
            ' ⚠ publicado diferente entre lojas' if linha['galpao_diverge'] else '')
    if linha['galpao_tipo'] == 'piso':
        return f"desconhecido (≥ {_int(linha['galpao'])} pelo kit, estimativa)"
    return 'desconhecido'


def texto_cobertura(dias, minimo):
    if dias is None:
        return 'sem giro'
    return (f'≥ {dias:.0f} (mínimo)' if minimo else f'{dias:.0f}')


def _tabela_tela(df, nomes):
    return pd.DataFrame({
        'Peça': df['peca'],
        'Produto': df['peca'].map(lambda s: nomes.get(s) or '(sem cadastro)'),
        'Galpão': df.apply(texto_galpao, axis=1),
        'Full ML (peças)': df['full'].map(_int),
        'Full Shopee (peças)': df['full_shopee'].map(_int),
        'Em transferência (fora da cobertura)': df['transferencia'].map(_int),
        'Prep center SP': df['prep_center'].map(lambda v: '—' if pd.isna(v) else _int(v)),
        'Venda/dia 7d': df['venda_dia_7d'].map(lambda v: f'{v:.1f}'),
        'Venda/dia 30d': df['venda_dia_30d'].map(lambda v: f'{v:.1f}'),
        'Cobertura (dias)': [texto_cobertura(d, m) for d, m in
                             zip(df['cobertura_dias'], df['cobertura_minima'])],
        'Cobertura só Full ML (dias)': [texto_cobertura(d, False) for d in df['cobertura_full_dias']],
        'R$ em jogo (30d)': df['em_jogo_30d'].map(_brl),
        'Kits que usam': df['qtd_kits'],
        'Aviso': df['kit_sem_composicao'].map(lambda v: 'kit sem composição' if v else ''),
    })


def render(engine):
    import streamlit as st

    st.subheader("📦 Cobertura em peça (ML + Shopee)")
    st.caption(
        "Estoque da API do Mercado Livre e da foto diária da Shopee, em UNIDADES DE "
        "PEÇA: o Full de cada kit vira peças pela composição. A cobertura soma galpão + "
        "Full ML + Full Shopee + prep center. Galpão só do anúncio da própria peça (o do "
        "kit vem dividido pelo UpSeller); vale o do ML, e \"pela Shopee\" quando só a "
        "Shopee publica a peça. \"Cobertura só Full ML\" é o Full do ML ÷ a venda. Venda "
        "de todos os marketplaces, 30 dias até ontem. O Full da Amazon ainda não entra, "
        "e o que está EM TRANSFERÊNCIA para o Full aparece em coluna própria mas não "
        "conta na cobertura: ela é conservadora.")

    from permissoes import ve_todas_lojas
    if not ve_todas_lojas():
        st.info("Esta visão é da empresa inteira (galpão único e venda de todas as "
                "lojas): disponível para quem vê todas as lojas.")
        return

    c1, c2 = st.columns([1, 2])
    prazo = c1.number_input("Prazo de reposição (dias)", min_value=1, max_value=120,
                            value=PRAZO_REPOSICAO_PADRAO, step=1, key="cobp_prazo")
    busca = c2.text_input("Buscar peça ou kit (SKU ou parte do nome)", key="cobp_busca")

    hoje = date.today()
    conn = engine.raw_connection()
    try:
        estoque, vendas, composicao, pendentes, mapa = ler(conn, hoje)
        estoque_shopee = ler_estoque_shopee(conn)
        pecas, kits_por_peca, avisos = montar(estoque, vendas, composicao, pendentes,
                                              mapa, prazo, estoque_shopee)
        nomes = ler_nomes(conn, set(pecas['peca']) if not pecas.empty else set())
        parciais = fotos_parciais(ler_contagens(conn) +
                                  ler_contagens(conn, MARKETPLACE_SHOPEE))
    finally:
        conn.close()
    if pecas.empty:
        st.info("Sem estoque nem venda para mostrar.")
        return

    agora_sp = datetime.now(timezone(timedelta(hours=-3)))
    atrasadas = lojas_atrasadas(avisos['datas_loja'], agora_sp.date(), agora_sp)
    datas = ', '.join(f"{l} {pd.Timestamp(d):%d/%m}" for l, d in sorted(avisos['datas_loja'].items()))
    if atrasadas:
        st.warning(f"⚠️ Estoque atrasado em {', '.join(atrasadas)}. Foto usada: {datas}.")
    else:
        st.caption(f"Foto do estoque: {datas}.")
    for loja, n_ult, ult, n_ant, ant in parciais:
        st.warning(f"⚠️ Foto possivelmente PARCIAL em {loja}: {n_ult} estoques em "
                   f"{pd.Timestamp(ult):%d/%m} contra {n_ant} em {pd.Timestamp(ant):%d/%m}. "
                   "Pode faltar estoque dessa loja nesta tela.")

    if busca:
        b = busca.strip().lower()
        alvo = pecas['peca'].str.lower().str.contains(b, regex=False) | \
            pecas['peca'].map(lambda s: b in (nomes.get(s) or '').lower()) | \
            pecas['skus_dependentes'].map(lambda t: any(b in s.lower() for s in t))
        pecas = pecas[alvo]

    rup = pecas['ruptura']
    skus_risco = skus_em_jogo(pecas, rup)
    receita_risco = sum(v for s, v in _receita_por_sku(vendas).items() if s in skus_risco)
    m1, m2, m3 = st.columns(3)
    m1.metric(f"Peças com cobertura < {prazo} dias", _int(rup.sum()))
    m2.metric("R$ em jogo (30d, SKUs distintos)", _brl(receita_risco))
    m3.metric("Peças com galpão desconhecido",
              _int((~pecas['galpao_tipo'].isin(['conhecido', 'shopee'])).sum()))

    st.markdown(f"### 🔥 Ruptura iminente (cobertura < {prazo} dias)")
    st.caption("Ordenado pelo R$ em jogo: receita de 30 dias dos SKUs que param se a "
               "peça acabar (a peça e os kits que a usam). Cobertura \"mínimo\" = galpão "
               "desconhecido ou estimado pelo kit; pode ser maior.")
    if rup.any():
        st.dataframe(_tabela_tela(pecas[rup], nomes), use_container_width=True,
                     hide_index=True)
    else:
        st.success("Nenhuma peça abaixo do prazo.")

    with st.expander(f"📋 Todas as peças ({len(pecas)})"):
        st.dataframe(_tabela_tela(pecas, nomes), use_container_width=True,
                     hide_index=True, height=420)

    com_kit = [s for s in pecas['peca'] if s in kits_por_peca]
    if com_kit:
        st.markdown("### 🧩 Kits que usam a peça")
        escolhida = st.selectbox("Peça", com_kit, key="cobp_peca",
                                 format_func=lambda s: f"{s} — {nomes.get(s) or ''}")
        det = pd.DataFrame(kits_por_peca[escolhida]).sort_values('Receita 30d do kit',
                                                                 ascending=False)
        det['Receita 30d do kit'] = det['Receita 30d do kit'].map(_brl)
        st.dataframe(det, use_container_width=True, hide_index=True)

    if avisos['galpao_divergente']:
        st.warning("⚠ Galpão publicado diferente entre lojas (usado o maior): "
                   + ', '.join(avisos['galpao_divergente']))
    if avisos['galpao_shopee_divergente']:
        st.warning("⚠ Galpão da Shopee diferente do ML (vale o do ML; é o mesmo galpão "
                   "físico, conferir o UpSeller): "
                   + '; '.join(f'{peca} ML {_int(ml)} × Shopee {_int(sh)}'
                               for peca, ml, sh in avisos['galpao_shopee_divergente']))
    if avisos['kits_sem_composicao']:
        st.warning("Kit sem composição (pendente em Gestão de SKUs → 🧩 Kits), contado "
                   "como ele mesmo: " + ', '.join(avisos['kits_sem_composicao']))


def _receita_por_sku(vendas):
    return {s: float(r or 0) for s, _q7, _q30, r in vendas}
