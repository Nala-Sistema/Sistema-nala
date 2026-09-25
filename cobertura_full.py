"""
MÓDULO: Cobertura do Full e sugestão de reabastecimento
Sistema Nala

Uma lógica só para Mercado Livre e Shopee, calculada sobre a camada comum:
a view `v_cobertura_full` (estoque e denominadores) e a view
`v_estoque_envio_manual` (agendamentos). As views só entregam números; os
níveis, a quantidade a enviar e o estado de cada agendamento saem daqui,
porque dependem de parâmetros que mudam por marketplace.

REGRAS (acertadas com o Thiago em 15/09/2026)

  VENDA POR DIA
    Duas janelas: 7 dias (a principal) e 30 dias. O denominador são os dias
    COM estoque, não os dias corridos — senão um item que ficou zerado parece
    vender pouco e a cobertura sai folgada justamente em quem quebrou.
    Se a janela de 7 dias tiver menos de 3 dias com estoque, ela não serve
    (1 dia com 5 vendas viraria 5/dia): usa-se a de 30 e avisa.
    O alarme sai da PIOR janela (a de maior venda por dia) e diz qual foi.

  COBERTURA E NÍVEL
    cobertura = unidades / venda por dia. O nível usa o disponível MAIS o
    que está em transferência: essas unidades (coleta a caminho ou
    transferência interna do ML) chegam em 2 a 3 dias, antes de qualquer
    envio novo. A cobertura só do disponível também é mostrada.

    lead time ~14 dias: ~10 para conseguir data de coleta + 3 a 4 até o ML
    disponibilizar. A folga (~7) é a semana em que não há data de coleta.

      cobertura < 7 dias                 ⚫ quebra garantida
      cobertura < lead time              🔴 zera mesmo agendando hoje
      cobertura < lead time + folga      🟠 última janela segura: agendar agora
      senão                              🟢

    A folga já mora DENTRO da faixa laranja (decisão do Thiago, 16/09): ela
    não é somada de novo nos níveis. Ela entra, sim, na quantidade a enviar,
    como estoque de segurança.

  ACELEROU
    Janela de 7 dias com venda por dia bem maior que a de 30 (cobertura de
    7d abaixo de 70% da de 30d) — e só com pelo menos 5 unidades vendidas na
    semana: abaixo disso é ruído (os logs provaram três vezes).

  QUANTO ENVIAR (dias-alvo informados pelo usuário)
    enviar = (dias_alvo + lead time + folga) × venda_dia
             − disponível − em transferência − agendado ainda não coletado
    Sem somar o lead time, o item zera no caminho. Sem descontar transferência
    e agendamento, envia em dobro.

  AGENDAMENTO ("agendado, ainda não coletado")
    Quando o ML coleta, as unidades aparecem sozinhas no estoque
    (`unidades_entrada_coleta`). O agendamento fecha pela QUANTIDADE:
      - só conta entrada dentro da janela do agendamento
        (data agendada − 1 até + 6);
      - só conta entrada em DIA DE ONDA: o dia (ou o anterior) em que o
        marketplace inteiro teve coleta relevante. Fora disso, 2 a 12
        unidades soltas aparecem como ruído e fechariam agendamento pequeno.
        A onda é do marketplace, não da loja: o galpão é um só e a coleta
        passa nas lojas no mesmo dia — por loja, a ML-YanniRJ teria um único
        dia de onda em 46;
      - agendamentos do mesmo estoque com janelas sobrepostas repartem a
        coleta do mais antigo para o mais novo, sem contar duas vezes;
      - 90% da quantidade já é "coletado" (a leitura da coleta pelo total
        acerta a quantidade exata em 46 de 63 casos, não em todos).
    Passou da data agendada + 3 dias sem nada: "coleta não aconteceu" — e o
    agendamento deixa de ser descontado do envio.
    Coleta avulsa, fora da onda, o usuário marca à mão na tela
    (`coletado_manual_em` / `coletado_manual_por`): vale como coletado.

  ORDEM DA LISTA
    Por margem perdida por dia (R$) = venda/dia × margem por unidade — preço
    médio × margem % dos últimos 30 dias em fact_vendas_snapshot, por SKU e
    loja, preferindo as vendas FULL. Margem negativa vai sozinha para o fim.
    Menos de 5 unidades líquidas em 30 dias sai da lista urgente e vai para
    "giro baixo". A data da última venda aparece junto: o upload de vendas
    atrasa.
    Atenção: a margem do sistema ainda NÃO desconta ads nem Full — é teto.
"""

import math
from dataclasses import dataclass, replace
from datetime import date, timedelta

QUEBRA_GARANTIDA = 'quebra_garantida'
VERMELHO = 'vermelho'
LARANJA = 'laranja'
VERDE = 'verde'
SEM_GIRO = 'sem_giro'

ICONES = {QUEBRA_GARANTIDA: '⚫', VERMELHO: '🔴', LARANJA: '🟠', VERDE: '🟢', SEM_GIRO: '⚪'}

AGUARDANDO = 'aguardando coleta'
COLETADO_EM_PARTE = 'coletado em parte'
COLETADO = 'coletado'
NAO_COLETADO = 'coleta não aconteceu'
SEM_SINAL = 'sem sinal de coleta'


@dataclass(frozen=True)
class Parametros:
    lead_time_dias: int = 14
    folga_semana_perdida_dias: int = 7
    quebra_garantida_dias: int = 7
    piso_dias_com_estoque_7d: int = 3
    piso_giro_baixo_30d: int = 5
    piso_vendidas_acelerou: int = 5
    fator_acelerou: float = 0.7
    alerta_bloqueio_dias: int = 60
    tolerancia_coleta_dias: int = 3
    janela_coleta_antes_dias: int = 1
    janela_coleta_depois_dias: int = 6
    fracao_coletado: float = 0.9
    onda_minima_unidades: int = 100

    @property
    def lead_time_com_folga(self):
        """Só para a quantidade a enviar — os níveis não somam a folga de novo."""
        return self.lead_time_dias + self.folga_semana_perdida_dias


# Padrão por marketplace. A Shopee começa igual ao ML até termos o lead time
# real do Full dela — revisar quando o coletor da Shopee existir.
PARAMETROS = {
    'MERCADO LIVRE': Parametros(),
    'SHOPEE': Parametros(),
}


def parametros(marketplace, **ajustes):
    """Padrão do marketplace, com o que o usuário mudou na tela por cima."""
    base = PARAMETROS.get(marketplace, Parametros())
    return replace(base, **{k: v for k, v in ajustes.items() if v is not None})


def _num(valor):
    if valor is None:
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def _int(valor):
    n = _num(valor)
    return int(n) if n is not None else 0


def nivel_por_cobertura(cobertura, p):
    if cobertura is None:
        return SEM_GIRO
    if cobertura < p.quebra_garantida_dias:
        return QUEBRA_GARANTIDA
    if cobertura < p.lead_time_dias:
        return VERMELHO
    if cobertura < p.lead_time_dias + p.folga_semana_perdida_dias:
        return LARANJA
    return VERDE


def avaliar(linha, p, agendado_aberto=0, dias_alvo=None, economia=None):
    """
    Avalia um estoque da `v_cobertura_full`.

    `agendado_aberto`: unidades agendadas e ainda não coletadas (ver
    `situacao_agendamentos`). None = ninguém informou nada.
    `economia`: preço e margem do SKU (ver `economia_por_estoque`). None = sem
    venda registrada no sistema.
    """
    disponivel = _int(linha.get('full_disponivel'))
    transito = _int(linha.get('full_em_transferencia'))
    venda_7 = _num(linha.get('venda_dia_7d'))
    venda_30 = _num(linha.get('venda_dia_30d'))
    dias_7 = _int(linha.get('dias_com_estoque_7d'))
    dias_30 = _int(linha.get('dias_com_estoque_30d'))

    avisos = []
    janelas = {}
    usa_7 = venda_7 is not None and dias_7 >= p.piso_dias_com_estoque_7d
    if usa_7:
        janelas['7d'] = venda_7
    if venda_30 is not None and dias_30 > 0:
        janelas['30d'] = venda_30
    if not usa_7 and '30d' in janelas:
        avisos.append('demanda estimada com poucos dias (janela de 7d com menos de '
                      f'{p.piso_dias_com_estoque_7d} dias com estoque)')

    # pior janela = maior venda por dia; no empate vale a de 7d, que é a principal
    janela, venda_dia = None, None
    for nome in ('7d', '30d'):
        if nome in janelas and (venda_dia is None or janelas[nome] > venda_dia):
            janela, venda_dia = nome, janelas[nome]

    def cobertura(unidades):
        if not venda_dia:
            return None
        return unidades / venda_dia

    cob_disponivel = cobertura(disponivel)
    cob_com_transito = cobertura(disponivel + transito)
    nivel = nivel_por_cobertura(cob_com_transito, p)
    if not venda_dia and disponivel == 0:
        avisos.append('sem estoque e sem venda recente no Full')

    acelerou = False
    if usa_7 and janelas.get('30d') and venda_7 and venda_7 > 0:
        vendidas_7 = _int(linha.get('vendidas_7d'))
        # cobertura 7d / cobertura 30d = venda 30d / venda 7d
        if vendidas_7 >= p.piso_vendidas_acelerou and janelas['30d'] / venda_7 < p.fator_acelerou:
            acelerou = True

    dias_bloqueio = _int(linha.get('dias_bloqueio_fiscal'))
    if dias_bloqueio > p.alerta_bloqueio_dias:
        avisos.append(f'{_int(linha.get("full_bloqueado_fiscal"))} unidade(s) bloqueada(s) por '
                      f'cobertura fiscal há {dias_bloqueio} dias — abrir chamado no ML '
                      '(risco de tarifa de estoque antigo)')
    if linha.get('dado_atrasado'):
        avisos.append(f'dado de {linha.get("data_do_dado")} — o coletor não rodou; '
                      'a cobertura real é menor que a mostrada')
    if linha.get('serie_divergente'):
        avisos.append('série com movimento que a API não listou: conferir no painel do ML')

    venda_30_liquida = _num(linha.get('venda_liquida_30d'))
    giro_baixo = (venda_30_liquida or 0) < p.piso_giro_baixo_30d
    margem_unitaria = (economia or {}).get('margem_unitaria')
    margem_dia = (venda_dia * margem_unitaria
                  if venda_dia is not None and margem_unitaria is not None else None)

    resultado = {
        'nivel': nivel,
        'icone': ICONES[nivel],
        'janela_que_disparou': janela,
        'venda_dia': venda_dia,
        'cobertura_disponivel': cob_disponivel,
        'cobertura_com_transito': cob_com_transito,
        'acelerou': acelerou,
        'alerta_bloqueio_fiscal': dias_bloqueio > p.alerta_bloqueio_dias,
        'giro_baixo': giro_baixo,
        'preco_medio': (economia or {}).get('preco_medio'),
        'margem_percentual': (economia or {}).get('margem_percentual'),
        'margem_perdida_dia': margem_dia,
        'data_ultima_venda': (economia or {}).get('ultima_venda'),
        'avisos': avisos,
    }
    if dias_alvo is not None:
        resultado['enviar'] = quanto_enviar(venda_dia, disponivel, transito,
                                            agendado_aberto or 0, dias_alvo, p)
        if agendado_aberto is None:
            resultado['avisos'].append('nenhum agendamento informado: se já há coleta '
                                       'marcada, cadastre para não enviar em dobro')
    return resultado


def quanto_enviar(venda_dia, disponivel, transito, agendado_aberto, dias_alvo, p):
    if not venda_dia:
        return 0
    necessario = (dias_alvo + p.lead_time_com_folga) * venda_dia
    return max(0, math.ceil(necessario - disponivel - transito - agendado_aberto))


def economia_por_estoque(skus_por_estoque, vendas):
    """
    `skus_por_estoque`: {(marketplace, loja, estoque_id): {sku, ...}}.
    `vendas`: linhas com marketplace, loja, sku, full (bool), unidades,
    receita, margem e ultima_venda (últimos 30 dias, agrupadas).

    Preço e margem do estoque = média ponderada dos seus SKUs NA MESMA LOJA,
    usando só as vendas FULL quando existem (o preço do Full pode ser outro).
    """
    por_chave = {}
    for v in vendas:
        chave = (v['marketplace'], v['loja'], (v['sku'] or '').strip().upper())
        por_chave.setdefault(chave, []).append(v)

    saida = {}
    for (marketplace, loja, estoque_id), skus in skus_por_estoque.items():
        linhas = []
        for sku in skus:
            linhas.extend(por_chave.get((marketplace, loja, (sku or '').strip().upper()), []))
        if not linhas:
            continue
        do_full = [l for l in linhas if l.get('full') and (l.get('unidades') or 0) > 0]
        base = do_full or linhas
        unidades = sum(_num(l.get('unidades')) or 0 for l in base)
        receita = sum(_num(l.get('receita')) or 0 for l in base)
        margem = sum(_num(l.get('margem')) or 0 for l in base)
        if unidades <= 0:
            continue
        saida[(marketplace, loja, estoque_id)] = {
            'preco_medio': receita / unidades,
            'margem_unitaria': margem / unidades,
            'margem_percentual': (margem / receita) if receita else None,
            'ultima_venda': max(l['ultima_venda'] for l in linhas if l.get('ultima_venda')),
            'so_full': bool(do_full),
        }
    return saida


URGENTES = (QUEBRA_GARANTIDA, VERMELHO, LARANJA)


def organizar(avaliados):
    """
    Três seções:
      urgentes   ⚫🔴🟠 com giro, por margem perdida por dia (maior primeiro;
                 sem dado de venda vai para o fim, depois da margem negativa);
      giro baixo menos de 5 unidades em 30 dias, qualquer nível;
      em dia     🟢 e sem giro com giro normal.
    """
    def ordem(item):
        m = item.get('margem_perdida_dia')
        return (m is None, -(m or 0))

    urgentes = sorted((a for a in avaliados if a['nivel'] in URGENTES and not a['giro_baixo']),
                      key=ordem)
    giro_baixo = sorted((a for a in avaliados if a['giro_baixo']), key=ordem)
    em_dia = sorted((a for a in avaliados if a['nivel'] not in URGENTES and not a['giro_baixo']),
                    key=ordem)
    return {'urgentes': urgentes, 'giro_baixo': giro_baixo, 'em_dia': em_dia}


def dias_de_onda(coleta_por_dia, p):
    """
    `coleta_por_dia`: {(marketplace, data): unidades de entrada de coleta somadas
    em todas as lojas}. Devolve o conjunto de (marketplace, data) que contam como
    dia de coleta — o dia da onda e o seguinte, porque a entrada pode aparecer
    na operação do dia depois.
    """
    ondas = {k for k, total in coleta_por_dia.items() if (total or 0) >= p.onda_minima_unidades}
    validos = set()
    for marketplace, dia in ondas:
        validos.add((marketplace, dia))
        validos.add((marketplace, dia + timedelta(days=1)))
    return validos


def situacao_agendamentos(agendamentos, entradas, dias_validos, hoje, parametros_de):
    """
    `agendamentos`: dicts com id, marketplace, loja, estoque_id, data_coleta,
    quantidade, fonte_tem_sinal_coleta e coletado_manual_em (linhas da
    v_estoque_envio_manual). Marcado à mão continua participando do FIFO, para
    que a coleta dele não seja atribuída a outro agendamento.
    `entradas`: {(marketplace, loja, estoque_id): [(data, unidades), ...]}.
    `dias_validos`: saída de `dias_de_onda`.
    `parametros_de`: função marketplace -> Parametros.

    Devolve os agendamentos com `coletado`, `situacao` e `a_descontar`.
    """
    por_estoque = {}
    for ag in agendamentos:
        chave = (ag['marketplace'], ag['loja'], ag['estoque_id'])
        por_estoque.setdefault(chave, []).append(dict(ag, coletado=0))

    saida = []
    for chave, lista in por_estoque.items():
        p = parametros_de(chave[0])
        lista.sort(key=lambda a: (a['data_coleta'], a.get('id') or 0))
        for ag in lista:
            ag['janela_inicio'] = ag['data_coleta'] - timedelta(days=p.janela_coleta_antes_dias)
            ag['janela_fim'] = ag['data_coleta'] + timedelta(days=p.janela_coleta_depois_dias)

        # FIFO: cada entrada vai primeiro para o agendamento mais antigo que a aceita
        for dia, unidades in sorted(entradas.get(chave, [])):
            if (chave[0], dia) not in dias_validos:
                continue
            resto = unidades or 0
            for ag in lista:
                if resto <= 0:
                    break
                if not (ag['janela_inicio'] <= dia <= ag['janela_fim']):
                    continue
                cabe = ag['quantidade'] - ag['coletado']
                if cabe <= 0:
                    continue
                pega = min(cabe, resto)
                ag['coletado'] += pega
                resto -= pega

        for ag in lista:
            ag.update(_classificar(ag, hoje, p))
            saida.append(ag)
    return saida


def _classificar(ag, hoje, p):
    quantidade = ag['quantidade']
    coletado = ag['coletado']
    janela_aberta = hoje <= ag['janela_fim']
    if ag.get('coletado_manual_em'):
        # coleta avulsa marcada na tela: vale mais que o sinal automático
        return {'situacao': COLETADO, 'a_descontar': 0}
    if coletado >= p.fracao_coletado * quantidade:
        return {'situacao': COLETADO, 'a_descontar': 0}
    if coletado > 0:
        return {'situacao': COLETADO_EM_PARTE,
                'a_descontar': quantidade - coletado if janela_aberta else 0}
    if not ag.get('fonte_tem_sinal_coleta'):
        # sem como saber (ex.: Shopee): desconta enquanto a janela estiver aberta
        return {'situacao': SEM_SINAL, 'a_descontar': quantidade if janela_aberta else 0}
    if hoje > ag['data_coleta'] + timedelta(days=p.tolerancia_coleta_dias):
        return {'situacao': NAO_COLETADO, 'a_descontar': 0}
    return {'situacao': AGUARDANDO, 'a_descontar': quantidade}


def agendado_por_estoque(situacoes):
    total = {}
    for ag in situacoes:
        chave = (ag['marketplace'], ag['loja'], ag['estoque_id'])
        total[chave] = total.get(chave, 0) + ag['a_descontar']
    return total


# ---------------------------------------------------------------------------
# Leitura do banco
# ---------------------------------------------------------------------------

SQL_COBERTURA = 'SELECT * FROM v_cobertura_full'

SQL_AGENDAMENTOS = """
    SELECT id, marketplace, loja, estoque_id, data_coleta, quantidade, observacao,
           criado_por, criado_em, fonte_tem_sinal_coleta,
           coletado_manual_em, coletado_manual_por
    FROM v_estoque_envio_manual
"""

SQL_SKUS = """
    SELECT DISTINCT marketplace, loja, estoque_id, UPPER(TRIM(sku)) AS sku
    FROM dim_estoque_anuncio
    WHERE sku IS NOT NULL AND TRIM(sku) <> ''
"""

# Na Shopee o equivalente a FULL ainda precisa ser confirmado (FBS) — hoje só
# o ML tem estoque diário, e para ele 'FULL' é o valor gravado pelo upload.
SQL_VENDAS_30D = """
    SELECT marketplace_origem AS marketplace, loja_origem AS loja,
           UPPER(TRIM(sku)) AS sku, (UPPER(logistica) = 'FULL') AS full,
           SUM(quantidade) AS unidades, SUM(valor_venda_efetivo) AS receita,
           SUM(margem_total) AS margem, MAX(data_venda) AS ultima_venda
    FROM fact_vendas_snapshot
    WHERE data_venda >= %(desde)s AND sku IS NOT NULL
    GROUP BY 1, 2, 3, 4
"""

SQL_ENTRADAS = """
    SELECT marketplace, loja, estoque_id, data, unidades_entrada_coleta
    FROM fact_estoque_diario
    WHERE data >= %(desde)s AND unidades_entrada_coleta > 0
"""

SQL_COLETA_POR_DIA = """
    SELECT marketplace, data, SUM(unidades_entrada_coleta) AS total
    FROM fact_estoque_diario
    WHERE data >= %(desde)s
    GROUP BY marketplace, data
"""


def _consultar(engine, sql, params=None):
    """
    raw_connection + cursor, como o resto do sistema (nunca pd.read_sql com
    engine). Diferente do _raw_query de performance_utils, NÃO engole erro:
    uma tela de cobertura vazia por falha pareceria "nenhuma quebra".
    """
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        colunas = [d[0] for d in cursor.description]
        linhas = [dict(zip(colunas, r)) for r in cursor.fetchall()]
        cursor.close()
        return linhas
    finally:
        conn.close()


def montar_painel(engine, dias_alvo=None, ajustes_por_marketplace=None, hoje=None):
    """Lê as views e devolve (seções de `organizar`, agendamentos com situação)."""
    hoje = hoje or date.today()
    ajustes_por_marketplace = ajustes_por_marketplace or {}

    def parametros_de(marketplace):
        return parametros(marketplace, **ajustes_por_marketplace.get(marketplace, {}))

    cobertura = _consultar(engine, SQL_COBERTURA)
    agendamentos = _consultar(engine, SQL_AGENDAMENTOS)

    situacoes = []
    if agendamentos:
        desde = min(a['data_coleta'] for a in agendamentos) - timedelta(days=7)
        entradas = {}
        for r in _consultar(engine, SQL_ENTRADAS, {'desde': desde}):
            entradas.setdefault((r['marketplace'], r['loja'], r['estoque_id']), []).append(
                (r['data'], int(r['unidades_entrada_coleta'])))
        validos = set()
        coleta = _consultar(engine, SQL_COLETA_POR_DIA, {'desde': desde})
        for marketplace in {r['marketplace'] for r in coleta}:
            validos |= dias_de_onda(
                {(r['marketplace'], r['data']): int(r['total'] or 0)
                 for r in coleta if r['marketplace'] == marketplace},
                parametros_de(marketplace))
        situacoes = situacao_agendamentos(agendamentos, entradas, validos, hoje, parametros_de)

    skus = {}
    for r in _consultar(engine, SQL_SKUS):
        skus.setdefault((r['marketplace'], r['loja'], r['estoque_id']), set()).add(r['sku'])
    vendas = _consultar(engine, SQL_VENDAS_30D, {'desde': hoje - timedelta(days=30)})
    economia = economia_por_estoque(skus, vendas)

    agendado = agendado_por_estoque(situacoes)
    avaliados = []
    for linha in cobertura:
        chave = (linha['marketplace'], linha['loja'], linha['estoque_id'])
        aberto = agendado.get(chave) if situacoes else None
        avaliados.append(dict(linha, **avaliar(linha, parametros_de(linha['marketplace']),
                                               agendado_aberto=aberto, dias_alvo=dias_alvo,
                                               economia=economia.get(chave))))
    return organizar(avaliados), situacoes
