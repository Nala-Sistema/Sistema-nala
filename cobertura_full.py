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

    lead time efetivo = lead time (~14 dias: ~10 para conseguir data de
    coleta + 3 a 4 até o ML disponibilizar) + folga para a semana em que não
    há data de coleta.

      cobertura < 7 dias                 ⚫ quebra garantida
      cobertura < lead efetivo           🔴 zera mesmo agendando hoje
      cobertura < lead efetivo + 7       🟠 última janela segura: agendar agora
      senão                              🟢

  ACELEROU
    Janela de 7 dias com venda por dia bem maior que a de 30 (cobertura de
    7d abaixo de 70% da de 30d) — e só com pelo menos 5 unidades vendidas na
    semana: abaixo disso é ruído (os logs provaram três vezes).

  QUANTO ENVIAR (dias-alvo informados pelo usuário)
    enviar = (dias_alvo + lead efetivo) × venda_dia
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
    janela_laranja_dias: int = 7
    piso_dias_com_estoque_7d: int = 3
    piso_vendidas_acelerou: int = 5
    fator_acelerou: float = 0.7
    alerta_bloqueio_dias: int = 60
    tolerancia_coleta_dias: int = 3
    janela_coleta_antes_dias: int = 1
    janela_coleta_depois_dias: int = 6
    fracao_coletado: float = 0.9
    onda_minima_unidades: int = 100

    @property
    def lead_time_efetivo(self):
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
    if cobertura < p.lead_time_efetivo:
        return VERMELHO
    if cobertura < p.lead_time_efetivo + p.janela_laranja_dias:
        return LARANJA
    return VERDE


def avaliar(linha, p, agendado_aberto=0, dias_alvo=None):
    """
    Avalia um estoque da `v_cobertura_full`.

    `agendado_aberto`: unidades agendadas e ainda não coletadas (ver
    `situacao_agendamentos`). None = ninguém informou nada.
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

    resultado = {
        'nivel': nivel,
        'icone': ICONES[nivel],
        'janela_que_disparou': janela,
        'venda_dia': venda_dia,
        'cobertura_disponivel': cob_disponivel,
        'cobertura_com_transito': cob_com_transito,
        'acelerou': acelerou,
        'alerta_bloqueio_fiscal': dias_bloqueio > p.alerta_bloqueio_dias,
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
    necessario = (dias_alvo + p.lead_time_efetivo) * venda_dia
    return max(0, math.ceil(necessario - disponivel - transito - agendado_aberto))


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
    quantidade e fonte_tem_sinal_coleta (linhas da v_estoque_envio_manual).
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
           criado_por, criado_em, fonte_tem_sinal_coleta
    FROM v_estoque_envio_manual
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
    """Lê as views e devolve (estoques avaliados, agendamentos com situação)."""
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

    agendado = agendado_por_estoque(situacoes)
    avaliados = []
    for linha in cobertura:
        chave = (linha['marketplace'], linha['loja'], linha['estoque_id'])
        aberto = agendado.get(chave) if situacoes else None
        avaliados.append(dict(linha, **avaliar(linha, parametros_de(linha['marketplace']),
                                               agendado_aberto=aberto, dias_alvo=dias_alvo)))
    return avaliados, situacoes
