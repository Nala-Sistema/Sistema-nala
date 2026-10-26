"""
PERFORMANCE SEMANAL — aba "Semana" do Sinais do Dia ([PERFORMANCE SEMANAL] E1,
26/10/2026). Especificação aprovada pelo Thiago em 23/10/2026:
C:\\Users\\thiag\\Nala-Analises\\2026-10-23_performance_proposta-semanal_v2.md
(33 regras; a E1 traz as 16 de ATAQUE). Causas: metodo\\CARDAPIO_DE_CAUSAS_
performance_2026-10-09.md.

O QUE FAZ
  Toda segunda as gestoras abrem a semana seg–dom fechada já analisada: o
  Top 30 de cada loja por receita de 30 dias (os 30 dias terminam no domingo),
  com as sugestões para VENDER MAIS (regras T1–T16) e a marcação de cada item.
  A coluna "Sugestão" da Mais Vendidos (modo Anúncio) mostra as mesmas
  sugestões, com a origem "performance".

REGRAS DE DADO
  - Calculado NA TELA, sem agendador: a semana fechada sai da data de hoje
    (Brasília). Nada de GitHub Actions nem script no PC.
  - A consulta do Top 30 é a MESMA da Mais Vendidos (top_anuncios.py), com o
    corte por loja. Venda só de fact_vendas_snapshot.
  - Full: a MESMA conta da aba Full por loja (full_por_loja.montar, 21 dias,
    por estoque_id, só quantidade). T1 = Full acabando com galpão > 0; T2 =
    Full e transferência zerados com galpão > 0.
  - Conversão = PEDIDOS ÷ visitas. ML: da semana. Shopee: pedidos de 30 dias
    ÷ views de 30 dias (janela móvel, última foto até o domingo); nunca
    comparada com a do ML. A comparação é com a mediana do Top 30 da própria
    loja (cada loja tem outro patamar).
  - Semana × média das 4 semanas seg–dom anteriores (só dias fechados).
  - ACOS/TACOS, nunca ROAS como juiz. TACOS = gasto ÷ receita TOTAL.
  - Anúncio irmão (T5) só SUGERE ("decisão caso a caso").
  - Itens já resolvidos (LKE-9945, K100-L-0390) NÃO são exceção no código:
    saem pela marcação "Já resolvido" / "Ruído" / "Proposital", que cala a
    mesma loja + anúncio + regra por SILENCIO_SEMANAS, salvo se o R$ em jogo
    passar de PIORA_FATOR_RS × o da marcação.
  - Prazo sugerido nunca é o mesmo dia: D+2 útil.
  - "Carga incompleta": por loja, o domingo tem de ter vendas (coletor rodou
    depois do domingo), ads, visitas (ML) ou views (Shopee) e estoque. Faltou:
    faixa de aviso na loja; nada é escondido.

R$ EM JOGO (por semana; aprovado pelo Thiago em 26/10)
  - T7: a venda perdida contra a mediana da loja = visitas × (mediana −
    conversão) × ticket por pedido (Shopee: os 30 dias × 7/30).
  - T6, T8: a receita da semana do anúncio.
  - Full/estoque (T1–T5, T12): venda/dia em R$ × dias sem estoque, no
    máximo 7 (uma semana).
  - Demais (proposta, a confirmar): T9, T10, T13 = receita da semana; T11 =
    frete cobrado na semana; T14, T15, T16 = o que a venda caiu contra a média.

MARCAÇÃO (tabela perf_semana_marcacao, sql/performance_semana_marcacao.sql)
  Só acrescenta; vale a mais recente de cada semana + loja + anúncio + regra.
  Quem marca: a responsável da loja (dim_loja_responsavel_ads: Larissa no ML,
  Patricia na Shopee) ou ADMIN, como a caixinha das ações de ads; o gatilho do
  banco confere. Antes do SQL aplicado, a aba mostra tudo e não marca.
"""

import math
import re
import time
from datetime import datetime, timedelta

import pandas as pd

import estoque_peca as ep
import full_por_loja as fpl
import sinais_dia as sd
import top_anuncios as ta

# ============================================================
# CONSTANTES — (p) = proposta da v2, calibrar com 4 semanas de marcação "ruído"
# ============================================================

TOP_N = 30
DIAS_RECEITA = 30
SEMANAS_BASE = 4

# G8 (T6/T7), seção 2 da v2
G8_FATOR_BAIXA = 0.5            # T7: conversão ≤ metade da mediana
G8_FATOR_ALTA = 1.5             # T6: conversão ≥ 1,5× a mediana
G8_MIN_VIS_ML = 100             # T7: visitas na semana
G8_MIN_VIEWS_SHOPEE = 400       # T7: views de 30 dias
G8_MIN_PED_T7 = 5               # T7: pedidos (ML semana; Shopee 30 dias) — corte de ruído
G8_MIN_PED_T6_ML = 5            # T6: pedidos na semana
G8_MIN_PED_T6_SHOPEE = 20       # T6: pedidos em 30 dias
NOTA_MIN = 70                   # T6 exige nota ≥ 70; T9 acende abaixo

ENVIO_PARADO_DIAS = 7           # T4 (p)
# T4 DESLIGADA (26/10): a v2 pede "sem unidades_recebidas", mas a coluna está
# vazia no ML desde 27/09 e nunca veio na Shopee. Ligada, usa o substituto
# "a transferência não diminuiu em nenhum dos 7 dias" (nada entrou), que
# aguarda o OK do Thiago.
T4_LIGADA = False
RECLAMACOES_MIN = 3             # T10 (p)
RECLAMACOES_DIAS = 30
# T10: "diferente do anunciado". Na Shopee não há código equivalente
# (WRONG_ITEM é item errado no envio, não anúncio): fica só o ML.
MOTIVOS_DIFERENTE = ('different_than_published',)
ORCAMENTO_TRAVANDO = 0.90       # T8 (p)
CTR_QUEDA = 0.20                # T13 (p)
CTR_FAIXA_IMP = 0.20            # T13: impressões ±20% da média
CTR_MIN_CLIQUES_BASE = 20       # T13 (p, NOVA): cliques nas 4 semanas, para o CTR não ser ruído
VENDA_BASE_PAUSADO = 250.0      # T12: R$/semana na base
GASTO_BASE_MIN = sd.MIN_GASTO_BASE_ESPIRAL          # T14/T16: R$ 20/semana na base
ESPIRAL_QUEDA_GASTO = sd.ESPIRAL_QUEDA_GASTO        # T16: gasto < 50% da média
ESPIRAL_QUEDA_VENDA = sd.ESPIRAL_QUEDA_VENDA        # T16: venda < 80% da média
DIAS_ROAS_SUBIU = sd.DIAS_ROAS_SUBIU                # T15: 14 dias
ROAS_VENDA_BASE = 0.85          # T15: venda < 85% da base = caiu
DIAS_EM_JOGO_MAX = 7            # R$ em jogo é por semana

SILENCIO_SEMANAS = 4            # (p)
PIORA_FATOR_RS = sd.PIORA_FATOR_RS                  # 1,5: volta antes se o R$ em jogo cresceu
PRAZO_MIN_UTEIS = 2             # nada "para hoje": D+2 útil
PRAZO_MAX = 60                  # igual ao gatilho
TTL_LEITURA_S = sd.TTL_LEITURA_S

MOTIVOS = {'vou_agir': 'Vale a pena / vou agir',
           'em_andamento': 'Já em andamento',
           'feito': 'Feito',
           'ruido': 'Ruído / não serve',
           'proposital': 'Decisão proposital',
           'falta_fornecedor': 'Falta no fornecedor',
           'ja_resolvido': 'Já resolvido',
           'outro': 'Outro'}
MOTIVOS_SILENCIAM = ('ruido', 'proposital', 'ja_resolvido')
MOTIVOS_PEDEM_PRAZO = ('vou_agir',)
MOTIVOS_PEDEM_NOTA = ('outro',)
NOTA_MAX = 500

# Regra -> (nome curto, quem decide, dias para checar, prova). Da seção 1 da v2.
REGRAS = {
    'T1': ('Full < 21 dias', 'gestora', 7,
           'Full > 0 todo dia e venda ≥ ritmo anterior'),
    'T2': ('Ruptura com galpão', 'gestora', 7,
           'Full > 0 todo dia e venda ≥ ritmo anterior'),
    'T3': ('Estoque travado no Full', 'gestora', 7, 'travado zerado'),
    'T4': ('Envio parado', 'gestora', 7, 'entrada no Full'),
    'T5': ('Anúncio irmão com Full parado', 'gestora + Thiago', 14,
           'venda do anúncio parado sobe sem perder a do irmão'),
    'T6': ('Converte bem, pouca visita', 'ads', 14,
           'visitas ≥ +20% com conversão ≥ 80% da anterior'),
    'T7': ('Muita visita, pouca conversão', 'gestora + [MKT]', 14,
           'conversão ≥ mediana da loja × 0,75'),
    'T8': ('Orçamento travando', 'ads', 7, 'gasto acompanha o orçamento novo'),
    'T9': ('Nota de experiência < 70', 'gestora + [MKT]', 30,
           'nota do anúncio e da família sobe um degrau (30 → 65 → 75 → 100)'),
    'T10': ('Reclamação "diferente do anunciado"', 'gestora + [MKT]', 30,
            '0 casos novos'),
    'T11': ('Frete cobrado na Shopee', 'gestora', 14, 'frete 0'),
    'T12': ('Pausado com estoque', 'gestora', 7, 'anúncio ativo vendendo'),
    'T13': ('CTR de ads caiu', 'gestora + [MKT]', 14, 'CTR ≥ média anterior'),
    'T14': ('Campanha parada com estoque', 'ads', 7, 'gasto e venda de volta'),
    'T15': ('ROAS objetivo subiu e venda caiu', 'ads', 14,
            'TACOS menor e venda ≥ 85% da base'),
    'T16': ('Espiral de ads', 'ads', 21, 'venda ≥ média base com TACOS ≤ 3%'),
}
ORIGEM = 'performance'

BRT = sd.BRT

# ============================================================
# JANELAS
# ============================================================


def semana_fechada(hoje):
    """A última semana seg–dom fechada antes de hoje, a base (4 semanas antes)
    e os 30 dias da receita (terminando no domingo)."""
    sem_fim = hoje - timedelta(days=hoje.weekday() + 1)        # domingo
    sem_ini = sem_fim - timedelta(days=6)                      # segunda
    base_fim = sem_ini - timedelta(days=1)
    base_ini = base_fim - timedelta(days=7 * SEMANAS_BASE - 1)
    return {'hoje': hoje, 'ontem': hoje - timedelta(days=1),
            'sem_ini': sem_ini, 'sem_fim': sem_fim,
            'base_ini': base_ini, 'base_fim': base_fim,
            'ini30': sem_fim - timedelta(days=DIAS_RECEITA - 1)}


def prazo_minimo(hoje):
    """D+2 útil (sábado e domingo não contam; feriado não é modelado)."""
    d, n = hoje, 0
    while n < PRAZO_MIN_UTEIS:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return d


# ============================================================
# SQL (só SELECT, exceto a marcação)
# ============================================================

# Estoque dia a dia dos anúncios do Top 30, pela ponte anúncio × estoque.
SQL_ESTOQUE = """
    WITH p AS (
        SELECT DISTINCT marketplace, loja, anuncio_id, estoque_id,
               COALESCE(em_full, false) AS em_full
          FROM dim_estoque_anuncio
         WHERE loja = ANY(%(lojas)s) AND anuncio_id = ANY(%(anuncios)s)
           AND estoque_id IS NOT NULL)
    SELECT p.loja, p.anuncio_id, p.estoque_id, p.em_full, f.data,
           COALESCE(f.full_disponivel, 0), COALESCE(f.full_em_transferencia, 0),
           COALESCE(f.full_bloqueado_fiscal, 0), COALESCE(f.full_outros_indisponivel, 0),
           COALESCE(f.unidades_recebidas, 0), f.galpao_disponivel
      FROM p
      JOIN fact_estoque_diario f
        ON f.marketplace = p.marketplace AND f.loja = p.loja AND f.estoque_id = p.estoque_id
     WHERE f.data BETWEEN %(hist_ini)s AND %(ontem)s
"""

# Campanhas: gasto da semana, os anúncios de cada campanha (base + semana) e
# o orçamento diário médio das fotos da semana (a foto das ~05h30 vale o dia).
SQL_CAMPANHAS = """
    WITH g AS (
        SELECT loja, id_campanha::text AS id_campanha, MAX(campanha) AS campanha,
               SUM(gasto_ads) FILTER (WHERE data BETWEEN %(sem_ini)s AND %(sem_fim)s) AS gasto_sem,
               array_agg(DISTINCT codigo_anuncio) AS anuncios
          FROM fact_ads_performance
         WHERE loja = ANY(%(lojas)s) AND data BETWEEN %(base_ini)s AND %(sem_fim)s
           AND id_campanha IS NOT NULL AND codigo_anuncio IS NOT NULL
         GROUP BY loja, id_campanha::text),
    o AS (
        SELECT loja, id_campanha, AVG(orcamento_diario) AS orc_medio, COUNT(*) AS dias_orc
          FROM (SELECT DISTINCT ON (loja, id_campanha::text, data_captura::date)
                       loja, id_campanha::text AS id_campanha, orcamento_diario
                  FROM fact_ads_campanha_config
                 WHERE loja = ANY(%(lojas)s)
                   AND data_captura >= %(sem_ini)s AND data_captura < %(sem_fim_mais1)s
                 ORDER BY loja, id_campanha::text, data_captura::date, data_captura DESC) x
         WHERE orcamento_diario > 0
         GROUP BY loja, id_campanha)
    SELECT g.loja, g.id_campanha, g.campanha, g.gasto_sem, g.anuncios, o.orc_medio, o.dias_orc
      FROM g LEFT JOIN o ON o.loja = g.loja AND o.id_campanha = g.id_campanha
"""

SQL_NOTA = """
    SELECT DISTINCT ON (loja, codigo_anuncio) loja, codigo_anuncio, data, valor,
           detalhe->>'principal_actionable'
      FROM fact_saude_anuncio
     WHERE marketplace = 'MERCADO LIVRE' AND loja = ANY(%(lojas)s)
       AND tipo_sinal = 'experiencia_compra' AND codigo_anuncio = ANY(%(anuncios)s)
       AND valor IS NOT NULL               -- sem nota (anúncio sem medição) não é nota 0
       AND data BETWEEN %(nota_ini)s AND %(hoje)s
     ORDER BY loja, codigo_anuncio, data DESC
"""

SQL_RECLAMACOES = """
    SELECT loja, codigo_anuncio, sku, COUNT(DISTINCT id_caso)
      FROM fact_reclamacoes
     WHERE loja = ANY(%(lojas)s) AND motivo_nome = ANY(%(motivos)s)
       AND data_abertura >= %(recl_ini)s AND data_abertura < %(sem_fim_mais1)s
     GROUP BY loja, codigo_anuncio, sku
"""

SQL_STATUS = """
    SELECT loja, anuncio_id, bool_and(NOT COALESCE(ativo, true)) AS inativo,
           string_agg(DISTINCT status, ', ') AS status
      FROM dim_estoque_anuncio
     WHERE loja = ANY(%(lojas)s) AND anuncio_id = ANY(%(anuncios)s)
     GROUP BY loja, anuncio_id
"""

# A última chegada de cada fonte por loja, para a "carga incompleta".
SQL_CARGA = """
    SELECT 'vendas' AS fonte, loja_origem AS loja, max(data_processamento) AS ultima
      FROM fact_vendas_snapshot
     WHERE loja_origem = ANY(%(lojas)s) AND arquivo_origem = 'API'
       AND data_venda >= %(carga_ini)s
     GROUP BY loja_origem
    UNION ALL
    SELECT 'ads', loja, max(data)::timestamp
      FROM fact_ads_performance
     WHERE loja = ANY(%(lojas)s) AND data >= %(carga_ini)s
     GROUP BY loja
    UNION ALL
    SELECT 'visitas', loja, max(data)::timestamp
      FROM vw_visitas_dia
     WHERE loja = ANY(%(lojas)s) AND data >= %(carga_ini)s
     GROUP BY loja
    UNION ALL
    SELECT 'views', loja, max(data)::timestamp
      FROM vw_views_shopee_30d
     WHERE loja = ANY(%(lojas)s) AND data >= %(carga_ini)s
     GROUP BY loja
    UNION ALL
    SELECT 'estoque', loja, max(data)::timestamp
      FROM fact_estoque_diario
     WHERE loja = ANY(%(lojas)s) AND data >= %(carga_ini)s
     GROUP BY loja
"""

# ---- marcação (a única escrita) --------------------------------------------
SQL_EXISTE_MARCACAO = "SELECT to_regclass('perf_semana_marcacao') IS NOT NULL"

SQL_MARCACOES = """
    SELECT DISTINCT ON (marketplace, loja, semana_ini, codigo_anuncio, regra)
           marketplace, loja, semana_ini, codigo_anuncio, regra, motivo, nota, prazo,
           medida, decidido_por, decidido_em
      FROM perf_semana_marcacao
     WHERE loja = ANY(%(lojas)s) AND semana_ini >= %(desde)s
     ORDER BY marketplace, loja, semana_ini, codigo_anuncio, regra, decidido_em DESC, id DESC
"""

SQL_INSERIR_MARCACAO = """
    INSERT INTO perf_semana_marcacao
           (marketplace, loja, semana_ini, codigo_anuncio, sku, regra, motivo, nota, prazo,
            medida, evidencia, sugestao, decidido_por)
    VALUES (%(marketplace)s, %(loja)s, %(semana_ini)s, %(codigo_anuncio)s, %(sku)s,
            %(regra)s, %(motivo)s, %(nota)s, %(prazo)s, %(medida)s, %(evidencia)s,
            %(sugestao)s, %(usuario)s)
"""

MARCACAO_COLS = ('marketplace', 'loja', 'semana_ini', 'codigo_anuncio', 'regra', 'motivo',
                 'nota', 'prazo', 'medida', 'decidido_por', 'decidido_em')

TODAS_AS_SQL = (SQL_ESTOQUE, SQL_CAMPANHAS, SQL_NOTA, SQL_RECLAMACOES, SQL_STATUS, SQL_CARGA,
                SQL_EXISTE_MARCACAO, SQL_MARCACOES, SQL_INSERIR_MARCACAO)


def params(j, lojas, anuncios):
    return {**j, 'lojas': list(lojas), 'anuncios': list(anuncios),
            'hist_ini': min(j['sem_ini'], j['ontem'] - timedelta(days=20)),
            'sem_fim_mais1': j['sem_fim'] + timedelta(days=1),
            'nota_ini': j['hoje'] - timedelta(days=10),
            'recl_ini': j['sem_fim'] - timedelta(days=RECLAMACOES_DIAS - 1),
            'carga_ini': j['sem_fim'] - timedelta(days=7),
            'motivos': list(MOTIVOS_DIFERENTE)}


# ============================================================
# CONTAS (sem banco)
# ============================================================

def _n(v):
    """None/NaN -> 0.0."""
    if v is None:
        return 0.0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(f) else f


def _pct1(x):
    return f"{x * 100:.1f}%".replace('.', ',')


def _d(x):
    return f"{pd.Timestamp(x):%d/%m}"


def _mediana(valores):
    v = sorted(x for x in valores if x is not None)
    if not v:
        return None
    m = len(v) // 2
    return v[m] if len(v) % 2 else (v[m - 1] + v[m]) / 2


def eh_ml(r):
    return r['mkt'] == sd.MARKETPLACE


def visita_conversao(r):
    """(visitas, pedidos, conversão) da G8: ML = semana; Shopee = 30 dias."""
    if eh_ml(r):
        vis, ped = _n(r.get('vis_sem')), _n(r.get('ped_sem'))
    else:
        vw = r.get('views_30d')
        if vw is None or (isinstance(vw, float) and math.isnan(vw)):
            return None, _n(r.get('ped')), None
        vis, ped = _n(vw), _n(r.get('ped'))
    return vis, ped, (ped / vis if vis > 0 else None)


def medianas_por_loja(linhas):
    """{loja: (mediana de visitas, mediana de conversão)} do Top 30 de cada loja."""
    por = {}
    for r in linhas:
        vis, _ped, conv = visita_conversao(r)
        if not vis:            # sem visita registrada fica fora (como no teste da v2)
            continue
        v = por.setdefault(r['loja'], ([], []))
        v[0].append(vis)
        if conv is not None:
            v[1].append(conv)
    return {l: (_mediana(a), _mediana(b)) for l, (a, b) in por.items()}


def resumir_estoque(linhas_est, j):
    """Linhas de SQL_ESTOQUE -> {(loja, anúncio): resumo da semana e da foto}.
    Galpão nunca é somado: só "tem/não tem" e o maior número informado."""
    por_an, por_eid = {}, {}
    for (loja, an, eid, em_full, data, full, transf, bloq, outros, receb, galpao) in linhas_est:
        data = pd.Timestamp(data).date()
        por_an.setdefault((loja, an), []).append(
            (eid, bool(em_full), data, int(full), int(transf), int(bloq), int(outros),
             int(receb), galpao))
    out = {}
    for k, linhas in por_an.items():
        em_full = any(x[1] for x in linhas)
        sem = [x for x in linhas if j['sem_ini'] <= x[2] <= j['sem_fim']]
        dias_full = len({x[2] for x in sem if x[3] > 0})
        dias_galpao = len({x[2] for x in sem if (x[8] or 0) > 0})
        dias_estoque = len({x[2] for x in sem if x[3] > 0 or (x[8] or 0) > 0})
        ult = max(x[2] for x in linhas)
        foto = [x for x in linhas if x[2] == ult]
        travado = sum(x[5] + x[6] for x in foto)
        tem_estoque_hoje = any(x[3] > 0 or (x[8] or 0) > 0 for x in foto)
        galpoes = [x[8] for x in foto if x[8] is not None]
        # T4: por estoque, transferência > 0 em todas as últimas ENVIO_PARADO_DIAS
        # fotos e sem nada entrar (a transferência não diminuiu de um dia para o
        # outro; unidades_recebidas não é preenchida, ver T4_LIGADA).
        parados = []
        eids = {x[0] for x in linhas} if T4_LIGADA else set()
        for eid in eids:
            dia = {x[2]: x for x in linhas if x[0] == eid}
            datas = sorted(dia, reverse=True)
            if len(datas) < ENVIO_PARADO_DIAS:
                continue
            ult7 = datas[:ENVIO_PARADO_DIAS]
            if ult7[-1] != ult7[0] - timedelta(days=ENVIO_PARADO_DIAS - 1):
                continue                                    # falta foto: não julga
            def nada_entrou(d):
                ant = dia.get(d - timedelta(days=1))
                return dia[d][4] > 0 and (ant is None or dia[d][4] >= ant[4])
            if all(nada_entrou(d) for d in ult7[:-1]) and dia[ult7[-1]][4] > 0:
                desde = ult7[-1]
                for d in datas[ENVIO_PARADO_DIAS:]:
                    if d == desde - timedelta(days=1) and nada_entrou(desde) and dia[d][4] > 0:
                        desde = d
                    else:
                        break
                parados.append((eid, dia[ult7[0]][4], desde))
        out[k] = {'em_full': em_full, 'dias_full': dias_full, 'dias_galpao': dias_galpao,
                  'dias_estoque': dias_estoque, 'dias_semana': len({x[2] for x in sem}),
                  'foto': ult, 'travado': travado, 'tem_estoque_hoje': tem_estoque_hoje,
                  'galpao': max(galpoes) if galpoes else None, 'envio_parado': parados}
    return out


def estoque_nos_7_dias(e):
    return bool(e) and e['dias_estoque'] >= 7


def full_nos_7_dias(e):
    """Anúncio Full: Full > 0 nos 7 dias; senão galpão > 0 nos 7 dias."""
    if not e:
        return False
    return (e['dias_full'] if e['em_full'] else e['dias_galpao']) >= 7


def _rec_dia(i):
    """Venda/dia do estoque em R$ (unidades/dia da conta do Full × preço médio de 30 dias)."""
    if not i['qtd_30']:
        return 0.0
    return i['venda_dia'] * i['rec_30'] / i['qtd_30']


def classificar_actionable(texto):
    t = (texto or '').lower()
    if re.search(r'defeit|lâmina|lamina|amassad|rachad|solto|quebrad|marca ou', t):
        return 'defeito'
    if re.search(r'catálogo|catalogo|correspond', t):
        return 'catalogo'
    if re.search(r'atras|prazo|demor', t):
        return 'atraso'
    return None


def _item(r, regra, sugestao, evidencia, em_jogo, hoje):
    nome, decide, dias, prova = REGRAS[regra]
    prazo = prazo_minimo(hoje)
    return {'mkt': r['mkt'], 'loja': r['loja'], 'chave': r['chave'], 'pos': int(_n(r['pos'])),
            'skus': r['skus'], 'sku': r.get('sku_top') or r['skus'], 'nome': r['nome'],
            'regra': regra, 'regra_nome': nome, 'tipo': 'ATAQUE', 'origem': ORIGEM,
            'sugestao': sugestao, 'evidencia': evidencia, 'em_jogo': round(max(0.0, em_jogo), 2),
            'decide': decide, 'prazo': prazo, 'checar_em': prazo + timedelta(days=dias),
            'prova': prova}


def _skus(r):
    return {s.strip() for s in (r['skus'] or '').split(',') if s.strip()}


# ---- regras (cada uma: linha do Top 30 + o contexto -> itens) ---------------

def regras_full(r, ctx, hoje):
    """T1, T2, T3, T4, T5 (estoque e Full)."""
    out = []
    loja, chave = r['loja'], r['chave']
    full_itens = [i for i in ctx['full'] if i['loja'] == loja and chave in i['anuncios']]
    pior = None
    for i in full_itens:
        if not (i['acabando'] and (i['galpao'] or 0) > 0):
            continue
        if pior is None or i['dias_full'] < pior['dias_full']:
            pior = i
    if pior is not None:
        i = pior
        rd = _rec_dia(i)
        if i['full'] + i['transf'] == 0:
            out.append(_item(
                r, 'T2', "Repor o Full; ads: reduzir, não pausar (ainda vende pelo galpão)",
                f"Full 0 e em transferência 0 · galpão {i['galpao']} · venda "
                f"{sd._dec(i['venda_dia'])} un./dia (foto {_d(i['data'])})",
                rd * DIAS_EM_JOGO_MAX, hoje))
        else:
            limite = max(prazo_minimo(hoje),
                         hoje + timedelta(days=int(i['dias_full']) - fpl.PRAZO_CHEGADA_DIAS))
            if i['enviar'] is None:
                quanto = f"Enviar ({i['obs']})"
            else:
                quanto = f"Enviar {i['enviar']} un. até {_d(limite)}"
            sai = ctx['parados'].get(loja) or []
            espaco = ("; para abrir espaço no Full desta loja: "
                      + ', '.join(f"{', '.join(p['skus'])} ({p['full']} un.)" for p in sai)
                      if sai else '')
            dias_sem = min(DIAS_EM_JOGO_MAX, max(0.0, i['prazo'] - i['dias_full']))
            out.append(_item(
                r, 'T1', quanto + espaco,
                f"Full {i['full']} + transf. {i['transf']} = {i['dias_full']:.0f} dias "
                f"(< {i['prazo']}) · {sd._dec(i['venda_dia'])} un./dia · galpão {i['galpao']}"
                + (f" · {i['obs']}" if i['obs'] and i['enviar'] is not None else ''),
                rd * dias_sem, hoje))
        # T5: outro anúncio da loja com o mesmo SKU e Full parado
        irmaos = [p for p in ctx['full'] if p['loja'] == loja and p['parado']
                  and not p['parado_suprimido'] and chave not in p['anuncios']
                  and set(p['skus']) & set(i['skus'])]
        for p in irmaos[:2]:
            out.append(_item(
                r, 'T5',
                f"SUGESTÃO, decisão caso a caso: o anúncio {p['anuncio']} tem {p['full']} un. "
                f"paradas no Full; avaliar direcionar a venda ou o ads para ele",
                f"irmão {p['anuncio']} · Full {p['full']} un."
                + (f" = {p['semanas_full']:.0f} semanas de venda" if p['semanas_full'] else
                   " sem venda em 30 dias"),
                out[0]['em_jogo'], hoje))
    e = ctx['estoque'].get((loja, chave))
    if e and e['travado'] > 0:
        vd = max((i['venda_dia'] for i in full_itens), default=0)
        rd = max((_rec_dia(i) for i in full_itens), default=0)
        dias = min(DIAS_EM_JOGO_MAX, e['travado'] / vd) if vd > 0 else 0
        out.append(_item(r, 'T3', f"Resolver nota fiscal ou retirada: {e['travado']} un. travadas",
                         f"bloqueado fiscal + outros indisponível = {e['travado']} un. "
                         f"(foto {_d(e['foto'])})", rd * dias, hoje))
    for eid, transf, desde in (e or {}).get('envio_parado', []):
        vd = max((i['venda_dia'] for i in full_itens), default=0)
        rd = max((_rec_dia(i) for i in full_itens), default=0)
        dias = min(DIAS_EM_JOGO_MAX, transf / vd) if vd > 0 else 0
        out.append(_item(r, 'T4', f"Cobrar a logística: {transf} un. em transferência desde "
                                  f"{_d(desde)}",
                         f"estoque {eid}: em transferência há {(hoje - desde).days} dias "
                         f"sem unidade recebida", rd * dias, hoje))
        break
    return out


def regras_g8(r, ctx, hoje, t1_aceso):
    """T6 e T7 (G8). Exigem estoque nos 7 dias da semana."""
    med = ctx['medianas'].get(r['loja'])
    vis, ped, conv = visita_conversao(r)
    if not med or vis is None or conv is None or med[0] is None or not med[1]:
        return []
    med_vis, med_conv = med
    if not estoque_nos_7_dias(ctx['estoque'].get((r['loja'], r['chave']))):
        return []
    ml = eh_ml(r)
    rot = 'visitas' if ml else 'views 30d'
    out = []
    ev = (f"{vis:.0f} {rot}, {ped:.0f} pedidos, conversão {_pct1(conv)} × mediana "
          f"{_pct1(med_conv)} ({med_vis:.0f} {rot}) do Top 30 da loja")
    if (vis >= med_vis and conv <= G8_FATOR_BAIXA * med_conv
            and vis >= (G8_MIN_VIS_ML if ml else G8_MIN_VIEWS_SHOPEE)
            and ped >= G8_MIN_PED_T7):
        ticket = (_n(r['rec_sem']) / ped if ml else _n(r['rec']) / ped) if ped else 0
        perdidos = vis * (med_conv - conv)
        em_jogo = perdidos * ticket * (1 if ml else 7 / DIAS_RECEITA)
        out.append(_item(r, 'T7', "Muito visto e pouco comprado: rever preço, capa, título "
                                  "e fotos [MKT]", ev, em_jogo, hoje))
    nota = ctx['nota'].get((r['loja'], r['chave']))
    nota_ok = not ml or nota is None or _n(nota[0]) >= NOTA_MIN
    if (conv >= G8_FATOR_ALTA * med_conv and vis <= med_vis
            and ped >= (G8_MIN_PED_T6_ML if ml else G8_MIN_PED_T6_SHOPEE)
            and nota_ok and not t1_aceso):
        txt_nota = '' if not ml else (' · sem nota no banco' if nota is None
                                      else f" · nota {_n(nota[0]):.0f}")
        out.append(_item(r, 'T6', "Converte bem e é pouco visto: avaliar mais exposição "
                                  "(baixar ROAS ou subir orçamento, G3)", ev + txt_nota,
                         _n(r['rec_sem']), hoje))
    return out


def regras_ads(r, ctx, hoje, t1_aceso):
    """T8, T13, T14, T15, T16."""
    if r['loja'] in ta.LOJAS_SEM_ADS:
        return []
    out = []
    loja, chave = r['loja'], r['chave']
    rec_sem, rec_base_sem = _n(r['rec_sem']), _n(r['rec_base']) / SEMANAS_BASE
    gasto_sem, gasto_base_sem = _n(r['gasto_sem']), _n(r['gasto_base']) / SEMANAS_BASE
    camps = [c for c in ctx['campanhas'] if c['loja'] == loja and chave in (c['anuncios'] or [])]
    e = ctx['estoque'].get((loja, chave))
    # T8
    if not t1_aceso:
        for c in camps:
            orc, dias = _n(c['orc_medio']), int(_n(c['dias_orc']))
            if orc <= 0 or dias == 0:
                continue
            uso = _n(c['gasto_sem']) / (orc * 7)
            if uso >= ORCAMENTO_TRAVANDO:
                out.append(_item(r, 'T8', "Campanha travando no orçamento: +10% a 20% a cada "
                                          "3 a 7 dias (G3)",
                                 f"campanha {c['campanha'] or c['id_campanha']}: gasto "
                                 f"{sd._brl(c['gasto_sem'])} na semana = {uso * 100:.0f}% do "
                                 f"orçamento ({sd._brl(orc)}/dia × 7)", rec_sem, hoje))
                break
    # T13
    imp_s, cli_s = _n(r['imp_sem']), _n(r['cli_sem'])
    imp_b, cli_b = _n(r['imp_base']), _n(r['cli_base'])
    if imp_s > 0 and imp_b > 0 and cli_b >= CTR_MIN_CLIQUES_BASE:
        ctr_s, ctr_b = cli_s / imp_s, cli_b / imp_b
        imp_media = imp_b / SEMANAS_BASE
        if (abs(imp_s / imp_media - 1) <= CTR_FAIXA_IMP and ctr_s <= (1 - CTR_QUEDA) * ctr_b):
            out.append(_item(r, 'T13', "CTR caiu com a mesma vitrine: nova capa ou título [MKT]",
                             f"CTR {ctr_s * 100:.2f}% × {ctr_b * 100:.2f}% da média "
                             f"(impressões {imp_s:.0f} × média {imp_media:.0f})".replace('.', ','),
                             rec_sem, hoje))
    queda = max(0.0, rec_base_sem - rec_sem)
    # T14
    if gasto_sem == 0 and gasto_base_sem >= GASTO_BASE_MIN and estoque_nos_7_dias(e):
        out.append(_item(r, 'T14', "Campanha parada com estoque: avaliar religar",
                         f"gasto 0 na semana × {sd._brl(gasto_base_sem)}/semana na base · "
                         f"estoque nos 7 dias", queda, hoje))
    # T15
    subidas = [s for s in ctx['roas'] if s['loja'] == loja and chave in s['anuncios']]
    tacos_s = gasto_sem / rec_sem if rec_sem > 0 else None
    tacos_b = gasto_base_sem / rec_base_sem if rec_base_sem > 0 else None
    if subidas and tacos_s is not None and tacos_b is not None:
        s = max(subidas, key=lambda x: x['dia'])
        if tacos_s >= tacos_b and rec_sem < ROAS_VENDA_BASE * rec_base_sem:
            out.append(_item(r, 'T15', f"Voltar o ROAS anterior ({sd._dec(_n(s['antes']))} "
                                       f"→ {sd._dec(_n(s['depois']))} em {_d(s['dia'])})",
                             f"TACOS {_pct1(tacos_s)} × {_pct1(tacos_b)} na base · venda "
                             f"{sd._brl(rec_sem)} × {sd._brl(rec_base_sem)}/semana",
                             queda, hoje))
    # T16 (fora quando o ROAS subiu: aí a causa é conhecida, T15)
    if (not subidas and gasto_base_sem >= GASTO_BASE_MIN
            and gasto_sem < ESPIRAL_QUEDA_GASTO * gasto_base_sem
            and rec_sem < ESPIRAL_QUEDA_VENDA * rec_base_sem and full_nos_7_dias(e)):
        out.append(_item(r, 'T16', "Com estoque: devolver o investimento aos poucos "
                                   "(+20% a 30% por ciclo)",
                         f"gasto {sd._brl(gasto_sem)} × {sd._brl(gasto_base_sem)}/semana · "
                         f"venda {sd._brl(rec_sem)} × {sd._brl(rec_base_sem)}/semana · "
                         f"estoque nos 7 dias", queda, hoje))
    return out


def regras_cadastro(r, ctx, hoje):
    """T9, T10, T11, T12."""
    out = []
    loja, chave = r['loja'], r['chave']
    rec_sem = _n(r['rec_sem'])
    nota = ctx['nota'].get((loja, chave))
    if eh_ml(r) and nota is not None and _n(nota[0]) < NOTA_MIN:
        tipo = classificar_actionable(nota[1])
        acao = {'defeito': "Defeito ou lote: separar o lote e cobrar o fornecedor",
                'catalogo': "Catálogo: foto e descrição fiéis ao produto real [MKT]",
                'atraso': "Atraso: levar ao Full"}.get(tipo, "Ver o motivo da nota no ML")
        out.append(_item(r, 'T9', acao + ". Não ligar ads e não abrir anúncio novo (a nota é "
                                         "herdada pela família)",
                         f"nota {_n(nota[0]):.0f} ({_d(nota[2])})"
                         + (f": {nota[1]}" if nota[1] else ''), rec_sem, hoje))
    n_an = ctx['recl'].get(('an', loja, chave), 0)
    n_sku = max((ctx['recl'].get(('sku', loja, s), 0) for s in _skus(r)), default=0)
    n = max(n_an, n_sku)
    if n >= RECLAMACOES_MIN:
        out.append(_item(r, 'T10', "Conferir anúncio × produto real [MKT]",
                         f"{n} reclamações \"diferente do anunciado\" em {RECLAMACOES_DIAS} dias "
                         f"({'no anúncio' if n_an >= n_sku else 'no SKU'})", rec_sem, hoje))
    if not eh_ml(r):
        fr = [ctx['frete'][(loja, s)] for s in _skus(r) if (loja, s) in ctx['frete']]
        if fr:
            ped = sum(x[0] for x in fr)
            rs = sum(x[1] for x in fr)
            out.append(_item(r, 'T11', "Corrigir peso e medida no cadastro (frete da Nala "
                                       "tem de ser 0)",
                             f"{ped:.0f} pedido(s) com frete na semana · {sd._brl(rs)}", rs, hoje))
    st_ = ctx['status'].get((loja, chave))
    e = ctx['estoque'].get((loja, chave))
    base_sem = _n(r['rec_base']) / SEMANAS_BASE
    if st_ and st_[0] and e and e['tem_estoque_hoje'] and base_sem >= VENDA_BASE_PAUSADO:
        out.append(_item(r, 'T12', "Pausado com estoque: conferir se é proposital; se não, "
                                   "reativar",
                         f"status {st_[1]} · estoque na foto de {_d(e['foto'])} · venda base "
                         f"{sd._brl(base_sem)}/semana", base_sem, hoje))
    return out


def regras_ataque(linhas, ctx, hoje):
    """Todas as regras de ataque sobre as linhas do Top 30 (dicts da SQL_ATAQUE)."""
    ctx = {**ctx, 'medianas': medianas_por_loja(linhas)}
    itens = []
    for r in linhas:
        if str(r['chave']).startswith('sem item '):
            continue
        f = regras_full(r, ctx, hoje)
        t1 = any(i['regra'] in ('T1', 'T2') for i in f)
        itens += f
        itens += regras_g8(r, ctx, hoje, t1)
        itens += regras_ads(r, ctx, hoje, t1)
        itens += regras_cadastro(r, ctx, hoje)
    return itens


# ---- marcação e silêncio -----------------------------------------------------

def chave_item(i):
    return (i['mkt'], i['loja'], i['chave'], i['regra'])


def aplicar_marcacoes(itens, marcas, sem_ini):
    """(ativos, silenciados). marcas = dicts de SQL_MARCACOES (a mais recente de
    cada semana). A marcação DESTA semana fica no item; a de semanas anteriores
    com motivo que silencia cala o item por SILENCIO_SEMANAS, salvo se o R$ em
    jogo passou de PIORA_FATOR_RS × o da marcação."""
    desta, antes = {}, {}
    limite = sem_ini - timedelta(days=7 * SILENCIO_SEMANAS)
    for m in marcas or []:
        k = (m['marketplace'], m['loja'], m['codigo_anuncio'], m['regra'])
        s = pd.Timestamp(m['semana_ini']).date()
        if s == sem_ini:
            desta[k] = m
        elif limite <= s < sem_ini and (k not in antes or
                                        pd.Timestamp(antes[k]['semana_ini']).date() < s):
            antes[k] = m
    ativos, silenciados = [], []
    for i in itens:
        k = chave_item(i)
        i = {**i, 'marcacao': desta.get(k), 'silencio': None}
        m = antes.get(k)
        if m and m['motivo'] in MOTIVOS_SILENCIAM:
            medida = _n(m['medida'])
            if medida > 0 and i['em_jogo'] > PIORA_FATOR_RS * medida:
                i['voltou'] = f"voltou: R$ em jogo {sd._brl(i['em_jogo'])} × {sd._brl(medida)}"
            else:
                i['silencio'] = m
                silenciados.append(i)
                continue
        ativos.append(i)
    return ativos, silenciados


def situacao(i):
    m = i.get('marcacao')
    if not m:
        return i.get('voltou') or 'aberta'
    if m['motivo'] in ('vou_agir', 'em_andamento'):
        return 'em acompanhamento'
    if m['motivo'] == 'feito':
        return 'feito'
    if m['motivo'] in MOTIVOS_SILENCIAM:
        return f"silencia {SILENCIO_SEMANAS} semanas"
    return 'marcada'


def sugestoes_de(itens):
    """{(loja, anúncio): [{'origem', 'texto'}]} para a coluna Sugestão. Fica fora
    o que a gestora marcou como ruído / proposital / já resolvido."""
    out = {}
    for i in sorted(itens, key=lambda x: -x['em_jogo']):
        m = i.get('marcacao')
        if m and m['motivo'] in MOTIVOS_SILENCIAM:
            continue
        out.setdefault((i['loja'], i['chave']), []).append(
            {'origem': i['origem'], 'texto': f"{i['regra']}: {i['sugestao']}"})
    return out


def validar_marcacao(item, motivo, nota, prazo, hoje):
    erros = []
    if motivo not in MOTIVOS:
        erros.append("motivo inválido")
    nota = (nota or '').strip()
    if motivo in MOTIVOS_PEDEM_NOTA and not nota:
        erros.append(f"\"{MOTIVOS.get(motivo, motivo)}\" pede uma nota")
    if len(nota) > NOTA_MAX:
        erros.append(f"nota com mais de {NOTA_MAX} caracteres")
    if motivo in MOTIVOS_PEDEM_PRAZO and prazo is None:
        erros.append(f"\"{MOTIVOS[motivo]}\" pede o prazo")
    if prazo is not None and not (prazo_minimo(hoje) <= prazo <= hoje + timedelta(days=PRAZO_MAX)):
        erros.append(f"prazo entre {_d(prazo_minimo(hoje))} (D+2 útil) e "
                     f"{_d(hoje + timedelta(days=PRAZO_MAX))}")
    return [f"{item['regra']} · {item['chave']}: {e}" for e in erros]


def gravar_marcacoes(conn, marcacoes, usuario, hoje, sem_ini, lojas_decide):
    """A ÚNICA gravação. marcacoes = [(item, motivo, nota, prazo)]. Valida tudo
    antes de tocar no banco; uma transação só. Devolve o nº gravado."""
    if not (usuario or '').strip():
        raise ValueError("Usuário não identificado.")
    fora = sorted({i['loja'] for i, *_ in marcacoes if i['loja'] not in lojas_decide})
    if fora:
        raise PermissionError("Só a responsável da loja marca: " + ', '.join(fora))
    erros = [e for i, mo, no, pr in marcacoes for e in validar_marcacao(i, mo, no, pr, hoje)]
    if erros:
        raise ValueError('; '.join(erros))
    cur = conn.cursor()
    try:
        for i, motivo, nota, prazo in marcacoes:
            cur.execute(SQL_INSERIR_MARCACAO, {
                'marketplace': i['mkt'], 'loja': i['loja'], 'semana_ini': sem_ini,
                'codigo_anuncio': i['chave'], 'sku': i['sku'], 'regra': i['regra'],
                'motivo': motivo, 'nota': (nota or '').strip() or None, 'prazo': prazo,
                'medida': i['em_jogo'], 'evidencia': i['evidencia'][:500],
                'sugestao': i['sugestao'][:1000], 'usuario': usuario})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return len(marcacoes)


def carga_incompleta(linhas_carga, lojas_mkt, j):
    """{loja: [o que falta do domingo]}. Vendas: o coletor rodou depois do
    domingo (data_processamento ≥ segunda 00h). Ads (lojas com ads), visitas
    (ML), views (Shopee) e estoque: dado com data ≥ domingo."""
    ult = {(f, l): u for f, l, u in linhas_carga}
    seg = datetime.combine(j['sem_fim'] + timedelta(days=1), datetime.min.time())
    dom = datetime.combine(j['sem_fim'], datetime.min.time())
    out = {}
    for loja, mkt in lojas_mkt.items():
        exig = [('vendas', seg), ('estoque', dom),
                ('visitas' if mkt == sd.MARKETPLACE else 'views', dom)]
        if loja not in ta.LOJAS_SEM_ADS:
            exig.append(('ads', dom))
        faltam = []
        for f, minimo in exig:
            u = ult.get((f, loja))
            if u is None or pd.Timestamp(u).to_pydatetime().replace(tzinfo=None) < minimo:
                faltam.append(f)
        if faltam:
            out[loja] = faltam
    return out


# ============================================================
# LEITURA
# ============================================================

def _ler(conn, sql, p):
    return sd._ler(conn, sql, p)


def lojas_por_mkt(engine, lojas_db):
    """{loja: marketplace} das lojas de ataque (dim_lojas)."""
    out = {}
    for mkt in (sd.MARKETPLACE, sd.SHOPEE):
        for l in sd.obter_lojas_ml(engine, mkt):
            if l in lojas_db:
                out[l] = mkt
    return out


def ler_dados(engine, hoje):
    """A leitura da semana (vai para a memória). {fonte: dado} e {fonte: erro}:
    uma fonte que falhe só desliga as regras dela. Sempre TODAS as lojas de
    ataque (o filtro de loja do usuário vem depois, em montar): o conflito de
    galpão do Full cruza as lojas (auditor 26/10, achado 1)."""
    j = semana_fechada(hoje)
    lojas_mkt = lojas_por_mkt(engine, ta.LOJAS_ATAQUE)
    lojas = list(lojas_mkt)
    dados, erros = {'j': j, 'lojas_mkt': lojas_mkt}, {}
    top, _ant = ta.consultar_ataque(engine, lojas, j['ini30'], j['sem_fim'], TOP_N,
                                    por_loja=True, semana=(j['sem_ini'], j['sem_fim']),
                                    base=(j['base_ini'], j['base_fim']))
    dados['top'] = top
    linhas = top.to_dict('records')
    anuncios = sorted({str(r['chave']) for r in linhas})
    p = params(j, lojas, anuncios)

    # Full: a mesma leitura e a mesma conta da aba Full por loja
    try:
        pacotes = {}
        for mkt in (sd.MARKETPLACE, sd.SHOPEE):
            # todas as lojas do marketplace: a mesma chave de memória da aba do
            # Sinais e da Full por loja (sem leitura fria a mais)
            lm = sd.obter_lojas_ml(engine, mkt)
            if lm:
                pacote, _mem = sd.obter_pacote(engine, hoje, lm, mkt)
                faltam = [k for k in ('foto', 'ponte', 'vendas') if k in pacote['erros']]
                if faltam:
                    raise RuntimeError(f"pacote do Sinais sem {faltam}")
                pacotes[mkt] = pacote
        conn = engine.raw_connection()
        try:
            try:
                datas = fpl.ler_datas(conn, lojas)
            except Exception:  # noqa: BLE001 — sem datas: 21 dias para todas
                datas = None
            custos = ep.ler_custos(conn, fpl.skus_da_leitura(pacotes))
        finally:
            conn.close()
        itens, _conf, _prazos, _l = fpl.montar(pacotes, custos, datas, hoje)
        dados['full'] = itens
    except Exception as e:  # noqa: BLE001
        erros['full'] = e
        dados['full'] = []

    conn = engine.raw_connection()
    try:
        for nome, sql in (('estoque', SQL_ESTOQUE), ('campanhas', SQL_CAMPANHAS),
                          ('nota', SQL_NOTA), ('recl', SQL_RECLAMACOES),
                          ('status', SQL_STATUS), ('carga', SQL_CARGA)):
            try:
                dados[nome] = _ler(conn, sql, p)
            except Exception as e:  # noqa: BLE001
                erros[nome] = e
                dados[nome] = []
        roas = []
        for mkt in (sd.MARKETPLACE, sd.SHOPEE):
            lm = [l for l, m in lojas_mkt.items() if m == mkt]
            if not lm:
                continue
            try:
                pr = {'marketplace': mkt, 'lojas': lm, 'amanha': hoje + timedelta(days=1),
                      'ini_roas': hoje - timedelta(days=DIAS_ROAS_SUBIU + 16),
                      'roas_desde': hoje - timedelta(days=DIAS_ROAS_SUBIU)}
                roas += _ler(conn, sd.SQL_ROAS_SUBIU, pr)
            except Exception as e:  # noqa: BLE001
                erros['roas'] = e
        dados['roas'] = roas
        try:
            import analise_produtos as ap      # T11: a MESMA SQL da aba Penalização de frete
            lojas_shopee = [l for l, m in lojas_mkt.items() if m == sd.SHOPEE]
            dados['frete'] = _ler(conn, ap.SQL_PENALIZACAO_FRETE_SEM_TK,
                                  ap.params_penalizacao_frete(j['sem_ini'], j['sem_fim'],
                                                              lojas_shopee)) \
                if lojas_shopee else []
            dados['frete_cols'] = 'ap'
        except Exception as e:  # noqa: BLE001
            erros['frete'] = e
            dados['frete'] = []
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
    finally:
        conn.close()
    dados['lido_em'] = datetime.now(BRT)
    return dados, erros


def contexto(dados):
    """Monta o contexto das regras a partir das linhas cruas."""
    j = dados['j']
    full = dados['full']
    parados, vistos = {}, set()
    for i in sorted((i for i in full if i['parado'] and not i['parado_suprimido']),
                    key=lambda i: -(i['rs_parado'] or 0)):
        if (i['loja'], i['skus']) in vistos:        # o mesmo SKU em 2 estoques: uma vez só
            continue
        vistos.add((i['loja'], i['skus']))
        parados.setdefault(i['loja'], [])
        if len(parados[i['loja']]) < 3:
            parados[i['loja']].append(i)
    camps = [{'loja': l, 'id_campanha': c, 'campanha': n, 'gasto_sem': g,
              'anuncios': list(a or []), 'orc_medio': o, 'dias_orc': d}
             for l, c, n, g, a, o, d in dados['campanhas']]
    por_camp = {(c['loja'], c['id_campanha']): c['anuncios'] for c in camps}
    roas = []
    for loja, id_c, campanha, dia, antes, depois, item_id in dados['roas']:
        an = set(por_camp.get((loja, str(id_c)), []))
        a1 = sd.anuncio_da_campanha(id_c, campanha) or item_id
        if a1:
            an.add(str(a1))
        roas.append({'loja': loja, 'dia': dia, 'antes': antes, 'depois': depois,
                     'anuncios': an})
    recl = {}
    for loja, an, sku, n in dados['recl']:
        if an:
            recl[('an', loja, an)] = recl.get(('an', loja, an), 0) + int(n)
        if sku:
            recl[('sku', loja, sku)] = recl.get(('sku', loja, sku), 0) + int(n)
    frete = {}
    for linha in dados['frete']:
        # colunas da SQL da Penalização: marketplace, loja, sku, nome, pedidos_total,
        # pedidos_penalizados, lista, frete_rs, ...
        loja, sku, ped_pen, frete_rs = linha[1], linha[2], linha[5], linha[7]
        if ped_pen and _n(frete_rs) > 0:
            frete[(loja, sku)] = (_n(ped_pen), _n(frete_rs))
    return {
        'full': full, 'parados': parados,
        'estoque': resumir_estoque(dados['estoque'], j),
        'campanhas': camps, 'roas': roas,
        'nota': {(l, a): (v, act, d) for l, a, d, v, act in dados['nota']},
        'recl': recl, 'frete': frete,
        'status': {(l, a): (ina, s) for l, a, ina, s in dados['status']},
    }


def ler_marcacoes(conn, lojas, sem_ini):
    """Marcações das últimas semanas; None se a tabela não existe (SQL não aplicado)."""
    if not _ler(conn, SQL_EXISTE_MARCACAO, {})[0][0]:
        return None
    linhas = _ler(conn, SQL_MARCACOES, {
        'lojas': list(lojas), 'desde': sem_ini - timedelta(days=7 * SILENCIO_SEMANAS)})
    return [dict(zip(MARCACAO_COLS, r)) for r in linhas]


def montar(dados, marcas, hoje, lojas=None):
    """(ativos, silenciados) da semana; lojas = só estas (None = todas)."""
    linhas = dados['top'].to_dict('records')
    itens = regras_ataque(linhas, contexto(dados), hoje)
    if lojas is not None:
        itens = [i for i in itens if i['loja'] in lojas]
    return aplicar_marcacoes(itens, marcas, dados['j']['sem_ini'])


# ---- memória (como a leitura do dia do Sinais) -------------------------------

_MEMORIA = {}


def obter_dados(engine, hoje):
    """Leitura da semana em memória por TTL_LEITURA_S (uma só para todos os
    usuários: o filtro de loja vem em montar); leitura com erro não fica."""
    k = hoje
    agora = time.time()
    if k in _MEMORIA and agora - _MEMORIA[k][0] < TTL_LEITURA_S:
        return _MEMORIA[k][1]
    dados, erros = ler_dados(engine, hoje)
    if not erros:
        _MEMORIA.clear()
        _MEMORIA[k] = (agora, (dados, erros))
    return dados, erros


def limpar_memoria():
    _MEMORIA.clear()


def sugestoes_ultima_semana(engine, lojas_db):
    """Para a Mais Vendidos: ({(loja, anúncio): [sugestões]}, (seg, dom))."""
    hoje = datetime.now(BRT).date()
    dados, _erros = obter_dados(engine, hoje)
    conn = engine.raw_connection()
    try:
        marcas = ler_marcacoes(conn, list(dados['lojas_mkt']), dados['j']['sem_ini'])
    except Exception:  # noqa: BLE001 — sem marcação, as sugestões seguem
        marcas = None
    finally:
        conn.close()
    ativos, _sil = montar(dados, marcas, hoje, set(lojas_db))
    j = dados['j']
    return sugestoes_de(ativos), (j['sem_ini'], j['sem_fim'])


# ============================================================
# TELA
# ============================================================

ROTULO_FONTE = {'full': 'Full (T1–T5)', 'estoque': 'estoque da semana', 'campanhas': 'campanhas',
                'nota': 'nota de experiência', 'recl': 'reclamações', 'status': 'status do anúncio',
                'carga': 'conferência da carga', 'roas': 'ROAS objetivo', 'frete': 'frete Shopee'}


def _estoque_txt(r, ctx):
    its = [i for i in ctx['full'] if i['loja'] == r['loja'] and r['chave'] in i['anuncios']]
    full = [i['dias_full'] for i in its if i['em_full'] and i['dias_full'] is not None]
    galp = [i['galpao'] for i in its if i['galpao'] is not None]
    partes = []
    if full:
        partes.append(f"Full {min(full):.0f} d")
    if galp:
        partes.append(f"galpão {max(galp)}")
    return ' · '.join(partes) or '—'


def tabela_itens(itens, linhas_por_chave, ctx):
    """DataFrame das colunas da seção 3 da v2 (um item por anúncio × regra)."""
    rows = []
    for i in sorted(itens, key=lambda x: (x['pos'], -x['em_jogo'])):
        r = linhas_por_chave.get((i['loja'], i['chave']), {})
        vis, ped, conv = visita_conversao(r) if r else (None, None, None)
        base = _n(r.get('rec_base')) / SEMANAS_BASE if r else 0
        sem = _n(r.get('rec_sem')) if r else 0
        nota = ctx['nota'].get((i['loja'], i['chave']))
        m = i.get('marcacao') or {}
        rows.append({
            'Posição': i['pos'], 'Loja': ta.LOJAS_ATAQUE.get(i['loja'], i['loja']),
            'SKU': i['skus'], 'Produto': i['nome'], 'Anúncio': i['chave'],
            'Receita 30d': sd._brl(r.get('rec')) if r else '—',
            'Semana × média 4 sem.': (f"{sd._brl(sem)} × {sd._brl(base)} "
                                      f"({(sem / base - 1) * 100:+.0f}%)" if base > 0
                                      else f"{sd._brl(sem)} × —"),
            'Visitas / conversão': ('—' if vis is None else
                                    f"{vis:.0f} {'vis. sem.' if eh_ml(r) else 'views 30d'} · "
                                    + (_pct1(conv) if conv is not None else '—')),
            'Estoque': _estoque_txt(r, ctx) if r else '—',
            'Nota': '—' if nota is None else f"{_n(nota[0]):.0f}",
            'Tipo': i['tipo'], 'Regra': f"{i['regra']} {i['regra_nome']}",
            'Sugestão': i['sugestao'], 'Origem': i['origem'], 'Evidência': i['evidencia'],
            'R$ em jogo/sem.': sd._brl(i['em_jogo']), 'Decide': i['decide'],
            'Prazo sugerido': _d(i['prazo']), 'Checar em': _d(i['checar_em']),
            'Prova': i['prova'],
            'Marcação': (f"{MOTIVOS.get(m['motivo'], m['motivo'])}"
                         + (f" · até {_d(m['prazo'])}" if m.get('prazo') else '')
                         + (f" · {m['nota']}" if m.get('nota') else '')
                         + f" ({m['decidido_por']}, {pd.Timestamp(m['decidido_em']):%d/%m %H:%M})"
                         if m else ''),
            'Situação': situacao(i),
        })
    return pd.DataFrame(rows)


def _usuario():
    import streamlit
    return (streamlit.session_state.get('usuario') or {}).get('username', '')


def render(st, engine):
    import streamlit
    from permissoes import _get_role, ve_todas_lojas

    agora = datetime.now(BRT)
    hoje = agora.date()
    j = semana_fechada(hoje)
    st.caption(
        f"Semana seg–dom fechada **{_d(j['sem_ini'])}–{_d(j['sem_fim'])}** × média das "
        f"{SEMANAS_BASE} semanas antes ({_d(j['base_ini'])}–{_d(j['base_fim'])}). Top {TOP_N} "
        f"de cada loja por receita de {DIAS_RECEITA} dias ({_d(j['ini30'])}–{_d(j['sem_fim'])}). "
        "ATAQUE = o que destrava venda. Conversão = pedidos ÷ visitas (ML, semana) ou "
        "pedidos ÷ views de 30 dias (Shopee): não se compara ML com Shopee. Full = a conta "
        "da aba Full por loja (21 dias). Prazo sugerido nunca é hoje (D+2 útil).")

    restricao = sd.restricao_de_lojas(engine)
    lojas_db = [l for l in ta.LOJAS_ATAQUE if restricao is None or l in restricao]
    if not lojas_db:
        st.caption("Nenhuma loja com API atribuída ao seu perfil.")
        return
    c1, c2 = st.columns([1, 5])
    if c1.button("Atualizar dados", key="ps_atualizar"):
        limpar_memoria()
        sd.limpar_memoria()            # o Full vem da leitura do dia do Sinais
    try:
        with st.spinner("Calculando a semana…"):
            dados, erros = obter_dados(engine, hoje)
    except Exception:  # noqa: BLE001
        st.error("Não consegui ler a semana agora. Tente de novo em instantes.")
        return
    if erros:
        st.warning("Fora desta leitura (as regras delas não acenderam): "
                   + ', '.join(ROTULO_FONTE.get(k, k) for k in erros) + ".")
    c2.caption(f"lido às {dados['lido_em']:%H:%M}")

    conn = engine.raw_connection()
    erro_marca = False
    try:
        try:
            marcas = ler_marcacoes(conn, lojas_db, j['sem_ini'])
        except Exception:  # noqa: BLE001
            marcas, erro_marca = None, True
        try:
            responsaveis = {}
            for mkt in (sd.MARKETPLACE, sd.SHOPEE):
                responsaveis.update(dict(_ler(conn, sd.SQL_RESPONSAVEIS,
                                              {'marketplace': mkt, 'lojas': lojas_db})))
        except Exception:  # noqa: BLE001
            responsaveis = {}
    finally:
        conn.close()
    sql_aplicado = marcas is not None
    usuario = _usuario()
    _ve, lojas_decide = sd.lojas_das_acoes(lojas_db, responsaveis, usuario, _get_role(),
                                           ve_todas_lojas())

    ctx = contexto(dados)
    ativos, silenciados = montar(dados, marcas, hoje, set(lojas_db))
    linhas = {(r['loja'], r['chave']): r for r in dados['top'].to_dict('records')}
    carga = carga_incompleta(dados['carga'], dados['lojas_mkt'], j) if 'carga' not in erros \
        else {}

    if not sql_aplicado:
        st.info("Marcação aguardando o SQL (sql/performance_semana_marcacao.sql): a aba "
                "mostra as sugestões e ainda não marca." if not erro_marca
                else "Marcação indisponível agora; as sugestões abaixo seguem valendo.")

    todas = tabela_itens(ativos, linhas, ctx)
    if not todas.empty:
        buf = __import__('io').BytesIO()
        todas.to_excel(buf, index=False, sheet_name='Semana')
        st.download_button("⬇️ Baixar Excel da semana", data=buf.getvalue(),
                           file_name=f"performance_semana_{j['sem_ini']}_{j['sem_fim']}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument."
                                "spreadsheetml.sheet", key="ps_xlsx")

    aba_ml, aba_sh = st.tabs(["Mercado Livre", "Shopee"])
    for aba, mkt in ((aba_ml, sd.MARKETPLACE), (aba_sh, sd.SHOPEE)):
        with aba:
            lojas_m = [l for l in lojas_db if dados['lojas_mkt'].get(l) == mkt]
            if not lojas_m:
                st.caption("Nenhuma loja deste marketplace no seu perfil.")
            for loja in lojas_m:
                try:
                    _render_loja(st, streamlit, loja, ativos, silenciados, linhas, ctx, carga,
                                 hoje, j, loja in lojas_decide and sql_aplicado,
                                 responsaveis.get(loja), engine, usuario, lojas_decide)
                except Exception:  # noqa: BLE001 — uma loja quebrada não esconde as outras
                    st.error(f"{ta.LOJAS_ATAQUE.get(loja, loja)}: não consegui mostrar esta "
                             "loja agora; as outras seguem.")


def _render_loja(st, streamlit, loja, ativos, silenciados, linhas, ctx, carga, hoje, j, decide,
                 resp, engine, usuario, lojas_decide):
    sigla = ta.LOJAS_ATAQUE.get(loja, loja)
    st.markdown("---")
    st.subheader(sigla)
    if loja in carga:
        st.warning(f"Carga incompleta do domingo {_d(j['sem_fim'])}: falta "
                   + ', '.join(carga[loja]) + ". Os números desta loja podem mudar; "
                   "confira de novo mais tarde.")
    da_loja = [i for i in ativos if i['loja'] == loja]
    sil = [i for i in silenciados if i['loja'] == loja]
    n_top = sum(1 for k in linhas if k[0] == loja)
    c1, c2, c3 = st.columns(3)
    c1.metric("Sugestões de ataque", len(da_loja))
    c2.metric("R$ em jogo/semana", sd._brl(sum(i['em_jogo'] for i in da_loja)))
    c3.metric(f"Anúncios no Top {TOP_N}", n_top)
    if not da_loja:
        st.caption("Nenhuma regra de ataque acesa nesta loja.")
    else:
        df = tabela_itens(da_loja, linhas, ctx)
        ordem = sorted(da_loja, key=lambda x: (x['pos'], -x['em_jogo']))
        if not decide:
            st.dataframe(df, hide_index=True, use_container_width=True)
            st.caption(f"Só {resp or '(sem responsável cadastrada)'} marca os itens desta loja.")
        else:
            rotulos = [''] + list(MOTIVOS.values())
            df.insert(0, 'Motivo', '')
            df.insert(1, 'Nota (marcação)', '')
            df.insert(2, 'Prazo (seu)', [None] * len(df))
            ed = st.data_editor(
                df, key=f"ps_ed_{loja}_{streamlit.session_state.get('ps_versao', 0)}", hide_index=True, use_container_width=True,
                disabled=[c for c in df.columns if c not in ('Motivo', 'Nota (marcação)',
                                                             'Prazo (seu)')],
                column_config={
                    'Motivo': st.column_config.SelectboxColumn('Motivo', options=rotulos),
                    'Prazo (seu)': st.column_config.DateColumn(
                        'Prazo (seu)', format="DD/MM/YYYY", min_value=prazo_minimo(hoje),
                        max_value=hoje + timedelta(days=PRAZO_MAX))})
            st.caption("Escolha o motivo nas linhas que quer marcar e clique em Gravar. "
                       "\"Vale a pena / vou agir\" pede o prazo; \"Outro\" pede a nota. "
                       f"Ruído, proposital e já resolvido calam o item por {SILENCIO_SEMANAS} "
                       "semanas (volta antes se piorar). Vale a marcação mais recente.")
            if st.button("Gravar marcações", key=f"ps_ok_{loja}"):
                por_rotulo = {v: k for k, v in MOTIVOS.items()}
                marc = []
                for idx, item in enumerate(ordem):
                    rot = ed['Motivo'].iloc[idx]
                    if not rot:
                        continue
                    pr = ed['Prazo (seu)'].iloc[idx]
                    pr = None if pr is None or pd.isna(pr) else pd.Timestamp(pr).date()
                    marc.append((item, por_rotulo[rot], ed['Nota (marcação)'].iloc[idx], pr))
                if not marc:
                    st.warning("Nenhuma linha com motivo.")
                else:
                    conn = engine.raw_connection()
                    try:
                        n = gravar_marcacoes(conn, marc, usuario, hoje, j['sem_ini'],
                                             lojas_decide)
                    except (ValueError, PermissionError) as e:
                        st.error(str(e))
                        return
                    except Exception:  # noqa: BLE001
                        st.error("Não consegui gravar agora; nada foi gravado. Tente de novo.")
                        return
                    finally:
                        conn.close()
                    st.success(f"{n} marcação(ões) gravada(s).")
                    # editor novo: o motivo escolhido não fica para um 2º clique
                    streamlit.session_state['ps_versao'] =                         streamlit.session_state.get('ps_versao', 0) + 1
                    streamlit.rerun()
    if sil:
        with st.expander(f"Silenciados pela marcação ({len(sil)})"):
            st.dataframe(pd.DataFrame([{
                'Regra': f"{i['regra']} {i['regra_nome']}", 'SKU': i['skus'], 'Produto': i['nome'],
                'Anúncio': i['chave'], 'R$ em jogo/sem.': sd._brl(i['em_jogo']),
                'Motivo': MOTIVOS.get(i['silencio']['motivo'], i['silencio']['motivo']),
                'Semana marcada': _d(i['silencio']['semana_ini']),
                'Por': i['silencio']['decidido_por']} for i in sil]),
                hide_index=True, use_container_width=True)
    with st.expander(f"Top {TOP_N} completo da loja (receita de {DIAS_RECEITA} dias)"):
        rows = [r for k, r in linhas.items() if k[0] == loja]
        if rows:
            df_top = pd.DataFrame(rows).sort_values('pos')
            tab = ta.tabela_ataque(df_top, DIAS_RECEITA, sugestoes_de(da_loja))
            st.dataframe(tab, hide_index=True, use_container_width=True)
