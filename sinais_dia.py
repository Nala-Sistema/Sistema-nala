"""
SINAIS DO DIA — painel diário das gestoras (frente [SINAIS DO DIA], v1, 05/10/2026)

Menu "📡 Sinais do Dia". Uma aba por marketplace (v1 = Mercado Livre); dentro,
as lojas uma embaixo da outra. Por loja:
  (a) Resumo: venda de ontem × média do MESMO dia da semana nas 4 semanas
      anteriores; mês até ontem × meta (dim_metas_loja, aba Performance).
  (b) Sinais: até MAX_SINAIS_POR_LOJA por loja, ordenados por R$ em jogo.

SÓ LEITURA, regras em SQL/Python determinísticos (sem IA), sem tabela nova,
parâmetros ligados. Plano aprovado pelo Thiago em 05/10/2026, com a calibragem
do [MESTRE ANÁLISES] (rodada manual na ML-LPT, 05/10):

  - A venda da API ainda entra depois: D-1 e D-2 ficam FORA das comparações
    (DIAS_ATRASO_API). Semana avaliada = D-9..D-3; base = as 4 semanas
    anteriores (D-37..D-10), comparada como MÉDIA SEMANAL (o sábado vende ~25%
    menos; semanas inteiras anulam o dia da semana; ciclo do mês não é modelado).
  - Dias de Full = (Full disponível + em transferência) ÷ venda/dia, venda/dia =
    max(7 dias ÷ 7, 30 dias ÷ 30), as duas janelas terminando em D-3: ritmo
    recente com piso na média de 30 (caso K-L-0421-A, que zerou antes do
    previsto).
  - Full POR estoque_id, NUNCA somado por SKU: o K-L-0421-A tem 2 anúncios na
    ML-LPT, um com 6 un. (o que vende) e outro com 89 (quase parado); somar
    esconde a ruptura. Anúncios que DIVIDEM o mesmo estoque_id somam a venda
    contra aquele Full (o estoque é o mesmo). A venda casa com o estoque por
    (loja, anúncio, SKU): unidade sempre a do SKU do anúncio (kit em kits).
  - "Espiral" (ads cortado + venda caindo) só acende se o anúncio TEVE estoque
    em todos os dias da semana (Full se o anúncio é Full, senão galpão > 0
    como sim/não — o galpão de kit vem dividido, nunca é somado). Sem estoque,
    a causa é ruptura e o sinal sai no bloco FULL (caso Garrafa LTT-CT3821,
    ML-Nala, ago/26).
  - Experiência por FAMÍLIA = mesma peça base pela composição dos kits
    (dim_kit_composicao), na mesma loja. "Todos os alicates" exige campo novo
    no cadastro: v2.
  - Custo por venda de ads = CPC ÷ conversão = gasto ÷ unidades de ads
    (units_quantity do coletor), comparado com a margem em R$ por unidade
    (margem_total ÷ quantidade, margem ANTES de mídia e Full). Só anúncio com
    UM SKU vendido na janela: anúncio com variações de SKU diferentes não
    divide R$ por unidades de produtos diferentes.
  - ACOS ≠ TACOS: a espiral mostra TACOS (gasto ÷ venda TOTAL do anúncio).
  - Estoque: só quantidade, nunca availability_type ("in_stock" vem com zero).
  - "O coletor rodou?" sai das datas de captura no banco de vendas (sem
    segredo novo para o banco do coletor).
  - Visitas: lê a VIEW vw_visitas_dia (frente [VISITAS ML]); enquanto ela não
    existir, "coleta de visitas em construção".

Reaproveita de estoque_peca.py só a leitura auxiliar (composição, mapeamento de
SKU, nomes, contagem de fotos parciais, atraso da foto); NÃO usa montar() (é
cobertura em peça da empresa inteira, outra pergunta) nem cobertura_full.py /
v_cobertura_full (não auditados).

Os limiares abaixo são CONSTANTES para a gestora e o [MESTRE ANÁLISES]
calibrarem nas 2 primeiras semanas.
"""

import re
from datetime import date, datetime, timedelta, timezone

import pandas as pd

import estoque_peca as ep

MARKETPLACE = 'MERCADO LIVRE'

# ============================================================
# LIMIARES (calibrar aqui)
# ============================================================

DIAS_ATRASO_API = 2              # D-1 e D-2 ainda recebem venda da API
SEMANAS_BASE = 4                 # base = média semanal das 4 semanas anteriores
QUEDA_VENDA = 0.50               # semana < 50% da média semanal → queda
ALTA_VENDA = 2.0                 # semana > 2× a média semanal → alta
RECEITA_MIN_RELEVANTE = 1000.0   # R$ nas 4 semanas de base: anúncio "relevante"

JANELA_RITMO_FULL = 7            # venda recente (dias, terminando em D-3)
JANELA_PISO_FULL = 30            # piso: média de 30 dias (terminando em D-3)
FULL_URGENTE_DIAS = 7
FULL_ATENCAO_DIAS = 14

DIAS_GASTO_SEM_VENDA = 3         # dias seguidos com gasto e zero venda de ads
ESPIRAL_QUEDA_GASTO = 0.50       # gasto da semana < 50% da média semanal
ESPIRAL_QUEDA_VENDA = 0.80       # venda da semana < 80% da média semanal
MIN_UNID_ADS_CUSTO = 3           # unidades de ads (30d) p/ julgar custo por venda

DIAS_REF_EXPERIENCIA = 7         # nota de hoje × nota de 7 dias atrás
MIN_ANUNCIOS_FAMILIA = 2         # anúncios da mesma peça que pioraram

MAX_SINAIS_POR_LOJA = 10
HORA_COLETA_COMPLETA = 10        # depois das 10h (Brasília) o dado do dia já devia ter chegado
DIAS_LEITURA_FRESCOR = 10

BRT = timezone(timedelta(hours=-3))


# ============================================================
# JANELAS
# ============================================================

def janelas(hoje):
    """Todas as datas que a tela usa, a partir de HOJE (Brasília)."""
    ontem = hoje - timedelta(days=1)
    fim = hoje - timedelta(days=DIAS_ATRASO_API + 1)                 # D-3
    sem_ini = fim - timedelta(days=6)                                # D-9
    base_fim = sem_ini - timedelta(days=1)                           # D-10
    base_ini = base_fim - timedelta(days=7 * SEMANAS_BASE - 1)       # D-37
    ritmo_ini = fim - timedelta(days=JANELA_RITMO_FULL - 1)
    piso_ini = fim - timedelta(days=JANELA_PISO_FULL - 1)
    mes_ini = ontem.replace(day=1)
    mesmo_dia = [ontem - timedelta(days=7 * k) for k in range(1, SEMANAS_BASE + 1)]
    return {
        'hoje': hoje, 'ontem': ontem, 'fim': fim,
        'sem_ini': sem_ini, 'base_ini': base_ini, 'base_fim': base_fim,
        'ritmo_ini': ritmo_ini, 'piso_ini': piso_ini,
        'mes_ini': mes_ini, 'mesmo_dia': mesmo_dia,
        'ini_resumo': min(mes_ini, mesmo_dia[-1]),
        'ini_vendas': min(base_ini, piso_ini),
        'recente_ini': hoje - timedelta(days=DIAS_GASTO_SEM_VENDA),
        'ini_curto': hoje - timedelta(days=DIAS_LEITURA_FRESCOR),
    }


# ============================================================
# SQL — parâmetros ligados (psycopg2); sem schema de propósito: o teste usa
# tabelas TEMPORÁRIAS com os mesmos nomes. Toda leitura filtra por data em
# coluna com índice e pela lista de lojas permitidas.
# ============================================================

SQL_LOJAS_ML = """
    SELECT loja FROM dim_lojas WHERE marketplace = %(marketplace)s ORDER BY loja
"""

SQL_RESUMO = """
    SELECT loja_origem AS loja,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda = %(ontem)s)          AS rec_ontem,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda = ANY(%(mesmo_dia)s)) AS rec_mesmo_dia,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda >= %(mes_ini)s)       AS rec_mes
      FROM fact_vendas_snapshot
     WHERE marketplace_origem = %(marketplace)s
       AND loja_origem = ANY(%(lojas)s)
       AND data_venda >= %(ini_resumo)s AND data_venda <= %(ontem)s
     GROUP BY loja_origem
"""

SQL_METAS = """
    SELECT loja_origem, meta_receita
      FROM dim_metas_loja
     WHERE ano_mes = %(ano_mes)s AND loja_origem = ANY(%(lojas)s)
"""

# Por (loja, anúncio, SKU): a unidade nunca mistura SKUs.
SQL_VENDAS_ANUNCIO = """
    SELECT loja_origem AS loja, codigo_anuncio AS anuncio, sku,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda >= %(sem_ini)s)   AS rec_sem,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda >= %(base_ini)s
                                              AND data_venda <= %(base_fim)s) AS rec_base,
           SUM(quantidade) FILTER (WHERE data_venda >= %(ritmo_ini)s)          AS qtd_ritmo,
           SUM(quantidade) FILTER (WHERE data_venda >= %(piso_ini)s)           AS qtd_30,
           SUM(valor_venda_efetivo) FILTER (WHERE data_venda >= %(piso_ini)s)  AS rec_30,
           SUM(margem_total) FILTER (WHERE data_venda >= %(piso_ini)s)         AS margem_30
      FROM fact_vendas_snapshot
     WHERE marketplace_origem = %(marketplace)s
       AND loja_origem = ANY(%(lojas)s)
       AND codigo_anuncio IS NOT NULL
       AND data_venda >= %(ini_vendas)s AND data_venda <= %(fim)s
     GROUP BY loja_origem, codigo_anuncio, sku
"""

SQL_PONTE = """
    SELECT DISTINCT loja, anuncio_id, estoque_id, sku, COALESCE(em_full, false) AS em_full
      FROM dim_estoque_anuncio
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
       AND estoque_id IS NOT NULL AND anuncio_id IS NOT NULL
"""

# Última foto de cada loja, um estoque por linha.
SQL_ESTOQUE_FOTO = """
    WITH ult AS (
        SELECT loja, max(data) AS data
          FROM fact_estoque_diario
         WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
           AND data >= %(ini_curto)s
         GROUP BY loja)
    SELECT f.loja, f.estoque_id, f.data,
           COALESCE(f.full_disponivel, 0)       AS full_disponivel,
           COALESCE(f.full_em_transferencia, 0) AS full_em_transferencia,
           COALESCE(f.unidades_recebidas, 0)    AS unidades_recebidas
      FROM fact_estoque_diario f
      JOIN ult u ON u.loja = f.loja AND u.data = f.data
     WHERE f.marketplace = %(marketplace)s
"""

# Estoque dia a dia na semana avaliada (para a espiral).
SQL_ESTOQUE_SEMANA = """
    SELECT loja, estoque_id, data,
           COALESCE(full_disponivel, 0) AS full_disponivel, galpao_disponivel
      FROM fact_estoque_diario
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
       AND data >= %(sem_ini)s AND data <= %(fim)s
"""

SQL_ADS = """
    SELECT loja, codigo_anuncio AS anuncio,
           SUM(gasto_ads) FILTER (WHERE data >= %(sem_ini)s AND data <= %(fim)s)      AS gasto_sem,
           SUM(gasto_ads) FILTER (WHERE data >= %(base_ini)s AND data <= %(base_fim)s) AS gasto_base,
           SUM(gasto_ads) FILTER (WHERE data = %(ontem)s)                             AS gasto_ontem,
           SUM(gasto_ads) FILTER (WHERE data >= %(recente_ini)s)                      AS gasto_recente,
           COUNT(DISTINCT data) FILTER (WHERE data >= %(recente_ini)s
                                          AND gasto_ads > 0)                          AS dias_com_gasto,
           SUM(vendas) FILTER (WHERE data >= %(recente_ini)s)                         AS unid_recente,
           SUM(gasto_ads) FILTER (WHERE data >= %(piso_ini)s AND data <= %(fim)s)     AS gasto_30,
           SUM(vendas) FILTER (WHERE data >= %(piso_ini)s AND data <= %(fim)s)        AS unid_ads_30,
           SUM(cliques) FILTER (WHERE data >= %(piso_ini)s AND data <= %(fim)s)       AS cliques_30
      FROM fact_ads_performance
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
       AND codigo_anuncio IS NOT NULL
       AND data >= %(ini_vendas)s AND data <= %(ontem)s
     GROUP BY loja, codigo_anuncio
"""

# Foto de config de hoje × a foto anterior de cada campanha. A foto sai às
# ~05h30: o que a gestora mudou ONTEM aparece na foto de HOJE.
SQL_CONFIG_MUDOU = """
    WITH c AS (
        SELECT DISTINCT ON (loja, id_campanha, data_captura::date)
               loja, id_campanha, campanha, data_captura::date AS dia,
               roas_objetivo, orcamento_diario
          FROM fact_ads_campanha_config
         WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
           AND data_captura >= %(ini_curto)s
         ORDER BY loja, id_campanha, data_captura::date, data_captura DESC),
    r AS (
        SELECT c.*, row_number() OVER (PARTITION BY loja, id_campanha ORDER BY dia DESC) AS k
          FROM c)
    SELECT a.loja, a.id_campanha, a.campanha, b.dia AS dia_antes, a.dia AS dia_agora,
           b.roas_objetivo AS roas_antes, a.roas_objetivo AS roas_agora,
           b.orcamento_diario AS orc_antes, a.orcamento_diario AS orc_agora
      FROM r a
      JOIN r b ON b.loja = a.loja AND b.id_campanha = a.id_campanha AND b.k = 2
     WHERE a.k = 1 AND a.dia = %(hoje)s
       AND (a.roas_objetivo IS DISTINCT FROM b.roas_objetivo
            OR a.orcamento_diario IS DISTINCT FROM b.orcamento_diario)
"""

SQL_EXPERIENCIA = """
    WITH d AS (
        SELECT loja, max(data) AS data
          FROM fact_saude_anuncio
         WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
           AND tipo_sinal = 'experiencia_compra'
           AND data >= %(ini_curto)s AND data <= %(hoje)s
         GROUP BY loja)
    SELECT s.loja, s.codigo_anuncio, s.data, s.valor, s.texto,
           r.valor AS valor_ref, r.texto AS texto_ref
      FROM fact_saude_anuncio s
      JOIN d ON d.loja = s.loja AND d.data = s.data
      LEFT JOIN fact_saude_anuncio r
             ON r.marketplace = s.marketplace AND r.loja = s.loja
            AND r.codigo_anuncio = s.codigo_anuncio AND r.tipo_sinal = s.tipo_sinal
            AND r.data = s.data - %(dias_ref)s
     WHERE s.marketplace = %(marketplace)s AND s.tipo_sinal = 'experiencia_compra'
       AND s.data >= %(ini_curto)s AND s.codigo_anuncio IS NOT NULL
"""

SQL_EXISTE_VISITAS = "SELECT to_regclass('vw_visitas_dia') IS NOT NULL"

SQL_VISITAS = """
    SELECT loja, codigo_anuncio AS anuncio,
           SUM(visitas) FILTER (WHERE data >= %(sem_ini)s)                       AS vis_sem,
           SUM(visitas) FILTER (WHERE data >= %(base_ini)s AND data <= %(base_fim)s) AS vis_base
      FROM vw_visitas_dia
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
       AND data >= %(base_ini)s AND data <= %(fim)s
     GROUP BY loja, codigo_anuncio
"""

# Última chegada de cada fonte, por loja (horário de Brasília, sem fuso).
SQL_FRESCOR = """
    SELECT 'Vendas' AS fonte, loja_origem AS loja, max(data_processamento) AS ultima
      FROM fact_vendas_snapshot
     WHERE marketplace_origem = %(marketplace)s AND loja_origem = ANY(%(lojas)s)
       AND data_venda >= %(ini_curto)s
     GROUP BY loja_origem
    UNION ALL
    SELECT 'Estoque', loja, max(data_captura)
      FROM fact_estoque_diario
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s) AND data >= %(ini_curto)s
     GROUP BY loja
    UNION ALL
    SELECT 'Ads', loja, max(data_importacao)
      FROM fact_ads_performance
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s) AND data >= %(ini_curto)s
     GROUP BY loja
    UNION ALL
    SELECT 'Config. de ads', loja, max(data_captura)
      FROM fact_ads_campanha_config
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s)
       AND data_captura >= %(ini_curto)s
     GROUP BY loja
    UNION ALL
    SELECT 'Experiência', loja, max(data_importacao)
      FROM fact_saude_anuncio
     WHERE marketplace = %(marketplace)s AND loja = ANY(%(lojas)s) AND data >= %(ini_curto)s
     GROUP BY loja
"""

FONTES_FRESCOR = ('Vendas', 'Estoque', 'Ads', 'Config. de ads', 'Experiência')

TODAS_AS_SQL = (SQL_LOJAS_ML, SQL_RESUMO, SQL_METAS, SQL_VENDAS_ANUNCIO, SQL_PONTE,
                SQL_ESTOQUE_FOTO, SQL_ESTOQUE_SEMANA, SQL_ADS, SQL_CONFIG_MUDOU,
                SQL_EXPERIENCIA, SQL_EXISTE_VISITAS, SQL_VISITAS, SQL_FRESCOR)


def params(hoje, lojas):
    p = janelas(hoje)
    p.update({'marketplace': MARKETPLACE, 'lojas': list(lojas),
              'dias_ref': DIAS_REF_EXPERIENCIA,
              'ano_mes': p['ontem'].strftime('%Y-%m')})
    return p


# ============================================================
# LEITURA (só SELECT; cada fonte isolada: uma que falhe não derruba as outras)
# ============================================================

def _ler(conn, sql, p):
    cur = conn.cursor()
    try:
        cur.execute(sql, p)
        return cur.fetchall()
    finally:
        cur.close()
        conn.rollback()


FONTES = {
    'resumo': SQL_RESUMO,
    'metas': SQL_METAS,
    'vendas': SQL_VENDAS_ANUNCIO,
    'ponte': SQL_PONTE,
    'foto': SQL_ESTOQUE_FOTO,
    'estoque_semana': SQL_ESTOQUE_SEMANA,
    'ads': SQL_ADS,
    'config': SQL_CONFIG_MUDOU,
    'experiencia': SQL_EXPERIENCIA,
    'frescor': SQL_FRESCOR,
}


def ler_tudo(conn, hoje, lojas):
    """{fonte: linhas} e {fonte: exceção}. Visitas só se a view existir
    (None = ainda não existe)."""
    p = params(hoje, lojas)
    dados, erros = {}, {}
    for nome, sql in FONTES.items():
        try:
            dados[nome] = _ler(conn, sql, p)
        except Exception as e:  # noqa: BLE001 — vira aviso por bloco na tela
            erros[nome] = e
    try:
        dados['composicao'] = _ler(conn, ep.SQL_COMPOSICAO, {})
        dados['mapa'] = dict(_ler(conn, ep.SQL_MAPEAMENTO, {}))
    except Exception as e:  # noqa: BLE001
        erros['composicao'] = e
    try:
        if _ler(conn, SQL_EXISTE_VISITAS, {})[0][0]:
            dados['visitas'] = _ler(conn, SQL_VISITAS, p)
        else:
            dados['visitas'] = None
    except Exception as e:  # noqa: BLE001
        erros['visitas'] = e
    return dados, erros


# ============================================================
# CONTAS (sem banco)
# ============================================================

def _f(v):
    return float(v) if v is not None else 0.0


def _brl(v):
    return ep._brl(v)


def _dec(v, casas=1):
    return f'{v:.{casas}f}'.replace('.', ',')


def _pct(v):
    return f'{v * 100:+.0f}%'.replace('.', ',')


def anuncio_da_campanha(id_campanha, campanha=None):
    """No ML cada campanha é um anúncio só: 'MLB4639959197 Torx + Allen'."""
    for t in (id_campanha, campanha):
        m = re.match(r'\s*(MLB\d+)', t or '')
        if m:
            return m.group(1)
    return None


def agregar_vendas(vendas):
    """[(loja, anuncio, sku, rec_sem, rec_base, qtd_ritmo, qtd_30, rec_30, margem_30)]
    -> (por_anuncio, por_anuncio_sku).
    por_anuncio[(loja, anuncio)]: R$ somados (R$ pode somar), SKUs, e
    unidades/margem SÓ quando o anúncio vendeu um SKU único."""
    por_sku = {}
    an = {}
    for loja, anuncio, sku, rs, rb, qr, q30, r30, m30 in vendas:
        por_sku[(loja, anuncio, sku)] = {'qtd_ritmo': _f(qr), 'qtd_30': _f(q30),
                                         'rec_30': _f(r30)}
        a = an.setdefault((loja, anuncio), {'rec_sem': 0.0, 'rec_base': 0.0, 'rec_30': 0.0,
                                            'skus': set(), 'qtd_30': 0.0, 'margem_30': None})
        a['rec_sem'] += _f(rs)
        a['rec_base'] += _f(rb)
        a['rec_30'] += _f(r30)
        if _f(q30) > 0 or _f(rs) > 0 or _f(rb) > 0:
            a['skus'].add(sku)
        if _f(q30) > 0:
            a['qtd_30'] += _f(q30)
            if m30 is not None:
                a['margem_30'] = (a['margem_30'] or 0.0) + float(m30)
    for a in an.values():
        a['sku_unico'] = len(a['skus']) == 1
        if not a['sku_unico']:
            a['qtd_30'], a['margem_30'] = None, None
    return an, por_sku


def _sinal(loja, tipo, anuncio, skus, numero, sugestao, em_jogo, urgente=False,
           familia=''):
    return {'loja': loja, 'tipo': tipo, 'anuncio': anuncio or '',
            'skus': tuple(sorted(skus or ())), 'numero': numero,
            'sugestao': sugestao, 'em_jogo': float(em_jogo or 0),
            'urgente': urgente, 'familia': familia}


def _rel(a):
    return a['rec_base'] >= RECEITA_MIN_RELEVANTE


def sinais_vendas(por_anuncio, j):
    out = []
    for (loja, anuncio), a in por_anuncio.items():
        if not _rel(a):
            continue
        base_sem = a['rec_base'] / SEMANAS_BASE
        sem = a['rec_sem']
        periodo = f"{j['sem_ini']:%d/%m}–{j['fim']:%d/%m}"
        if sem < QUEDA_VENDA * base_sem:
            out.append(_sinal(
                loja, 'VENDAS', anuncio, a['skus'],
                f"Queda: semana {periodo} {_brl(sem)} × média {_brl(base_sem)}/sem "
                f"({_pct(sem / base_sem - 1)})",
                "Ver Full/estoque, preço, posição e se o ads foi mexido",
                base_sem - sem))
        elif sem > ALTA_VENDA * base_sem:
            out.append(_sinal(
                loja, 'VENDAS', anuncio, a['skus'],
                f"Alta: semana {periodo} {_brl(sem)} × média {_brl(base_sem)}/sem "
                f"({_pct(sem / base_sem - 1)})",
                "Garantir Full para o novo ritmo; campanha não travar no orçamento",
                sem - base_sem))
    return out


def _ponte_mapeada(ponte, mapa):
    """[(loja, anuncio, estoque_id, sku, em_full)] com o SKU corrigido
    (dim_sku_mapeamento, um salto, igual às vendas)."""
    return [(l, a, e, mapa.get(s, s) if s else s, bool(f)) for l, a, e, s, f in ponte]


def estoques(ponte):
    """{(loja, estoque_id): {'anuncios': set, 'skus': set, 'em_full': bool}}"""
    out = {}
    for loja, anuncio, eid, sku, em_full in ponte:
        e = out.setdefault((loja, eid), {'anuncios': set(), 'skus': set(), 'em_full': False})
        e['anuncios'].add(anuncio)
        if sku:
            e['skus'].add(sku)
        e['em_full'] |= em_full
    return out


def sinais_full(foto, ponte, por_sku, ads_por_anuncio, j):
    """Cobertura do Full POR estoque_id. foto: [(loja, eid, data, full, transf,
    recebidas)]; ponte já mapeada."""
    est = estoques(ponte)
    out = []
    for loja, eid, data, full, transf, recebidas in foto:
        e = est.get((loja, eid))
        if not e or not e['em_full']:
            continue
        # venda do estoque: cada anúncio dele × cada SKU dele (mesmo produto)
        vend = {}
        for a in e['anuncios']:
            for s in e['skus']:
                v = por_sku.get((loja, a, s))
                if v:
                    vend[a] = vend.get(a, {'qtd_ritmo': 0.0, 'qtd_30': 0.0, 'rec_30': 0.0})
                    for k in vend[a]:
                        vend[a][k] += v[k]
        qtd_ritmo = sum(v['qtd_ritmo'] for v in vend.values())
        qtd_30 = sum(v['qtd_30'] for v in vend.values())
        rec_30 = sum(v['rec_30'] for v in vend.values())
        dia = max(qtd_ritmo / JANELA_RITMO_FULL, qtd_30 / JANELA_PISO_FULL)
        # o anúncio que vende (o de maior venda em 30d), senão qualquer um
        anuncio = (max(vend, key=lambda a: vend[a]['qtd_30']) if vend
                   else sorted(e['anuncios'])[0])
        ads_ontem = sum(_f(ads_por_anuncio.get((loja, a), {}).get('gasto_ontem'))
                        for a in e['anuncios'])
        full, transf, recebidas = int(full), int(transf), int(recebidas)
        if full == 0 and ads_ontem > 0:
            out.append(_sinal(
                loja, 'FULL', anuncio, e['skus'],
                f"Full zerado com ads ligado (gasto ontem {_brl(ads_ontem)}"
                + (f"; {transf} em transferência" if transf else '') + ")",
                "Baixar/pausar o ads até o Full voltar", max(rec_30, ads_ontem),
                urgente=True))
        elif dia > 0:
            dias = (full + transf) / dia
            if dias < FULL_ATENCAO_DIAS:
                urg = dias < FULL_URGENTE_DIAS
                out.append(_sinal(
                    loja, 'FULL', anuncio, e['skus'],
                    f"{dias:.0f} dias de Full ({full} disp. + {transf} em transf. ÷ "
                    f"{_dec(dia)}/dia; 7d {_dec(qtd_ritmo / JANELA_RITMO_FULL)}/dia, "
                    f"30d {_dec(qtd_30 / JANELA_PISO_FULL)}/dia)",
                    "Enviar ao Full agora" if urg else "Programar envio ao Full",
                    rec_30, urgente=urg))
        if recebidas > 0:
            out.append(_sinal(
                loja, 'FULL', anuncio, e['skus'],
                f"Envio entrou no Full em {pd.Timestamp(data):%d/%m}: +{recebidas} un. "
                f"(Full agora {full})",
                "Conferir se o ads/preço voltou ao normal", rec_30))
    return out


def dias_com_estoque(estoque_semana, ponte):
    """{(loja, anuncio): nº de dias da semana com estoque}. Anúncio Full:
    full_disponivel > 0; fora do Full: galpão > 0 (só sim/não, nunca somado)."""
    por_est = {}
    for loja, anuncio, eid, _sku, em_full in ponte:
        por_est.setdefault((loja, eid), []).append((anuncio, em_full))
    dias = {}
    for loja, eid, data, full, galpao in estoque_semana:
        for anuncio, em_full in por_est.get((loja, eid), ()):
            tem = (full or 0) > 0 if em_full else (galpao or 0) > 0
            if tem:
                dias.setdefault((loja, anuncio), set()).add(data)
    return {k: len(v) for k, v in dias.items()}


def sinais_ads(ads, config, por_anuncio, dias_estoque, j, com_estoque=True):
    """ads: {(loja, anuncio): dict}; config: linhas de SQL_CONFIG_MUDOU.
    com_estoque=False (a leitura do estoque da semana falhou): a espiral não é
    julgada, porque sem estoque não dá para separar espiral de ruptura."""
    out = []
    for loja, idc, camp, d_antes, _hoje, r0, r1, o0, o1 in config:
        anuncio = anuncio_da_campanha(idc, camp)
        a = por_anuncio.get((loja, anuncio), {})
        partes = []
        if r0 != r1:
            partes.append(f"ROAS objetivo {_f(r0):.1f} → {_f(r1):.1f}".replace('.', ','))
        if o0 != o1:
            partes.append(f"orçamento {_brl(_f(o0))} → {_brl(_f(o1))}/dia")
        out.append(_sinal(
            loja, 'ADS', anuncio or camp, a.get('skus'),
            "Mudou ontem: " + '; '.join(partes) + f" (foto de {pd.Timestamp(d_antes):%d/%m} → hoje)",
            "Acompanhar venda e TACOS nos próximos 3 dias", a.get('rec_30', 0.0)))

    semana = 7
    for (loja, anuncio), d in ads.items():
        a = por_anuncio.get((loja, anuncio), {'rec_sem': 0.0, 'rec_base': 0.0,
                                               'rec_30': 0.0, 'skus': set(),
                                               'qtd_30': None, 'margem_30': None,
                                               'sku_unico': False})
        # gasto com zero venda de ads há N dias
        if int(d.get('dias_com_gasto') or 0) >= DIAS_GASTO_SEM_VENDA and _f(d.get('unid_recente')) == 0:
            out.append(_sinal(
                loja, 'ADS', anuncio, a['skus'],
                f"{DIAS_GASTO_SEM_VENDA} dias com gasto e zero venda de ads "
                f"({_brl(_f(d.get('gasto_recente')))})",
                "Rever ROAS objetivo ou pausar a campanha", _f(d.get('gasto_recente'))))

        # custo por venda acima da margem por unidade (só SKU único)
        unid, gasto, cliques = _f(d.get('unid_ads_30')), _f(d.get('gasto_30')), _f(d.get('cliques_30'))
        if (a.get('sku_unico') and a.get('qtd_30') and a.get('margem_30') is not None
                and unid >= MIN_UNID_ADS_CUSTO):
            custo_un = gasto / unid
            margem_un = a['margem_30'] / a['qtd_30']
            if custo_un > margem_un:
                cpc = gasto / cliques if cliques else None
                conv = unid / cliques if cliques else None
                txt = (f"Custo por venda de ads {_brl(custo_un)}/un"
                       + (f" (CPC {_brl(cpc)} ÷ conversão {conv * 100:.1f}%)".replace('.', ',')
                          if cpc is not None else '')
                       + f" × margem {_brl(margem_un)}/un (30d)")
                out.append(_sinal(loja, 'ADS', anuncio, a['skus'], txt,
                                  "Subir ROAS objetivo (cada venda de ads dá prejuízo)",
                                  (custo_un - margem_un) * unid))

        # espiral: gasto cortado E venda caindo — só com estoque a semana toda
        gasto_base_sem = _f(d.get('gasto_base')) / SEMANAS_BASE
        rec_base_sem = a['rec_base'] / SEMANAS_BASE
        if (com_estoque and gasto_base_sem > 0 and _rel(a)
                and _f(d.get('gasto_sem')) < ESPIRAL_QUEDA_GASTO * gasto_base_sem
                and a['rec_sem'] < ESPIRAL_QUEDA_VENDA * rec_base_sem):
            tacos_sem = _f(d.get('gasto_sem')) / a['rec_sem'] if a['rec_sem'] else None
            tacos_base = gasto_base_sem / rec_base_sem if rec_base_sem else None
            dias = dias_estoque.get((loja, anuncio), 0)
            queda = rec_base_sem - a['rec_sem']
            if dias >= semana:
                out.append(_sinal(
                    loja, 'ADS', anuncio, a['skus'],
                    f"Espiral: gasto de ads {_brl(_f(d.get('gasto_sem')))} × média "
                    f"{_brl(gasto_base_sem)}/sem e venda {_pct(a['rec_sem'] / rec_base_sem - 1)}"
                    + (f"; TACOS {tacos_sem * 100:.1f}% (era {tacos_base * 100:.1f}%)".replace('.', ',')
                       if tacos_sem is not None and tacos_base is not None else ''),
                    "Com estoque a semana toda: devolver o investimento aos poucos (+20–30%/ciclo)",
                    queda))
            else:
                out.append(_sinal(
                    loja, 'FULL', anuncio, a['skus'],
                    f"Ruptura: estoque em só {dias} de 7 dias da semana; venda "
                    f"{_pct(a['rec_sem'] / rec_base_sem - 1)} e ads cortado — a causa é estoque",
                    "Repor estoque antes de mexer no ads", queda, urgente=True))
    return out


def familias(ponte, comp):
    """{(loja, anuncio): set(peças)} pela composição (kit → peças; o resto é
    ele mesmo)."""
    out = {}
    for loja, anuncio, _eid, sku, _f_ in ponte:
        if sku:
            out.setdefault((loja, anuncio), set()).update(p for p, _q in ep.pecas_do_sku(sku, comp))
    return out


def sinais_experiencia(saude, ponte, comp, por_anuncio):
    """saude: [(loja, anuncio, data, valor, texto, valor_ref, texto_ref)]."""
    fam = familias(ponte, comp)
    nota = {(l, a): (v, t, vr, tr) for l, a, _d, v, t, vr, tr in saude}

    def piorou(k):
        v, _t, vr, _tr = nota[k]
        return v is not None and vr is not None and float(v) < float(vr)

    # membros de cada família (mesma loja, mesma peça) que têm nota
    membros = {}
    for k in nota:
        for peca in fam.get(k, ()):
            membros.setdefault((k[0], peca), set()).add(k)

    def principal(loja, peca, ks):
        # o anúncio da própria peça; senão o de maior receita
        proprios = [k for k in ks if por_anuncio.get(k, {}).get('skus') == {peca}]
        cand = proprios or list(ks)
        return max(cand, key=lambda k: (por_anuncio.get(k, {}).get('rec_30', 0.0), k[1]))

    def texto_familia(loja, peca):
        ks = membros[(loja, peca)]
        if len(ks) < 2:
            return ''
        distintas = sorted({float(nota[k][0]) for k in ks if nota[k][0] is not None})
        p = principal(loja, peca, ks)
        v_p = nota[p][0]
        return (f"{peca}: {len(ks)} anúncios, notas {', '.join(f'{n:.0f}' for n in distintas)}"
                f" · principal {p[1]} em {'—' if v_p is None else f'{float(v_p):.0f}'}")

    out, cobertos = [], set()
    for (loja, peca), ks in sorted(membros.items(), key=lambda x: -len(x[1])):
        pioraram = sorted(k for k in ks if piorou(k) and k not in cobertos)
        if len(pioraram) >= MIN_ANUNCIOS_FAMILIA:
            cobertos |= set(pioraram)
            em_jogo = sum(por_anuncio.get(k, {}).get('rec_30', 0.0) for k in pioraram)
            de_para = '; '.join(f"{k[1]} {float(nota[k][2]):.0f}→{float(nota[k][0]):.0f}"
                                for k in pioraram)
            out.append(_sinal(
                loja, 'EXPERIÊNCIA', ', '.join(k[1] for k in pioraram), {peca},
                f"{len(pioraram)} anúncios da mesma peça pioraram em "
                f"{DIAS_REF_EXPERIENCIA} dias: {de_para}",
                "Ver o que muda entre os anúncios (kit, embalagem, reclamação, prazo)",
                em_jogo, familia=texto_familia(loja, peca)))
    for k in sorted(nota):
        if k in cobertos or not piorou(k):
            continue
        v, t, vr, tr = nota[k]
        a = por_anuncio.get(k, {})
        textos = [texto_familia(k[0], p) for p in sorted(fam.get(k, ()))
                  if (k[0], p) in membros]
        out.append(_sinal(
            k[0], 'EXPERIÊNCIA', k[1], a.get('skus') or set(),
            f"Experiência de compra {float(vr):.0f} ({tr or '—'}) → {float(v):.0f} ({t or '—'}) "
            f"em {DIAS_REF_EXPERIENCIA} dias",
            "Abrir o anúncio no ML e ver a ação principal sugerida",
            a.get('rec_30', 0.0), familia=' | '.join(x for x in textos if x)))
    return out


def sinais_visitas(visitas, por_anuncio, j):
    out = []
    for loja, anuncio, vs, vb in visitas:
        a = por_anuncio.get((loja, anuncio))
        if not a or not _rel(a):
            continue
        base_sem = _f(vb) / SEMANAS_BASE
        sem = _f(vs)
        if base_sem <= 0:
            continue
        if sem < QUEDA_VENDA * base_sem:
            txt, sug = "Queda de visitas", "Ver posição, preço, ads e se o anúncio foi pausado"
        elif sem > ALTA_VENDA * base_sem:
            txt, sug = "Alta de visitas", "Ver se a conversão acompanha (preço, estoque)"
        else:
            continue
        out.append(_sinal(
            loja, 'VISITAS', anuncio, a['skus'],
            f"{txt}: semana {sem:.0f} × média {base_sem:.0f}/sem ({_pct(sem / base_sem - 1)})",
            sug, a['rec_base'] / SEMANAS_BASE))
    return out


def ads_por_anuncio(linhas):
    cols = ('gasto_sem', 'gasto_base', 'gasto_ontem', 'gasto_recente', 'dias_com_gasto',
            'unid_recente', 'gasto_30', 'unid_ads_30', 'cliques_30')
    return {(r[0], r[1]): dict(zip(cols, r[2:])) for r in linhas}


def montar_sinais(dados, hoje):
    """Lista de sinais (dicts) de todas as lojas, a partir do que ler_tudo leu.
    Fonte que não veio (erro) só deixa de gerar os seus sinais."""
    j = janelas(hoje)
    mapa = dados.get('mapa') or {}
    comp = ep.agrupar_composicao(dados.get('composicao') or [])
    por_anuncio, por_sku = agregar_vendas(dados.get('vendas') or [])
    ponte = _ponte_mapeada(dados.get('ponte') or [], mapa)
    ads = ads_por_anuncio(dados.get('ads') or [])

    sinais = []
    if 'vendas' in dados:
        sinais += sinais_vendas(por_anuncio, j)
    if 'foto' in dados and 'ponte' in dados and 'vendas' in dados:
        sinais += sinais_full(dados['foto'], ponte, por_sku, ads, j)
    if 'ads' in dados or 'config' in dados:
        dias_est = dias_com_estoque(dados.get('estoque_semana') or [], ponte)
        sinais += sinais_ads(ads, dados.get('config') or [], por_anuncio, dias_est, j,
                             com_estoque='estoque_semana' in dados and 'ponte' in dados)
    if 'experiencia' in dados:
        sinais += sinais_experiencia(dados['experiencia'], ponte, comp, por_anuncio)
    if dados.get('visitas'):
        sinais += sinais_visitas(dados['visitas'], por_anuncio, j)

    # Espiral/ruptura explicam a queda: o sinal genérico de VENDAS do mesmo
    # anúncio sai, para não ocupar duas linhas das 10.
    explicados = {(s['loja'], s['anuncio']) for s in sinais
                  if s['numero'].startswith(('Espiral', 'Ruptura'))}
    return [s for s in sinais
            if not (s['tipo'] == 'VENDAS' and s['numero'].startswith('Queda')
                    and (s['loja'], s['anuncio']) in explicados)]


def sinais_da_loja(sinais, loja, limite=MAX_SINAIS_POR_LOJA):
    """(os `limite` primeiros por R$ em jogo, o resto). Urgente desempata."""
    da_loja = sorted((s for s in sinais if s['loja'] == loja),
                     key=lambda s: (-s['em_jogo'], not s['urgente'], s['tipo'], s['anuncio']))
    return da_loja[:limite], da_loja[limite:]


def resumo_lojas(linhas_resumo, metas, lojas, hoje):
    j = janelas(hoje)
    por = {r[0]: r for r in linhas_resumo}
    meta = {l: _f(m) for l, m in metas}
    import calendar
    dias_mes = calendar.monthrange(j['ontem'].year, j['ontem'].month)[1]
    out = {}
    for loja in lojas:
        _l, ontem, mesmo_dia, mes = por.get(loja, (loja, None, None, None))
        media = _f(mesmo_dia) / SEMANAS_BASE
        m = meta.get(loja)
        out[loja] = {
            'ontem': _f(ontem),
            'media_mesmo_dia': media,
            'var_ontem': (_f(ontem) / media - 1) if media > 0 else None,
            'mes': _f(mes),
            'meta': m if m else None,
            'pct_meta': (_f(mes) / m) if m else None,
            'esperado_linear': (m * j['ontem'].day / dias_mes) if m else None,
        }
    return out


def avaliar_frescor(linhas, lojas, agora):
    """[(nível, texto)] por loja: 'atrasado' quando a fonte não chegou hoje e já
    passou da HORA_COLETA_COMPLETA; 'cedo' antes disso. Fonte sem nenhum dado
    em 10 dias (ex.: loja sem ads) não acusa atraso."""
    ult = {(l, f): u for f, l, u in linhas}
    out = {}
    for loja in lojas:
        avisos = []
        for fonte in FONTES_FRESCOR:
            u = ult.get((loja, fonte))
            if u is None:
                continue
            if pd.Timestamp(u).date() < agora.date():
                nivel = 'atrasado' if agora.hour >= HORA_COLETA_COMPLETA else 'cedo'
                avisos.append((nivel, f"{fonte}: último dado {pd.Timestamp(u):%d/%m %H:%M}"))
        out[loja] = avisos
    return out


# ============================================================
# PERMISSÃO
# ============================================================

def restricao_de_lojas(engine):
    """None = vê todas as lojas; lista = só estas. Lista VAZIA = usuário
    restrito sem loja cadastrada: NÃO vê nada (o bug conhecido do card aberto,
    em que lista vazia vira "sem restrição", não se repete aqui)."""
    from permissoes import ve_todas_lojas, get_lojas_usuario
    if ve_todas_lojas():
        return None
    return list(get_lojas_usuario(engine) or [])


def lojas_visiveis(lojas_ml, restricao):
    return list(lojas_ml) if restricao is None else [l for l in lojas_ml if l in restricao]


# ============================================================
# TELA
# ============================================================

ROTULO_FONTE = {
    'resumo': 'resumo de vendas', 'metas': 'metas', 'vendas': 'vendas por anúncio',
    'ponte': 'anúncios × estoque', 'foto': 'foto do Full', 'estoque_semana': 'estoque da semana',
    'ads': 'ads', 'config': 'mudanças de ROAS/orçamento', 'experiencia': 'experiência de compra',
    'frescor': 'datas de chegada', 'composicao': 'composição de kits', 'visitas': 'visitas',
}

ICONE = {'VENDAS': '📉', 'FULL': '📦', 'ADS': '📣', 'EXPERIÊNCIA': '⭐', 'VISITAS': '👀'}


def tabela_sinais(sinais, nomes):
    def produto(skus):
        return ' / '.join(nomes.get(s) or '(sem cadastro)' for s in skus) if skus else ''
    return pd.DataFrame({
        'Loja': [s['loja'] for s in sinais],
        'Tipo': [('🔴 ' if s['urgente'] else '') + ICONE.get(s['tipo'], '') + ' ' + s['tipo']
                 for s in sinais],
        'SKU': [', '.join(s['skus']) for s in sinais],
        'Produto': [produto(s['skus']) for s in sinais],
        'Anúncio': [s['anuncio'] for s in sinais],
        'O que disparou': [s['numero'] for s in sinais],
        'Sugestão': [s['sugestao'] for s in sinais],
        'R$ em jogo': [_brl(s['em_jogo']) for s in sinais],
        'Família (mesma peça, mesma loja)': [s['familia'] for s in sinais],
    })


def _render_resumo(st, loja, r, ontem):
    c1, c2, c3 = st.columns(3)
    c1.metric(f"Venda de ontem ({ontem:%d/%m}) · parcial", _brl(r['ontem']),
              None if r['var_ontem'] is None else _pct(r['var_ontem']) + " vs mesmo dia da semana",
              help=f"Comparada com a média dos {SEMANAS_BASE} mesmos dias da semana "
                   f"anteriores ({_brl(r['media_mesmo_dia'])}). Parcial: a API ainda "
                   "completa D-1 e D-2.")
    c2.metric("Mês até ontem · parcial", _brl(r['mes']))
    if r['meta']:
        c3.metric("Meta do mês", _brl(r['meta']),
                  f"{r['pct_meta'] * 100:.0f}% atingido · esperado linear "
                  f"{_brl(r['esperado_linear'])}", delta_color='off')
    else:
        c3.metric("Meta do mês", "não cadastrada", help="Cadastrar na aba Performance.")


def _render_mercado_livre(st, engine):
    agora = datetime.now(BRT)
    hoje = agora.date()

    restricao = restricao_de_lojas(engine)
    if restricao is not None and not restricao:
        st.caption("Nenhuma loja atribuída ao seu perfil.")
        return

    conn = engine.raw_connection()
    try:
        try:
            lojas_ml = [r[0] for r in _ler(conn, SQL_LOJAS_ML, {'marketplace': MARKETPLACE})]
        except Exception:  # noqa: BLE001
            st.error("Não consegui ler a lista de lojas agora. Tente de novo em instantes.")
            return
        lojas = lojas_visiveis(lojas_ml, restricao)
        if not lojas:
            st.caption("Nenhuma loja do Mercado Livre atribuída ao seu perfil.")
            return

        dados, erros = ler_tudo(conn, hoje, lojas)
        parciais = []
        try:
            parciais = [p for p in ep.fotos_parciais(ep.ler_contagens(conn)) if p[0] in lojas]
        except Exception:  # noqa: BLE001
            pass
        sinais = montar_sinais(dados, hoje)
        skus = {s for x in sinais for s in x['skus']}
        try:
            nomes = ep.ler_nomes(conn, skus)
        except Exception:  # noqa: BLE001
            nomes = {}
    finally:
        conn.close()

    j = janelas(hoje)
    st.caption(
        f"Dado de {j['ontem']:%d/%m} (ontem). Sinais de venda comparam a semana "
        f"{j['sem_ini']:%d/%m}–{j['fim']:%d/%m} com a média semanal de "
        f"{j['base_ini']:%d/%m}–{j['base_fim']:%d/%m}: os 2 últimos dias ficam fora porque a "
        "venda da API ainda entra depois.")
    if erros:
        st.error("Não consegui ler agora: " + ', '.join(ROTULO_FONTE.get(k, k) for k in erros)
                 + ". Os sinais dessas fontes ficaram de fora; o resto está abaixo.")
    if dados.get('visitas') is None and 'visitas' not in erros:
        st.info("👀 Visitas: coleta de visitas em construção.")

    frescor = avaliar_frescor(dados.get('frescor') or [], lojas, agora)
    resumo = resumo_lojas(dados.get('resumo') or [], dados.get('metas') or [], lojas, hoje)
    datas_foto = {r[0]: r[2] for r in dados.get('foto') or []}
    atrasadas = set(ep.lojas_atrasadas(datas_foto, hoje, agora))

    for loja in lojas:
        st.markdown("---")
        st.subheader(loja)
        avisos = frescor.get(loja, [])
        atr = [t for n, t in avisos if n == 'atrasado']
        cedo = [t for n, t in avisos if n == 'cedo']
        if atr:
            st.warning("⚠️ O coletor de hoje ainda não trouxe: " + '; '.join(atr))
        elif cedo:
            st.caption("⏳ O coletor do dia pode ainda não ter rodado: " + '; '.join(cedo))
        if loja in atrasadas:
            st.warning(f"⚠️ Foto do estoque atrasada: {pd.Timestamp(datas_foto[loja]):%d/%m}.")
        for l, n_ult, ult, n_ant, ant in parciais:
            if l == loja:
                st.warning(f"⚠️ Foto do estoque possivelmente PARCIAL: {n_ult} estoques em "
                           f"{pd.Timestamp(ult):%d/%m} contra {n_ant} em {pd.Timestamp(ant):%d/%m}.")

        if 'resumo' in erros:
            st.error("Resumo indisponível agora.")
        else:
            _render_resumo(st, loja, resumo[loja], j['ontem'])

        top, resto = sinais_da_loja(sinais, loja)
        if not top:
            st.success("Nenhum sinal hoje.")
            continue
        st.dataframe(tabela_sinais(top, nomes), use_container_width=True, hide_index=True)
        if resto:
            with st.expander(f"Mais {len(resto)} sinais (menor R$ em jogo)"):
                st.dataframe(tabela_sinais(resto, nomes), use_container_width=True,
                             hide_index=True)


def render(engine):
    import streamlit as st

    st.title("📡 Sinais do Dia")
    st.caption("Painel diário: o que mudou ontem em cada loja, ordenado pelo R$ em jogo. "
               "Só leitura; os limiares estão em calibragem.")
    aba_ml, aba_shopee = st.tabs(["Mercado Livre", "Shopee"])
    with aba_ml:
        try:
            _render_mercado_livre(st, engine)
        except Exception:  # noqa: BLE001
            st.error("Os sinais do Mercado Livre não carregaram agora. Tente de novo em "
                     "instantes; se persistir, avise o time do sistema.")
    with aba_shopee:
        st.info("Shopee: em seguida (v2).")
