"""
KITS — COMPOSIÇÃO kit → peça × quantidade (frente [KITS], 1ª entrega, 30/09/2026)

Aba "🧩 Kits" de Gestão de SKUs. Carrega o export de kits do UpSeller para
dim_kit_composicao (sql/kits_composicao.sql).

REGRAS (plano aprovado pelo Mestre em 30/09/2026)
  - FONTE ÚNICA da composição: o export de kits do UpSeller (colunas KIT SKU /
    SKU de Produto / Qtd. SKU de Produto, uma linha por peça). Nada mais grava
    dim_kit_composicao.
  - Carga INCREMENTAL com prévia: kit novo entra, composição alterada é
    atualizada (e a peça que saiu do kit é apagada), kit que está no banco e não
    veio no arquivo NÃO é apagado, só listado.
  - SKU comparado com o cadastro (dim_produtos) EXATO, sem upper(): 9 SKUs do
    cadastro têm minúscula (k3-L-0426...). Do arquivo só sai o que o Excel
    põe a mais: tab, espaço inquebrável e espaço nas pontas.
  - Composição sempre do arquivo, nunca do nome (K3-L-0359 = 3× L-0358).
  - Kit ou peça sem cadastro: o kit INTEIRO vai para
    dim_kit_composicao_pendente (não entra pela metade). Se o kit já tem
    composição em vigor, ela continua valendo até a pendência resolver.
  - Kit dentro de kit é recusado.
  - Não mexe em custo, margem nem venda.

A gravação e a prévia usam a MESMA função (planejar), e a gravação refaz o
plano dentro da transação: se o banco mudou desde a prévia, nada é gravado.
"""

import re
import unicodedata

import pandas as pd

# Cabeçalhos do export do UpSeller, já normalizados (_chave_cabecalho).
COLUNAS_UPSELLER = {
    'kit': 'kit sku',
    'peca': 'sku de produto',
    'qtd': 'qtd. sku de produto',
}

# O que o Excel costuma trazer dentro da célula e o btrim do banco não tira.
_ESPACOS_EXCEL = re.compile(r'[\t\r\n   ]')


# ============================================================
# LEITURA DO ARQUIVO (sem banco)
# ============================================================

def normalizar_sku(valor):
    """SKU como texto limpo, ou None se vazio. Não muda maiúscula/minúscula."""
    if valor is None:
        return None
    if isinstance(valor, float):
        if valor != valor:          # NaN
            return None
        if valor.is_integer():      # SKU numérico lido como 123.0
            valor = int(valor)
    texto = _ESPACOS_EXCEL.sub(' ', str(valor)).strip()
    if texto == '' or texto.lower() in ('nan', 'none'):
        return None
    return texto


def _quantidade(valor):
    """Inteiro > 0, ou None se não for."""
    if valor is None:
        return None
    try:
        numero = float(str(valor).strip().replace(',', '.'))
    except (TypeError, ValueError):
        return None
    if numero != numero or not numero.is_integer() or numero <= 0:
        return None
    return int(numero)


def _chave_cabecalho(texto):
    sem_acento = unicodedata.normalize('NFKD', str(texto))
    sem_acento = ''.join(c for c in sem_acento if not unicodedata.combining(c))
    return ' '.join(_ESPACOS_EXCEL.sub(' ', sem_acento).lower().split())


def ler_export_upseller(df):
    """
    DataFrame do export (lido com dtype=object) → (linhas, erro).

    linhas: [{'linha', 'kit', 'peca', 'qtd'}] com SKU normalizado e qtd já
    convertida (None quando inválida). erro: texto quando falta coluna.
    """
    por_chave = {_chave_cabecalho(c): c for c in df.columns}
    faltando = [nome for nome in COLUNAS_UPSELLER.values() if nome not in por_chave]
    if faltando:
        return [], ("O arquivo não parece o export de kits do UpSeller: faltam as "
                    "colunas " + ', '.join(f'"{f}"' for f in faltando) + '.')
    col = {k: por_chave[v] for k, v in COLUNAS_UPSELLER.items()}
    linhas = []
    for idx, row in df.iterrows():
        kit = normalizar_sku(row[col['kit']])
        peca = normalizar_sku(row[col['peca']])
        bruto = row[col['qtd']]
        if kit is None and peca is None and _quantidade(bruto) is None:
            continue                        # linha em branco
        linhas.append({'linha': idx + 2, 'kit': kit, 'peca': peca,
                       'qtd': _quantidade(bruto), 'qtd_bruta': bruto})
    return linhas, None


def montar_kits(linhas):
    """
    Linhas → (kits, recusados).

    kits: {kit: {peca: qtd}} só dos kits sem erro de arquivo.
    recusados: {kit: [motivos]}; o kit inteiro sai, nunca pela metade.
    """
    kits, recusados = {}, {}

    def _recusar(kit, motivo):
        recusados.setdefault(kit, []).append(motivo)

    for ln in linhas:
        kit, peca, qtd = ln['kit'], ln['peca'], ln['qtd']
        if kit is None:
            _recusar(f"(linha {ln['linha']})", 'linha sem KIT SKU')
            continue
        if peca is None:
            _recusar(kit, f"linha {ln['linha']}: sem SKU de Produto")
            continue
        if qtd is None:
            _recusar(kit, f"linha {ln['linha']}: quantidade inválida "
                          f"({ln['qtd_bruta']!r}) para {peca}")
            continue
        if kit == peca:
            _recusar(kit, f"linha {ln['linha']}: o kit aparece como peça dele mesmo")
            continue
        comp = kits.setdefault(kit, {})
        if peca in comp and comp[peca] != qtd:
            _recusar(kit, f"{peca} aparece duas vezes com quantidades "
                          f"diferentes ({comp[peca]} e {qtd})")
            continue
        comp[peca] = qtd

    for kit in recusados:
        kits.pop(kit, None)
    return kits, recusados


def texto_composicao(comp):
    """{'L-0320': 2, 'L-0321': 1} → '2× L-0320 + 1× L-0321'."""
    if not comp:
        return '—'
    return ' + '.join(f'{q}× {p}' for p, q in sorted(comp.items()))


# ============================================================
# PLANO (sem banco): o que a carga faria
# ============================================================

def traduzir(kits, mapa):
    """
    Aplica as correções de dim_sku_mapeamento (sku_errado -> sku_correto) no
    kit e na peça. UM salto só, igual às vendas: A->X->B vira X.
    Só correção registrada por gente; nada é deduzido do nome.

    Devolve (kits_oficiais, traducoes, recusados): traducoes = {(original,
    oficial)}; recusados = {kit original: [motivos]} quando a correção junta
    dois kits ou duas peças num SKU só (o arquivo ficaria ambíguo).
    """
    def _um(sku):
        return mapa.get(sku, sku)

    traducoes, recusados, por_oficial = set(), {}, {}
    for kit, comp in kits.items():
        oficial = _um(kit)
        if oficial != kit:
            traducoes.add((kit, oficial))
        nova = {}
        for peca, qtd in comp.items():
            p = _um(peca)
            if p != peca:
                traducoes.add((peca, p))
            if p in nova:
                recusados.setdefault(kit, []).append(
                    f'duas peças viram o mesmo SKU {p} pela correção de SKU')
            nova[p] = qtd
        if kit not in recusados:
            por_oficial.setdefault(oficial, []).append((kit, nova))

    oficiais = {}
    for oficial, origens in por_oficial.items():
        if len(origens) > 1:
            nomes = ', '.join(sorted(k for k, _ in origens))
            for k, _ in origens:
                recusados.setdefault(k, []).append(
                    f'{nomes} viram o mesmo kit {oficial} pela correção de SKU')
            continue
        oficiais[oficial] = origens[0][1]
    return oficiais, traducoes, recusados


def planejar(kits, cadastro, atual, pendentes_atuais, recusados_arquivo=None,
             mapa=None):
    """
    kits: {kit: {peca: qtd}} a carregar, com o SKU como veio do arquivo.
    cadastro: set dos SKUs de dim_produtos (comparação EXATA).
    atual: {kit: {peca: qtd}} em vigor em dim_kit_composicao.
    pendentes_atuais: {kit: {peca: qtd}} em dim_kit_composicao_pendente.
    mapa: {sku_errado: sku_correto} de dim_sku_mapeamento (um salto só).

    Devolve um dict só com listas ordenadas e tuplas (comparável: a gravação
    refaz o plano e compara com o da prévia).
    """
    mapa = mapa or {}
    recusados = {k: list(v) for k, v in (recusados_arquivo or {}).items()}
    kits_brutos = kits
    kits, traducoes, recusados_trad = traduzir(kits, mapa)
    for k, motivos in recusados_trad.items():
        recusados.setdefault(k, []).extend(motivos)
    avisos = {}
    for original, oficial in traducoes:
        aviso = []
        if oficial in mapa:
            aviso.append(f'{oficial} também tem correção ({mapa[oficial]}): '
                         'aplicado um salto só')
        if oficial not in cadastro:
            aviso.append(f'{oficial} não está no cadastro')
        avisos[(original, oficial)] = '; '.join(aviso)

    # Pendência guardada com o SKU de antes da correção: lida pelo oficial.
    pend_oficial = {mapa.get(k, k): k for k in pendentes_atuais}
    # Kit à espera de cadastro também é kit: não pode entrar como peça.
    kits_conhecidos = set(kits) | set(atual) | set(pend_oficial)
    # peça -> kits EM VIGOR que a contêm (fora os que o arquivo substitui)
    pai_em_vigor = {}
    for k, comp in atual.items():
        if k in kits:
            continue
        for p in comp:
            pai_em_vigor.setdefault(p, []).append(k)

    novos, alterados, iguais, pendentes = [], [], [], []
    for kit in sorted(kits):
        comp = kits[kit]
        aninhadas = sorted(p for p in comp if p in kits_conhecidos)
        if aninhadas:
            recusados.setdefault(kit, []).append(
                'kit dentro de kit: ' + ', '.join(aninhadas) + ' também é kit')
            continue
        # O de fora é que é recusado (acima). Este só pega o kit que já é peça
        # de um kit EM VIGOR: gravá-lo criaria kit dentro de kit no banco.
        if kit in pai_em_vigor:
            recusados.setdefault(kit, []).append(
                'kit dentro de kit: já é peça de ' + ', '.join(sorted(pai_em_vigor[kit])))
            continue

        pecas_sem = sorted(p for p in comp if p not in cadastro)
        kit_ok = kit in cadastro
        if not kit_ok or pecas_sem:
            pendentes.append((kit, tuple(sorted(comp.items())), kit_ok,
                              tuple(pecas_sem), kit in atual))
        elif kit not in atual:
            novos.append((kit, tuple(sorted(comp.items()))))
        elif atual[kit] != comp:
            alterados.append((kit, tuple(sorted(atual[kit].items())),
                              tuple(sorted(comp.items()))))
        else:
            iguais.append(kit)

    no_arquivo = set(kits) | set(kits_brutos) | set(recusados)
    ausentes = sorted(k for k in atual if k not in no_arquivo)
    entram = {k for k, _ in novos} | {k for k, _, _ in alterados} | set(iguais)
    kits_pend = {k for k, *_ in pendentes}
    # Saem da tabela de pendências: as que entraram, e as guardadas com o SKU
    # de antes da correção que continuam pendentes (voltam com o oficial).
    pendencias_resolvidas = sorted(k for k in pendentes_atuais
                                   if mapa.get(k, k) in entram)
    pendencias_renomeadas = sorted(k for k in pendentes_atuais
                                   if mapa.get(k, k) != k and mapa.get(k, k) in kits_pend)

    return {
        'novos': novos,
        'alterados': alterados,
        'iguais': sorted(iguais),
        'pendentes': pendentes,
        'ausentes': ausentes,
        'recusados': sorted((k, tuple(v)) for k, v in recusados.items()),
        'pendencias_resolvidas': pendencias_resolvidas,
        'pendencias_renomeadas': pendencias_renomeadas,
        'traducoes': sorted((o, f, avisos[(o, f)]) for o, f in traducoes),
    }


def tem_o_que_gravar(plano):
    return bool(plano['novos'] or plano['alterados'] or plano['pendentes']
                or plano['pendencias_resolvidas'] or plano['pendencias_renomeadas'])


# ============================================================
# BANCO — SQL com parâmetros ligados (psycopg2); nomes sem schema de
# propósito: o teste cria tabelas TEMPORÁRIAS com os mesmos nomes.
# ============================================================

SQL_TRAVAR = """
    LOCK TABLE dim_kit_composicao, dim_kit_composicao_pendente
    IN SHARE ROW EXCLUSIVE MODE
"""

SQL_CADASTRO = "SELECT sku FROM dim_produtos WHERE sku = ANY(%(skus)s)"

SQL_COMPOSICAO = "SELECT kit_sku, peca_sku, quantidade FROM dim_kit_composicao"

SQL_PENDENTES = """
    SELECT kit_sku, peca_sku, quantidade, arquivo_origem
      FROM dim_kit_composicao_pendente
"""

# Só atualiza a linha cuja quantidade mudou: a que não mudou guarda
# carregado_em, arquivo_origem e usuario de quando entrou.
SQL_GRAVAR_COMPOSICAO = """
    INSERT INTO dim_kit_composicao
        (kit_sku, peca_sku, quantidade, arquivo_origem, usuario)
    SELECT n.kit_sku, n.peca_sku, n.quantidade, n.arquivo_origem, %(usuario)s
      FROM unnest(%(kits)s::text[], %(pecas)s::text[], %(qtds)s::int[],
                  %(arquivos)s::text[])
           AS n(kit_sku, peca_sku, quantidade, arquivo_origem)
    ON CONFLICT (kit_sku, peca_sku) DO UPDATE SET
        quantidade     = EXCLUDED.quantidade,
        arquivo_origem = EXCLUDED.arquivo_origem,
        usuario        = EXCLUDED.usuario,
        atualizado_em  = now()
    WHERE dim_kit_composicao.quantidade IS DISTINCT FROM EXCLUDED.quantidade
"""

# Peça que saiu de um kit alterado. Só alcança os kits desta carga.
SQL_APAGAR_PECAS_QUE_SAIRAM = """
    DELETE FROM dim_kit_composicao c
     WHERE c.kit_sku = ANY(%(kits_da_carga)s)
       AND NOT EXISTS (
           SELECT 1
             FROM unnest(%(kits)s::text[], %(pecas)s::text[]) AS n(kit_sku, peca_sku)
            WHERE n.kit_sku = c.kit_sku AND n.peca_sku = c.peca_sku)
"""

# Pendência que resolveu: o kit inteiro sai da tabela.
SQL_APAGAR_PENDENTES = """
    DELETE FROM dim_kit_composicao_pendente WHERE kit_sku = ANY(%(kits)s)
"""

# Upsert: a pendência que continua guarda registrado_em (o "Desde" da tela).
SQL_GRAVAR_PENDENTES = """
    INSERT INTO dim_kit_composicao_pendente
        (kit_sku, peca_sku, quantidade, kit_cadastrado, peca_cadastrada,
         arquivo_origem, usuario)
    SELECT n.kit_sku, n.peca_sku, n.quantidade, n.kit_cadastrado,
           n.peca_cadastrada, n.arquivo_origem, %(usuario)s
      FROM unnest(%(kits)s::text[], %(pecas)s::text[], %(qtds)s::int[],
                  %(kit_cad)s::boolean[], %(peca_cad)s::boolean[],
                  %(arquivos)s::text[])
           AS n(kit_sku, peca_sku, quantidade, kit_cadastrado, peca_cadastrada,
                arquivo_origem)
    ON CONFLICT (kit_sku, peca_sku) DO UPDATE SET
        quantidade      = EXCLUDED.quantidade,
        kit_cadastrado  = EXCLUDED.kit_cadastrado,
        peca_cadastrada = EXCLUDED.peca_cadastrada,
        arquivo_origem  = EXCLUDED.arquivo_origem,
        usuario         = EXCLUDED.usuario
"""

# Peça que saiu da composição de um kit pendente (o arquivo novo mudou o kit).
SQL_APAGAR_PECAS_PENDENTES_QUE_SAIRAM = """
    DELETE FROM dim_kit_composicao_pendente c
     WHERE c.kit_sku = ANY(%(kits_pendentes)s)
       AND NOT EXISTS (
           SELECT 1
             FROM unnest(%(kits)s::text[], %(pecas)s::text[]) AS n(kit_sku, peca_sku)
            WHERE n.kit_sku = c.kit_sku AND n.peca_sku = c.peca_sku)
"""

# Correções de SKU registradas por gente (mesma tabela das vendas).
SQL_MAPEAMENTO = "SELECT sku_errado, sku_correto FROM dim_sku_mapeamento"

SQL_MAPEAMENTO_DE = """
    SELECT sku_correto FROM dim_sku_mapeamento WHERE sku_errado = %(sku_errado)s
"""

# DO NOTHING de propósito: dim_sku_mapeamento também é lida pelo coletor de
# vendas e pela aba Vendas Pendentes; correção existente nunca é trocada daqui.
SQL_GRAVAR_MAPEAMENTO = """
    INSERT INTO dim_sku_mapeamento (sku_errado, sku_correto, data_criacao)
    VALUES (%(sku_errado)s, %(sku_correto)s, now())
    ON CONFLICT (sku_errado) DO NOTHING
"""

# Venda dos últimos 90 dias: snapshot (venda oficial) + vendas ainda
# pendentes (SKU sem cadastro, fora do snapshot). Reprocessada não conta de
# novo: já está no snapshot.
SQL_VENDAS_90D = """
    SELECT sku, SUM(receita) AS receita, SUM(qtd) AS qtd
      FROM (SELECT sku, valor_venda_efetivo AS receita, quantidade AS qtd
              FROM fact_vendas_snapshot
             WHERE sku = ANY(%(skus)s) AND data_venda >= CURRENT_DATE - 90
            UNION ALL
            SELECT sku, valor_venda_efetivo, quantidade
              FROM fact_vendas_pendentes
             WHERE sku = ANY(%(skus)s) AND data_venda >= CURRENT_DATE - 90
               AND status = 'Pendente') v
     GROUP BY sku
"""

TODAS_AS_SQL = (SQL_TRAVAR, SQL_CADASTRO, SQL_COMPOSICAO, SQL_PENDENTES,
                SQL_GRAVAR_COMPOSICAO, SQL_APAGAR_PECAS_QUE_SAIRAM,
                SQL_APAGAR_PENDENTES, SQL_GRAVAR_PENDENTES,
                SQL_APAGAR_PECAS_PENDENTES_QUE_SAIRAM, SQL_MAPEAMENTO,
                SQL_MAPEAMENTO_DE, SQL_GRAVAR_MAPEAMENTO, SQL_VENDAS_90D)


class PreviaDesatualizada(Exception):
    """O banco mudou entre a prévia e o clique em gravar."""


def _agrupar(linhas):
    grupo = {}
    for kit, peca, qtd in linhas:
        grupo.setdefault(kit, {})[peca] = int(qtd)
    return grupo


def ler_estado(cur, kits):
    """
    (cadastro, atual, pendentes_atuais, arquivo_por_pendente, mapa) do banco.
    O cadastro cobre o SKU do arquivo e o SKU para onde a correção aponta.
    """
    cur.execute(SQL_COMPOSICAO)
    atual = _agrupar(cur.fetchall())
    cur.execute(SQL_PENDENTES)
    rows = cur.fetchall()
    pendentes_atuais = _agrupar((k, p, q) for k, p, q, _ in rows)
    arquivo_pendente = {k: a for k, _, _, a in rows}
    cur.execute(SQL_MAPEAMENTO)
    mapa = {a: b for a, b in cur.fetchall()}
    skus = set(kits) | {p for c in kits.values() for p in c}
    skus |= {mapa[s] for s in skus if s in mapa}
    cadastro = set()
    if skus:
        cur.execute(SQL_CADASTRO, {'skus': sorted(skus)})
        cadastro = {r[0] for r in cur.fetchall()}
    return cadastro, atual, pendentes_atuais, arquivo_pendente, mapa


def _aplicar(cur, plano, arquivo_por_kit, usuario):
    """
    Grava o plano. Chamar dentro da transação, depois de SQL_TRAVAR.
    arquivo_por_kit: {kit oficial: arquivo}.
    """
    entram = [(k, dict(c)) for k, c in plano['novos']]
    entram += [(k, dict(depois)) for k, _, depois in plano['alterados']]
    linhas = [(k, p, q, arquivo_por_kit[k]) for k, c in entram for p, q in sorted(c.items())]
    if linhas:
        kits, pecas, qtds, arquivos = (list(x) for x in zip(*linhas))
        cur.execute(SQL_GRAVAR_COMPOSICAO, {'kits': kits, 'pecas': pecas, 'qtds': qtds,
                                            'arquivos': arquivos, 'usuario': usuario})
        cur.execute(SQL_APAGAR_PECAS_QUE_SAIRAM,
                    {'kits_da_carga': sorted({k for k, _ in entram}),
                     'kits': kits, 'pecas': pecas})

    # Pendências: as que resolveram saem inteiras; as do plano são gravadas
    # por upsert (guardam o registrado_em) e perdem a peça que saiu do kit.
    kits_pend = [k for k, *_ in plano['pendentes']]
    sair = sorted(set(plano['pendencias_resolvidas']) | set(plano['pendencias_renomeadas']))
    if sair:
        cur.execute(SQL_APAGAR_PENDENTES, {'kits': sair})
    linhas_p = []
    for kit, comp, kit_ok, pecas_sem, _ in plano['pendentes']:
        for peca, qtd in comp:
            linhas_p.append((kit, peca, qtd, kit_ok, peca not in pecas_sem,
                             arquivo_por_kit[kit]))
    if linhas_p:
        k, p, q, kc, pc, a = (list(x) for x in zip(*linhas_p))
        cur.execute(SQL_GRAVAR_PENDENTES, {'kits': k, 'pecas': p, 'qtds': q,
                                           'kit_cad': kc, 'peca_cad': pc,
                                           'arquivos': a, 'usuario': usuario})
        cur.execute(SQL_APAGAR_PECAS_PENDENTES_QUE_SAIRAM,
                    {'kits_pendentes': sorted(set(kits_pend)), 'kits': k, 'pecas': p})
    return {'novos': len(plano['novos']), 'alterados': len(plano['alterados']),
            'pendentes': len(kits_pend),
            'pendencias_resolvidas': len(plano['pendencias_resolvidas'])}


def previa(conn, kits, recusados):
    """Plano da carga, só leitura."""
    cur = conn.cursor()
    try:
        cadastro, atual, pend, _, mapa = ler_estado(cur, kits)
        return planejar(kits, cadastro, atual, pend, recusados, mapa)
    finally:
        cur.close()
        conn.rollback()


def gravar(conn, kits, recusados, arquivo, usuario, plano_da_previa):
    """
    Grava a carga numa transação só. Refaz o plano com o banco travado e
    levanta PreviaDesatualizada se ele não for o que a pessoa viu.
    """
    cur = conn.cursor()
    try:
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute(SQL_TRAVAR)
        cadastro, atual, pend, _, mapa = ler_estado(cur, kits)
        plano = planejar(kits, cadastro, atual, pend, recusados, mapa)
        if plano != plano_da_previa:
            raise PreviaDesatualizada(
                'O cadastro, a composição ou as correções de SKU mudaram desde a '
                'prévia. Nada foi gravado; confira a prévia de novo.')
        oficiais = {mapa.get(k, k) for k in kits}
        resumo = _aplicar(cur, plano, {k: arquivo for k in oficiais}, usuario)
        conn.commit()
        return resumo
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def reprocessar_pendentes(conn, usuario):
    """
    Move para dim_kit_composicao os kits pendentes cujo cadastro já foi feito
    (o kit inteiro); os que continuam pendentes têm as marcas atualizadas.
    """
    cur = conn.cursor()
    try:
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute(SQL_TRAVAR)
        cur.execute(SQL_PENDENTES)
        rows = cur.fetchall()
        kits = _agrupar((k, p, q) for k, p, q, _ in rows)
        cadastro, atual, pend, arquivo_pendente, mapa = ler_estado(cur, kits)
        plano = planejar(kits, cadastro, atual, pend, mapa=mapa)
        # Na releitura das pendências não há "ausente": o que não foi
        # reprocessado continua pendente, e composição em vigor não se mexe.
        plano['ausentes'] = []
        arquivos = {mapa.get(k, k): a for k, a in arquivo_pendente.items()}
        resumo = _aplicar(cur, plano, arquivos, usuario)
        resumo['recusados'] = plano['recusados']
        resumo['traducoes'] = plano['traducoes']
        conn.commit()
        return resumo
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def corrigir_sku(conn, sku_errado, sku_correto):
    """
    Registra sku_errado -> sku_correto em dim_sku_mapeamento (a mesma tabela
    das vendas). Devolve (gravou, mensagem). Nunca troca correção existente.

    Recusa:
      - sku_correto fora do cadastro;
      - sku_errado que ESTÁ no cadastro: a correção desviaria as vendas dele
        (o coletor e o upload aplicam a mesma tabela);
      - sku_errado que já tem correção (mostra a que existe).
    """
    errado, correto = normalizar_sku(sku_errado), normalizar_sku(sku_correto)
    if not errado or not correto:
        return False, 'Informe o SKU do UpSeller e o SKU do cadastro.'
    if errado == correto:
        return False, 'Os dois SKUs são iguais: não há o que corrigir.'
    gravou = False
    cur = conn.cursor()
    try:
        cur.execute(SQL_CADASTRO, {'skus': [errado, correto]})
        cadastrados = {r[0] for r in cur.fetchall()}
        if correto not in cadastrados:
            return False, (f'{correto} não está no cadastro. Cadastre em ⚙️ Gerenciar SKU '
                           'ou confira a grafia (maiúscula/minúscula conta).')
        if errado in cadastrados:
            return False, (f'{errado} está cadastrado: uma correção desviaria as vendas '
                           'dele para outro SKU. Nada foi gravado.')
        cur.execute(SQL_MAPEAMENTO_DE, {'sku_errado': errado})
        existe = cur.fetchone()
        if existe:
            return False, (f'{errado} já tem correção registrada: → {existe[0]}. '
                           'Ela também vale para as vendas, por isso não é trocada daqui.')
        cur.execute(SQL_MAPEAMENTO_DE, {'sku_errado': correto})
        cadeia = cur.fetchone()
        cur.execute(SQL_GRAVAR_MAPEAMENTO, {'sku_errado': errado, 'sku_correto': correto})
        if cur.rowcount == 0:       # outra sessão gravou no meio
            return False, f'{errado} acabou de ganhar correção em outra tela. Recarregue.'
        conn.commit()
        gravou = True
        aviso = (f' Atenção: {correto} também tem correção (→ {cadeia[0]}); vale um '
                 'salto só.' if cadeia else '')
        return True, (f'Correção registrada: {errado} → {correto}. Vale também para '
                      f'vendas que chegarem com {errado}.{aviso}')
    finally:
        cur.close()
        if not gravou:          # recusa, erro ou corrida: nada fica aberto
            conn.rollback()


def vendas_90d(conn, skus):
    """{sku: (receita, qtd)} dos últimos 90 dias (snapshot + pendentes)."""
    if not skus:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(SQL_VENDAS_90D, {'skus': sorted(set(skus))})
        return {s: (float(r or 0), int(q or 0)) for s, r, q in cur.fetchall()}
    finally:
        cur.close()
        conn.rollback()


# ============================================================
# TELA
# ============================================================

def _tabela(linhas, colunas):
    return pd.DataFrame(linhas, columns=colunas)


def _render_previa(plano):
    import streamlit as st

    c = st.columns(6)
    c[0].metric("Kits novos", len(plano['novos']))
    c[1].metric("Composição alterada", len(plano['alterados']))
    c[2].metric("Sem mudança", len(plano['iguais']))
    c[3].metric("Pendentes (cadastro)", len(plano['pendentes']))
    c[4].metric("No banco, fora do arquivo", len(plano['ausentes']))
    c[5].metric("Recusados", len(plano['recusados']))

    if plano['traducoes']:
        with st.expander(f"🔁 SKU corrigido pelas correções registradas "
                         f"({len(plano['traducoes'])})", expanded=True):
            st.caption("Correções de dim_sku_mapeamento (as mesmas das vendas), um "
                       "salto só. Nada é deduzido do nome.")
            st.dataframe(_tabela([(o, f, a or '—') for o, f, a in plano['traducoes']],
                                 ['No UpSeller', 'No sistema', 'Aviso']),
                         use_container_width=True, hide_index=True)

    if plano['alterados']:
        with st.expander(f"✏️ Composição alterada ({len(plano['alterados'])}) — confira",
                         expanded=True):
            st.dataframe(_tabela(
                [(k, texto_composicao(dict(a)), texto_composicao(dict(d)))
                 for k, a, d in plano['alterados']],
                ['Kit', 'Hoje', 'Passa a ser']), use_container_width=True, hide_index=True)
    if plano['novos']:
        with st.expander(f"🆕 Kits novos ({len(plano['novos'])})"):
            st.dataframe(_tabela([(k, texto_composicao(dict(c))) for k, c in plano['novos']],
                                 ['Kit', 'Composição']),
                         use_container_width=True, hide_index=True)
    if plano['pendentes']:
        with st.expander(f"⏳ Vão para Pendências de kit ({len(plano['pendentes'])}) — "
                         "falta cadastro em Gestão de SKUs"):
            st.dataframe(_tabela(
                [(k, texto_composicao(dict(c)), 'sim' if ok else 'NÃO',
                  ', '.join(sem) or '—',
                  'sim (continua valendo a de hoje)' if vigor else 'não')
                 for k, c, ok, sem, vigor in plano['pendentes']],
                ['Kit', 'Composição no arquivo', 'Kit cadastrado', 'Peças sem cadastro',
                 'Tem composição em vigor']), use_container_width=True, hide_index=True)
    if plano['recusados']:
        with st.expander(f"⛔ Recusados ({len(plano['recusados'])}) — não entram"):
            st.dataframe(_tabela([(k, '; '.join(m)) for k, m in plano['recusados']],
                                 ['Kit', 'Motivo']), use_container_width=True, hide_index=True)
    if plano['ausentes']:
        with st.expander(f"📋 No banco mas fora deste arquivo ({len(plano['ausentes'])}) "
                         "— NÃO serão apagados"):
            st.dataframe(_tabela([(k,) for k in plano['ausentes']], ['Kit']),
                         use_container_width=True, hide_index=True)


def _render_pendencias(engine, usuario):
    import streamlit as st

    st.markdown("### ⏳ Pendências de kit")
    st.caption("Kits do export que esperam cadastro do kit ou de alguma peça. "
               "Cadastre o SKU em **⚙️ Gerenciar SKU**, ou, se o SKU do UpSeller só está "
               "escrito diferente do sistema, use **Corrigir SKU** abaixo. Depois clique "
               "em Reprocessar. O kit só entra inteiro.")
    df = pd.read_sql(
        """SELECT kit_sku, peca_sku, quantidade, kit_cadastrado, peca_cadastrada,
                  arquivo_origem, registrado_em
             FROM dim_kit_composicao_pendente ORDER BY kit_sku, peca_sku""", engine)
    if df.empty:
        st.success("Nenhuma pendência.")
        return

    # Venda de 90 dias do kit: pelo SKU guardado e pelo da correção, se houver
    # (a venda chega com o SKU oficial).
    mapa = dict(pd.read_sql(SQL_MAPEAMENTO, engine).values.tolist())
    kits = sorted(df['kit_sku'].unique())
    conn = engine.raw_connection()
    try:
        vendas = vendas_90d(conn, set(kits) | {mapa[k] for k in kits if k in mapa})
    finally:
        conn.close()

    resumo, sem_cadastro = [], {}
    for kit, g in df.groupby('kit_sku', sort=True):
        falta = [] if g['kit_cadastrado'].iloc[0] else [kit]
        falta += list(g.loc[~g['peca_cadastrada'], 'peca_sku'])
        receita = sum(vendas.get(s, (0, 0))[0] for s in {kit, mapa.get(kit, kit)})
        qtd = sum(vendas.get(s, (0, 0))[1] for s in {kit, mapa.get(kit, kit)})
        for s in falta:
            sem_cadastro[s] = max(sem_cadastro.get(s, 0), receita)
        resumo.append({
            'Kit': kit,
            'Composição': texto_composicao(dict(zip(g['peca_sku'], g['quantidade']))),
            'Falta cadastrar': ', '.join(falta) or '(só reprocessar)',
            'Vendas 90d (R$)': receita,
            'Kits vendidos 90d': qtd,
            'Desde': pd.to_datetime(g['registrado_em'].min()).strftime('%d/%m/%Y'),
            'Arquivo': g['arquivo_origem'].iloc[0],
        })
    tab = pd.DataFrame(resumo).sort_values(['Vendas 90d (R$)', 'Kit'],
                                           ascending=[False, True])
    com_venda = tab[tab['Vendas 90d (R$)'] > 0]
    parados = tab[tab['Vendas 90d (R$)'] <= 0]

    def _brl(v):
        return 'R$ ' + f'{v:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    st.markdown(f"**🔥 Com venda nos últimos 90 dias ({len(com_venda)})** — resolver primeiro")
    if com_venda.empty:
        st.caption("Nenhum kit pendente vendeu nos últimos 90 dias.")
    else:
        st.dataframe(com_venda.assign(**{'Vendas 90d (R$)': com_venda['Vendas 90d (R$)'].map(_brl)}),
                     use_container_width=True, hide_index=True)
    with st.expander(f"💤 Sem venda em 90 dias ({len(parados)})"):
        st.dataframe(parados.drop(columns=['Vendas 90d (R$)', 'Kits vendidos 90d']),
                     use_container_width=True, hide_index=True)

    _render_corrigir_sku(engine, sem_cadastro, mapa)

    if st.button("🔄 Reprocessar pendências", key="kits_reprocessar"):
        conn = engine.raw_connection()
        try:
            r = reprocessar_pendentes(conn, usuario)
        except Exception as e:
            st.error(f"❌ Nada foi gravado. Erro: {e}")
            return
        finally:
            conn.close()
        st.success(f"✅ {r['novos'] + r['alterados']} kit(s) entraram na composição; "
                   f"{r['pendentes']} continuam pendentes.")
        if r['traducoes']:
            st.info("SKU corrigido: " + '; '.join(
                f"{o} → {f}" + (f" ({a})" if a else '') for o, f, a in r['traducoes']))
        if r['recusados']:
            st.warning("Recusados (continuam pendentes): "
                       + '; '.join(f"{k}: {' / '.join(m)}" for k, m in r['recusados']))


def _render_corrigir_sku(engine, sem_cadastro, mapa):
    """Padrão de Vendas Pendentes: o SKU do UpSeller aponta para o do sistema."""
    import streamlit as st

    opcoes = [s for s, _ in sorted(sem_cadastro.items(), key=lambda x: (-x[1], x[0]))
              if s not in mapa]
    with st.expander("✏️ Corrigir SKU (o UpSeller escreve diferente do sistema)"):
        st.caption("Grava em dim_sku_mapeamento, a MESMA tabela das correções de "
                   "vendas: vale para a carga de kits E para vendas que chegarem com "
                   "esse SKU. Correção existente nunca é trocada daqui.")
        if not opcoes:
            st.caption("Nenhum SKU pendente sem correção.")
            return
        with st.form("kits_corrigir_sku"):
            errado = st.selectbox("SKU como está no UpSeller (sem cadastro)", opcoes,
                                  key="kits_sku_errado")
            correto = st.text_input("SKU no sistema (tem de estar cadastrado)",
                                    key="kits_sku_correto")
            enviar = st.form_submit_button("Registrar correção")
        if enviar:
            conn = engine.raw_connection()
            try:
                ok, msg = corrigir_sku(conn, errado, correto)
            finally:
                conn.close()
            (st.success if ok else st.warning)(msg)
            if ok:
                st.info("Agora clique em 🔄 Reprocessar pendências.")


def _render_composicao(engine):
    import streamlit as st

    st.markdown("### 🔎 Composição em vigor")
    busca = st.text_input("Buscar kit ou peça (SKU exato ou parte dele)",
                          key="kits_busca")
    df = pd.read_sql(
        """SELECT kit_sku, peca_sku, quantidade, atualizado_em
             FROM dim_kit_composicao ORDER BY kit_sku, peca_sku""", engine)
    st.caption(f"{df['kit_sku'].nunique()} kits, {df['peca_sku'].nunique()} peças.")
    if busca:
        b = busca.strip()
        kits = set(df.loc[df['kit_sku'].str.contains(b, regex=False)
                          | df['peca_sku'].str.contains(b, regex=False), 'kit_sku'])
        df = df[df['kit_sku'].isin(kits)]
    linhas = [{'Kit': k,
               'Composição': texto_composicao(dict(zip(g['peca_sku'], g['quantidade']))),
               'Atualizado em': pd.to_datetime(g['atualizado_em'].max()).strftime('%d/%m/%Y')}
              for k, g in df.groupby('kit_sku', sort=True)]
    st.dataframe(pd.DataFrame(linhas), use_container_width=True, hide_index=True,
                 height=300)


def render_aba_kits(engine, is_admin):
    import streamlit as st

    st.subheader("🧩 Kits — composição (fonte: export do UpSeller)")
    # "Administrador" aqui = Admin ou Controladoria, igual às outras abas de
    # Gestão de SKUs (is_admin vem de gestao_skus.main).
    if not is_admin:
        st.warning("⚠️ Acesso restrito a administradores.")
        return
    usuario = (st.session_state.get('usuario') or {}).get('username')

    # Cada seção isolada: erro de banco numa delas vira aviso, não traceback,
    # e não esconde as outras.
    for nome, secao, args in (
            ("a carga do export", _render_carga, (engine, usuario)),
            ("as pendências de kit", _render_pendencias, (engine, usuario)),
            ("a composição em vigor", _render_composicao, (engine,))):
        try:
            secao(*args)
        except Exception as e:
            st.error(f"❌ Não consegui carregar {nome}. Nada foi gravado. "
                     f"Tente de novo; se continuar, avise com esta mensagem:\n\n"
                     f"{type(e).__name__}: {str(e)[:300]}")
        st.markdown("---")


def _render_carga(engine, usuario):
    import streamlit as st

    st.caption("Suba o export de kits do UpSeller (Export_Kit_*.xlsx). A carga é "
               "incremental: kit novo entra, composição alterada é atualizada, e kit "
               "que não veio no arquivo NÃO é apagado. Nada grava sem a prévia.")
    arq = st.file_uploader("Export de kits do UpSeller", type=['xlsx'], key="kits_upload")
    if arq is not None:
        try:
            df = pd.read_excel(arq, dtype=object)
        except Exception as e:
            st.error(f"Não consegui ler o arquivo: {e}")
            df = None
        if df is not None:
            linhas, erro = ler_export_upseller(df)
            if erro:
                st.error(erro)
            else:
                kits, recusados = montar_kits(linhas)
                # O clique no botão roda a página de novo. O que a pessoa VIU é
                # o plano da execução anterior: é contra ele que a gravação
                # compara, não contra o recalculado agora.
                chave = (arq.name, arq.size)
                visto = st.session_state.get('kits_plano_visto')
                plano_visto = visto[1] if visto and visto[0] == chave else None
                conn = engine.raw_connection()
                try:
                    plano = previa(conn, kits, recusados)
                finally:
                    conn.close()
                st.session_state['kits_plano_visto'] = (chave, plano)
                st.markdown(f"**{arq.name}** — {len(linhas)} linhas, "
                            f"{len(kits) + len(recusados)} kits.")
                _render_previa(plano)
                if not tem_o_que_gravar(plano):
                    st.info("Nada a gravar: a composição do banco já é a do arquivo.")
                elif st.button("✅ Gravar composição", type="primary", key="kits_gravar"):
                    conn = engine.raw_connection()
                    try:
                        if plano_visto is None:
                            raise PreviaDesatualizada(
                                'Confira a prévia acima e clique em gravar de novo.')
                        r = gravar(conn, kits, recusados, arq.name, usuario, plano_visto)
                    except PreviaDesatualizada as e:
                        st.warning(str(e))
                        r = None
                    except Exception as e:
                        st.error(f"❌ Carga revertida — nada foi gravado.\n\nErro: {e}")
                        r = None
                    finally:
                        conn.close()
                    if r:
                        st.success(
                            f"✅ Gravado: {r['novos']} kit(s) novo(s), {r['alterados']} "
                            f"alterado(s), {r['pendentes']} em pendência, "
                            f"{r['pendencias_resolvidas']} pendência(s) resolvida(s).")
