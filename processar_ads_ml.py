"""
MÓDULO: Leitura do relatório de Ads (Product Ads) do Mercado Livre
Sistema Nala

Objetivo: trazer para dentro do sistema o gasto de mídia do Mercado Livre,
que é o maior canal da operação e até aqui não tinha nenhum dado no banco —
`fact_ads_performance` estava com zero linhas enquanto o ML respondia por
mais da metade da receita.

O ARQUIVO
  Export "Relatório de anúncios" do painel de Publicidade do ML.
  Nome de origem: report-pads_report-<conta>-<timestamp>.xlsx
  Três abas: Ajuda, Glossário e "Relatório Anúncios patrocinados".
  O cabeçalho fica na linha 2 da aba de dados (índice 1), mas aqui ele é
  PROCURADO em vez de fixado: o ML já mudou o preâmbulo antes, e uma linha
  fixa quebra silenciosamente devolvendo um DataFrame vazio.

GRANULARIDADE
  O relatório sai agregado no período inteiro ("Agrupamento de dados: Total"),
  tipicamente uma semana. O painel do ML não oferece abertura diária, e no
  volume da operação o diário seria pior para decidir: um anúncio com ~17
  cliques/dia tem a taxa de conversão virada por uma única venda. Por isso o
  período é gravado como intervalo (periodo_inicio/periodo_fim) em vez de dia.
  A coluna `data` legada recebe o fim do período, para continuar válida.

DEDUPLICAÇÃO
  A chave inclui a CAMPANHA, não só o anúncio. Quando um anúncio é movido de
  campanha no meio do período, o ML emite uma linha por campanha, com o
  status "Movido" e o resultado separado em cada uma. Chavear só por
  (anúncio, período) faria a segunda linha parecer repetida e o gasto dela
  seria descartado.

O QUE NÃO VEM AQUI
  `visitas` fica nula: o ML não reporta visita no relatório de publicidade.
  Esse dado existe no "Relatório de desempenho das publicações", que é outro
  arquivo, tem periodicidade própria e cobre o anúncio inteiro (orgânico +
  pago). Ele é assunto de outro módulo — este aqui não depende dele para
  funcionar, de propósito.
"""

import re
import pandas as pd


MARKETPLACE = 'MERCADO LIVRE'

# Colunas do resultado normalizado, na ordem em que são gravadas.
COLUNAS_NORM = [
    'periodo_inicio', 'periodo_fim', 'campanha', 'titulo', 'codigo_anuncio',
    'status', 'impressoes', 'cliques', 'cpc', 'ctr', 'conversao',
    'receita_ads', 'gasto_ads', 'acos', 'roas', 'vendas_diretas',
    'vendas_indiretas', 'vendas', 'receita_direta', 'receita_indireta',
]


# ============================================================
# HELPERS
# ============================================================

def _num(v):
    """
    Converte para float aceitando vírgula decimal. Devolve None quando a
    célula não é número.

    O ML escreve '-' nas métricas que não existem naquele anúncio (CTR sem
    impressão, ACOS sem receita). Virar 0.0 seria mentira: zero de ACOS é
    diferente de ACOS indefinido, e a média sairia errada.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace('R$', '').replace('%', '').replace(' ', '')
    if not s or s.lower() in ('nan', 'none', '-', 'inf', 'inf%'):
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _int(v):
    """Inteiro para impressões, cliques e contagem de vendas."""
    n = _num(v)
    return None if n is None else int(round(n))


def _texto(v):
    s = str(v or '').strip()
    return '' if s.lower() in ('nan', 'none') else s


def _achar_col(df, *termos):
    """
    Primeira coluna cujo nome contém todos os termos (case-insensitive).

    Os nomes reais trazem quebra de linha no meio ('CPC \\n(Custo por
    clique)'), então comparar por igualdade não funciona.
    """
    for c in df.columns:
        nome = str(c).lower().replace('\n', ' ')
        if all(t.lower() in nome for t in termos):
            return c
    return None


def _data(v):
    """
    Lê as datas do relatório, que vêm no formato '17-ago-2026'.

    pandas não reconhece o mês abreviado em português, então a conversão é
    feita na mão antes de tentar o parser genérico.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if hasattr(v, 'date'):
        return v.date()
    s = _texto(v).lower()
    if not s:
        return None
    meses = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
             'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
    m = re.match(r'(\d{1,2})[-/ ]([a-zç]{3,})[-/ ](\d{4})', s)
    if m:
        mes = meses.get(m.group(2)[:3])
        if mes:
            from datetime import date
            return date(int(m.group(3)), mes, int(m.group(1)))
    try:
        return pd.to_datetime(s, dayfirst=True).date()
    except Exception:
        return None


def normalizar_mlb(valor):
    """Garante o prefixo MLB. O relatório já traz prefixado, mas o de
    desempenho traz só o número — normalizar aqui evita divergência quando
    os dois forem cruzados."""
    s = _texto(valor).upper().replace('MLB', '')
    n = re.sub(r'\D', '', s)
    return ('MLB' + n) if n else ''


def identificar_loja(nome_arquivo, padrao=None):
    """
    Descobre a loja pelo nome do arquivo.

    O export do ML não diz de qual conta veio — o número no nome
    (report-pads_report-23768-... / -164217-...) é o id da conta de
    publicidade, não da loja. Como os arquivos já são salvos no Drive com o
    padrão '<periodo>_ADS_ML-<loja>.xlsx', é dele que a loja sai. Quando o
    nome não segue o padrão, devolve o valor informado pelo usuário na tela.
    """
    s = _texto(nome_arquivo)
    m = re.search(r'ADS[_ ]?(ML-[A-Za-zÀ-ú]+)', s, re.IGNORECASE)
    if m:
        return m.group(1).replace('ml-', 'ML-')
    m = re.search(r'\b(ML-[A-Za-zÀ-ú]+)\b', s, re.IGNORECASE)
    if m:
        return m.group(1).replace('ml-', 'ML-')
    return padrao


# ============================================================
# LEITURA
# ============================================================

def _localizar_aba(xl):
    """Aba de dados. Procura pelo nome; se não achar, usa a maior."""
    for nome in xl.sheet_names:
        if 'patrocinad' in str(nome).lower():
            return nome
    return xl.sheet_names[-1]


def _localizar_header(df_cru):
    """
    Linha do cabeçalho: a primeira que tenha 'Desde' e 'Campanha'.

    Procurar em vez de fixar protege contra mudança de preâmbulo do ML — que
    já aconteceu e, com linha fixa, produziria uma tabela vazia sem erro.
    """
    for i in range(min(60, len(df_cru))):
        linha = [str(x) for x in df_cru.iloc[i].tolist()]
        if any('Desde' in x for x in linha) and any('Campanha' in x for x in linha):
            return i
    return None


def ler_relatorio_ads_ml(arquivo, loja=None, nome_arquivo=None):
    """
    Lê o export de anúncios patrocinados e devolve (df_normalizado, meta).

    meta traz loja, periodo_inicio, periodo_fim, linhas_lidas,
    linhas_com_gasto e avisos — a tela usa isso para mostrar o que entrou
    antes de gravar.
    """
    nome = nome_arquivo or getattr(arquivo, 'name', '') or str(arquivo)
    meta = {'loja': identificar_loja(nome, loja), 'periodo_inicio': None,
            'periodo_fim': None, 'linhas_lidas': 0, 'linhas_com_gasto': 0,
            'avisos': []}

    xl = pd.ExcelFile(arquivo)
    aba = _localizar_aba(xl)
    cru = pd.read_excel(arquivo, sheet_name=aba, header=None)

    i = _localizar_header(cru)
    if i is None:
        meta['avisos'].append(
            'Cabeçalho não encontrado. O arquivo não parece ser o "Relatório '
            'de anúncios" do painel de Publicidade do ML.')
        return pd.DataFrame(columns=COLUNAS_NORM), meta

    df = pd.read_excel(arquivo, sheet_name=aba, header=i)
    df = df[df[df.columns[0]].notna()]

    col = {
        'desde': _achar_col(df, 'desde'),
        'ate': _achar_col(df, 'até') or _achar_col(df, 'ate'),
        'campanha': _achar_col(df, 'campanha'),
        'titulo': _achar_col(df, 'título', 'anúncio') or _achar_col(df, 'titulo'),
        'codigo': _achar_col(df, 'código', 'anúncio') or _achar_col(df, 'codigo'),
        'status': _achar_col(df, 'status'),
        'impressoes': _achar_col(df, 'impress'),
        'cliques': _achar_col(df, 'clique'),
        'cpc': _achar_col(df, 'cpc'),
        'ctr': _achar_col(df, 'ctr'),
        'cvr': _achar_col(df, 'cvr'),
        'receita': _achar_col(df, 'receita', 'moeda'),
        'investimento': _achar_col(df, 'investimento'),
        'acos': _achar_col(df, 'acos'),
        'roas': _achar_col(df, 'roas'),
        'v_diretas': _achar_col(df, 'vendas diretas'),
        'v_indiretas': _achar_col(df, 'vendas indiretas'),
        'v_pub': _achar_col(df, 'vendas por publicidade'),
        'r_diretas': _achar_col(df, 'receita por vendas diretas'),
        'r_indiretas': _achar_col(df, 'receita por vendas indiretas'),
    }
    if not col['codigo'] or not col['investimento']:
        meta['avisos'].append(
            'Faltam as colunas de código do anúncio ou de investimento — '
            'o arquivo não tem o formato esperado.')
        return pd.DataFrame(columns=COLUNAS_NORM), meta

    linhas = []
    for _, r in df.iterrows():
        mlb = normalizar_mlb(r.get(col['codigo']))
        if not mlb:
            continue
        linhas.append({
            'periodo_inicio': _data(r.get(col['desde'])),
            'periodo_fim': _data(r.get(col['ate'])),
            'campanha': _texto(r.get(col['campanha']))[:200],
            'titulo': _texto(r.get(col['titulo'])),
            'codigo_anuncio': mlb,
            'status': _texto(r.get(col['status']))[:30],
            'impressoes': _int(r.get(col['impressoes'])),
            'cliques': _int(r.get(col['cliques'])),
            'cpc': _num(r.get(col['cpc'])),
            'ctr': _num(r.get(col['ctr'])),
            'conversao': _num(r.get(col['cvr'])),
            'receita_ads': _num(r.get(col['receita'])),
            'gasto_ads': _num(r.get(col['investimento'])),
            'acos': _num(r.get(col['acos'])),
            'roas': _num(r.get(col['roas'])),
            'vendas_diretas': _int(r.get(col['v_diretas'])),
            'vendas_indiretas': _int(r.get(col['v_indiretas'])),
            'vendas': _int(r.get(col['v_pub'])),
            'receita_direta': _num(r.get(col['r_diretas'])),
            'receita_indireta': _num(r.get(col['r_indiretas'])),
        })

    out = pd.DataFrame(linhas, columns=COLUNAS_NORM)
    meta['linhas_lidas'] = len(out)
    if not out.empty:
        meta['linhas_com_gasto'] = int((out['gasto_ads'].fillna(0) > 0).sum())
        inicios = [d for d in out['periodo_inicio'] if d]
        fins = [d for d in out['periodo_fim'] if d]
        meta['periodo_inicio'] = min(inicios) if inicios else None
        meta['periodo_fim'] = max(fins) if fins else None
        if not meta['periodo_inicio'] or not meta['periodo_fim']:
            meta['avisos'].append(
                'Não foi possível ler o período das linhas — a gravação '
                'precisa dele para não duplicar.')
        movidos = int((out['status'].str.lower() == 'movido').sum())
        if movidos:
            meta['avisos'].append(
                f'{movidos} anúncio(s) com status "Movido": o ML separa o '
                'resultado por campanha, e as duas linhas são mantidas.')
    if not meta['loja']:
        meta['avisos'].append(
            'Loja não identificada pelo nome do arquivo — informe na tela.')
    return out, meta


# ============================================================
# SCHEMA
# ============================================================

def garantir_schema_ads_performance(engine):
    """
    Amplia `fact_ads_performance` para caber tudo o que o ML entrega.

    A tabela nasceu com 14 colunas pensadas num modelo diário genérico e
    perderia mais da metade do relatório: campanha, receita de ads, ACOS,
    ROAS, CPC, a separação entre venda direta e indireta e o status.

    Perder direta/indireta seria o pior: é o que mostra o anúncio que vende
    OUTRO produto da loja. Sem isso, uma campanha que parece ruim sozinha
    mas puxa venda de vizinho seria cortada por engano.

    A ampliação é feita com ADD COLUMN IF NOT EXISTS e a tabela está vazia,
    então não há dado a migrar. `data` continua NOT NULL e recebe o fim do
    período, mantendo válido qualquer uso antigo da coluna.
    """
    novas = [
        ('periodo_inicio', 'DATE'),
        ('periodo_fim', 'DATE'),
        ('campanha', 'VARCHAR(200)'),
        ('titulo', 'TEXT'),
        ('status', 'VARCHAR(30)'),
        ('cpc', 'NUMERIC(12,4)'),
        ('receita_ads', 'NUMERIC(14,2)'),
        ('acos', 'NUMERIC(12,4)'),
        ('roas', 'NUMERIC(12,4)'),
        ('vendas_diretas', 'INTEGER'),
        ('vendas_indiretas', 'INTEGER'),
        ('receita_direta', 'NUMERIC(14,2)'),
        ('receita_indireta', 'NUMERIC(14,2)'),
        ('arquivo_origem', 'VARCHAR(255)'),
    ]
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for nome, tipo in novas:
            cursor.execute(
                f'ALTER TABLE fact_ads_performance '
                f'ADD COLUMN IF NOT EXISTS {nome} {tipo}')
        # Chave de deduplicação. Inclui a campanha por causa do "Movido".
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_ads_perf_periodo
            ON fact_ads_performance
               (marketplace, loja, periodo_inicio, periodo_fim,
                codigo_anuncio, campanha)
        """)
        conn.commit()
        return True, ''
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, str(e)[:300]
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def garantir_tabela_desempenho_anuncios(engine):
    """
    Cria a tabela do "Relatório de desempenho das publicações", vazia.

    Nada no módulo de Ads lê ou escreve nela hoje — e é intencional que o
    Ads não dependa dela. Fica criada agora porque criar depois custaria o
    mesmo trabalho com o banco já em uso, e porque é ela que vai receber
    visitas, conversão do funil orgânico, qualidade do anúncio e opiniões,
    quando a análise de escala entrar.

    Diferença importante para a tabela de ads: esta cobre o anúncio inteiro
    (orgânico + pago) e tem periodicidade própria, então é tabela separada,
    ligada por codigo_anuncio.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_anuncios_desempenho (
                id                    SERIAL PRIMARY KEY,
                marketplace           VARCHAR(30) NOT NULL DEFAULT 'MERCADO LIVRE',
                loja                  VARCHAR(120),
                periodo_inicio        DATE NOT NULL,
                periodo_fim           DATE NOT NULL,
                codigo_anuncio        VARCHAR(60) NOT NULL,
                sku                   VARCHAR(120),
                titulo                TEXT,
                variacao              VARCHAR(120),
                status_anuncio        VARCHAR(30),
                qualidade_anuncio     VARCHAR(40),
                experiencia_compra    VARCHAR(40),
                visitas_unicas        INTEGER,
                quantidade_vendas     INTEGER,
                compradores_unicos    INTEGER,
                unidades_vendidas     INTEGER,
                vendas_brutas         NUMERIC(14,2),
                participacao          NUMERIC(12,4),
                conv_visita_venda     NUMERIC(12,4),
                conv_visita_comprador NUMERIC(12,4),
                total_opinioes        INTEGER,
                opinioes_ruins        INTEGER,
                opinioes_boas         INTEGER,
                arquivo_origem        VARCHAR(255),
                data_importacao       TIMESTAMP DEFAULT NOW(),
                UNIQUE (marketplace, loja, periodo_inicio, periodo_fim,
                        codigo_anuncio)
            )
        """)
        conn.commit()
        return True, ''
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, str(e)[:300]
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# GRAVAÇÃO
# ============================================================

def gravar_ads_ml(engine, df, loja, arquivo_nome, incluir_sem_gasto=False):
    """
    Grava as linhas em `fact_ads_performance`, em lote.

    Por padrão só entram as linhas com gasto ou com impressão: o relatório
    traz todo anúncio já cadastrado na campanha, e a maioria vem zerada
    (desativada ou sem circulação). Guardar tudo encheria a tabela de linhas
    que não informam nada. `incluir_sem_gasto=True` guarda mesmo assim,
    quando se quiser o retrato completo da conta.

    Reimportar o mesmo período regrava em vez de duplicar: o ON CONFLICT usa
    a chave (marketplace, loja, período, anúncio, campanha). Isso é o que
    permite subir de novo um arquivo corrigido sem limpar nada antes.

    Devolve dict com {inseridos, atualizados, ignorados, gasto_total,
    mensagem}.
    """
    vazio = {'inseridos': 0, 'atualizados': 0, 'ignorados': 0,
             'gasto_total': 0.0, 'mensagem': ''}
    if df is None or df.empty:
        vazio['mensagem'] = 'Nada a gravar — o arquivo não produziu linhas.'
        return vazio
    if not loja:
        vazio['mensagem'] = 'Loja não informada — a gravação foi cancelada.'
        return vazio

    from psycopg2.extras import execute_values

    linhas = []
    ignoradas = 0
    for _, r in df.iterrows():
        if not r['periodo_inicio'] or not r['periodo_fim']:
            ignoradas += 1
            continue
        gasto = r['gasto_ads'] or 0
        impr = r['impressoes'] or 0
        if not incluir_sem_gasto and gasto <= 0 and impr <= 0:
            ignoradas += 1
            continue
        vendas = r['vendas'] or 0
        linhas.append((
            r['periodo_fim'],                       # data (legado)
            MARKETPLACE, loja,
            r['codigo_anuncio'], r['campanha'] or '', r['titulo'],
            r['status'], r['periodo_inicio'], r['periodo_fim'],
            r['impressoes'], r['cliques'], r['cpc'], r['ctr'],
            r['conversao'], r['receita_ads'], r['gasto_ads'],
            r['acos'], r['roas'],
            r['vendas_diretas'], r['vendas_indiretas'], vendas,
            r['receita_direta'], r['receita_indireta'],
            (gasto / vendas) if vendas else None,    # cpa
            (arquivo_nome or '')[:255],
        ))

    if not linhas:
        vazio['ignorados'] = ignoradas
        vazio['mensagem'] = (
            'Nenhuma linha com gasto ou impressão no período — nada gravado.')
        return vazio

    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        execute_values(cursor, """
            INSERT INTO fact_ads_performance
                (data, marketplace, loja, codigo_anuncio, campanha, titulo,
                 status, periodo_inicio, periodo_fim, impressoes, cliques,
                 cpc, ctr, conversao, receita_ads, gasto_ads, acos, roas,
                 vendas_diretas, vendas_indiretas, vendas, receita_direta,
                 receita_indireta, cpa, arquivo_origem)
            VALUES %s
            ON CONFLICT (marketplace, loja, periodo_inicio, periodo_fim,
                         codigo_anuncio, campanha)
            DO UPDATE SET
                data = EXCLUDED.data,
                titulo = EXCLUDED.titulo,
                status = EXCLUDED.status,
                impressoes = EXCLUDED.impressoes,
                cliques = EXCLUDED.cliques,
                cpc = EXCLUDED.cpc,
                ctr = EXCLUDED.ctr,
                conversao = EXCLUDED.conversao,
                receita_ads = EXCLUDED.receita_ads,
                gasto_ads = EXCLUDED.gasto_ads,
                acos = EXCLUDED.acos,
                roas = EXCLUDED.roas,
                vendas_diretas = EXCLUDED.vendas_diretas,
                vendas_indiretas = EXCLUDED.vendas_indiretas,
                vendas = EXCLUDED.vendas,
                receita_direta = EXCLUDED.receita_direta,
                receita_indireta = EXCLUDED.receita_indireta,
                cpa = EXCLUDED.cpa,
                arquivo_origem = EXCLUDED.arquivo_origem,
                data_importacao = NOW()
        """, linhas, page_size=500)
        gravadas = cursor.rowcount
        conn.commit()
        gasto_total = float(sum((l[15] or 0) for l in linhas))
        return {
            'inseridos': gravadas,
            'atualizados': 0,
            'ignorados': ignoradas,
            'gasto_total': gasto_total,
            'mensagem': (
                f'{gravadas} linha(s) gravada(s) para {loja}. '
                f'Investimento no período: R$ {gasto_total:,.2f}. '
                f'{ignoradas} linha(s) sem gasto e sem impressão ignorada(s).'
            ).replace(',', 'X').replace('.', ',').replace('X', '.'),
        }
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        vazio['mensagem'] = f'Erro ao gravar: {str(e)[:300]}'
        return vazio
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# LEITURA AGREGADA (para a tela)
# ============================================================

def resumo_por_loja(engine, periodo_inicio=None, periodo_fim=None):
    """
    Gasto de ads do ML por loja e período, com o TACOS calculado contra a
    receita real de `fact_vendas_snapshot`.

    O TACOS sai daqui e não do ACOS do próprio relatório: o ACOS do ML mede
    o investimento contra a receita ATRIBUÍDA a ads, enquanto o TACOS mede
    contra a receita TOTAL da loja — que é a conta que interessa para
    margem. As duas convivem na tela.
    """
    # As condições são acumuladas numa lista e unidas por AND. Montar o WHERE
    # concatenando texto condicional já produziu SQL com dois WHERE aqui.
    cond = ['a.marketplace = %s']
    params = [MARKETPLACE]
    if periodo_inicio and periodo_fim:
        cond.append('a.periodo_inicio >= %s AND a.periodo_fim <= %s')
        params += [periodo_inicio, periodo_fim]
    where = ' AND '.join(cond)

    sql = f"""
        WITH ads AS (
            SELECT a.loja,
                   MIN(a.periodo_inicio) AS de,
                   MAX(a.periodo_fim)    AS ate,
                   SUM(a.gasto_ads)      AS gasto,
                   SUM(a.receita_ads)    AS receita_ads,
                   SUM(a.impressoes)     AS impressoes,
                   SUM(a.cliques)        AS cliques,
                   SUM(a.vendas)         AS vendas
              FROM fact_ads_performance a
             WHERE {where}
             GROUP BY a.loja
        )
        SELECT ads.*,
               COALESCE(v.receita_total, 0) AS receita_total,
               CASE WHEN COALESCE(v.receita_total,0) > 0
                    THEN 100.0 * ads.gasto / v.receita_total END AS tacos
          FROM ads
          LEFT JOIN (
                SELECT loja_origem,
                       SUM(valor_venda_efetivo) AS receita_total
                  FROM fact_vendas_snapshot
                 WHERE marketplace_origem = %s
                 GROUP BY loja_origem
          ) v ON v.loja_origem = ads.loja
         ORDER BY ads.gasto DESC
    """
    return pd.read_sql(sql, engine, params=tuple(params + [MARKETPLACE]))
