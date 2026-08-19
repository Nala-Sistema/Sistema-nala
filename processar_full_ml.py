"""
MÓDULO: Leitura dos relatórios de custo de Full do Mercado Livre
Sistema Nala

Objetivo: transformar os relatórios que o ML entrega em linhas normalizadas
de custo, atribuídas por anúncio, para alimentar a margem real.

O ML entrega DOIS relatórios diferentes, que se complementam:

  A) Relatorio_Tarifas_Full_<Mês><Ano>.xlsx      (header na linha 5)
     3 abas — armazenamento, coleta e armazenamento prolongado.
     A aba de armazenamento NÃO traz SKU; as outras duas trazem.

  B) <dd-mm-aa>_<dd-mm-aa>_Custos_por_serviço_armazenamento.xlsx
     Abas Resumo e Detalhe (header do Detalhe na linha 4).
     O Detalhe traz o custo dia a dia POR ANÚNCIO — é daqui que sai a
     armazenagem atribuída, sem rateio, agregada por mês calendário.

Regra de qual usar para quê:
  - armazenagem          -> arquivo B, aba Detalhe   (direta por anúncio)
  - coleta               -> arquivo A, aba coleta    (direta por SKU/MLB)
  - armazenamento longo  -> arquivo A, aba prolongado(direta por SKU/MLB)

A aba "Tarifa de armazenamento" do arquivo A serve apenas como total de
conferência — usá-la para atribuir obrigaria a ratear, o que é pior. Ela
diverge do arquivo B em ~2,5% no mesmo período, por causa ainda não
identificada; como é 0,04% da receita, não bloqueia o uso, mas vale
monitorar se a diferença crescer.

O relatório acumulado (ex: 14/02 a 19/08) pode ser subido sempre: a
deduplicação é por mês para armazenagem e pelo "Nº do custo" do ML para as
cobranças de evento, então nunca duplica.

Nenhum dos arquivos identifica a loja. `identificar_loja()` resolve isso
cruzando os MLBs com fact_vendas_snapshot.
"""

import re
import pandas as pd


TIPO_ARMAZENAGEM = 'FULL_ARMAZENAGEM'
TIPO_COLETA = 'FULL_COLETA'
TIPO_PROLONGADO = 'FULL_ARMAZENAGEM_PROLONGADA'

# Colunas do resultado normalizado, iguais para os três tipos de custo.
COLUNAS_NORM = [
    'tipo', 'sku', 'codigo_anuncio', 'produto', 'referencia',
    'valor', 'unidades', 'periodo_inicio', 'periodo_fim', 'observacao',
]


# ============================================================
# HELPERS
# ============================================================

def _num(v):
    """Converte para float aceitando vírgula decimal. Devolve 0.0 se não der."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace('R$', '').replace(' ', '')
    if not s or s.lower() in ('nan', 'none', '-'):
        return 0.0
    # 1.234,56 -> 1234.56 ; 1234.56 fica como está
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _texto(v):
    s = str(v or '').strip()
    return '' if s.lower() in ('nan', 'none') else s


def extrair_mlbs(valor):
    """
    Extrai códigos de anúncio de uma célula.

    O relatório às vezes traz DOIS anúncios na mesma linha, separados por
    ' | ' (o mesmo estoque físico servindo dois anúncios), e o prefixo MLB
    aparece só no primeiro: '4170806455 | 4184218955' ou
    'MLB4170806455 | 5768629470'. Devolve todos já prefixados.
    """
    s = _texto(valor)
    if not s:
        return []
    return ['MLB' + n for n in re.findall(r'(\d{9,})', s.replace('MLB', ''))]


def dividir_em_centavos(valor, n):
    """
    Divide um valor entre n anúncios devolvendo centavos que somam exato.

    Necessário porque a coluna `valor` é numeric(10,2): dividir e arredondar
    cada parte independentemente faz a soma divergir do valor original. Pior,
    Python arredonda meio-para-par e o Postgres meio-para-cima, então a
    divergência nem é reproduzível fora do banco.

    Aqui cada parte recebe o piso em centavos e o resto é distribuído de um
    em um, então a soma das partes é sempre igual ao valor arredondado.
    """
    if n <= 1:
        return [round(valor, 2)]
    centavos = int(round(valor * 100))
    base, resto = divmod(abs(centavos), n)
    sinal = -1 if centavos < 0 else 1
    partes = [base + (1 if i < resto else 0) for i in range(n)]
    return [sinal * p / 100.0 for p in partes]


def _achar_col(df, *termos):
    """Primeira coluna cujo nome contém todos os termos (case-insensitive)."""
    for c in df.columns:
        nome = str(c).lower()
        if all(t.lower() in nome for t in termos):
            return c
    return None


# ============================================================
# ARQUIVO A — Relatorio_Tarifas_Full
# ============================================================

def ler_relatorio_tarifas_full(caminho):
    """
    Lê o consolidado de tarifas do Full.

    Devolve (df_normalizado, avisos). Só coleta e armazenamento prolongado
    entram no normalizado — a aba de armazenamento fica de fora porque não
    tem SKU, e o valor dela é devolvido em `avisos` como total de conferência.
    """
    avisos = []
    linhas = []
    abas = pd.ExcelFile(caminho).sheet_names

    for aba in abas:
        nome = aba.lower()
        df = pd.read_excel(caminho, sheet_name=aba, header=5)
        df.columns = [str(c).strip() for c in df.columns]

        col_valor = _achar_col(df, 'valor', 'custo') or _achar_col(df, 'valor', 'tarifa')
        col_data = _achar_col(df, 'data', 'custo') or _achar_col(df, 'data', 'tarifa')
        col_sku = next((c for c in df.columns if c.strip().upper() == 'SKU'), None)
        col_prod = _achar_col(df, 'título') or _achar_col(df, 'titulo') or _achar_col(df, 'produto')
        col_un = _achar_col(df, 'unidades')
        # Nº do custo/tarifa: identificador unico que o proprio ML da a cada
        # cobranca. E a chave de deduplicacao das cobrancas de evento.
        col_ref = next(
            (c for c in df.columns
             if c.strip().lower().startswith('n') and ('custo' in c.lower() or 'tarifa' in c.lower())
             and 'estorn' not in c.lower() and 'valor' not in c.lower()),
            None,
        )

        # A coluna do anúncio não tem cabeçalho estável — acha pela presença de MLB
        col_mlb = next(
            (c for c in df.columns
             if df[c].astype(str).str.contains(r'MLB\d', na=False, regex=True).any()),
            None,
        )

        if 'coleta' in nome:
            tipo = TIPO_COLETA
        elif 'prolonga' in nome:
            tipo = TIPO_PROLONGADO
        else:
            total = pd.to_numeric(df[col_valor], errors='coerce').sum() if col_valor else 0.0
            avisos.append(
                f"Aba '{aba}': R$ {total:,.2f} não atribuído — esta aba não traz SKU. "
                f"Use o relatório de Custos por serviço de armazenamento para atribuir."
            )
            continue

        if not col_mlb or not col_valor:
            avisos.append(f"Aba '{aba}': ignorada, não encontrei coluna de anúncio ou de valor.")
            continue

        datas = pd.to_datetime(df[col_data], errors='coerce') if col_data else None

        for i, row in df.iterrows():
            valor = _num(row[col_valor])
            mlbs = extrair_mlbs(row[col_mlb])
            if valor == 0 or not mlbs:
                continue
            data = datas.iloc[i] if datas is not None and pd.notna(datas.iloc[i]) else None
            # Linha com 2 anúncios: divide o valor igualmente entre eles.
            # É o único critério disponível dentro do próprio arquivo.
            partes = dividir_em_centavos(valor, len(mlbs))
            for mlb, parte in zip(mlbs, partes):
                linhas.append({
                    'tipo': tipo,
                    'sku': _texto(row[col_sku]) if col_sku else '',
                    'codigo_anuncio': mlb,
                    'produto': _texto(row[col_prod])[:100] if col_prod else '',
                    'referencia': _texto(row[col_ref]) if col_ref else '',
                    'valor': parte,
                    'unidades': _num(row[col_un]) if col_un else 0.0,
                    'periodo_inicio': data,
                    'periodo_fim': data,
                    'observacao': f'{len(mlbs)} anúncios na linha' if len(mlbs) > 1 else '',
                })

    df_out = pd.DataFrame(linhas, columns=COLUNAS_NORM)
    return df_out, avisos


# ============================================================
# ARQUIVO B — Custos por serviço de armazenamento
# ============================================================

def ler_custos_armazenamento_mensal(caminho):
    """
    Le o detalhe de armazenagem gerando UMA LINHA POR ANUNCIO POR MES.

    A aba Detalhe traz uma coluna por dia do ciclo. Agregar por mes calendario
    resolve tres coisas de uma vez:

      1. Sobreposicao. O ML sempre oferece o relatorio acumulado desde o
         inicio, entao todo download novo cobre periodos ja lancados. Com o
         mes como chave, subir de novo regrava os mesmos meses.

      2. Mes calendario. Os ciclos do ML vao do dia 20 ao 19 e variam por
         loja, entao "custo de julho" nao sai de um ciclo. Somando as colunas
         de dia por mes, a apuracao mensal e exata.

      3. Precisao. Guardar dia a dia obrigaria a arredondar valores de
         R$ 0,007 para centavos — o que distorce o custo dos itens pequenos,
         justamente os que precisamos medir. Somando o mes com precisao cheia
         e arredondando uma vez, o desvio contra o Resumo cai de R$ 0,25 para
         R$ 0,06 no arquivo real, e o volume cai 95% (6.432 -> 292 linhas).

    Processar o arquivo inteiro sempre, em vez de so os meses que faltam, e
    proposital: o relatorio e acumulado porque o ML recalcula, e uma correcao
    retroativa precisa entrar. Ler o arquivo completo leva ~0,5s.

    Devolve (df_normalizado, avisos).
    """
    avisos = []
    df = pd.read_excel(caminho, sheet_name='Detalhe', header=4)
    df.columns = [str(c).strip() for c in df.columns]

    col_sku = next((c for c in df.columns if c.strip().upper() == 'SKU'), None)
    col_anun = _achar_col(df, 'anuncio') or _achar_col(df, 'anúncio')
    col_prod = _achar_col(df, 'produto')
    col_tam = _achar_col(df, 'tamanho')
    cols_dia = [c for c in df.columns if re.match(r'^\d{2}/\d{2}/\d{4}$', str(c).strip())]

    if not (col_sku and col_anun and cols_dia):
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'Arquivo sem as colunas esperadas (SKU / anuncio / colunas de dia).'
        ]

    # A aba alterna linha de produto com linha de unidades; a de unidades nao
    # tem SKU, entao filtrar por SKU preenchido ja descarta.
    prod = df[df[col_sku].notna() & (df[col_sku].astype(str).str.strip() != '')]

    ultimo_dia_relatorio = max(pd.to_datetime(c, dayfirst=True) for c in cols_dia)

    linhas = []
    for _, row in prod.iterrows():
        sku = _texto(row[col_sku])
        mlbs = extrair_mlbs(row[col_anun]) or ['']
        produto = _texto(row[col_prod])[:100] if col_prod else ''
        tamanho = _texto(row[col_tam]) if col_tam else ''

        # Soma as colunas de dia por mes calendario, com precisao cheia
        por_mes = {}
        for c in cols_dia:
            valor = _num(row[c])
            if valor <= 0:
                continue
            dia = pd.to_datetime(c, dayfirst=True)
            por_mes[(dia.year, dia.month)] = por_mes.get((dia.year, dia.month), 0.0) + valor

        for (ano, mes), total_mes in sorted(por_mes.items()):
            ini = pd.Timestamp(year=ano, month=mes, day=1)
            # periodo_fim e o ultimo dia COBERTO PELO RELATORIO, nao o fim do
            # mes calendario. O mes corrente vem sempre parcial (o ciclo do ML
            # vai ate o dia 19), e gravar 31/08 faria o painel de status dizer
            # que agosto esta completo quando falta metade.
            fim = min(ini + pd.offsets.MonthEnd(0), ultimo_dia_relatorio)
            for mlb, parte in zip(mlbs, dividir_em_centavos(total_mes, len(mlbs))):
                if parte == 0:
                    continue
                linhas.append({
                    'tipo': TIPO_ARMAZENAGEM,
                    'sku': sku,
                    'codigo_anuncio': mlb,
                    'produto': produto,
                    'referencia': '',
                    'valor': parte,
                    'unidades': 0.0,
                    'periodo_inicio': ini,
                    'periodo_fim': fim,
                    'observacao': (f'tamanho {tamanho}' if tamanho else ''),
                })

    df_out = pd.DataFrame(linhas, columns=COLUNAS_NORM)

    try:
        res = pd.read_excel(caminho, sheet_name='Resumo', header=None)
        total_resumo = pd.to_numeric(res[5], errors='coerce').dropna().max()
        total_lido = df_out['valor'].sum()
        # Tolerancia relativa: o arredondamento por mes gera centavos de desvio
        # em arquivos grandes, e um limite fixo viraria alarme falso.
        if total_resumo and abs(total_lido - total_resumo) > max(0.50, total_resumo * 0.001):
            avisos.append(
                f'DIVERGENCIA: o detalhe soma R$ {total_lido:,.2f} '
                f'mas o Resumo diz R$ {total_resumo:,.2f}.'
            )
    except Exception:
        avisos.append('Nao foi possivel conferir contra a aba Resumo.')

    return df_out, avisos


def detectar_e_ler(arquivo):
    """
    Descobre qual dos dois relatórios de Full é o arquivo e o lê.

    Os dois têm assinatura clara nos nomes das abas, então não faz sentido
    pedir para quem sobe escolher o tipo — é um clique a mais e uma chance
    a mais de errar.

    Devolve (df_normalizado, avisos, rotulo). Se não reconhecer, devolve
    df vazio e o aviso explicando o que foi encontrado.
    """
    try:
        abas = [str(s).lower() for s in pd.ExcelFile(arquivo).sheet_names]
    except Exception as e:
        return pd.DataFrame(columns=COLUNAS_NORM), [f'Não consegui abrir o arquivo: {e}'], None

    if any('coleta' in a for a in abas):
        df, avisos = ler_relatorio_tarifas_full(arquivo)
        return df, avisos, 'Tarifas Full (coleta + armazenamento antigo)'

    if any('detalhe' in a for a in abas):
        df, avisos = ler_custos_armazenamento_mensal(arquivo)
        return df, avisos, 'Custos por serviço de armazenamento (mensal)'

    return pd.DataFrame(columns=COLUNAS_NORM), [
        'Arquivo não reconhecido. Esperava um relatório de custos de '
        'armazenamento (com aba "Detalhe") ou de tarifas do Full (com aba '
        f'"Custo por serviço de coleta"). Abas encontradas: {", ".join(abas)}.'
    ], None


def gravar_custos_extras(engine, df, marketplace, loja, arquivo_nome,
                         forma_rateio='direto_por_anuncio'):
    """
    Grava as linhas normalizadas em fact_custos_extras, em lote.

    DEDUPLICACAO POR NATUREZA DO CUSTO, nao por nome de arquivo:

      - Armazenagem e custo DIARIO. A chave e (loja, tipo, dia). Antes de
        inserir, apaga o intervalo de dias que o arquivo cobre. Subir o
        relatorio acumulado por cima de um recorte antigo regrava os mesmos
        dias em vez de somar.

      - Coleta e armazenamento prolongado sao cobrancas de EVENTO, e o ML da
        um numero unico a cada uma ("N do custo"). A chave e esse numero.
        Verificado nos arquivos reais: 40/40 e 74/74 numeros distintos.

    Chavear por nome de arquivo — como era antes — nao resolvia: o ML oferece
    sempre o acumulado desde o inicio, entao cada download tem nome novo e
    cobre periodo ja lancado.

    A gravacao e em lote (execute_values). Sao ~6.400 linhas por loja no
    relatorio completo; uma a uma levaria dezenas de segundos contra o Neon.

    Devolve dict com {inseridos, removidos, total, mensagem}.
    """
    if df is None or df.empty:
        return {'inseridos': 0, 'removidos': 0, 'total': 0.0,
                'mensagem': 'Nada a gravar — o arquivo nao produziu linhas de custo.'}

    from psycopg2.extras import execute_values

    # Limites reais das colunas VARCHAR de fact_custos_extras. Cortar aqui, na
    # camada que conhece o schema, em vez de confiar que quem monta o
    # DataFrame lembrou do tamanho.
    LIM = {'tipo': 50, 'sku': 120, 'codigo_anuncio': 60, 'referencia': 100,
           'forma_rateio': 100, 'arquivo_origem': 255}

    def _cortar(valor, campo):
        if valor is None:
            return None
        s = str(valor).strip()
        return s[:LIM[campo]] if s else None

    def _data(v):
        return v.date() if hasattr(v, 'date') else v

    registros = []
    for _, r in df.iterrows():
        valor = float(r['valor'])
        if valor == 0:
            continue
        registros.append((
            _cortar(r['tipo'], 'tipo'), marketplace, loja,
            _cortar(r.get('sku'), 'sku'),
            _cortar(r.get('codigo_anuncio'), 'codigo_anuncio'),
            valor,
            _data(r['periodo_inicio']), _data(r['periodo_fim']),
            _cortar(forma_rateio, 'forma_rateio'),
            _cortar(r.get('referencia'), 'referencia'),
            _cortar(arquivo_nome, 'arquivo_origem'),
            # observacoes e TEXT: cabe o titulo do produto junto da nota
            ' | '.join(x for x in [_texto(r.get('produto')), _texto(r.get('observacao'))] if x) or None,
        ))

    if not registros:
        return {'inseridos': 0, 'removidos': 0, 'total': 0.0,
                'mensagem': 'Nada a gravar — todas as linhas tinham valor zero.'}

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        removidos = 0

        # --- Armazenagem: limpa o intervalo de dias coberto pelo arquivo ---
        arm = df[df['tipo'] == TIPO_ARMAZENAGEM]
        if not arm.empty:
            ini = _data(arm['periodo_inicio'].min())
            fim = _data(arm['periodo_fim'].max())
            cursor.execute(
                """DELETE FROM fact_custos_extras
                   WHERE marketplace = %s AND loja = %s AND tipo = %s
                     AND periodo_inicio BETWEEN %s AND %s""",
                (marketplace, loja, TIPO_ARMAZENAGEM, ini, fim),
            )
            removidos += cursor.rowcount

        # --- Eventos: limpa pelos numeros de cobranca que vieram no arquivo ---
        eventos = df[df['tipo'] != TIPO_ARMAZENAGEM]
        refs = sorted({str(x).strip() for x in eventos.get('referencia', []) if str(x).strip()})
        if refs:
            cursor.execute(
                """DELETE FROM fact_custos_extras
                   WHERE marketplace = %s AND loja = %s AND referencia = ANY(%s)""",
                (marketplace, loja, refs),
            )
            removidos += cursor.rowcount

        execute_values(cursor, """
            INSERT INTO fact_custos_extras
                (tipo, marketplace, loja, sku, codigo_anuncio, valor,
                 periodo_inicio, periodo_fim, rateado, forma_rateio,
                 referencia, data_lancamento, arquivo_origem, observacoes)
            VALUES %s
        """, registros, template="(%s,%s,%s,%s,%s,%s,%s,%s,FALSE,%s,%s,CURRENT_DATE,%s,%s)",
             page_size=500)

        inseridos = len(registros)
        conn.commit()
        cursor.close()
    finally:
        conn.close()

    total = float(df['valor'].sum())
    msg = f'{inseridos} lancamento(s) gravado(s), somando R$ {total:,.2f}.'
    if removidos:
        msg += f' {removidos} lancamento(s) do mesmo periodo/cobranca foram substituidos.'
    return {'inseridos': inseridos, 'removidos': removidos, 'total': total, 'mensagem': msg}


def identificar_loja(engine, codigos_anuncio, marketplace='MERCADO LIVRE'):
    """
    Descobre a que loja o relatório pertence cruzando os anúncios com as vendas.

    Nenhum dos relatórios do ML informa a loja. Como um anúncio pertence a uma
    única loja, o cruzamento resolve — e serve de trava contra arquivo subido
    na loja errada.

    Devolve (loja, confianca_pct, detalhe) — loja é None se não der para decidir.
    """
    codigos = sorted({c for c in codigos_anuncio if c})
    if not codigos:
        return None, 0.0, 'Nenhum código de anúncio no arquivo.'

    sql = """
        SELECT loja_origem, COUNT(DISTINCT codigo_anuncio) AS n
        FROM fact_vendas_snapshot
        WHERE marketplace_origem = %s AND codigo_anuncio = ANY(%s)
        GROUP BY loja_origem ORDER BY n DESC
    """
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (marketplace, codigos))
        linhas = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not linhas:
        return None, 0.0, f'Nenhum dos {len(codigos)} anúncios foi encontrado nas vendas.'

    total = sum(n for _, n in linhas)
    loja, n = linhas[0]
    conf = 100.0 * n / total
    detalhe = ' | '.join(f'{l}: {q}' for l, q in linhas)
    return loja, round(conf, 1), detalhe


# ============================================================
# STATUS / COBRANCA DE ROTINA
# ============================================================

# Prazos de envio: a cada 15 dias, ate o dia 3 e ate o dia 18.
# Escolhidos com o Thiago a partir do ciclo do ML, que fecha no dia 19.
DIAS_PRAZO = (3, 18)

TIPOS_FULL = [
    (TIPO_ARMAZENAGEM, 'Armazenagem', 'Custos por serviço de armazenamento'),
    (TIPO_COLETA, 'Coleta', 'Relatório de Tarifas Full'),
    (TIPO_PROLONGADO, 'Estoque antigo', 'Relatório de Tarifas Full'),
]


def ultimo_prazo(hoje=None):
    """
    Devolve o prazo de envio mais recente que ja passou.

    Com prazos nos dias 3 e 18, em 20/08 o ultimo prazo foi 18/08; em 10/08
    foi 03/08; em 02/08 foi 18/07. Serve de referencia para cobrar o envio:
    se nao houve upload desde esse prazo, a rotina atrasou.
    """
    import datetime as _dt
    hoje = hoje or _dt.date.today()
    candidatos = []
    for delta_mes in (0, -1):
        ano, mes = hoje.year, hoje.month + delta_mes
        if mes < 1:
            ano, mes = ano - 1, mes + 12
        for d in DIAS_PRAZO:
            candidatos.append(_dt.date(ano, mes, d))
    passados = [d for d in candidatos if d <= hoje]
    return max(passados) if passados else min(candidatos)


def proximo_prazo(hoje=None):
    """Proximo prazo de envio a vencer."""
    import datetime as _dt
    hoje = hoje or _dt.date.today()
    candidatos = []
    for delta_mes in (0, 1):
        ano, mes = hoje.year, hoje.month + delta_mes
        if mes > 12:
            ano, mes = ano + 1, mes - 12
        for d in DIAS_PRAZO:
            candidatos.append(_dt.date(ano, mes, d))
    futuros = [d for d in candidatos if d > hoje]
    return min(futuros)


def status_custos_full(engine, marketplace='MERCADO LIVRE'):
    """
    Status de envio dos custos de Full, por loja e tipo.

    A cobranca e sobre a ROTINA: houve upload desde o ultimo prazo? Uma loja
    pode estar com o dado coberto ate ontem e ainda assim precisar de um envio
    novo no proximo ciclo, e e isso que o gestor precisa ver.

    A data de cobertura vai junto porque diz outra coisa: ate quando o custo
    esta lancado. Subir hoje um relatorio que termina em junho cumpre a rotina
    mas nao atualiza o dado — as duas informacoes se complementam.

    Devolve DataFrame com loja, tipo, ate (cobertura), subido_em e atrasado.
    """
    prazo = ultimo_prazo()
    df = pd.read_sql("""
        SELECT l.loja, t.tipo,
               MAX(c.periodo_fim)     AS ate,
               MAX(c.data_lancamento) AS subido_em
        FROM dim_lojas l
        CROSS JOIN (SELECT unnest(%(tipos)s::text[]) AS tipo) t
        LEFT JOIN fact_custos_extras c
               ON c.loja = l.loja AND c.tipo = t.tipo AND c.marketplace = %(mkt)s
        WHERE l.marketplace = %(mkt)s
          AND COALESCE(l.visivel_no_painel, TRUE)
        GROUP BY l.loja, t.tipo
        ORDER BY l.loja, t.tipo
    """, engine, params={'mkt': marketplace, 'tipos': [t[0] for t in TIPOS_FULL]})

    df['subido_em'] = pd.to_datetime(df['subido_em']).dt.date
    df['ate'] = pd.to_datetime(df['ate']).dt.date
    df['atrasado'] = df['subido_em'].isna() | (df['subido_em'] < prazo)
    return df
