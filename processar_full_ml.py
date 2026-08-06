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
     armazenagem atribuída, sem rateio.

Regra de qual usar para quê:
  - armazenagem          -> arquivo B, aba Detalhe   (direta por anúncio)
  - coleta               -> arquivo A, aba coleta    (direta por SKU/MLB)
  - armazenamento longo  -> arquivo A, aba prolongado(direta por SKU/MLB)

A aba "Tarifa de armazenamento" do arquivo A serve apenas como total de
conferência — usá-la para atribuir obrigaria a ratear, o que é pior.

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
    'tipo', 'sku', 'codigo_anuncio', 'produto',
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
            for mlb in mlbs:
                linhas.append({
                    'tipo': tipo,
                    'sku': _texto(row[col_sku]) if col_sku else '',
                    'codigo_anuncio': mlb,
                    'produto': _texto(row[col_prod])[:120] if col_prod else '',
                    'valor': round(valor / len(mlbs), 4),
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

def ler_custos_armazenamento(caminho):
    """
    Lê o detalhe de armazenagem, que traz o custo por anúncio.

    A aba Detalhe alterna linhas: uma com o produto e os custos diários,
    a seguinte com as unidades armazenadas de cada dia. Só a primeira
    interessa para valor — a de unidades é identificada por não ter SKU.

    Usa a coluna "Custos acumulados até <data>", que já é o total do ciclo
    por anúncio, em vez de somar as colunas diárias.

    Devolve (df_normalizado, avisos).
    """
    avisos = []
    df = pd.read_excel(caminho, sheet_name='Detalhe', header=4)
    df.columns = [str(c).strip() for c in df.columns]

    col_sku = next((c for c in df.columns if c.strip().upper() == 'SKU'), None)
    col_anun = _achar_col(df, 'anúncio') or _achar_col(df, 'anuncio')
    col_prod = _achar_col(df, 'produto')
    col_acum = _achar_col(df, 'acumulados')
    col_tam = _achar_col(df, 'tamanho')

    if not (col_sku and col_anun and col_acum):
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'Arquivo de armazenamento sem as colunas esperadas (SKU / anúncio / acumulados).'
        ]

    # Período vem do título da aba, não de uma coluna
    ini = fim = None
    cab = pd.read_excel(caminho, sheet_name='Detalhe', header=None, nrows=1)
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', str(cab.iloc[0].tolist()))
    if m:
        ini = pd.to_datetime(m.group(1), dayfirst=True)
        fim = pd.to_datetime(m.group(2), dayfirst=True)
    else:
        avisos.append('Não consegui ler o período no cabeçalho — informe manualmente.')

    linhas = []
    for _, row in df.iterrows():
        sku = _texto(row[col_sku])
        mlbs = extrair_mlbs(row[col_anun])
        valor = _num(row[col_acum])
        if not sku or valor == 0:
            continue  # linha de unidades, ou anúncio sem custo no ciclo

        # Estoque no Full de produto sem anúncio ativo: mantém a linha com o
        # SKU e anúncio vazio, em vez de descartar — senão o total não fecha
        # com o Resumo. Nos arquivos reais isso é ~0,1% do valor.
        if not mlbs:
            mlbs = ['']

        for mlb in mlbs:
            linhas.append({
                'tipo': TIPO_ARMAZENAGEM,
                'sku': sku,
                'codigo_anuncio': mlb,
                'produto': _texto(row[col_prod])[:120] if col_prod else '',
                'valor': round(valor / len(mlbs), 4),
                'unidades': 0.0,
                'periodo_inicio': ini,
                'periodo_fim': fim,
                'observacao': (
                    (f'{len(mlbs)} anúncios na linha; ' if len(mlbs) > 1 else '')
                    + (f'tamanho {_texto(row[col_tam])}' if col_tam else '')
                ).strip('; '),
            })

    df_out = pd.DataFrame(linhas, columns=COLUNAS_NORM)

    # Conferência contra a aba Resumo — se divergir, o parser errou algo
    try:
        res = pd.read_excel(caminho, sheet_name='Resumo', header=None)
        total_resumo = pd.to_numeric(res[5], errors='coerce').dropna().max()
        total_lido = df_out['valor'].sum()
        if total_resumo and abs(total_lido - total_resumo) > 0.05:
            avisos.append(
                f'DIVERGÊNCIA: Detalhe soma R$ {total_lido:,.2f} '
                f'mas o Resumo diz R$ {total_resumo:,.2f}.'
            )
    except Exception:
        avisos.append('Não foi possível conferir contra a aba Resumo.')

    return df_out, avisos


# ============================================================
# IDENTIFICAÇÃO DA LOJA
# ============================================================

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
