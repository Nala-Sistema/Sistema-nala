"""
MÓDULO: Configuração das campanhas de Ads (ROAS objetivo, orçamento, sinal)
Sistema Nala

POR QUE ESTE MÓDULO EXISTE
  O relatório semanal de Ads traz o RESULTADO (gasto, receita, ACOS, ROAS
  realizado, cliques, impressões). Ele não traz a ALAVANCA.

  No Mercado Livre quase nunca se aumenta orçamento — e o ML raramente gasta
  tudo o que se coloca. O que de fato se opera é o ROAS OBJETIVO: baixa-se um
  pouco e a plataforma passa a gastar mais. Sem esse número, o módulo de
  estratégia consegue dizer "este anúncio piorou", mas nunca "piorou porque
  em 04/09 o objetivo caiu de 14.9 para 12".

  Este módulo guarda esse número. São três campos por campanha:
  ROAS objetivo, orçamento diário e o sinal que a própria plataforma exibe.

POR QUE É UM SNAPSHOT, E NÃO UM "ESTADO ATUAL"
  A tabela é chaveada por (marketplace, loja, campanha, DATA DA CAPTURA).
  Guardar só o valor de hoje responderia "quanto está agora?", que é a
  pergunta fácil. A pergunta que decide dinheiro é "o que mudou, quando, e o
  que aconteceu depois" — e essa só o histórico responde.

  Consequência prática: capturar toda segunda dá uma linha por semana;
  capturar no dia em que o gestor mexe dá a data exata do evento. A tabela
  aceita as duas coisas sem mudar nada, inclusive várias capturas no mesmo
  dia. É de propósito: começa semanal e aperta o ritmo quando quiser.

O SINAL DA PLATAFORMA NÃO É DIAGNÓSTICO
  A coluna chama-se `diagnostico_ml`, com a origem no nome, e nunca
  `diagnostico`. O ML exibe "APRENDENDO" / "Excelente" e a Shopee sugere
  metas de ROAS, mas o interesse deles é que se gaste mais — o nosso é
  margem. Seguir esses conselhos custa dinheiro. O campo entra como baliza,
  jamais como veredito, e nenhuma recomendação do módulo pode tê-lo como
  causa principal.

DE ONDE VEM O DADO
  Da tela de campanhas do painel, que não tem export. Dois formatos entram:

  1. CSV do capturador (o "bookmarklet" da barra de favoritos). É o caminho
     normal: um clique, ele varre a tabela virando as páginas e baixa o CSV
     já com a data/hora da captura dentro.
  2. HTML da página salva com Ctrl+S. É o plano B — funciona hoje, sem
     depender de o navegador deixar o capturador rodar, mas pega só a página
     que estava na tela (o ML pagina de 10 em 10).

  Ler os dois custa pouco e evita ficar refém de um só.

POR QUE REGEX E NÃO UM PARSER DE HTML
  `beautifulsoup4` NÃO está no requirements.txt. Está instalado na máquina
  local, então usar bs4 funcionaria aqui e quebraria no Streamlit Cloud, que
  instala só o que está no arquivo. Regex sobre a <table> resolve e não
  acrescenta dependência.

A LIGAÇÃO COM O SKU JÁ EXISTE
  Nada aqui refaz match. `fact_ads_performance` já grava campanha,
  codigo_anuncio (MLB) e título na mesma linha, vindos do relatório semanal.
  Basta juntar por (marketplace, loja, campanha). Uma campanha pode ter mais
  de um MLB — é o caso do anúncio "Movido" — e nesse caso o ROAS objetivo da
  campanha vale para todos os anúncios dela.
"""

import csv
import io
import re
from datetime import datetime

import pandas as pd


MARKETPLACE_ML = 'MERCADO LIVRE'
MARKETPLACE_SHOPEE = 'SHOPEE'

COLUNAS_NORM = [
    'campanha', 'roas_objetivo', 'orcamento_diario', 'diagnostico_ml',
]


# ============================================================
# HELPERS
# ============================================================

def _texto(v):
    if v is None:
        return ''
    s = str(v).replace('\xa0', ' ').replace('&nbsp;', ' ')
    return re.sub(r'\s+', ' ', s).strip()


def _num(v):
    """
    Converte o número como o painel escreve para float.

    O painel mistura os dois formatos na mesma tela: "R$ 12" e "R$ 213,6"
    (vírgula decimal, padrão BR) convivem com "9.9x" e "14.9x" no ROAS
    objetivo (ponto decimal, porque o campo é editável e o ML usa o formato
    interno ali). Tratar tudo como BR transformaria 9.9 em 99.
    """
    s = _texto(v)
    if not s or s == '-':
        return None
    s = s.replace('R$', '').replace('x', '').replace('%', '').strip()
    s = re.sub(r'[▲▼●↑↓]', '', s).strip()
    if not s:
        return None
    tem_ponto, tem_virgula = '.' in s, ',' in s
    if tem_ponto and tem_virgula:
        # "1.234,56" — ponto é milhar
        s = s.replace('.', '').replace(',', '.')
    elif tem_virgula:
        s = s.replace(',', '.')
    # só ponto: já está no formato certo ("9.9")
    try:
        return float(s)
    except ValueError:
        return None


def _limpar_nome_campanha(v):
    """
    Tira o "1 anúncio patrocinado" que o ML imprime embaixo do nome.

    Esse sufixo vem na MESMA célula do nome e, se ficar, o nome nunca casa
    com o `campanha` de fact_ads_performance — que é justamente a chave que
    liga esta tabela ao SKU.
    """
    s = _texto(v)
    s = re.sub(r'\s*\d+\s+an[úu]ncios?\s+patrocinados?.*$', '', s, flags=re.I)
    s = re.sub(r'^Selecionar campanha\s*', '', s, flags=re.I)
    return s.strip()


def _diagnostico(v):
    """
    Reduz o texto do sinal à etiqueta.

    O ML imprime "APRENDENDO Sua campanha vai se estabilizar em 2 dias." numa
    célula só. Guardar a frase inteira encheria a coluna de texto que muda
    todo dia ("em 2 dias" vira "em 1 dia") e faria a detecção de mudança
    disparar sozinha sem nada ter mudado.
    """
    s = _texto(v)
    if not s:
        return ''
    for rotulo in ('APRENDENDO', 'Excelente', 'Bom', 'Regular', 'Ruim',
                   'Pausado', 'Ativo'):
        if s.upper().startswith(rotulo.upper()):
            return rotulo
    return s[:40]


# ============================================================
# LEITURA — HTML DA PÁGINA SALVA
# ============================================================

def _celulas(bloco_tr):
    """Texto de cada <td>/<th> da linha, sem tags e sem script/style/svg."""
    x = re.sub(r'<(script|style|svg|path)[^>]*>.*?</\1>', ' ', bloco_tr,
               flags=re.S | re.I)
    saida = []
    for td in re.findall(r'<t[dh]\b.*?</t[dh]>', x, re.S | re.I):
        txt = re.sub(r'<[^>]+>', '\t', td)
        saida.append(' '.join(c.strip() for c in txt.split('\t') if c.strip()))
    return saida


def _achar_indice(cabecalho, *termos):
    """Índice da 1ª coluna cujo título contenha algum dos termos."""
    for i, c in enumerate(cabecalho):
        alvo = _texto(c).lower()
        for t in termos:
            if t in alvo:
                return i
    return None


def ler_html_campanhas_ml(conteudo, nome_arquivo=''):
    """
    Lê a página de campanhas do ML salva com Ctrl+S.

    As colunas são PROCURADAS pelo nome do cabeçalho, não fixadas por
    posição: o painel permite ao usuário escolher e reordenar colunas, então
    posição fixa quebraria em silêncio na tela de outro gestor.

    Devolve (df, meta). `meta['avisos']` traz o que o operador precisa saber
    — em especial o aviso de paginação, porque o ML mostra 10 campanhas por
    página e a página salva contém só o que estava na tela.
    """
    meta = {'avisos': [], 'marketplace': MARKETPLACE_ML,
            'total_pagina': 0, 'total_conta': None,
            'arquivo': nome_arquivo or ''}
    vazio = pd.DataFrame(columns=COLUNAS_NORM)

    if isinstance(conteudo, bytes):
        html = conteudo.decode('utf-8', errors='replace')
    else:
        html = conteudo or ''
    if not html.strip():
        meta['avisos'].append('O arquivo veio vazio.')
        return vazio, meta

    m = re.search(r'<table.*?</table>', html, re.S | re.I)
    if not m:
        meta['avisos'].append(
            'Não encontrei a tabela de campanhas neste arquivo. Confirme que '
            'salvou a página de **Publicidade → Campanhas** do ML, com a '
            'tabela visível na tela.')
        return vazio, meta

    linhas_tr = re.findall(r'<tr\b.*?</tr>', m.group(0), re.S | re.I)
    if len(linhas_tr) < 2:
        meta['avisos'].append('A tabela veio sem linhas de campanha.')
        return vazio, meta

    cabecalho = _celulas(linhas_tr[0])
    i_nome = _achar_indice(cabecalho, 'nome da campanha', 'campanha')
    i_roas = _achar_indice(cabecalho, 'roas objetivo', 'roas alvo',
                           'meta de roas')
    i_orc = _achar_indice(cabecalho, 'orçamento diário', 'orcamento diario',
                          'orçamento')
    i_diag = _achar_indice(cabecalho, 'diagnóstico', 'diagnostico')

    if i_nome is None or i_roas is None:
        meta['avisos'].append(
            'A tabela não tem as colunas "Nome da campanha" e "ROAS '
            'Objetivo". No painel, ative essas colunas antes de salvar.')
        return vazio, meta

    registros = []
    for tr in linhas_tr[1:]:
        c = _celulas(tr)
        if len(c) <= max(i for i in (i_nome, i_roas, i_orc, i_diag)
                         if i is not None):
            continue
        nome = _limpar_nome_campanha(c[i_nome])
        if not nome:
            continue
        registros.append({
            'campanha': nome[:200],
            'roas_objetivo': _num(c[i_roas]),
            'orcamento_diario': _num(c[i_orc]) if i_orc is not None else None,
            'diagnostico_ml': (_diagnostico(c[i_diag])
                               if i_diag is not None else ''),
        })

    df = pd.DataFrame(registros, columns=COLUNAS_NORM)
    meta['total_pagina'] = len(df)

    # Paginação: "10 de 61 campanhas" aparece no rodapé da tabela.
    texto_puro = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html))
    mp = re.search(r'(\d+)\s+de\s+(\d+)\s+campanhas', texto_puro)
    if mp:
        na_pagina, no_total = int(mp.group(1)), int(mp.group(2))
        meta['total_conta'] = no_total
        if no_total > na_pagina:
            meta['avisos'].append(
                f'Esta página tem **{na_pagina} de {no_total} campanhas**. '
                f'O resto ficou de fora. Aumente o número de itens por '
                f'página no painel antes de salvar, ou use o capturador.')
    return df, meta


# ============================================================
# LEITURA — CSV DO CAPTURADOR
# ============================================================

def ler_csv_captura(conteudo, nome_arquivo=''):
    """
    Lê o CSV gerado pelo capturador do navegador.

    Formato esperado (cabeçalho na 1ª linha, separador ';'):
        data_captura;campanha;roas_objetivo;orcamento_diario;diagnostico_ml

    `data_captura` vem de dentro do arquivo porque é a data da FOTO, que não
    é a data do upload: uma captura feita na quinta pode só ser subida na
    segunda, e gravar a data do upload apagaria justamente o "quando" que dá
    sentido à tabela.
    """
    meta = {'avisos': [], 'marketplace': None, 'total_pagina': 0,
            'total_conta': None, 'data_captura': None,
            'arquivo': nome_arquivo or ''}
    vazio = pd.DataFrame(columns=COLUNAS_NORM)

    if isinstance(conteudo, bytes):
        texto = conteudo.decode('utf-8-sig', errors='replace')
    else:
        texto = conteudo or ''
    if not texto.strip():
        meta['avisos'].append('O arquivo veio vazio.')
        return vazio, meta

    amostra = texto[:2000]
    sep = ';' if amostra.count(';') >= amostra.count(',') else ','
    leitor = csv.DictReader(io.StringIO(texto), delimiter=sep)
    if not leitor.fieldnames:
        meta['avisos'].append('O CSV veio sem cabeçalho.')
        return vazio, meta

    campos = {re.sub(r'[^a-z_]', '', (c or '').strip().lower().replace(' ', '_')): c
              for c in leitor.fieldnames}

    def col(*nomes):
        for n in nomes:
            if n in campos:
                return campos[n]
        return None

    c_nome = col('campanha', 'nome_da_campanha', 'nome')
    c_roas = col('roas_objetivo', 'roas_alvo', 'meta_de_roas', 'roas')
    c_orc = col('orcamento_diario', 'orcamento')
    c_diag = col('diagnostico_ml', 'diagnostico')
    c_data = col('data_captura', 'data')
    c_mkt = col('marketplace')

    if not c_nome or not c_roas:
        meta['avisos'].append(
            'O CSV precisa ter ao menos as colunas `campanha` e '
            '`roas_objetivo`. Recapture com o capturador atualizado.')
        return vazio, meta

    registros = []
    datas = set()
    marketplaces = set()
    for linha in leitor:
        nome = _limpar_nome_campanha(linha.get(c_nome))
        if not nome:
            continue
        registros.append({
            'campanha': nome[:200],
            'roas_objetivo': _num(linha.get(c_roas)),
            'orcamento_diario': _num(linha.get(c_orc)) if c_orc else None,
            'diagnostico_ml': (_diagnostico(linha.get(c_diag))
                               if c_diag else ''),
        })
        if c_data:
            d = _texto(linha.get(c_data))[:19]
            if d:
                datas.add(d)
        if c_mkt:
            mk = _texto(linha.get(c_mkt)).upper()
            if mk:
                marketplaces.add(mk)

    df = pd.DataFrame(registros, columns=COLUNAS_NORM)
    meta['total_pagina'] = len(df)

    if datas:
        bruta = sorted(datas)[0]
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                meta['data_captura'] = datetime.strptime(bruta[:len(
                    datetime.now().strftime(fmt))], fmt)
                break
            except ValueError:
                continue
        if meta['data_captura'] is None:
            meta['avisos'].append(
                f'Não entendi a data de captura do arquivo ("{bruta}"). '
                f'Vou usar a data de hoje — confira antes de gravar.')
    if marketplaces:
        alvo = marketplaces.pop()
        if 'SHOPEE' in alvo:
            meta['marketplace'] = MARKETPLACE_SHOPEE
        elif 'MERCADO' in alvo or 'ML' in alvo:
            meta['marketplace'] = MARKETPLACE_ML
    return df, meta


def ler_captura_campanhas(arquivo, nome_arquivo=''):
    """
    Porta de entrada única: decide entre CSV e HTML pelo conteúdo.

    Decide pelo conteúdo e não pela extensão porque a página salva do ML
    às vezes chega com nome trocado, e um .html renomeado para .csv daria um
    erro incompreensível para o gestor.
    """
    nome = nome_arquivo or getattr(arquivo, 'name', '') or ''
    bruto = arquivo.read() if hasattr(arquivo, 'read') else arquivo
    if isinstance(bruto, bytes):
        espia = bruto[:4000].decode('utf-8', errors='replace').lower()
    else:
        espia = str(bruto)[:4000].lower()
    if '<table' in espia or '<html' in espia or '<!doctype html' in espia:
        return ler_html_campanhas_ml(bruto, nome)
    return ler_csv_captura(bruto, nome)


# ============================================================
# SCHEMA
# ============================================================

def garantir_tabela_campanha_config(engine):
    """
    Cria `fact_ads_campanha_config`.

    A chave única inclui `data_captura` com hora: duas fotos no mesmo dia são
    legítimas (o gestor mexe no ROAS de manhã e de novo à tarde) e chavear só
    pela data faria a segunda sobrescrever a primeira, perdendo exatamente o
    evento que interessa registrar.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_ads_campanha_config (
                id                SERIAL PRIMARY KEY,
                marketplace       VARCHAR(30)  NOT NULL,
                loja              VARCHAR(60)  NOT NULL,
                campanha          VARCHAR(200) NOT NULL,
                data_captura      TIMESTAMP    NOT NULL,
                roas_objetivo     NUMERIC(12,4),
                orcamento_diario  NUMERIC(12,2),
                diagnostico_ml    VARCHAR(40),
                arquivo_origem    VARCHAR(255),
                data_importacao   TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_campanha_config
            ON fact_ads_campanha_config
               (marketplace, loja, campanha, data_captura)
        """)
        # A consulta mais frequente é "última foto desta loja" e depois
        # "histórico desta campanha" — as duas passam por aqui.
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS ix_campanha_config_loja_data
            ON fact_ads_campanha_config (marketplace, loja, data_captura DESC)
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

def gravar_campanha_config(engine, df, marketplace, loja, data_captura,
                           arquivo_nome=''):
    """
    Grava uma foto inteira em lote.

    Regravar a mesma foto substitui em vez de duplicar (ON CONFLICT sobre a
    chave), para que subir de novo um arquivo corrigido não exija limpar
    nada antes — mesmo comportamento do upload do relatório de ads.

    Devolve dict {gravadas, ignoradas, mensagem}.
    """
    resultado = {'gravadas': 0, 'ignoradas': 0, 'mensagem': ''}
    if df is None or df.empty:
        resultado['mensagem'] = 'Nada a gravar — o arquivo não produziu linhas.'
        return resultado
    if not loja or not marketplace:
        resultado['mensagem'] = 'Loja ou marketplace não informado.'
        return resultado
    if data_captura is None:
        resultado['mensagem'] = 'Data da captura não informada.'
        return resultado

    from psycopg2.extras import execute_values

    linhas = []
    ignoradas = 0
    for _, r in df.iterrows():
        campanha = _texto(r.get('campanha'))
        # Sem ROAS objetivo a linha não cumpre o propósito da tabela: o
        # orçamento sozinho não é a alavanca que se opera.
        if not campanha or r.get('roas_objetivo') is None:
            ignoradas += 1
            continue
        linhas.append((
            marketplace, loja, campanha[:200], data_captura,
            r.get('roas_objetivo'), r.get('orcamento_diario'),
            (r.get('diagnostico_ml') or '')[:40],
            (arquivo_nome or '')[:255],
        ))

    if not linhas:
        resultado['ignoradas'] = ignoradas
        resultado['mensagem'] = (
            'Nenhuma campanha com ROAS objetivo no arquivo — nada gravado.')
        return resultado

    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        execute_values(cursor, """
            INSERT INTO fact_ads_campanha_config
                (marketplace, loja, campanha, data_captura, roas_objetivo,
                 orcamento_diario, diagnostico_ml, arquivo_origem)
            VALUES %s
            ON CONFLICT (marketplace, loja, campanha, data_captura)
            DO UPDATE SET
                roas_objetivo = EXCLUDED.roas_objetivo,
                orcamento_diario = EXCLUDED.orcamento_diario,
                diagnostico_ml = EXCLUDED.diagnostico_ml,
                arquivo_origem = EXCLUDED.arquivo_origem,
                data_importacao = NOW()
        """, linhas, page_size=500)
        gravadas = cursor.rowcount
        conn.commit()
        resultado['gravadas'] = gravadas
        resultado['ignoradas'] = ignoradas
        resultado['mensagem'] = (
            f'{gravadas} campanha(s) registrada(s) para {loja} na foto de '
            f'{data_captura:%d/%m/%Y %H:%M}.'
            + (f' {ignoradas} linha(s) sem ROAS objetivo ignorada(s).'
               if ignoradas else ''))
        return resultado
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        resultado['mensagem'] = f'Erro ao gravar: {str(e)[:200]}'
        return resultado
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
# LEITURA DO BANCO
# ============================================================

def ultima_captura(engine, marketplace, loja):
    """Data da foto mais recente desta loja, ou None se nunca houve."""
    try:
        df = pd.read_sql(
            "SELECT MAX(data_captura) AS ultima "
            "FROM fact_ads_campanha_config "
            "WHERE marketplace = %(mk)s AND loja = %(loja)s",
            engine, params={'mk': marketplace, 'loja': loja})
        if df.empty or pd.isna(df.iloc[0]['ultima']):
            return None
        return pd.to_datetime(df.iloc[0]['ultima']).to_pydatetime()
    except Exception:
        return None


def detectar_mudancas(engine, marketplace, loja, data_captura):
    """
    Compara a foto de `data_captura` com a foto imediatamente anterior.

    É isto que transforma a tabela em log: sem a comparação, o histórico
    existe mas ninguém olha. Com ela, o sistema consegue afirmar "em 04/09 o
    ROAS objetivo do MLB4033433891 caiu de 14.9 para 12" sem ninguém digitar.

    Campanhas novas e campanhas que sumiram entram como evento próprio —
    campanha que some é pausa ou exclusão, que é decisão tão relevante
    quanto mexer no objetivo.

    Devolve DataFrame com campanha, tipo, antes, depois. Vazio na primeira
    foto (não há com o que comparar) — o que é resposta correta, não erro.
    """
    colunas = ['campanha', 'tipo', 'campo', 'antes', 'depois']
    try:
        anterior = pd.read_sql(
            "SELECT MAX(data_captura) AS d FROM fact_ads_campanha_config "
            "WHERE marketplace = %(mk)s AND loja = %(loja)s "
            "  AND data_captura < %(dt)s",
            engine, params={'mk': marketplace, 'loja': loja,
                            'dt': data_captura})
        if anterior.empty or pd.isna(anterior.iloc[0]['d']):
            return pd.DataFrame(columns=colunas)
        data_ant = anterior.iloc[0]['d']

        df = pd.read_sql(
            "SELECT campanha, data_captura, roas_objetivo, orcamento_diario, "
            "       diagnostico_ml "
            "FROM fact_ads_campanha_config "
            "WHERE marketplace = %(mk)s AND loja = %(loja)s "
            "  AND data_captura IN (%(a)s, %(b)s)",
            engine, params={'mk': marketplace, 'loja': loja,
                            'a': data_ant, 'b': data_captura})
        if df.empty:
            return pd.DataFrame(columns=colunas)

        df['data_captura'] = pd.to_datetime(df['data_captura'])
        alvo = pd.to_datetime(data_captura)
        agora = df[df['data_captura'] == alvo].set_index('campanha')
        antes = df[df['data_captura'] != alvo].set_index('campanha')

        eventos = []
        for campanha in agora.index.difference(antes.index):
            eventos.append({
                'campanha': campanha, 'tipo': 'CAMPANHA NOVA',
                'campo': 'roas_objetivo', 'antes': None,
                'depois': agora.loc[campanha, 'roas_objetivo']})
        for campanha in antes.index.difference(agora.index):
            eventos.append({
                'campanha': campanha, 'tipo': 'CAMPANHA SUMIU',
                'campo': 'roas_objetivo',
                'antes': antes.loc[campanha, 'roas_objetivo'],
                'depois': None})

        rotulos = {
            'roas_objetivo': 'ROAS OBJETIVO',
            'orcamento_diario': 'ORÇAMENTO DIÁRIO',
            'diagnostico_ml': 'SINAL DA PLATAFORMA',
        }
        for campanha in agora.index.intersection(antes.index):
            for campo, rotulo in rotulos.items():
                v_antes = antes.loc[campanha, campo]
                v_depois = agora.loc[campanha, campo]
                if pd.isna(v_antes) and pd.isna(v_depois):
                    continue
                if campo == 'diagnostico_ml':
                    igual = _texto(v_antes) == _texto(v_depois)
                else:
                    try:
                        igual = (v_antes is not None and v_depois is not None
                                 and abs(float(v_antes) - float(v_depois)) < 1e-6)
                    except (TypeError, ValueError):
                        igual = _texto(v_antes) == _texto(v_depois)
                if igual:
                    continue
                sentido = ''
                if campo != 'diagnostico_ml':
                    try:
                        sentido = (' ↓' if float(v_depois) < float(v_antes)
                                   else ' ↑')
                    except (TypeError, ValueError):
                        sentido = ''
                eventos.append({
                    'campanha': campanha, 'tipo': rotulo + sentido,
                    'campo': campo, 'antes': v_antes, 'depois': v_depois})

        if not eventos:
            return pd.DataFrame(columns=colunas)
        return pd.DataFrame(eventos, columns=colunas).sort_values(
            ['tipo', 'campanha']).reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=colunas)


def config_com_anuncios(engine, marketplace, loja, data_captura=None):
    """
    Junta a foto das campanhas com os anúncios (MLB) do relatório de ads.

    A junção é por (marketplace, loja, campanha) contra
    `fact_ads_performance`, usando o período de relatório mais recente. É a
    ponte campanha → MLB → SKU, e ela já vem pronta do relatório semanal:
    nenhum match manual é necessário aqui.

    Uma campanha pode devolver mais de uma linha quando tem mais de um
    anúncio (caso "Movido") — o ROAS objetivo é da campanha e vale para
    todos eles.
    """
    try:
        if data_captura is None:
            data_captura = ultima_captura(engine, marketplace, loja)
        if data_captura is None:
            return pd.DataFrame()
        return pd.read_sql("""
            WITH ult AS (
                SELECT MAX(periodo_fim) AS fim
                FROM fact_ads_performance
                WHERE marketplace = %(mk)s AND loja = %(loja)s
            )
            SELECT c.campanha,
                   c.roas_objetivo,
                   c.orcamento_diario,
                   c.diagnostico_ml,
                   p.codigo_anuncio,
                   p.titulo,
                   p.gasto_ads,
                   p.receita_ads,
                   p.acos,
                   p.roas AS roas_realizado,
                   p.periodo_inicio,
                   p.periodo_fim
            FROM fact_ads_campanha_config c
            LEFT JOIN fact_ads_performance p
                   ON p.marketplace = c.marketplace
                  AND p.loja        = c.loja
                  AND p.campanha    = c.campanha
                  AND p.periodo_fim = (SELECT fim FROM ult)
            WHERE c.marketplace  = %(mk)s
              AND c.loja         = %(loja)s
              AND c.data_captura = %(dt)s
            ORDER BY c.campanha, p.codigo_anuncio
        """, engine, params={'mk': marketplace, 'loja': loja,
                             'dt': data_captura})
    except Exception:
        return pd.DataFrame()
