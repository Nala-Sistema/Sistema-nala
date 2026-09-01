"""
MÓDULO: Fechamento mensal de estoque valorizado
Sistema Nala

Objetivo: no dia 1º de cada mês, transformar os relatórios de posição de
estoque de cada local em linhas normalizadas e valorizadas, para saber
quanto dinheiro está parado no galpão e em cada Full.

QUATRO FORMATOS, UM RESULTADO:

  A) ML — "Relatório geral de estoque" (relatório full ML <LOJA> <mês>.xlsx)
     Aba Resumo. Cabeçalho em TRÊS linhas (grupo, sub-grupo, sub-sub), com
     os dados por SKU logo abaixo. As outras abas do arquivo são recortes
     por qualidade do mesmo estoque — somá-las daria o mesmo número, mas
     sem a quebra por motivo, então lemos só a Resumo.

  B) Amazon — Inventário FBA (.csv)
     Traz o seller-SKU com sufixo -FBA, que NÃO é o nosso SKU. A resolução
     é por ASIN, pela mesma dim_config_marketplace que processar_amazon.py
     já usa nas vendas — nas 3 lojas isso resolve 100% das unidades.

  C) Shopee — Current Inventory Report (.xlsx), aba Total.
     Traz o nosso SKU direto em "Seller SKU ID".

  D) Galpão — Lista_de_Estoque_*.xlsx do Upseller.
     Armazém único ("My Warehouse"): o Upseller enxerga só o galpão, não
     os Fulls.

REGRAS DE NEGÓCIO (acertadas com o Thiago em 01/09/2026):

  disponível    = o que está no local e é nosso para vender.
                  ML: aptas para venda + em transferência.
                  Shopee: Sellable + Reserved.  Amazon: available + fc-transfer.
                  Galpão: Disponível + Ocupado ("Ocupado" é reserva de cliente).

  indisponível  = todo o resto que está no local. Vai valorizado igual —
                  unidade extraviada continua sendo dinheiro nosso parado —
                  guardando o motivo com o nome original do relatório, para
                  responder depois "quanto tenho preso em extravio no ML?".

  em trânsito   = o que saiu de um lugar e ainda não chegou no outro.
                  Existem DOIS tipos, e eles não se somam:
                    ENTRADA_FULL — saiu do galpão, indo para o CD do
                      marketplace. Some do Upseller ao sair e só reaparece
                      como "entrada pendente" no relatório do canal, então
                      SOMA no imobilizado. Não há dupla contagem: a coluna
                      "Em Trânsito(Transferência)" do Upseller vem zerada
                      (se um dia vier preenchida, `ler_upseller` avisa).
                    IMPORTACAO — compra do fornecedor a caminho do galpão.
                      Fica em linha própria, FORA do total de estoque:
                      mercadoria que ainda não chegou não é estoque.

  custo         = dim_produtos_custos.preco_compra, congelado na linha no
                  momento do upload. Nunca o preco_a_ser_considerado (que
                  embute embalagem, mão de obra e ads) e nunca o Custo Médio
                  do próprio Upseller — este diverge ~5,6% do nosso. Congelar
                  é o que faz um mês fechado parar de mudar de valor quando
                  alguém edita um custo, como aconteceu em agosto/2026.

Toda coluna é procurada PELO NOME, nunca por posição: o conjunto de colunas
muda entre versões do mesmo relatório (o inventário FBA de maio/2026 não tem
`fc-transfer`, o de setembro tem). Foi ler por posição que quebrou a leitura
do Resumo do Full em agosto.
"""

import json
import re
import unicodedata

import pandas as pd


TRANSITO_ENTRADA_FULL = 'ENTRADA_FULL'
TRANSITO_IMPORTACAO = 'IMPORTACAO'

LOCAL_GALPAO = 'GALPAO'

# Colunas do resultado normalizado, iguais para os quatro formatos.
COLUNAS_NORM = [
    'sku', 'disponivel', 'indisponivel', 'transito',
    'motivos_indisponivel', 'motivos_transito',
]


# ============================================================
# HELPERS DE LEITURA
# ============================================================

def _norm(texto):
    """
    Normaliza rótulo de coluna para comparação: sem acento, minúsculo, sem
    espaço duplo, e cortado na primeira quebra de linha.

    Os relatórios do ML embutem a explicação da coluna no próprio cabeçalho
    ("Estoque antigo\\nPor que afetam minha métrica?"), então o texto útil é
    só o que vem antes do \\n.
    """
    s = str(texto or '').split('\n')[0]
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\s+', ' ', s).strip().lower()
    # Célula vazia do pandas chega como float('nan'), que é truthy: sem isto
    # ela viraria o texto 'nan' e venceria o rótulo verdadeiro do cabeçalho
    # de cima ao montar as três linhas de header do ML.
    return '' if s in ('nan', 'none', 'nat') else s


def _int(valor):
    """
    Converte célula em inteiro, tolerando '1.234', '1,00', vazio e texto.

    Quantidade que não dá para ler vira 0 e não explode o fechamento inteiro
    — a conferência de total na tela é que denuncia se algo veio errado.
    """
    s = str(valor or '').strip()
    if not s or s.lower() in ('nan', 'none', '-'):
        return 0
    s = s.replace('.', '').replace(',', '.')
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return 0


def _texto(valor):
    s = str(valor or '').strip()
    return '' if s.lower() in ('nan', 'none') else s


def _br(valor, casas=0):
    """Número no formato brasileiro, para as mensagens de retorno."""
    return f'{valor:,.{casas}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def _achar_coluna(df, candidatos, exato=False):
    """
    Devolve o nome real da coluna que casa com algum candidato, ou None.

    `exato=True` é obrigatório quando um rótulo é prefixo de outro: no ML,
    "não aptas para venda" contém "aptas para venda", e casar por 'contém'
    somaria a quantidade errada na coluna certa.
    """
    mapa = {_norm(c): c for c in df.columns}
    for cand in candidatos:
        alvo = _norm(cand)
        if alvo in mapa:
            return mapa[alvo]
    if exato:
        return None
    for norm_col, col in mapa.items():
        for cand in candidatos:
            if _norm(cand) in norm_col:
                return col
    return None


def _linha(motivos):
    """Descarta motivos zerados — JSONB com uma dúzia de zeros só polui."""
    return {k: v for k, v in motivos.items() if v}


def _montar(registros):
    """Consolida por SKU e devolve o DataFrame normalizado."""
    if not registros:
        return pd.DataFrame(columns=COLUNAS_NORM)

    juntos = {}
    for r in registros:
        sku = r['sku']
        if sku not in juntos:
            juntos[sku] = {
                'sku': sku, 'disponivel': 0, 'indisponivel': 0, 'transito': 0,
                'motivos_indisponivel': {}, 'motivos_transito': {},
            }
        alvo = juntos[sku]
        alvo['disponivel'] += r['disponivel']
        alvo['indisponivel'] += r['indisponivel']
        alvo['transito'] += r['transito']
        for chave in ('motivos_indisponivel', 'motivos_transito'):
            for motivo, qtd in r[chave].items():
                alvo[chave][motivo] = alvo[chave].get(motivo, 0) + qtd

    return pd.DataFrame(list(juntos.values()), columns=COLUNAS_NORM)


# ============================================================
# A) MERCADO LIVRE — Relatório geral de estoque, aba Resumo
# ============================================================

# (rótulo no relatório, destino). A ordem importa: 'aptas para venda' só é
# testado depois de 'nao aptas para venda', e o casamento é exato.
_ML_COLUNAS = [
    ('nao aptas para venda',      'indisponivel'),
    ('aptas para venda',          'disponivel'),
    ('em transferencia',          'disponivel'),
    ('devolvidas pelo comprador', 'indisponivel'),
    ('extraviadas',               'indisponivel'),
    ('em revisao',                'indisponivel'),
    ('vendas canceladas',         'indisponivel'),
]

# 'Entrada pendente' aparece DUAS vezes na aba Resumo: uma sob "Unidades a
# caminho do Full" (a quantidade real) e outra sob "Unidades distribuídas por
# ação recomendada" (o mesmo número, repetido no bloco de recomendação). Sem
# olhar o grupo, pegaríamos a segunda e contaríamos trânsito em dobro.
_ML_GRUPO_CAMINHO = 'unidades a caminho do full'


def _ml_cabecalho(bruto):
    """
    Acha as três linhas de cabeçalho da aba Resumo e devolve, por coluna,
    o par (grupo, rótulo).

    O cabeçalho do ML ocupa 3 linhas: a primeira traz os grupos ("Unidades no
    Full"), as duas seguintes abrem os grupos em colunas. A linha do 'SKU' é
    a âncora — ela existe em todas as versões do relatório que vimos.
    """
    idx_hdr = None
    for i in range(min(30, len(bruto))):
        if any(_norm(v) == 'sku' for v in bruto.iloc[i].tolist()):
            idx_hdr = i
            break
    if idx_hdr is None:
        return None, None, None

    linha_grupo = [_norm(v) for v in bruto.iloc[idx_hdr].tolist()]
    sub1 = [_norm(v) for v in bruto.iloc[idx_hdr + 1].tolist()] if idx_hdr + 1 < len(bruto) else []
    sub2 = [_norm(v) for v in bruto.iloc[idx_hdr + 2].tolist()] if idx_hdr + 2 < len(bruto) else []

    # O grupo vale até a próxima coluna que declara um grupo novo.
    grupos, atual = [], ''
    for v in linha_grupo:
        if v:
            atual = v
        grupos.append(atual)

    def _em(lista, j):
        return lista[j] if j < len(lista) else ''

    # Rótulo da coluna é o mais específico que existir: sub-sub, senão sub,
    # senão o próprio grupo (colunas simples como SKU não têm sub).
    rotulos = []
    for j in range(len(linha_grupo)):
        rotulos.append(_em(sub2, j) or _em(sub1, j) or _em(linha_grupo, j))

    col_sku = next((j for j, v in enumerate(linha_grupo) if v == 'sku'), None)
    return idx_hdr, col_sku, list(zip(grupos, rotulos))


def ler_ml(arquivo):
    """
    Lê a aba Resumo do Relatório geral de estoque do ML.

    Devolve (df_normalizado, avisos).
    """
    avisos = []
    bruto = pd.read_excel(arquivo, sheet_name='Resumo', dtype=str, header=None)
    idx_hdr, col_sku, colunas = _ml_cabecalho(bruto)
    if col_sku is None:
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'Não achei a coluna SKU na aba Resumo — o arquivo é mesmo o '
            '"Relatório geral de estoque" do ML?'
        ]

    # Mapeia destino -> lista de índices de coluna
    destinos = {'disponivel': [], 'indisponivel': [], 'transito': []}
    nomes = {}
    for j, (grupo, rotulo) in enumerate(colunas):
        if rotulo == 'entrada pendente':
            if grupo == _ML_GRUPO_CAMINHO:
                destinos['transito'].append(j)
                nomes[j] = 'entrada pendente'
            continue
        for alvo, destino in _ML_COLUNAS:
            if rotulo == alvo or rotulo.startswith(alvo):
                destinos[destino].append(j)
                nomes[j] = alvo
                break

    if not destinos['disponivel']:
        avisos.append('Nenhuma coluna de unidades aptas/em transferência foi '
                      'encontrada — confira o layout do arquivo.')
    if not destinos['transito']:
        avisos.append('Não achei "Entrada pendente" sob "Unidades a caminho do '
                      'Full" — o trânsito deste arquivo entrou como zero.')

    registros = []
    for i in range(idx_hdr + 1, len(bruto)):
        linha = bruto.iloc[i].tolist()
        sku = _texto(linha[col_sku]) if col_sku < len(linha) else ''
        if not sku:
            continue
        reg = {'sku': sku, 'disponivel': 0, 'indisponivel': 0, 'transito': 0,
               'motivos_indisponivel': {}, 'motivos_transito': {}}
        for destino, indices in destinos.items():
            for j in indices:
                if j >= len(linha):
                    continue
                qtd = _int(linha[j])
                if not qtd:
                    continue
                reg[destino] += qtd
                if destino == 'indisponivel':
                    m = reg['motivos_indisponivel']
                    m[nomes[j]] = m.get(nomes[j], 0) + qtd
                elif destino == 'transito':
                    m = reg['motivos_transito']
                    m[nomes[j]] = m.get(nomes[j], 0) + qtd
        registros.append(reg)

    return _montar(registros), avisos


# ============================================================
# B) AMAZON — Inventário FBA (.csv)
# ============================================================

def buscar_mapa_asin(engine):
    """
    ASIN -> nosso SKU, da dim_config_marketplace.

    Mesma fonte que processar_amazon._buscar_config_amazon usa para as
    vendas. Ficar na mesma tabela é o que garante que estoque e venda falem
    do mesmo SKU: se um ASIN for reconfigurado, os dois seguem juntos.
    """
    sql = """
        SELECT asin, sku FROM dim_config_marketplace
        WHERE marketplace = 'AMAZON' AND ativo = true
          AND asin IS NOT NULL AND sku IS NOT NULL
    """
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()
    mapa = {}
    for asin, sku in rows:
        a, s = _texto(asin), _texto(sku)
        if a and s and a not in mapa:
            mapa[a] = s
    return mapa


def resolver_sku_amazon(sku_amz, asin, mapa_asin, mapeamento_skus):
    """
    Mesma ordem de resolução de processar_amazon.py: config por ASIN primeiro,
    depois corta o sufixo -FBA/-DBA, depois dim_sku_mapeamento.
    """
    asin = _texto(asin)
    if asin and asin in mapa_asin:
        return mapa_asin[asin]

    base = _texto(sku_amz).split('-FBA')[0].split('-DBA')[0].strip()
    if base in mapeamento_skus:
        return mapeamento_skus[base]
    sku_amz = _texto(sku_amz)
    if sku_amz in mapeamento_skus:
        return mapeamento_skus[sku_amz]
    return base or sku_amz


def ler_amazon(arquivo, mapa_asin=None, mapeamento_skus=None):
    """
    Lê o Inventário FBA. Devolve (df_normalizado, avisos).

    `inbound-quantity` é o total do que está a caminho e já é a soma de
    working + shipped + received — usar as três junto com ela contaria em
    dobro. Elas entram só como motivo.
    """
    mapa_asin = mapa_asin or {}
    mapeamento_skus = mapeamento_skus or {}
    avisos = []

    df = pd.read_csv(arquivo, dtype=str, sep=None, engine='python',
                     encoding='utf-8-sig')

    c_sku = _achar_coluna(df, ['sku'], exato=True)
    c_asin = _achar_coluna(df, ['asin'], exato=True)
    c_disp = _achar_coluna(df, ['available'], exato=True)
    c_fctr = _achar_coluna(df, ['fc-transfer'], exato=True)
    c_unfu = _achar_coluna(df, ['unfulfillable-quantity'], exato=True)
    c_inb = _achar_coluna(df, ['inbound-quantity'], exato=True)
    subs_inb = [(rot, _achar_coluna(df, [rot], exato=True)) for rot in
                ('inbound-working', 'inbound-shipped', 'inbound-received')]

    if c_sku is None or c_disp is None:
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'O arquivo não tem as colunas `sku` e `available` — não parece o '
            'Inventário FBA.'
        ]
    if c_asin is None:
        avisos.append('Sem coluna `asin`: os SKUs vão sair pelo corte do '
                      'sufixo -FBA, que resolve menos casos.')
    if c_fctr is None:
        avisos.append('Esta versão do relatório não traz `fc-transfer`; as '
                      'unidades em transferência entre centros ficaram de fora.')

    registros, sem_sku = [], 0
    for _, r in df.iterrows():
        sku = resolver_sku_amazon(r.get(c_sku), r.get(c_asin) if c_asin else '',
                                  mapa_asin, mapeamento_skus)
        if not sku:
            sem_sku += 1
            continue

        indisp = _int(r.get(c_unfu)) if c_unfu else 0
        motivos_ind = _linha({'unfulfillable': indisp})

        motivos_tr = _linha({rot: _int(r.get(col)) for rot, col in subs_inb if col})
        transito = _int(r.get(c_inb)) if c_inb else sum(motivos_tr.values())

        registros.append({
            'sku': sku,
            'disponivel': _int(r.get(c_disp)) + (_int(r.get(c_fctr)) if c_fctr else 0),
            'indisponivel': indisp,
            'transito': transito,
            'motivos_indisponivel': motivos_ind,
            'motivos_transito': motivos_tr,
        })

    if sem_sku:
        avisos.append(f'{sem_sku} linha(s) sem SKU legível foram ignoradas.')
    return _montar(registros), avisos


# ============================================================
# C) SHOPEE — Current Inventory Report, aba Total
# ============================================================

def ler_shopee(arquivo):
    """
    Lê a aba Total do Current Inventory Report. Devolve (df, avisos).

    Trânsito é só `Pending ASN Inbound`: o ASN é o aviso de embarque, emitido
    quando a carga sai. As colunas de IR (Inbound Request) são pedido de envio
    ainda não aprovado — a mercadoria continua no galpão e já está contada lá,
    então somá-las contaria duas vezes.
    """
    avisos = []
    try:
        df = pd.read_excel(arquivo, sheet_name='Total', dtype=str)
    except Exception:
        df = pd.read_excel(arquivo, sheet_name=0, dtype=str)
        avisos.append('Não achei a aba "Total"; li a primeira aba do arquivo.')

    c_sku = _achar_coluna(df, ['seller sku id', 'seller sku'])
    c_sell = _achar_coluna(df, ['sellable'], exato=True)
    c_resv = _achar_coluna(df, ['reserved'], exato=True)
    c_unse = _achar_coluna(df, ['unsellable'], exato=True)
    c_asn = _achar_coluna(df, ['pending asn inbound'])

    if c_sku is None or c_sell is None:
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'O arquivo não tem "Seller SKU ID" e "Sellable" — não parece o '
            'Current Inventory Report da Shopee.'
        ]

    registros = []
    for _, r in df.iterrows():
        sku = _texto(r.get(c_sku))
        if not sku:
            continue
        indisp = _int(r.get(c_unse)) if c_unse else 0
        asn = _int(r.get(c_asn)) if c_asn else 0
        registros.append({
            'sku': sku,
            'disponivel': _int(r.get(c_sell)) + (_int(r.get(c_resv)) if c_resv else 0),
            'indisponivel': indisp,
            'transito': asn,
            'motivos_indisponivel': _linha({'unsellable': indisp}),
            'motivos_transito': _linha({'pending asn inbound': asn}),
        })

    return _montar(registros), avisos


# ============================================================
# D) GALPÃO — Lista de Estoque do Upseller
# ============================================================

def ler_upseller(arquivo):
    """
    Lê a Lista de Estoque do Upseller (galpão). Devolve (df, avisos).

    O trânsito aqui é IMPORTACAO — compra a caminho do galpão, que fica fora
    do total de estoque. A perna galpão -> Full não aparece neste arquivo:
    "Em Trânsito(Transferência)" vem zerada e a unidade some do Disponível ao
    sair. Se um dia essa coluna vier preenchida, avisamos — passaria a haver
    dupla contagem com a "entrada pendente" do relatório do marketplace.
    """
    avisos = []
    df = pd.read_excel(arquivo, sheet_name=0, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    c_sku = _achar_coluna(df, ['sku'], exato=True)
    c_disp = _achar_coluna(df, ['disponivel'], exato=True)
    c_ocup = _achar_coluna(df, ['ocupado'], exato=True)
    c_compra = _achar_coluna(df, ['em transito(compra)', 'em transito (compra)'])
    c_transf = _achar_coluna(df, ['em transito(transferencia)',
                                  'em transito (transferencia)'])

    if c_sku is None or c_disp is None:
        return pd.DataFrame(columns=COLUNAS_NORM), [
            'O arquivo não tem "SKU" e "Disponível" — não parece a Lista de '
            'Estoque do Upseller.'
        ]

    registros, transferencia = [], 0
    for _, r in df.iterrows():
        sku = _texto(r.get(c_sku))
        if not sku:
            continue
        if c_transf:
            transferencia += _int(r.get(c_transf))
        compra = _int(r.get(c_compra)) if c_compra else 0
        registros.append({
            'sku': sku,
            'disponivel': _int(r.get(c_disp)) + (_int(r.get(c_ocup)) if c_ocup else 0),
            'indisponivel': 0,
            'transito': compra,
            'motivos_indisponivel': {},
            'motivos_transito': _linha({'importacao (compra)': compra}),
        })

    if transferencia:
        avisos.append(
            f'ATENÇÃO: "Em Trânsito(Transferência)" veio com {transferencia} '
            'unidade(s), e sempre veio zerada. Se isso for carga indo para um '
            'Full, ela também aparece como entrada pendente no relatório do '
            'marketplace e o fechamento vai contar duas vezes. Confira antes '
            'de gravar.'
        )
    return _montar(registros), avisos


# ============================================================
# VALORIZAÇÃO
# ============================================================

def buscar_precos_compra(engine):
    """
    SKU -> preço de compra. É este o custo do fechamento, por decisão do
    Thiago: sem embalagem, mão de obra nem ads, que são custo de vender e
    não de ter em estoque.
    """
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.sku, pc.preco_compra
            FROM dim_produtos p
            LEFT JOIN dim_produtos_custos pc ON pc.sku = p.sku
        """)
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    precos = {}
    for sku, preco in rows:
        s = _texto(sku)
        if not s:
            continue
        try:
            precos[s] = float(preco) if preco is not None else None
        except (TypeError, ValueError):
            precos[s] = None
    return precos


def valorizar(df, precos):
    """
    Acrescenta custo e valores ao DataFrame normalizado.

    SKU sem cadastro entra com custo nulo e valor zero — nunca é descartado.
    Sumir com a linha esconderia unidade real do fechamento; deixar valendo
    zero, com `sku_cadastrado` falso, deixa o buraco visível na tela.
    """
    if df is None or df.empty:
        vazio = df.copy() if df is not None else pd.DataFrame(columns=COLUNAS_NORM)
        for col in ('custo_unitario', 'valor_disponivel', 'valor_indisponivel',
                    'valor_transito'):
            vazio[col] = []
        vazio['sku_cadastrado'] = []
        return vazio

    out = df.copy()
    out['custo_unitario'] = out['sku'].map(lambda s: precos.get(s))
    out['sku_cadastrado'] = out['sku'].map(lambda s: s in precos)

    custo = out['custo_unitario'].fillna(0.0).astype(float)
    for qtd_col, val_col in (('disponivel', 'valor_disponivel'),
                             ('indisponivel', 'valor_indisponivel'),
                             ('transito', 'valor_transito')):
        out[val_col] = (out[qtd_col].astype(float) * custo).round(2)
    return out


# ============================================================
# GRAVAÇÃO
# ============================================================

def gravar_fechamento(engine, df, data_referencia, local_estoque, marketplace,
                      loja, transito_tipo, arquivo_nome):
    """
    Grava a posição de um local num mês de referência.

    Substitui o local inteiro naquele mês antes de inserir: subir o arquivo de
    novo tem que corrigir o fechamento, não empilhar em cima dele. E é por
    local, nunca pelo mês todo — refazer o ML-Nala não pode derrubar o galpão.

    Devolve dict com {inseridos, removidos, unidades, valor, mensagem}.
    """
    if df is None or df.empty:
        return {'inseridos': 0, 'removidos': 0, 'unidades': 0, 'valor': 0.0,
                'mensagem': 'Nada a gravar — o arquivo não produziu linhas.'}

    from psycopg2.extras import execute_values

    registros = []
    for _, r in df.iterrows():
        qtds = (int(r['disponivel']), int(r['indisponivel']), int(r['transito']))
        if not any(qtds):
            continue  # SKU zerado em tudo não é posição de estoque
        custo = r.get('custo_unitario')
        registros.append((
            data_referencia, local_estoque, str(r['sku'])[:120],
            marketplace, loja,
            qtds[0], qtds[1], qtds[2],
            transito_tipo if qtds[2] else None,
            json.dumps(r.get('motivos_indisponivel') or {}, ensure_ascii=False),
            json.dumps(r.get('motivos_transito') or {}, ensure_ascii=False),
            float(custo) if custo is not None and pd.notna(custo) else None,
            float(r.get('valor_disponivel') or 0),
            float(r.get('valor_indisponivel') or 0),
            float(r.get('valor_transito') or 0),
            bool(r.get('sku_cadastrado')),
            str(arquivo_nome)[:255],
        ))

    if not registros:
        return {'inseridos': 0, 'removidos': 0, 'unidades': 0, 'valor': 0.0,
                'mensagem': 'Nada a gravar — todos os SKUs vieram zerados.'}

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """DELETE FROM fact_estoque_mensal
               WHERE data_referencia = %s AND local_estoque = %s""",
            (data_referencia, local_estoque),
        )
        removidos = cursor.rowcount

        execute_values(cursor, """
            INSERT INTO fact_estoque_mensal (
                data_referencia, local_estoque, sku, marketplace, loja,
                qtd_disponivel, qtd_indisponivel, qtd_transito, transito_tipo,
                motivos_indisponivel, motivos_transito,
                custo_unitario, valor_disponivel, valor_indisponivel,
                valor_transito, sku_cadastrado, arquivo_origem, data_upload
            ) VALUES %s
        """, registros,
             template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,"
                      "%s,%s,%s,%s,%s,%s,NOW())",
             page_size=500)

        inseridos = len(registros)
        conn.commit()
        cursor.close()
    finally:
        conn.close()

    unidades = int(df['disponivel'].sum() + df['indisponivel'].sum())
    valor = float(df['valor_disponivel'].sum() + df['valor_indisponivel'].sum())
    msg = (f'{inseridos} SKU(s) gravado(s) — {_br(unidades)} unidade(s) em '
           f'estoque, R$ {_br(valor, 2)}.')
    if removidos:
        msg += f' Substituí {removidos} linha(s) do fechamento anterior deste local.'
    return {'inseridos': inseridos, 'removidos': removidos,
            'unidades': unidades, 'valor': valor, 'mensagem': msg}
