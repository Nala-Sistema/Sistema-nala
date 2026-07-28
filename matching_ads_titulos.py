"""
matching_ads_titulos.py — Dicionário de títulos → SKU (Shopee) e match de anúncios

Sistema Nala — Módulo Ads — Passo 1 (fundação)

Objetivo:
  O relatório de VENDAS da Shopee traz, em cada linha, o título do produto
  ("Nome do Produto") + o SKU vendido + o "SKU pai" (Nº de referência do SKU
  principal, que agrupa todas as variações de um mesmo anúncio).

  Este módulo transforma esses títulos num DICIONÁRIO acumulativo
  (dim_titulo_sku_shopee): título normalizado → conjunto de SKUs que já
  venderam sob aquele título. É contra esse dicionário que os títulos dos
  ANÚNCIOS (fact_ads_shopee.nome_anuncio) serão casados depois.

  Regra importante: só entra no dicionário produto que JÁ VENDEU pelo menos
  uma vez. Produto anunciado mas sem nenhuma venda não casa automaticamente —
  fica para vínculo manual (e é um alerta em si: gasto de ads sem retorno).

Convenções do projeto respeitadas:
  - raw_connection() + cursor em TODAS as queries (nunca pd.read_sql com engine)
  - SAVEPOINT por linha em loops de INSERT/UPDATE
  - CREATE TABLE IF NOT EXISTS (schema criado/garantido em runtime, como app.py)
"""

import re
import unicodedata
from datetime import date


# ============================================================
# NORMALIZAÇÃO DE TÍTULO
# ------------------------------------------------------------
# Usada dos DOIS lados (venda e anúncio) para comparar "maçã com maçã":
#   - tira acento e caixa (maiúscula/minúscula)
#   - troca aspas/pontuação por espaço
#   - MANTÉM números (o "3" de "Kit 3 Peças" distingue produtos!)
#   - colapsa espaços
# ============================================================

def normalizar_titulo(texto):
    """
    Normaliza um título para comparação.

    Ex: 'Kit 3 Peças Escovas de Aço "Circular" 4.500 RPM'
      → 'kit 3 pecas escovas de aco circular 4 500 rpm'

    Retorna string vazia se a entrada for None/vazia.
    """
    if texto is None:
        return ''
    s = str(texto)
    # Remove acentos (NFKD + descarta marcas de combinação)
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('ascii')
    # Remove o caractere de substituição (�) que sobra de encoding ruim
    s = s.replace('�', ' ')
    s = s.lower()
    # Tudo que não for letra/número vira espaço (aspas, colchetes, hífens, etc.)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    # Colapsa espaços
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ============================================================
# DICIONÁRIO título → SKU (tabela dim_titulo_sku_shopee)
# ============================================================

def garantir_tabela_dicionario(engine):
    """
    Cria a tabela do dicionário se ainda não existir. Idempotente.

    Colunas:
      loja_origem        — loja no formato de fact_vendas_snapshot.loja_origem
      titulo_original    — título como veio do relatório de vendas (para exibir)
      titulo_normalizado — versão normalizada (chave de comparação)
      sku                — SKU vendido
      sku_pai            — Nº de referência do SKU principal (agrupa variações)
      primeira_venda / ultima_venda — janela em que esse título+SKU foi visto
      qtd_vendas         — soma de quantidade vendida (só um indicador de peso)
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_titulo_sku_shopee (
                id                 SERIAL PRIMARY KEY,
                loja_origem        VARCHAR(120) NOT NULL,
                titulo_original    TEXT NOT NULL,
                titulo_normalizado TEXT NOT NULL,
                sku                VARCHAR(120) NOT NULL,
                sku_pai            VARCHAR(120),
                primeira_venda     DATE,
                ultima_venda       DATE,
                qtd_vendas         INTEGER DEFAULT 0,
                atualizado_em      TIMESTAMP DEFAULT NOW(),
                UNIQUE (loja_origem, titulo_normalizado, sku)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS ix_dic_titulo_norm
            ON dim_titulo_sku_shopee (loja_origem, titulo_normalizado)
        """)
        conn.commit()
        return True
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
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


def _agregar_pares(df_vendas):
    """
    A partir do DataFrame de vendas já processado, agrega os pares
    (titulo, sku, sku_pai) distintos com min/max data e soma de quantidade.

    Espera as colunas: 'titulo', 'sku', 'codigo_anuncio' (sku pai), 'data', 'qtd'.
    Retorna lista de dicts prontos para upsert (com titulo_normalizado calculado).
    """
    agregados = {}  # (titulo_norm, sku) -> dados

    for _, row in df_vendas.iterrows():
        titulo = str(row.get('titulo', '') or '').strip()
        sku = str(row.get('sku', '') or '').strip()
        if not titulo or not sku or sku.lower() in ('nan', 'none', ''):
            continue

        titulo_norm = normalizar_titulo(titulo)
        if not titulo_norm:
            continue

        sku_pai = str(row.get('codigo_anuncio', '') or '').strip()
        data_venda = row.get('data')
        if not isinstance(data_venda, date):
            data_venda = None
        try:
            qtd = int(row.get('qtd', 0) or 0)
        except Exception:
            qtd = 0

        chave = (titulo_norm, sku)
        if chave not in agregados:
            agregados[chave] = {
                'titulo_original': titulo,
                'titulo_normalizado': titulo_norm,
                'sku': sku,
                'sku_pai': sku_pai or None,
                'primeira_venda': data_venda,
                'ultima_venda': data_venda,
                'qtd_vendas': qtd,
            }
        else:
            a = agregados[chave]
            a['qtd_vendas'] += qtd
            if data_venda is not None:
                if a['primeira_venda'] is None or data_venda < a['primeira_venda']:
                    a['primeira_venda'] = data_venda
                if a['ultima_venda'] is None or data_venda > a['ultima_venda']:
                    a['ultima_venda'] = data_venda
            if not a['sku_pai'] and sku_pai:
                a['sku_pai'] = sku_pai

    return list(agregados.values())


def atualizar_dicionario_de_vendas(engine, loja_origem, df_vendas):
    """
    Alimenta o dicionário título→SKU a partir de um lote de vendas Shopee.

    Chamada ao final da gravação de vendas (gravar_vendas_shopee). É segura
    para re-execução: usa UPSERT por (loja, titulo_normalizado, sku).

    Parâmetros:
        loja_origem : mesmo valor gravado em fact_vendas_snapshot.loja_origem
        df_vendas   : DataFrame processado (com coluna 'titulo')

    Retorna: (inseridos_ou_atualizados, erros)
    """
    if df_vendas is None or df_vendas.empty:
        return 0, 0
    if 'titulo' not in df_vendas.columns:
        # Compatibilidade: relatório processado por versão antiga (sem título)
        return 0, 0

    garantir_tabela_dicionario(engine)
    pares = _agregar_pares(df_vendas)
    if not pares:
        return 0, 0

    gravados = 0
    erros = 0
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for p in pares:
            try:
                cursor.execute("SAVEPOINT sp_dic")
                cursor.execute("""
                    INSERT INTO dim_titulo_sku_shopee (
                        loja_origem, titulo_original, titulo_normalizado,
                        sku, sku_pai, primeira_venda, ultima_venda, qtd_vendas
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (loja_origem, titulo_normalizado, sku)
                    DO UPDATE SET
                        ultima_venda   = GREATEST(dim_titulo_sku_shopee.ultima_venda, EXCLUDED.ultima_venda),
                        primeira_venda = LEAST(dim_titulo_sku_shopee.primeira_venda, EXCLUDED.primeira_venda),
                        qtd_vendas     = dim_titulo_sku_shopee.qtd_vendas + EXCLUDED.qtd_vendas,
                        titulo_original = EXCLUDED.titulo_original,
                        sku_pai        = COALESCE(dim_titulo_sku_shopee.sku_pai, EXCLUDED.sku_pai),
                        atualizado_em  = NOW()
                """, (
                    loja_origem, p['titulo_original'], p['titulo_normalizado'],
                    p['sku'], p['sku_pai'], p['primeira_venda'],
                    p['ultima_venda'], p['qtd_vendas'],
                ))
                cursor.execute("RELEASE SAVEPOINT sp_dic")
                gravados += 1
            except Exception:
                erros += 1
                try:
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_dic")
                except Exception:
                    pass
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
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

    return gravados, erros


# ============================================================
# MOTOR DE MATCH: anúncio (título) → SKU(s) via dicionário
# ------------------------------------------------------------
# Classifica cada anúncio comparando o título normalizado contra
# o dicionário de vendas:
#   EXATO   — título idêntico          → automático (alta confiança)
#   PREFIXO — um é começo do outro      → automático (trata truncamento)
#   FUZZY   — parecença alta (>= 0.85)  → automático (alta confiança)
#   DUVIDA  — parecença média           → sugere, precisa confirmação
#   SEM     — nada parecido             → manual (como hoje)
# Só o EXATO/PREFIXO/FUZZY entram como "automático"; DUVIDA/SEM nunca
# gravam sozinhos.
# ============================================================

from difflib import SequenceMatcher

# Limiares (calibrados com dados reais LPT jul/2026; ajustáveis depois)
_MIN_CHARS_PREFIXO = 15    # tamanho mínimo do lado curto para valer prefixo
_LIMIAR_FUZZY_AUTO = 0.85  # >= vira automático
_LIMIAR_FUZZY_DUVIDA = 0.55  # entre isto e AUTO → duvidoso
_MAX_CANDIDATOS = 3        # nº de SKUs mais parecidos oferecidos nos duvidosos


def _carregar_indice_dicionario(engine, loja_origem):
    """
    Carrega o dicionário de uma loja em memória, agrupado por título
    normalizado. Retorna dict:
        { titulo_normalizado: {
              'titulo_original': str,
              'skus': [sku, ...],
              'sku_pai': str|None,
              'ultima_venda': date|None,
          } }
    """
    indice = {}
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT titulo_normalizado, titulo_original, sku, sku_pai, ultima_venda
            FROM dim_titulo_sku_shopee
            WHERE loja_origem = %s
            ORDER BY titulo_normalizado, sku
        """, (loja_origem,))
        for tn, to, sku, sku_pai, ult in cursor.fetchall():
            entrada = indice.setdefault(tn, {
                'titulo_original': to,
                'skus': [],
                'sku_pai': None,
                'ultima_venda': None,
            })
            if sku and sku not in entrada['skus']:
                entrada['skus'].append(sku)
            if sku_pai and not entrada['sku_pai']:
                entrada['sku_pai'] = sku_pai
            if ult is not None and (entrada['ultima_venda'] is None or ult > entrada['ultima_venda']):
                entrada['ultima_venda'] = ult
    except Exception:
        pass
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
    return indice


def classificar_anuncio(nome_anuncio, indice):
    """
    Classifica UM anúncio contra o índice do dicionário (em memória).

    Parâmetros:
        nome_anuncio : título do anúncio (fact_ads_shopee.nome_anuncio)
        indice       : saída de _carregar_indice_dicionario()

    Retorna dict:
        {
          'nome_anuncio', 'tipo', 'score', 'automatico' (bool),
          'skus' (list), 'sku_pai', 'titulo_venda' (original do match),
          'candidatos' (lista dos títulos mais parecidos p/ o usuário escolher)
        }
    Cada candidato: {'titulo_venda', 'skus', 'sku_pai', 'score'}.
    """
    base = {
        'nome_anuncio': nome_anuncio,
        'tipo': 'SEM',
        'score': 0.0,
        'automatico': False,
        'skus': [],
        'sku_pai': None,
        'titulo_venda': None,
        'titulo_normalizado': None,
        'candidatos': [],
    }
    na = normalizar_titulo(nome_anuncio)
    if not na or not indice:
        return base

    # Ranking geral por parecença (reaproveitado p/ fuzzy e p/ candidatos)
    ranked = sorted(
        ((SequenceMatcher(None, na, ch).ratio(), ch) for ch in indice),
        key=lambda x: x[0], reverse=True
    )
    # Candidatos: os títulos mais parecidos, p/ o usuário escolher nos duvidosos.
    # Guarda até _MAX_CANDIDATOS acima de um piso baixo (0.40).
    base['candidatos'] = [
        {
            'titulo_venda': indice[ch]['titulo_original'],
            'titulo_normalizado': ch,
            'skus': list(indice[ch]['skus']),
            'sku_pai': indice[ch]['sku_pai'],
            'score': round(float(sc), 3),
        }
        for sc, ch in ranked[:_MAX_CANDIDATOS] if sc >= 0.40
    ]

    def _preencher(tipo, chave, score, automatico):
        e = indice[chave]
        base.update({
            'tipo': tipo,
            'score': round(float(score), 3),
            'automatico': automatico,
            'skus': list(e['skus']),
            'sku_pai': e['sku_pai'],
            'titulo_venda': e['titulo_original'],
            'titulo_normalizado': chave,
        })
        return base

    # 1) EXATO
    if na in indice:
        return _preencher('EXATO', na, 1.0, True)

    # 2) PREFIXO / containment (trata truncamento do relatório geral)
    melhor_pref = None
    melhor_pref_score = -1.0
    for chave in indice:
        if len(min(na, chave, key=len)) < _MIN_CHARS_PREFIXO:
            continue
        if chave.startswith(na) or na.startswith(chave):
            s = SequenceMatcher(None, na, chave).ratio()
            if s > melhor_pref_score:
                melhor_pref_score = s
                melhor_pref = chave
    if melhor_pref is not None:
        return _preencher('PREFIXO', melhor_pref, melhor_pref_score, True)

    # 3) FUZZY (melhor parecença do ranking já calculado)
    if ranked:
        melhor_score, melhor = ranked[0]
        if melhor_score >= _LIMIAR_FUZZY_AUTO:
            return _preencher('FUZZY', melhor, melhor_score, True)
        if melhor_score >= _LIMIAR_FUZZY_DUVIDA:
            return _preencher('DUVIDA', melhor, melhor_score, False)

    return base


def sugerir_matches_anuncios(engine, loja_ads, lista_anuncios=None):
    """
    Roda o motor de match para todos os anúncios de uma loja.

    Parâmetros:
        loja_ads       : loja no formato ADS (ex: 'LPT Store', 'Nala-Lit')
        lista_anuncios : opcional; se None, carrega nomes distintos de
                         fact_ads_shopee para a loja.

    Retorna: lista de dicts (saída de classificar_anuncio), ordenada
             por tipo (automáticos primeiro, depois duvidosos, depois sem).
    """
    # Traduz loja ADS → loja_origem (mesma chave do dicionário/vendas)
    try:
        from processar_ads_shopee import LOJA_ADS_PARA_ORIGEM
        loja_origem = LOJA_ADS_PARA_ORIGEM.get(loja_ads, loja_ads)
    except Exception:
        loja_origem = loja_ads

    indice = _carregar_indice_dicionario(engine, loja_origem)

    # Carrega anúncios se não vieram prontos
    if lista_anuncios is None:
        lista_anuncios = []
        conn = None
        cursor = None
        try:
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT nome_anuncio FROM fact_ads_shopee
                WHERE loja = %s ORDER BY nome_anuncio
            """, (loja_ads,))
            lista_anuncios = [r[0] for r in cursor.fetchall()]
        except Exception:
            lista_anuncios = []
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

    resultados = [classificar_anuncio(a, indice) for a in lista_anuncios]

    # Ordena: automáticos, depois duvidosos, depois sem match
    ordem = {'EXATO': 0, 'PREFIXO': 1, 'FUZZY': 2, 'DUVIDA': 3, 'SEM': 4}
    resultados.sort(key=lambda r: (ordem.get(r['tipo'], 9), -r['score']))
    return resultados


def contar_dicionario(engine, loja_origem=None):
    """
    Retorna (titulos_distintos, pares_titulo_sku) do dicionário — para
    diagnóstico/telas. Se loja_origem for None, conta o total geral.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        if loja_origem:
            cursor.execute("""
                SELECT COUNT(DISTINCT titulo_normalizado), COUNT(*)
                FROM dim_titulo_sku_shopee WHERE loja_origem = %s
            """, (loja_origem,))
        else:
            cursor.execute("""
                SELECT COUNT(DISTINCT titulo_normalizado), COUNT(*)
                FROM dim_titulo_sku_shopee
            """)
        r = cursor.fetchone()
        return (int(r[0]), int(r[1])) if r else (0, 0)
    except Exception:
        return (0, 0)
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
