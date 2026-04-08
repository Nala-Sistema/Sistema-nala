"""
PERFORMANCE UTILS - Sistema Nala
Versão: 1.1 (06/04/2026)

Funções auxiliares para o módulo Performance:
- Modelos de projeção (Linear, Início Forte, Meio Forte, Final Fraco)
- Queries de vendas, metas, tags, histórico
- Cálculos de projeção, performance, margem

VERSÃO 1.1 (06/04/2026):
  - Regra dos 3 Meses: construir_tabela_performance agora inclui anúncios
    que venderam nos 3 meses anteriores no universo de anúncios, mesmo que
    não tenham vendas no mês selecionado. Garante que no início do mês a
    tela não fique vazia e permita preencher metas preventivamente.
"""

import pandas as pd
from datetime import datetime, date, timedelta
from calendar import monthrange
from database_utils import get_engine

# ============================================================
# MODELOS DE PROJEÇÃO
# ============================================================

# Peso por semana do mês (soma = 1.0)
MODELOS_PROJECAO = {
    'Linear':        {'sem1': 0.25, 'sem2': 0.25, 'sem3': 0.25, 'sem4': 0.25,
                      'desc': 'Vendas distribuídas igualmente ao longo do mês'},
    'Início Forte':  {'sem1': 0.30, 'sem2': 0.28, 'sem3': 0.24, 'sem4': 0.18,
                      'desc': '~60% das vendas nos primeiros 15 dias (padrão salário BR)'},
    'Meio Forte':    {'sem1': 0.22, 'sem2': 0.30, 'sem3': 0.28, 'sem4': 0.20,
                      'desc': 'Pico entre dia 5-20, queda nas pontas'},
    'Final Fraco':   {'sem1': 0.28, 'sem2': 0.27, 'sem3': 0.25, 'sem4': 0.20,
                      'desc': 'Últimos 10 dias = ~20% das vendas'},
}

def pct_esperado_ate_dia(dia_atual, dias_mes, modelo='Linear'):
    """
    Retorna a fração (0.0 a 1.0) de vendas esperada até o dia_atual
    dado o modelo de projeção selecionado.
    """
    if dia_atual <= 0:
        return 0.0
    if dia_atual >= dias_mes:
        return 1.0
    if modelo == 'Linear' or modelo not in MODELOS_PROJECAO:
        return dia_atual / dias_mes

    pesos = MODELOS_PROJECAO[modelo]
    # Dividir mês em 4 semanas proporcionais
    q1 = dias_mes * 0.25
    q2 = dias_mes * 0.50
    q3 = dias_mes * 0.75

    if dia_atual <= q1:
        return pesos['sem1'] * (dia_atual / q1)
    elif dia_atual <= q2:
        return pesos['sem1'] + pesos['sem2'] * ((dia_atual - q1) / (q2 - q1))
    elif dia_atual <= q3:
        return pesos['sem1'] + pesos['sem2'] + pesos['sem3'] * ((dia_atual - q2) / (q3 - q2))
    else:
        return (pesos['sem1'] + pesos['sem2'] + pesos['sem3'] +
                pesos['sem4'] * ((dia_atual - q3) / (dias_mes - q3)))


def calcular_projecao(realizado, dia_atual, dias_mes, modelo='Linear'):
    """Projeta valor total do mês baseado no realizado e modelo."""
    pct = pct_esperado_ate_dia(dia_atual, dias_mes, modelo)
    if pct <= 0:
        return 0.0
    return realizado / pct


def calcular_performance(projecao, meta):
    """Retorna % de performance (projeção / meta)."""
    if not meta or meta <= 0:
        return None
    return (projecao / meta) * 100


# ============================================================
# HELPERS DE DATA
# ============================================================

def get_ano_mes(dt=None):
    """Retorna string 'YYYY-MM' para a data."""
    if dt is None:
        dt = date.today()
    return dt.strftime('%Y-%m')


def get_primeiro_ultimo_dia(ano_mes_str):
    """Retorna (primeiro_dia, ultimo_dia) como date objects."""
    ano, mes = int(ano_mes_str[:4]), int(ano_mes_str[5:7])
    primeiro = date(ano, mes, 1)
    _, ultimo_dia = monthrange(ano, mes)
    ultimo = date(ano, mes, ultimo_dia)
    return primeiro, ultimo


def get_mes_anterior(ano_mes_str, meses=1):
    """Retorna ano_mes N meses atrás."""
    ano, mes = int(ano_mes_str[:4]), int(ano_mes_str[5:7])
    for _ in range(meses):
        mes -= 1
        if mes < 1:
            mes = 12
            ano -= 1
    return f"{ano:04d}-{mes:02d}"


def get_dias_vendas(ano_mes_str):
    """Retorna (dias_vendas, dias_mes) para o mês."""
    primeiro, ultimo = get_primeiro_ultimo_dia(ano_mes_str)
    hoje = date.today()
    dias_mes = ultimo.day
    if hoje.year == primeiro.year and hoje.month == primeiro.month:
        dias_vendas = (hoje - primeiro).days + 1
    elif hoje > ultimo:
        dias_vendas = dias_mes
    else:
        dias_vendas = 0
    return dias_vendas, dias_mes


def _raw_query(engine, sql, params=None):
    """Executa query e retorna DataFrame."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        if cursor.description:
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return pd.DataFrame(rows, columns=cols)
        cursor.close()
        conn.close()
        return pd.DataFrame()
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return pd.DataFrame()


def _raw_execute(engine, sql, params=None):
    """Executa DML e faz commit."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return affected
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return -1


# ============================================================
# QUERIES — LOJAS
# ============================================================

def buscar_lojas_por_marketplace(engine, marketplace):
    """Retorna lista de lojas de um marketplace."""
    df = _raw_query(engine,
        "SELECT loja FROM dim_lojas WHERE marketplace = %s ORDER BY loja",
        (marketplace,))
    return df['loja'].tolist() if not df.empty else []


def buscar_todas_lojas(engine):
    """Retorna DataFrame com todas as lojas."""
    return _raw_query(engine,
        "SELECT loja, marketplace FROM dim_lojas ORDER BY marketplace, loja")


# ============================================================
# QUERIES — METAS DE LOJA
# ============================================================

def buscar_meta_loja(engine, loja, ano_mes):
    """Busca meta de uma loja em um mês."""
    df = _raw_query(engine,
        "SELECT * FROM dim_metas_loja WHERE loja_origem = %s AND ano_mes = %s",
        (loja, ano_mes))
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def salvar_meta_loja(engine, loja, marketplace, ano_mes, meta_receita, modelo='Linear', usuario=None):
    """Salva/atualiza meta de loja (UPSERT)."""
    sql = """
        INSERT INTO dim_metas_loja (loja_origem, marketplace, ano_mes, meta_receita, modelo_projecao, usuario_definiu, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (loja_origem, ano_mes)
        DO UPDATE SET meta_receita = EXCLUDED.meta_receita,
                      modelo_projecao = EXCLUDED.modelo_projecao,
                      usuario_definiu = EXCLUDED.usuario_definiu,
                      updated_at = NOW()
    """
    return _raw_execute(engine, sql, (loja, marketplace, ano_mes, meta_receita, modelo, usuario))


# ============================================================
# QUERIES — METAS DE ANÚNCIO
# ============================================================

def buscar_metas_anuncio(engine, loja, ano_mes):
    """Busca todas as metas de anúncios de uma loja em um mês."""
    return _raw_query(engine,
        "SELECT * FROM dim_metas_anuncio WHERE loja_origem = %s AND ano_mes = %s",
        (loja, ano_mes))


def salvar_metas_anuncio_lote(engine, metas_list):
    """
    Salva metas de anúncios em lote.
    metas_list = [{'loja_origem', 'marketplace', 'codigo_anuncio', 'logistica',
                   'ano_mes', 'meta_quantidade', 'observacao'}, ...]
    """
    sql = """
        INSERT INTO dim_metas_anuncio
            (loja_origem, marketplace, codigo_anuncio, logistica, ano_mes, meta_quantidade, observacao, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (loja_origem, codigo_anuncio, COALESCE(logistica, ''), ano_mes)
        DO UPDATE SET meta_quantidade = EXCLUDED.meta_quantidade,
                      observacao = EXCLUDED.observacao,
                      updated_at = NOW()
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for m in metas_list:
            cursor.execute(sql, (
                m['loja_origem'], m['marketplace'], m['codigo_anuncio'],
                m.get('logistica') or None, m['ano_mes'],
                int(m.get('meta_quantidade', 0)),
                m.get('observacao') or None
            ))
        conn.commit()
        cursor.close()
        conn.close()
        return len(metas_list)
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return -1


# ============================================================
# QUERIES — VENDAS REALIZADAS (com desconto de devoluções)
# ============================================================

def buscar_realizados_mes(engine, loja, ano_mes, marketplace=None):
    """
    Busca vendas realizadas por anúncio no mês, descontando devoluções.
    Para Amazon: agrupa por codigo_anuncio + logistica.
    Para outros: agrupa APENAS por codigo_anuncio (evita duplicatas).
    """
    primeiro, ultimo = get_primeiro_ultimo_dia(ano_mes)
    is_amazon = marketplace and 'AMAZON' in marketplace.upper()

    # SQL diferente por marketplace para evitar duplicatas na raiz
    if is_amazon:
        sql_vendas = """
            SELECT codigo_anuncio, MAX(sku) as sku, logistica,
                   SUM(quantidade)::float as qtd_vendas,
                   SUM(valor_venda_efetivo)::float as fat_vendas,
                   AVG(margem_percentual)::float as margem_atual
            FROM fact_vendas_snapshot
            WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
              AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
            GROUP BY codigo_anuncio, logistica
        """
    else:
        sql_vendas = """
            SELECT codigo_anuncio, MAX(sku) as sku, NULL as logistica,
                   SUM(quantidade)::float as qtd_vendas,
                   SUM(valor_venda_efetivo)::float as fat_vendas,
                   AVG(margem_percentual)::float as margem_atual
            FROM fact_vendas_snapshot
            WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
              AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
            GROUP BY codigo_anuncio
        """

    df_v = _raw_query(engine, sql_vendas, (loja, primeiro, ultimo))

    if not df_v.empty:
        for c in ['qtd_vendas', 'fat_vendas', 'margem_atual']:
            if c in df_v.columns:
                df_v[c] = pd.to_numeric(df_v[c], errors='coerce').fillna(0)

    # Devoluções
    if is_amazon:
        sql_dev = """
            SELECT codigo_anuncio, tipo_logistica as logistica,
                   SUM(quantidade)::float as qtd_dev,
                   SUM(valor_venda_efetivo)::float as fat_dev
            FROM fact_devolucoes
            WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
              AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
            GROUP BY codigo_anuncio, tipo_logistica
        """
    else:
        sql_dev = """
            SELECT codigo_anuncio, NULL as logistica,
                   SUM(quantidade)::float as qtd_dev,
                   SUM(valor_venda_efetivo)::float as fat_dev
            FROM fact_devolucoes
            WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
              AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
            GROUP BY codigo_anuncio
        """
    df_d = _raw_query(engine, sql_dev, (loja, primeiro, ultimo))

    if not df_d.empty:
        for c in ['qtd_dev', 'fat_dev']:
            if c in df_d.columns:
                df_d[c] = pd.to_numeric(df_d[c], errors='coerce').fillna(0)

    if df_v.empty:
        return pd.DataFrame(columns=['codigo_anuncio', 'sku', 'logistica',
                                     'qtd_realizado', 'fat_realizado', 'margem_atual'])

    # Merge vendas com devoluções
    if not df_d.empty:
        if is_amazon:
            df = df_v.merge(df_d, on=['codigo_anuncio', 'logistica'], how='left')
        else:
            df = df_v.merge(df_d[['codigo_anuncio', 'qtd_dev', 'fat_dev']],
                            on='codigo_anuncio', how='left')
    else:
        df = df_v.copy()
        df['qtd_dev'] = 0
        df['fat_dev'] = 0

    df['qtd_dev'] = pd.to_numeric(df['qtd_dev'], errors='coerce').fillna(0).astype(int)
    df['fat_dev'] = pd.to_numeric(df['fat_dev'], errors='coerce').fillna(0).astype(float)
    df['qtd_vendas'] = pd.to_numeric(df['qtd_vendas'], errors='coerce').fillna(0).astype(int)
    df['fat_vendas'] = pd.to_numeric(df['fat_vendas'], errors='coerce').fillna(0).astype(float)
    df['margem_atual'] = pd.to_numeric(df['margem_atual'], errors='coerce').fillna(0).astype(float)
    df['qtd_realizado'] = (df['qtd_vendas'] - df['qtd_dev']).clip(lower=0)
    df['fat_realizado'] = (df['fat_vendas'] - df['fat_dev']).clip(lower=0)
    df['margem_atual'] = df['margem_atual'].round(2)

    return df[['codigo_anuncio', 'sku', 'logistica', 'qtd_realizado', 'fat_realizado', 'margem_atual']]


# ============================================================
# QUERIES — HISTÓRICO (meses anteriores)
# ============================================================

def buscar_historico_meses(engine, loja, ano_mes_ref, meses_atras=3, marketplace=None):
    """
    Busca vendas agrupadas por anúncio para N meses anteriores.
    Retorna dict: {'YYYY-MM': DataFrame, ...}
    """
    is_amazon = marketplace and 'AMAZON' in marketplace.upper()
    resultado = {}
    for i in range(1, meses_atras + 1):
        mes = get_mes_anterior(ano_mes_ref, i)
        primeiro, ultimo = get_primeiro_ultimo_dia(mes)

        if is_amazon:
            group_cols = "codigo_anuncio, sku, logistica"
        else:
            group_cols = "codigo_anuncio, sku"

        sql = f"""
            SELECT {group_cols},
                   SUM(quantidade) as qtd, SUM(valor_venda_efetivo) as fat
            FROM fact_vendas_snapshot
            WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
              AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
            GROUP BY {group_cols}
        """
        df = _raw_query(engine, sql, (loja, primeiro, ultimo))
        if not df.empty:
            if 'qtd' in df.columns:
                df['qtd'] = pd.to_numeric(df['qtd'], errors='coerce').fillna(0).astype(int)
            if 'fat' in df.columns:
                df['fat'] = pd.to_numeric(df['fat'], errors='coerce').fillna(0).astype(float)
        if not is_amazon and not df.empty:
            df['logistica'] = None
        resultado[mes] = df
    return resultado


# ============================================================
# QUERIES — PREÇO MÉDIO MÊS ANTERIOR
# ============================================================

def buscar_preco_medio_mes_anterior(engine, loja, ano_mes_ref, marketplace=None):
    """
    Retorna dict: {(codigo_anuncio, logistica): preco_medio, ...}
    Preço médio = receita / quantidade do mês anterior.
    """
    mes_ant = get_mes_anterior(ano_mes_ref, 1)
    primeiro, ultimo = get_primeiro_ultimo_dia(mes_ant)
    is_amazon = marketplace and 'AMAZON' in marketplace.upper()

    if is_amazon:
        group_cols = "codigo_anuncio, logistica"
    else:
        group_cols = "codigo_anuncio"

    sql = f"""
        SELECT {group_cols},
               SUM(valor_venda_efetivo) / NULLIF(SUM(quantidade), 0) as preco_medio
        FROM fact_vendas_snapshot
        WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
          AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
        GROUP BY {group_cols}
    """
    df = _raw_query(engine, sql, (loja, primeiro, ultimo))
    if df.empty:
        return {}

    result = {}
    for _, row in df.iterrows():
        if is_amazon:
            key = (row['codigo_anuncio'], row.get('logistica'))
        else:
            key = (row['codigo_anuncio'], None)
        result[key] = float(row['preco_medio'] or 0)
    return result


# ============================================================
# QUERIES — MARGEM MÊS ANTERIOR
# ============================================================

def buscar_margem_mes_anterior(engine, loja, ano_mes_ref, marketplace=None):
    """Retorna dict: {(codigo_anuncio, logistica): margem_media, ...}"""
    mes_ant = get_mes_anterior(ano_mes_ref, 1)
    primeiro, ultimo = get_primeiro_ultimo_dia(mes_ant)
    is_amazon = marketplace and 'AMAZON' in marketplace.upper()

    if is_amazon:
        group_cols = "codigo_anuncio, logistica"
    else:
        group_cols = "codigo_anuncio"

    sql = f"""
        SELECT {group_cols}, AVG(margem_percentual) as margem_media
        FROM fact_vendas_snapshot
        WHERE loja_origem = %s AND data_venda >= %s AND data_venda <= %s
          AND codigo_anuncio IS NOT NULL AND TRIM(codigo_anuncio) != ''
        GROUP BY {group_cols}
    """
    df = _raw_query(engine, sql, (loja, primeiro, ultimo))
    if df.empty:
        return {}

    result = {}
    for _, row in df.iterrows():
        key = (row['codigo_anuncio'], row.get('logistica') if is_amazon else None)
        result[key] = round(float(row['margem_media'] or 0), 2)
    return result


# ============================================================
# QUERIES — TAGS DE ANÚNCIOS
# ============================================================

def buscar_tags_anuncios_dict(engine, marketplace=None):
    """Retorna dict: {(marketplace, codigo_anuncio): {tag_curva, tag_status, observacoes}}"""
    sql = "SELECT marketplace, codigo_anuncio, tag_curva, tag_status, observacoes FROM dim_tags_anuncio"
    params = ()
    if marketplace:
        sql += " WHERE marketplace = %s"
        params = (marketplace,)
    df = _raw_query(engine, sql, params)
    result = {}
    for _, row in df.iterrows():
        result[(row['marketplace'], row['codigo_anuncio'])] = {
            'tag_curva': row.get('tag_curva'),
            'tag_status': row.get('tag_status'),
            'observacoes': row.get('observacoes'),
        }
    return result


def buscar_opcoes_tags(engine, tipo='anuncio'):
    """Retorna lista de tags ativas do cardápio."""
    df = _raw_query(engine,
        "SELECT nome_tag, cor FROM dim_tags_opcoes WHERE tipo = %s AND ativo = TRUE ORDER BY nome_tag",
        (tipo,))
    return df['nome_tag'].tolist() if not df.empty else []


# ============================================================
# QUERIES — NOMES DE PRODUTOS
# ============================================================

def buscar_nomes_produtos(engine):
    """Retorna dict: {sku: nome}"""
    df = _raw_query(engine, "SELECT sku, nome FROM dim_produtos WHERE status = 'Ativo'")
    if df.empty:
        return {}
    return {row['sku']: row['nome'] for _, row in df.iterrows()}
def buscar_skus_config_amazon(engine):
    """Retorna dict: {asin: sku} da dim_config_marketplace Amazon."""
    df = _raw_query(engine,
        "SELECT DISTINCT asin, sku FROM dim_config_marketplace WHERE marketplace = 'AMAZON' AND ativo = true AND asin IS NOT NULL AND sku IS NOT NULL")
    if df.empty:
        return {}
    return {str(row['asin']).strip(): str(row['sku']).strip() for _, row in df.iterrows()}


# ============================================================
# QUERIES — RESUMO GERAL (TAB GERAL)
# ============================================================

def buscar_resumo_geral(engine, ano_mes):
    """Busca resumo de performance por loja para a tab Geral."""
    primeiro, ultimo = get_primeiro_ultimo_dia(ano_mes)

    sql = """
        SELECT v.loja_origem, v.marketplace_origem,
               SUM(v.quantidade) as qtd_realizado,
               SUM(v.valor_venda_efetivo) as fat_realizado
        FROM fact_vendas_snapshot v
        WHERE v.data_venda >= %s AND v.data_venda <= %s
          AND v.codigo_anuncio IS NOT NULL AND TRIM(v.codigo_anuncio) != ''
        GROUP BY v.loja_origem, v.marketplace_origem
    """
    df_vendas = _raw_query(engine, sql, (primeiro, ultimo))

    sql_metas = "SELECT loja_origem, marketplace, meta_receita, modelo_projecao FROM dim_metas_loja WHERE ano_mes = %s"
    df_metas = _raw_query(engine, sql_metas, (ano_mes,))

    # Devoluções
    sql_dev = """
        SELECT loja_origem, marketplace_origem,
               SUM(quantidade) as qtd_dev, SUM(valor_venda_efetivo) as fat_dev
        FROM fact_devolucoes
        WHERE data_venda >= %s AND data_venda <= %s
        GROUP BY loja_origem, marketplace_origem
    """
    df_dev = _raw_query(engine, sql_dev, (primeiro, ultimo))

    return df_vendas, df_metas, df_dev


# ============================================================
# CONSTRUIR DATAFRAME COMPLETO DE PERFORMANCE
# ============================================================

def construir_tabela_performance(engine, loja, marketplace, ano_mes, modelo_projecao='Linear'):
    """
    Constrói o DataFrame completo de performance para uma loja/mês.
    Retorna DataFrame com todas as colunas para exibição.

    v1.1 — REGRA DOS 3 MESES: O universo de anúncios agora inclui todos os
    anúncios que tiveram vendas nos 3 meses anteriores, além do mês atual
    e das metas existentes. Isso garante que no início do mês a tela não
    fique vazia e permita preencher metas preventivamente.
    """
    is_amazon = 'AMAZON' in marketplace.upper()
    dias_vendas, dias_mes = get_dias_vendas(ano_mes)

    # 1. Realizados do mês (vendas - devoluções)
    df_real = buscar_realizados_mes(engine, loja, ano_mes, marketplace)

    # 2. Metas do mês
    df_metas = buscar_metas_anuncio(engine, loja, ano_mes)

    # 3. Preço médio e margem do mês anterior
    precos = buscar_preco_medio_mes_anterior(engine, loja, ano_mes, marketplace)
    margens_ant = buscar_margem_mes_anterior(engine, loja, ano_mes, marketplace)

    # 4. Nomes dos produtos
    nomes = buscar_nomes_produtos(engine)

    # 5. Tags
    tags = buscar_tags_anuncios_dict(engine, marketplace)

    # 6. Histórico (3 meses anteriores)
    historico = buscar_historico_meses(engine, loja, ano_mes, 3, marketplace)

    # Montar lista de anúncios únicos (da realização OU das metas OU do histórico)
    anuncios = set()
    # Primeiro: anúncios do realizado (com SKU correto)
    if not df_real.empty:
        for _, r in df_real.iterrows():
            log = r.get('logistica')
            log = None if pd.isna(log) else log
            anuncios.add((r['codigo_anuncio'], r['sku'], log))
    # Depois: anúncios das metas que NÃO aparecem no realizado
    codigos_existentes = {(a[0], a[2]) for a in anuncios}  # (codigo_anuncio, logistica)
    if not df_metas.empty:
        for _, r in df_metas.iterrows():
            log = r.get('logistica')
            log = None if pd.isna(log) else log
            if (r['codigo_anuncio'], log) not in codigos_existentes:
                anuncios.add((r['codigo_anuncio'], '', log))

    # ── REGRA DOS 3 MESES (v1.1) ──────────────────────────────
    # Incluir anúncios que venderam nos 3 meses anteriores,
    # mesmo que não tenham vendas no mês selecionado nem metas.
    codigos_existentes = {(a[0], a[2]) for a in anuncios}  # atualiza após metas
    for mes_key, df_hist in historico.items():
        if not df_hist.empty:
            for _, h in df_hist.iterrows():
                log = h.get('logistica')
                log = None if pd.isna(log) else log
                if (h['codigo_anuncio'], log) not in codigos_existentes:
                    anuncios.add((h['codigo_anuncio'], h.get('sku', ''), log))
                    codigos_existentes.add((h['codigo_anuncio'], log))
    # ── FIM REGRA DOS 3 MESES ─────────────────────────────────
  
    # Fallback: SKUs da config Amazon para ASINs sem venda no snapshot
    config_skus = buscar_skus_config_amazon(engine) if is_amazon else {}
  
    if not anuncios:
        return pd.DataFrame()

    rows = []
    for cod_anuncio, sku_from_real, logistica in anuncios:
        # Realizados
        if not df_real.empty:
            if is_amazon:
                mask = (df_real['codigo_anuncio'] == cod_anuncio) & (df_real['logistica'] == logistica)
            else:
                mask = df_real['codigo_anuncio'] == cod_anuncio
            match = df_real[mask]
        else:
            match = pd.DataFrame()

        if not match.empty:
            r = match.iloc[0]
            qtd_real = int(r['qtd_realizado'])
            fat_real = float(r['fat_realizado'])
            margem_at = float(r['margem_atual'])
            sku = str(r['sku'])
        else:
            qtd_real, fat_real, margem_at = 0, 0.0, 0.0
            sku = sku_from_real or ''
        # Fallback: buscar SKU da config Amazon
        if (not sku or sku in ('', 'None', 'nan')) and is_amazon:
            sku = config_skus.get(cod_anuncio, '')
        # Meta
        meta_qtd = 0
        obs = ''
        if not df_metas.empty:
            if is_amazon:
                m_mask = ((df_metas['codigo_anuncio'] == cod_anuncio) &
                          (df_metas['logistica'].fillna('') == (logistica or '')))
            else:
                m_mask = df_metas['codigo_anuncio'] == cod_anuncio
            m_match = df_metas[m_mask]
            if not m_match.empty:
                meta_qtd = int(m_match.iloc[0]['meta_quantidade'])
                obs = str(m_match.iloc[0].get('observacao') or '')

        # Preço médio e meta faturamento
        key = (cod_anuncio, logistica if is_amazon else None)
        preco_med = precos.get(key, 0)
        meta_fat = meta_qtd * preco_med if preco_med > 0 else 0

        # Margem mês anterior
        margem_ant = margens_ant.get(key, 0)

        # Projeção
        if dias_vendas > 0 and fat_real > 0:
            proj_fat = calcular_projecao(fat_real, dias_vendas, dias_mes, modelo_projecao)
            proj_qtd = calcular_projecao(qtd_real, dias_vendas, dias_mes, modelo_projecao)
        else:
            proj_fat, proj_qtd = 0.0, 0.0

        # Performance
        perf = calcular_performance(proj_fat, meta_fat)

        # Tags
        tag_info = tags.get((marketplace, cod_anuncio), {})

        # Histórico
        hist_data = {}
        for mes_key, df_hist in historico.items():
            if not df_hist.empty:
                if is_amazon:
                    h_mask = (df_hist['codigo_anuncio'] == cod_anuncio) & (df_hist['logistica'] == logistica)
                else:
                    h_mask = df_hist['codigo_anuncio'] == cod_anuncio
                h_match = df_hist[h_mask]
                if not h_match.empty:
                    hist_data[mes_key] = {
                        'qtd': int(h_match['qtd'].sum()),
                        'fat': float(h_match['fat'].sum())
                    }
            if mes_key not in hist_data:
                hist_data[mes_key] = {'qtd': 0, 'fat': 0.0}

        nome_produto = nomes.get(sku, '')

        row = {
            'codigo_anuncio': cod_anuncio,
            'sku': sku,
            'produto': nome_produto,
            'logistica': logistica or '',
            'curva': tag_info.get('tag_curva', ''),
            'tag': tag_info.get('tag_status', ''),
            'margem_ant': margem_ant,
            'margem_atual': margem_at,
            'meta_qtd': meta_qtd,
            'meta_fat': round(meta_fat, 2),
            'preco_medio': round(preco_med, 2),
            'qtd_realizado': qtd_real,
            'fat_realizado': round(fat_real, 2),
            'proj_qtd': round(proj_qtd),
            'proj_fat': round(proj_fat, 2),
            'performance': round(perf, 1) if perf is not None else None,
            'observacao': obs,
        }

        # Histórico como colunas
        meses_hist = sorted(historico.keys(), reverse=True)
        for idx, mes_key in enumerate(meses_hist):
            h = hist_data.get(mes_key, {'qtd': 0, 'fat': 0.0})
            row[f'hist_{idx+1}_qtd'] = h['qtd']
            row[f'hist_{idx+1}_fat'] = round(h['fat'], 2)
            row[f'hist_{idx+1}_mes'] = mes_key

        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        # Deduplica: para não-Amazon, por codigo_anuncio; para Amazon, por codigo_anuncio+logistica
        if is_amazon:
            df = df.drop_duplicates(subset=['codigo_anuncio', 'logistica'], keep='first')
        else:
            df = df.drop_duplicates(subset=['codigo_anuncio'], keep='first')
        df = df.sort_values('fat_realizado', ascending=False).reset_index(drop=True)
    return df
