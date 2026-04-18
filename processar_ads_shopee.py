"""
processar_ads_shopee.py — Processador de relatórios de Ads da Shopee
Sistema Nala — Módulo Ads — v2

Correções e melhorias em relação a v1:
  1. BUG FIX — TACOS zerado: calcular_tacos() agora filtra
     marketplace_origem = 'SHOPEE' (maiúsculas, como está no banco) e
     traduz loja_ads → loja_origem via LOJA_ADS_PARA_ORIGEM.
  2. MATCH TEMPORAL + MÚLTIPLO:
       - buscar_skus_match() retorna LISTA de SKUs vigentes em uma data.
       - atualizar_matches_sku() fecha registros que saíram da lista e
         abre novos registros com data_inicio = CURRENT_DATE.
     Funções antigas (buscar_match_sku, salvar_match_sku) permanecem
     por compatibilidade — chamam internamente as novas.
  3. 5 COLUNAS NOVAS capturadas do CSV:
        Data de Início              → data_inicio_anuncio
        Data de Encerramento        → data_fim_anuncio   (NULL = "Ilimitado")
        Taxa de Conversão Direta    → taxa_conversao_direta
        Custo por Conversão Direta  → custo_por_conversao_direta
        ROAS Direto                 → roas_direto
  4. Helper data_fim_efetiva(data_fim_anuncio, periodo_fim) — aplica
     MIN das duas datas (ou periodo_fim se data_fim for NULL). Usado
     pelo dashboard para separar período pago do período orgânico.

Tipos de relatório suportados:
  1. Relatório Geral (Todos os Anúncios CPC)
  2. Relatório de Grupo de Anúncios
  3. Relatório de Produto Individual

Estrutura do CSV:
  - Linhas 1-6: Metadados (nome da loja, ID, período, etc.)
  - Linha 7:    Vazia
  - Linha 8:    Header das colunas
  - Linha 9+:   Dados
"""

import pandas as pd
import io
import re
from datetime import datetime, date


# ============================================================
# MAPEAMENTO DE LOJAS
# ------------------------------------------------------------
# Os relatórios de ads da Shopee usam nomes curtos.
# fact_vendas_snapshot.loja_origem usa nomes longos.
# Este dicionário faz a tradução ads → origem.
# ============================================================

LOJA_ADS_PARA_ORIGEM = {
    'Nala-Lit': 'Shopee Lithouse(Nala)',
    'litstoreshop': 'Shopee Litstore(Yanni)',
    'LPT Store': 'Shopee-LPT',
}


# ============================================================
# FUNÇÕES DE PARSING
# ============================================================

def parse_numero_br(valor):
    """Converte valor numérico que pode ter formato BR ou EN"""
    if pd.isna(valor):
        return 0.0
    s = str(valor).replace('%', '').strip()
    if s in ['-', '', '0', '0.0', '0,0', '0.00', '0,00']:
        return 0.0
    # Remove pontos de milhar e troca vírgula por ponto
    # "1.234,56" → "1234.56" | "1234.56" → "1234.56"
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_data_anuncio(valor):
    """
    Converte 'Data de Início' / 'Data de Encerramento' do CSV.
    Formatos aceitos:
        '23/11/2025 00:00:00'  → date(2025, 11, 23)
        '25/04/2026 23:59:59'  → date(2026, 4, 25)
        '23/11/2025'           → date(2025, 11, 23)
        'Ilimitado'            → None
        '-' / '' / NaN         → None
    """
    if pd.isna(valor):
        return None
    s = str(valor).strip()
    if s in ['', '-', 'Ilimitado', 'nan', 'None', 'NaT']:
        return None
    for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y']:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def data_fim_efetiva(data_fim_anuncio, periodo_fim):
    """
    Retorna a menor data entre data_fim_anuncio e periodo_fim.
    Se data_fim_anuncio for None ("Ilimitado"), retorna periodo_fim.

    Regra de negócio: o anúncio só rodou até a menor das duas datas.
    Depois disso, qualquer venda do SKU é orgânica (sem ads).
    """
    if data_fim_anuncio is None:
        return periodo_fim
    if periodo_fim is None:
        return data_fim_anuncio
    return min(data_fim_anuncio, periodo_fim)


def extrair_metadados_csv(arquivo):
    """
    Lê as primeiras linhas do CSV para extrair metadados.
    Retorna dict com: tipo_relatorio, loja, id_loja, periodo_inicio, periodo_fim
    """
    try:
        if hasattr(arquivo, 'read'):
            arquivo.seek(0)
            raw = arquivo.read()
            if isinstance(raw, bytes):
                texto = raw.decode('utf-8-sig')
            else:
                texto = raw
            arquivo.seek(0)
        else:
            with open(arquivo, 'r', encoding='utf-8-sig') as f:
                texto = f.read()

        linhas = texto.split('\n')[:8]
        meta = {
            'tipo_relatorio': 'geral',
            'loja': '',
            'id_loja': '',
            'periodo_inicio': None,
            'periodo_fim': None,
            'nome_grupo': None,
            'nome_produto': None,
            'id_produto': None,
        }

        for linha in linhas:
            linha = linha.strip().replace('\r', '')

            # Detectar tipo de relatório
            if 'Ad Group' in linha or 'Grupo de Anúncios' in linha:
                meta['tipo_relatorio'] = 'grupo'
                match = re.search(r'(Grupo de Anúncios[^-]*)', linha)
                if match:
                    meta['nome_grupo'] = match.group(1).strip()
            elif 'Relatório de Anúncios de Produto' in linha or 'Product Ad' in linha:
                meta['tipo_relatorio'] = 'produto'

            # Loja
            if linha.startswith('Nome da loja,'):
                meta['loja'] = linha.split(',', 1)[1].strip()
            elif linha.startswith('ID da Loja,'):
                meta['id_loja'] = linha.split(',', 1)[1].strip()

            # Período
            if linha.startswith('Período,') or linha.startswith('Periodo,'):
                periodo_str = linha.split(',', 1)[1].strip()
                # Formato: "09/12/2025 - 09/03/2026"
                match = re.search(r'(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})', periodo_str)
                if match:
                    meta['periodo_inicio'] = datetime.strptime(match.group(1), '%d/%m/%Y').date()
                    meta['periodo_fim'] = datetime.strptime(match.group(2), '%d/%m/%Y').date()

            # Produto (para relatório individual)
            if linha.startswith('Nome do Produto'):
                meta['nome_produto'] = linha.split(',', 1)[1].strip()
            if linha.startswith('ID do Produto,'):
                meta['id_produto'] = linha.split(',', 1)[1].strip()

        return meta

    except Exception as e:
        return {'erro': str(e)}


def detectar_header_ads_shopee(arquivo):
    """
    Detecta a linha do header no CSV de ads.
    Procura pela coluna '#', 'Nome do Anúncio' ou 'Palavra-chave/Localização'.
    Retorna skiprows (0-indexed) ou 7 (default).
    """
    try:
        if hasattr(arquivo, 'read'):
            arquivo.seek(0)

        for skiprows in range(6, 14):
            try:
                if hasattr(arquivo, 'read'):
                    arquivo.seek(0)
                df_test = pd.read_csv(arquivo, skiprows=skiprows, nrows=1, encoding='utf-8-sig')
                colunas = [str(c).lower() for c in df_test.columns]
                if ('#' in colunas
                        or 'nome do anúncio' in colunas
                        or 'palavra-chave/localização' in colunas):
                    return skiprows
            except Exception:
                continue
        return 7  # default

    except Exception:
        return 7


def processar_csv_ads_shopee(arquivo, loja_override=None):
    """
    Processa um CSV de ads da Shopee.

    Parâmetros:
        arquivo: file-like object ou path do CSV
        loja_override: nome da loja (sobrescreve o detectado no CSV)

    Retorna:
        (df_processado, metadados) ou (None, mensagem_erro)
    """
    try:
        # 1. Extrair metadados
        meta = extrair_metadados_csv(arquivo)
        if 'erro' in meta:
            return None, f"Erro ao ler metadados: {meta['erro']}"

        loja = loja_override or meta.get('loja', 'Desconhecida')

        # 2. Detectar header
        if hasattr(arquivo, 'read'):
            arquivo.seek(0)
        skiprows = detectar_header_ads_shopee(arquivo)

        # 3. Ler dados
        if hasattr(arquivo, 'read'):
            arquivo.seek(0)
        df = pd.read_csv(arquivo, skiprows=skiprows, encoding='utf-8-sig')

        if df.empty:
            return None, "Arquivo vazio ou sem dados"

        # 4. Identificar colunas disponíveis (case-insensitive, acento-tolerante)
        colunas_map = {}
        for col in df.columns:
            col_lower = str(col).strip().lower()
            if col_lower == '#':
                colunas_map['indice'] = col
            elif 'nome do anúncio' in col_lower or 'anúncio / nome' in col_lower:
                colunas_map['nome'] = col
            elif 'id do produto' in col_lower:
                colunas_map['id_produto'] = col
            elif 'tipos de anúncios' in col_lower or 'tipo' in col_lower:
                colunas_map['tipo'] = col
            elif col_lower == 'status' or 'status do anúncio' in col_lower:
                colunas_map['status'] = col
            elif 'método de lance' in col_lower:
                colunas_map['metodo'] = col
            # ---- DATAS DO ANÚNCIO (NOVAS) ----
            elif col_lower in ('data de início', 'data de inicio'):
                colunas_map['data_inicio_anuncio'] = col
            elif col_lower == 'data de encerramento':
                colunas_map['data_fim_anuncio'] = col
            # ---- MÉTRICAS ----
            elif col_lower == 'impressões' or col_lower == 'impressoes':
                colunas_map['impressoes'] = col
            elif col_lower == 'cliques':
                colunas_map['cliques'] = col
            elif col_lower == 'ctr':
                colunas_map['ctr'] = col
            elif col_lower == 'conversões' and 'diret' not in col_lower:
                colunas_map['conversoes'] = col
            elif col_lower == 'conversões diretas':
                colunas_map['conversoes_diretas'] = col
            # NOVO: taxa de conversão direta
            elif col_lower in ('taxa de conversão direta', 'taxa de conversao direta'):
                colunas_map['taxa_conversao_direta'] = col
            # NOVO: custo por conversão direta
            elif col_lower in ('custo por conversão direta', 'custo por conversao direta'):
                colunas_map['custo_por_conversao_direta'] = col
            elif col_lower == 'itens vendidos' and 'diret' not in col_lower:
                colunas_map['itens_vendidos'] = col
            elif col_lower == 'itens vendidos diretos':
                colunas_map['itens_vendidos_diretos'] = col
            elif col_lower == 'gmv':
                colunas_map['gmv'] = col
            elif col_lower == 'receita direta':
                colunas_map['receita_direta'] = col
            elif col_lower == 'despesas':
                colunas_map['despesas'] = col
            # NOVO: ROAS Direto
            elif col_lower == 'roas direto':
                colunas_map['roas_direto'] = col
            elif col_lower == 'acos' and 'direto' not in col_lower:
                colunas_map['acos'] = col
            elif col_lower == 'acos direto':
                colunas_map['acos_direto'] = col
            elif 'palavra-chave' in col_lower:
                colunas_map['palavra_chave'] = col
            elif 'posicionamento médio' in col_lower:
                colunas_map['posicionamento'] = col

        # 5. Construir DataFrame padronizado
        registros = []

        # Para relatórios de grupo: pular linha 0 (resumo do grupo)
        if meta['tipo_relatorio'] == 'grupo':
            df_dados = df.iloc[1:] if len(df) > 1 else df
        else:
            df_dados = df

        for _, row in df_dados.iterrows():
            nome = str(row.get(colunas_map.get('nome', ''), '')).strip()
            if not nome or nome == 'nan' or nome == '-':
                continue

            # Ignorar linhas que são o resumo do próprio grupo/produto
            if 'Grupo de Anúncios' in nome and meta['tipo_relatorio'] == 'grupo':
                continue

            despesas = parse_numero_br(row.get(colunas_map.get('despesas', ''), 0))

            registro = {
                'loja': loja,
                'periodo_inicio': meta.get('periodo_inicio'),
                'periodo_fim': meta.get('periodo_fim'),
                'nome_anuncio': nome[:500],
                'id_produto': str(row.get(colunas_map.get('id_produto', ''), '')).strip().replace('-', ''),
                'tipo_anuncio': str(row.get(colunas_map.get('tipo', ''), '')).strip(),
                'metodo_lance': str(row.get(colunas_map.get('metodo', ''), '')).strip(),
                'status_anuncio': str(row.get(colunas_map.get('status', ''), '')).strip(),
                # DATAS DO ANÚNCIO (NOVAS)
                'data_inicio_anuncio': parse_data_anuncio(row.get(colunas_map.get('data_inicio_anuncio', ''), None)),
                'data_fim_anuncio': parse_data_anuncio(row.get(colunas_map.get('data_fim_anuncio', ''), None)),
                # MÉTRICAS
                'impressoes': int(parse_numero_br(row.get(colunas_map.get('impressoes', ''), 0))),
                'cliques': int(parse_numero_br(row.get(colunas_map.get('cliques', ''), 0))),
                'ctr': parse_numero_br(row.get(colunas_map.get('ctr', ''), 0)),
                'conversoes': int(parse_numero_br(row.get(colunas_map.get('conversoes', ''), 0))),
                'conversoes_diretas': int(parse_numero_br(row.get(colunas_map.get('conversoes_diretas', ''), 0))),
                # NOVAS
                'taxa_conversao_direta': parse_numero_br(row.get(colunas_map.get('taxa_conversao_direta', ''), 0)),
                'custo_por_conversao_direta': parse_numero_br(row.get(colunas_map.get('custo_por_conversao_direta', ''), 0)),
                # MÉTRICAS CONT.
                'itens_vendidos': int(parse_numero_br(row.get(colunas_map.get('itens_vendidos', ''), 0))),
                'itens_vendidos_diretos': int(parse_numero_br(row.get(colunas_map.get('itens_vendidos_diretos', ''), 0))),
                'gmv': parse_numero_br(row.get(colunas_map.get('gmv', ''), 0)),
                'receita_direta': parse_numero_br(row.get(colunas_map.get('receita_direta', ''), 0)),
                'despesas': despesas,
                'acos': parse_numero_br(row.get(colunas_map.get('acos', ''), 0)),
                'acos_direto': parse_numero_br(row.get(colunas_map.get('acos_direto', ''), 0)),
                # NOVA
                'roas_direto': parse_numero_br(row.get(colunas_map.get('roas_direto', ''), 0)),
                'grupo_anuncio': meta.get('nome_grupo', None),
            }

            # Para relatórios de grupo, preencher tipo e método do grupo
            if meta['tipo_relatorio'] == 'grupo' and not registro['tipo_anuncio']:
                registro['tipo_anuncio'] = 'Anúncio de Produto (Grupo)'
            if meta['tipo_relatorio'] == 'produto' and not registro['tipo_anuncio']:
                registro['tipo_anuncio'] = 'Anúncio de Produto Individual'

            registros.append(registro)

        if not registros:
            return None, "Nenhum registro válido encontrado no arquivo"

        df_resultado = pd.DataFrame(registros)

        # Info para exibição
        meta['total_registros'] = len(df_resultado)
        meta['total_despesas'] = df_resultado['despesas'].sum()
        meta['total_gmv'] = df_resultado['gmv'].sum()
        meta['loja'] = loja

        return df_resultado, meta

    except Exception as e:
        return None, f"Erro ao processar: {str(e)}"


def gravar_ads_shopee(df_ads, arquivo_nome, engine):
    """
    Grava registros de ads no banco fact_ads_shopee.
    Usa UPSERT (INSERT ON CONFLICT UPDATE) para evitar duplicatas.
    SAVEPOINT por linha — falha individual não derruba a transação toda.

    Retorna: (gravados, erros, duplicatas)
    """
    gravados = 0
    erros = []
    duplicatas = 0

    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        for _, row in df_ads.iterrows():
            try:
                cursor.execute("SAVEPOINT sp_ads")

                cursor.execute("""
                    INSERT INTO fact_ads_shopee (
                        loja, periodo_inicio, periodo_fim, nome_anuncio, id_produto,
                        tipo_anuncio, metodo_lance, status_anuncio,
                        data_inicio_anuncio, data_fim_anuncio,
                        impressoes, cliques, ctr, conversoes, conversoes_diretas,
                        taxa_conversao_direta, custo_por_conversao_direta,
                        itens_vendidos, itens_vendidos_diretos,
                        gmv, receita_direta, despesas,
                        acos, acos_direto, roas_direto,
                        grupo_anuncio, arquivo_origem
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (loja, periodo_inicio, periodo_fim, nome_anuncio, id_produto)
                    DO UPDATE SET
                        tipo_anuncio = EXCLUDED.tipo_anuncio,
                        metodo_lance = EXCLUDED.metodo_lance,
                        status_anuncio = EXCLUDED.status_anuncio,
                        data_inicio_anuncio = EXCLUDED.data_inicio_anuncio,
                        data_fim_anuncio = EXCLUDED.data_fim_anuncio,
                        impressoes = EXCLUDED.impressoes,
                        cliques = EXCLUDED.cliques,
                        ctr = EXCLUDED.ctr,
                        conversoes = EXCLUDED.conversoes,
                        conversoes_diretas = EXCLUDED.conversoes_diretas,
                        taxa_conversao_direta = EXCLUDED.taxa_conversao_direta,
                        custo_por_conversao_direta = EXCLUDED.custo_por_conversao_direta,
                        itens_vendidos = EXCLUDED.itens_vendidos,
                        itens_vendidos_diretos = EXCLUDED.itens_vendidos_diretos,
                        gmv = EXCLUDED.gmv,
                        receita_direta = EXCLUDED.receita_direta,
                        despesas = EXCLUDED.despesas,
                        acos = EXCLUDED.acos,
                        acos_direto = EXCLUDED.acos_direto,
                        roas_direto = EXCLUDED.roas_direto,
                        grupo_anuncio = EXCLUDED.grupo_anuncio,
                        arquivo_origem = EXCLUDED.arquivo_origem,
                        data_upload = CURRENT_TIMESTAMP
                """, (
                    row['loja'], row['periodo_inicio'], row['periodo_fim'],
                    row['nome_anuncio'], row.get('id_produto', ''),
                    row['tipo_anuncio'], row['metodo_lance'], row['status_anuncio'],
                    row.get('data_inicio_anuncio'), row.get('data_fim_anuncio'),
                    row['impressoes'], row['cliques'], row['ctr'],
                    row['conversoes'], row['conversoes_diretas'],
                    row.get('taxa_conversao_direta', 0), row.get('custo_por_conversao_direta', 0),
                    row['itens_vendidos'], row['itens_vendidos_diretos'],
                    row['gmv'], row['receita_direta'], row['despesas'],
                    row['acos'], row['acos_direto'], row.get('roas_direto', 0),
                    row.get('grupo_anuncio', None), arquivo_nome
                ))

                cursor.execute("RELEASE SAVEPOINT sp_ads")
                gravados += 1

            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_ads")
                if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
                    duplicatas += 1
                else:
                    erros.append(f"{row.get('nome_anuncio', '?')[:40]}: {str(e)[:80]}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        erros.append(f"Erro geral: {str(e)}")
    finally:
        cursor.close()
        conn.close()

    return gravados, erros, duplicatas


# ============================================================
# FUNÇÕES DE MATCH SKU (TEMPORAL + MÚLTIPLO)
# ============================================================

def buscar_skus_match(engine, loja, nome_produto_ads, data_ref=None):
    """
    Retorna LISTA de SKUs vigentes para um anúncio em uma data específica.

    Parâmetros:
        loja: nome da loja no formato ads (ex: 'Nala-Lit')
        nome_produto_ads: nome exato do anúncio
        data_ref: data de referência (default = hoje)

    Regra temporal:
        data_inicio <= data_ref AND (data_fim IS NULL OR data_fim >= data_ref)

    Retorna: lista de strings de SKU (pode ser vazia).
    """
    if data_ref is None:
        data_ref = date.today()
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sku FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee'
              AND loja = %s
              AND nome_produto_ads = %s
              AND data_inicio <= %s
              AND (data_fim IS NULL OR data_fim >= %s)
            ORDER BY data_inicio DESC, sku
        """, (loja, nome_produto_ads, data_ref, data_ref))
        return [r[0] for r in cursor.fetchall()]
    except Exception:
        return []
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


def buscar_match_sku(engine, loja, nome_produto_ads):
    """
    COMPATIBILIDADE: retorna o PRIMEIRO SKU vigente hoje, ou None.
    Use buscar_skus_match() para obter a lista completa.
    """
    lista = buscar_skus_match(engine, loja, nome_produto_ads)
    return lista[0] if lista else None


def atualizar_matches_sku(engine, loja, nome_produto_ads, id_produto_ads, lista_skus_novos):
    """
    Atualiza a lista de SKUs vinculados a um anúncio.

    Regras:
      - SKUs atuais que NÃO estão em lista_skus_novos → fecha (data_fim = CURRENT_DATE)
      - SKUs em lista_skus_novos que NÃO estavam ativos → abre novo (data_inicio = CURRENT_DATE)
      - SKUs presentes em ambos → não altera

    Parâmetros:
        loja: nome da loja no formato ads (ex: 'Nala-Lit')
        nome_produto_ads: nome do anúncio
        id_produto_ads: ID do produto na Shopee (opcional)
        lista_skus_novos: lista de strings de SKU

    Retorna: True em sucesso, False em erro.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Normalizar lista (remover vazios/None)
        lista_novas = [str(s).strip() for s in (lista_skus_novos or []) if s and str(s).strip()]

        # 1) SKUs atualmente ativos (data_fim IS NULL)
        cursor.execute("""
            SELECT sku FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee'
              AND loja = %s
              AND nome_produto_ads = %s
              AND data_fim IS NULL
        """, (loja, nome_produto_ads))
        atuais = [r[0] for r in cursor.fetchall()]

        # 2) Fechar SKUs que saíram da lista
        a_fechar = [s for s in atuais if s not in lista_novas]
        for sku in a_fechar:
            cursor.execute("SAVEPOINT sp_match_close")
            try:
                cursor.execute("""
                    UPDATE dim_ads_produto_sku
                    SET data_fim = CURRENT_DATE
                    WHERE marketplace = 'Shopee'
                      AND loja = %s
                      AND nome_produto_ads = %s
                      AND sku = %s
                      AND data_fim IS NULL
                """, (loja, nome_produto_ads, sku))
                cursor.execute("RELEASE SAVEPOINT sp_match_close")
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_match_close")

        # 3) Abrir SKUs novos (que não estavam ativos)
        a_abrir = [s for s in lista_novas if s not in atuais]
        for sku in a_abrir:
            cursor.execute("SAVEPOINT sp_match_open")
            try:
                cursor.execute("""
                    INSERT INTO dim_ads_produto_sku (
                        marketplace, loja, nome_produto_ads, id_produto_ads,
                        sku, data_inicio, data_fim
                    ) VALUES ('Shopee', %s, %s, %s, %s, CURRENT_DATE, NULL)
                    ON CONFLICT (marketplace, loja, nome_produto_ads, sku, data_inicio)
                    DO NOTHING
                """, (loja, nome_produto_ads, id_produto_ads or '', sku))
                cursor.execute("RELEASE SAVEPOINT sp_match_open")
            except Exception:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_match_open")

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


def salvar_match_sku(engine, loja, nome_produto_ads, id_produto_ads, sku):
    """
    COMPATIBILIDADE: substitui TODOS os matches pelo único SKU informado.
    Fecha qualquer outro SKU ativo e abre um registro novo para o SKU passado.
    Use atualizar_matches_sku() para múltiplos SKUs.
    """
    return atualizar_matches_sku(engine, loja, nome_produto_ads, id_produto_ads, [sku])


# ============================================================
# CÁLCULO DE TACOS
# ============================================================

def calcular_tacos(engine, loja_ads, skus, periodo_inicio, periodo_fim):
    """
    Calcula TACOS cruzando fact_ads_shopee com fact_vendas_snapshot.

    Parâmetros:
        loja_ads: nome da loja no formato ads (ex: 'Nala-Lit')
                  — traduzida para loja_origem via LOJA_ADS_PARA_ORIGEM
        skus: string (um único SKU) OU lista de strings
              — com múltiplos SKUs, soma as vendas de TODOS (match múltiplo)
        periodo_inicio / periodo_fim: período do relatório de ads

    BUGS CORRIGIDOS nesta versão:
      - marketplace_origem filtrado como 'SHOPEE' (era 'Shopee')
      - loja_ads traduzida para loja_origem via LOJA_ADS_PARA_ORIGEM

    Retorna dict com:
        skus, receita_total, qtd_total, investimento, itens_ads,
        gmv_ads, receita_direta, tacos, acos, pct_organico
    Ou {'erro': str} em caso de falha.
    """
    # Normalizar skus em lista
    if isinstance(skus, str):
        skus_lista = [skus]
    else:
        skus_lista = list(skus) if skus else []
    skus_lista = [str(s).strip() for s in skus_lista if s and str(s).strip()]

    if not skus_lista:
        return {'erro': 'Nenhum SKU informado'}

    # Traduzir loja_ads → loja_origem
    loja_origem = LOJA_ADS_PARA_ORIGEM.get(loja_ads, loja_ads)

    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(skus_lista))

        # 1) Vendas totais dos SKUs no período (ads + orgânico)
        cursor.execute(f"""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(SUM(quantidade), 0)
            FROM fact_vendas_snapshot
            WHERE sku IN ({placeholders})
              AND loja_origem = %s
              AND marketplace_origem = 'SHOPEE'
              AND data_venda BETWEEN %s AND %s
        """, tuple(skus_lista) + (loja_origem, periodo_inicio, periodo_fim))
        vendas = cursor.fetchone()
        receita_total = float(vendas[0])
        qtd_total = int(vendas[1])

        # 2) Ads dos SKUs no período (cruza pelo sku_match)
        cursor.execute(f"""
            SELECT COALESCE(SUM(despesas), 0),
                   COALESCE(SUM(itens_vendidos_diretos), 0),
                   COALESCE(SUM(gmv), 0),
                   COALESCE(SUM(receita_direta), 0)
            FROM fact_ads_shopee
            WHERE sku_match IN ({placeholders})
              AND loja = %s
              AND periodo_inicio >= %s AND periodo_fim <= %s
        """, tuple(skus_lista) + (loja_ads, periodo_inicio, periodo_fim))
        ads = cursor.fetchone()
        investimento = float(ads[0])
        itens_ads = int(ads[1])
        gmv_ads = float(ads[2])
        receita_direta_ads = float(ads[3])

        # Métricas derivadas
        tacos = (investimento / receita_total * 100) if receita_total > 0 else None
        pct_organico = max(0, ((qtd_total - itens_ads) / qtd_total * 100)) if qtd_total > 0 else None
        acos = (investimento / gmv_ads * 100) if gmv_ads > 0 else None

        return {
            'skus': skus_lista,
            'receita_total': receita_total,
            'qtd_total': qtd_total,
            'investimento': investimento,
            'itens_ads': itens_ads,
            'gmv_ads': gmv_ads,
            'receita_direta': receita_direta_ads,
            'tacos': tacos,
            'acos': acos,
            'pct_organico': pct_organico,
        }

    except Exception as e:
        return {'erro': str(e)}
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
