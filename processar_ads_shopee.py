"""
processar_ads_shopee.py — Processador de relatórios de Ads da Shopee
Sistema Nala — Módulo Ads

Tipos de relatório suportados:
1. Relatório Geral (Todos os Anúncios CPC) — exportado da Central de Marketing
2. Relatório de Grupo de Anúncios — exportado de um grupo específico
3. Relatório de Produto Individual — exportado de um anúncio específico

Estrutura do CSV:
- Linhas 1-6: Metadados (nome da loja, ID, período, etc.)
- Linha 7: Vazia
- Linha 8: Header das colunas
- Linha 9+: Dados
"""

import pandas as pd
import io
import re
from datetime import datetime


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
    # Cuidado: "1.234,56" → "1234.56" | "1234.56" → "1234.56"
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


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
                # Extrair nome do grupo
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
    Procura pela coluna '#' ou 'Palavra-chave' nas primeiras 15 linhas.
    Retorna o número da linha (0-indexed) ou None.
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
                if '#' in colunas or 'palavra-chave/localização' in colunas:
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

        # 4. Identificar colunas disponíveis
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
                'impressoes': int(parse_numero_br(row.get(colunas_map.get('impressoes', ''), 0))),
                'cliques': int(parse_numero_br(row.get(colunas_map.get('cliques', ''), 0))),
                'ctr': parse_numero_br(row.get(colunas_map.get('ctr', ''), 0)),
                'conversoes': int(parse_numero_br(row.get(colunas_map.get('conversoes', ''), 0))),
                'conversoes_diretas': int(parse_numero_br(row.get(colunas_map.get('conversoes_diretas', ''), 0))),
                'itens_vendidos': int(parse_numero_br(row.get(colunas_map.get('itens_vendidos', ''), 0))),
                'itens_vendidos_diretos': int(parse_numero_br(row.get(colunas_map.get('itens_vendidos_diretos', ''), 0))),
                'gmv': parse_numero_br(row.get(colunas_map.get('gmv', ''), 0)),
                'receita_direta': parse_numero_br(row.get(colunas_map.get('receita_direta', ''), 0)),
                'despesas': despesas,
                'acos': parse_numero_br(row.get(colunas_map.get('acos', ''), 0)),
                'acos_direto': parse_numero_br(row.get(colunas_map.get('acos_direto', ''), 0)),
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
                        impressoes, cliques, ctr, conversoes, conversoes_diretas,
                        itens_vendidos, itens_vendidos_diretos,
                        gmv, receita_direta, despesas, acos, acos_direto,
                        grupo_anuncio, arquivo_origem
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (loja, periodo_inicio, periodo_fim, nome_anuncio, id_produto)
                    DO UPDATE SET
                        tipo_anuncio = EXCLUDED.tipo_anuncio,
                        metodo_lance = EXCLUDED.metodo_lance,
                        status_anuncio = EXCLUDED.status_anuncio,
                        impressoes = EXCLUDED.impressoes,
                        cliques = EXCLUDED.cliques,
                        ctr = EXCLUDED.ctr,
                        conversoes = EXCLUDED.conversoes,
                        conversoes_diretas = EXCLUDED.conversoes_diretas,
                        itens_vendidos = EXCLUDED.itens_vendidos,
                        itens_vendidos_diretos = EXCLUDED.itens_vendidos_diretos,
                        gmv = EXCLUDED.gmv,
                        receita_direta = EXCLUDED.receita_direta,
                        despesas = EXCLUDED.despesas,
                        acos = EXCLUDED.acos,
                        acos_direto = EXCLUDED.acos_direto,
                        grupo_anuncio = EXCLUDED.grupo_anuncio,
                        arquivo_origem = EXCLUDED.arquivo_origem,
                        data_upload = CURRENT_TIMESTAMP
                """, (
                    row['loja'], row['periodo_inicio'], row['periodo_fim'],
                    row['nome_anuncio'], row.get('id_produto', ''),
                    row['tipo_anuncio'], row['metodo_lance'], row['status_anuncio'],
                    row['impressoes'], row['cliques'], row['ctr'],
                    row['conversoes'], row['conversoes_diretas'],
                    row['itens_vendidos'], row['itens_vendidos_diretos'],
                    row['gmv'], row['receita_direta'], row['despesas'],
                    row['acos'], row['acos_direto'],
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


def buscar_match_sku(engine, loja, nome_produto_ads):
    """Busca SKU já mapeado para um produto de ads"""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sku FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee' AND loja = %s AND nome_produto_ads = %s
        """, (loja, nome_produto_ads))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except Exception:
        return None


def salvar_match_sku(engine, loja, nome_produto_ads, id_produto_ads, sku):
    """Salva ou atualiza matching produto ads → SKU"""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dim_ads_produto_sku (marketplace, loja, nome_produto_ads, id_produto_ads, sku)
            VALUES ('Shopee', %s, %s, %s, %s)
            ON CONFLICT (marketplace, loja, nome_produto_ads)
            DO UPDATE SET sku = EXCLUDED.sku, id_produto_ads = EXCLUDED.id_produto_ads
        """, (loja, nome_produto_ads, id_produto_ads or '', sku))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False


def calcular_tacos(engine, loja, sku, periodo_inicio, periodo_fim):
    """
    Calcula TACOS cruzando fact_ads_shopee com fact_vendas_snapshot.
    Retorna dict com: receita_total, qtd_total, investimento, tacos, pct_organico
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Vendas totais do SKU no período
        cursor.execute("""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(SUM(quantidade), 0)
            FROM fact_vendas_snapshot
            WHERE sku = %s AND loja_origem = %s
              AND data_venda BETWEEN %s AND %s
        """, (sku, loja, periodo_inicio, periodo_fim))
        vendas = cursor.fetchone()
        receita_total = float(vendas[0])
        qtd_total = int(vendas[1])

        # Ads do SKU no período
        cursor.execute("""
            SELECT COALESCE(SUM(despesas), 0),
                   COALESCE(SUM(itens_vendidos), 0),
                   COALESCE(SUM(gmv), 0)
            FROM fact_ads_shopee
            WHERE sku_match = %s AND loja = %s
              AND periodo_inicio >= %s AND periodo_fim <= %s
        """, (sku, loja, periodo_inicio, periodo_fim))
        ads = cursor.fetchone()
        investimento = float(ads[0])
        itens_ads = int(ads[1])
        gmv_ads = float(ads[2])

        cursor.close()
        conn.close()

        tacos = (investimento / receita_total * 100) if receita_total > 0 else None
        pct_organico = max(0, ((qtd_total - itens_ads) / qtd_total * 100)) if qtd_total > 0 else None
        acos = (investimento / gmv_ads * 100) if gmv_ads > 0 else None

        return {
            'sku': sku,
            'receita_total': receita_total,
            'qtd_total': qtd_total,
            'investimento': investimento,
            'itens_ads': itens_ads,
            'gmv_ads': gmv_ads,
            'tacos': tacos,
            'acos': acos,
            'pct_organico': pct_organico,
        }

    except Exception as e:
        return {'erro': str(e)}
