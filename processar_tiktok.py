"""
PROCESSADOR TIKTOK SHOP - Sistema Nala
VERSAO 1.0 (01/07/2026):
  - Suporte a dois relatórios: Financeiro (liquidado) e Em Espera (estimado)
  - Detecção dinâmica de cabeçalho (em espera tem metadados antes do header)
  - SKU resolvido via dim_sku_mapeamento; sem mapeamento -> pendente
  - Fluxo: em espera -> staging em fact_vendas_pendentes (motivo 'Em espera TikTok')
           financeiro -> apaga staging do pedido + grava snapshot (se SKU mapeado)
                                                 + grava pendente (se SKU não mapeado)
  - DELETE-before-INSERT por período na snapshot (evita duplicatas em reupload)
  - SAVEPOINT por linha em todas as gravações
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from database_utils import (
    buscar_custos_skus,
    buscar_skus_validos,
    gravar_venda_pendente,
)

_MOTIVO_EMESPERA = 'Em espera TikTok'
_MOTIVO_SKU_NAO_MAPEADO = 'TikTok - SKU não mapeado'
_LOGISTICA = 'SFP'
_DEFAULT_COMISSAO_PCT = 12.0  # 6% plataforma + 6% SFP
_DEFAULT_TAXA_ITEM = 4.0      # R$4 por unidade vendida


# ============================================================
# HELPERS INTERNOS
# ============================================================

def _limpar_valor(valor):
    if pd.isna(valor) or str(valor).strip() in ('', '/', 'nan'):
        return 0.0
    try:
        return float(str(valor).replace(',', '.').strip())
    except (ValueError, TypeError):
        return 0.0


def _parse_data(valor_str):
    s = str(valor_str).strip()
    if not s or s in ('/', 'nan', ''):
        return None
    for fmt in ('%Y/%m/%d', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None


def _detectar_header(df_raw, ancora='ID do pedido/ajuste'):
    """Detecta a linha de cabeçalho varrendo as primeiras 15 linhas."""
    # Tentar primeiro como header normal (linha 0)
    if ancora in df_raw.columns:
        return df_raw

    for i in range(min(15, len(df_raw))):
        if ancora in df_raw.iloc[i].values:
            novo_header = df_raw.iloc[i].values
            df = df_raw.iloc[i + 1:].copy()
            df.columns = novo_header
            df.reset_index(drop=True, inplace=True)
            return df
    return None


def _buscar_config_tiktok(engine, loja):
    """Busca comissao_pct e taxa_fixa em dim_config_marketplace para TIKTOK."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT comissao_percentual, taxa_fixa
            FROM dim_config_marketplace
            WHERE marketplace = 'TIKTOK' AND loja = %s AND ativo = true
            LIMIT 1
        """, (loja,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return float(row[0]), float(row[1])
    except Exception:
        pass
    return _DEFAULT_COMISSAO_PCT, _DEFAULT_TAXA_ITEM


def _buscar_tiktok_sku_map(engine):
    """Carrega mapeamento id_sku_tiktok → sku_interno da dim_tiktok_skus."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id_sku_tiktok, sku_interno FROM dim_tiktok_skus "
            "WHERE sku_interno IS NOT NULL AND sku_interno != ''"
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def _upsert_tiktok_nomes(engine, nomes_dict):
    """Upsert nome_produto/nome_sku_variante sem sobrescrever sku_interno."""
    if not nomes_dict:
        return
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_tiktok_skus (
                id_sku_tiktok     VARCHAR(50) PRIMARY KEY,
                nome_produto      TEXT,
                nome_sku_variante TEXT,
                sku_interno       VARCHAR(120),
                atualizado_em     TIMESTAMP DEFAULT NOW()
            )
        """)
        for sku_tiktok, (nome_prod, nome_var) in nomes_dict.items():
            cursor.execute("""
                INSERT INTO dim_tiktok_skus (id_sku_tiktok, nome_produto, nome_sku_variante)
                VALUES (%s, %s, %s)
                ON CONFLICT (id_sku_tiktok)
                DO UPDATE SET nome_produto = EXCLUDED.nome_produto,
                              nome_sku_variante = EXCLUDED.nome_sku_variante,
                              atualizado_em = NOW()
            """, (sku_tiktok, nome_prod, nome_var))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass  # nome enrichment não é crítico


# ============================================================
# PARSER ÚNICO (financeiro e em_espera têm as mesmas colunas)
# ============================================================

def _processar_df(df, fonte, loja, imposto_pct, tiktok_sku_map, custos_dict):
    """
    Processa um DataFrame já com header detectado.
    fonte: 'financeiro' | 'em_espera'

    Retorna:
      vendas_ok       - linhas com SKU mapeado (-> snapshot, tanto financeiro quanto em_espera)
      pendentes_lista - linhas sem SKU mapeado (-> fact_vendas_pendentes)
      datas           - lista de date para extrair período
      skus_sem_custo  - set de SKUs mapeados mas sem custo cadastrado
      skus_nao_map    - set de IDs TikTok sem mapeamento
      linhas_desc     - count de linhas descartadas
      nomes_dict      - {sku_tiktok: (nome_produto, nome_sku_variante)} para upsert
      descartes_lista - detalhe de cada linha descartada (numero_pedido, sku, motivo)
                        para persistir em fact_vendas_descartadas
    """
    col_valor_total = ('Valor total a ser liquidado' if fonte == 'financeiro'
                       else 'Valor estimado a ser liquidado')

    vendas_ok = []
    pendentes_lista = []
    datas = []
    skus_sem_custo = set()
    skus_nao_map = set()
    linhas_desc = 0
    nomes_dict = {}
    descartes_lista = []

    for _, row in df.iterrows():
        try:
            tipo = str(row.get('Tipo de transação', 'Pedido')).strip()
            pedido_id = str(row.get('ID do pedido/ajuste', '')).strip()
            sku_tiktok = str(row.get('ID do SKU', '')).strip()
            vendas_liq = _limpar_valor(row.get('Vendas líquidas dos produtos', 0))

            if tipo != 'Pedido':
                linhas_desc += 1
                descartes_lista.append({
                    'numero_pedido': pedido_id,
                    'sku': sku_tiktok,
                    'status_original': tipo,
                    'motivo_descarte': f"Tipo de transação diferente de 'Pedido': {tipo}",
                    'receita_estimada': max(vendas_liq, 0),
                    'tarifa_venda_estimada': 0,
                    'tarifa_envio_estimada': 0,
                    'logistica': _LOGISTICA,
                })
                continue

            if vendas_liq <= 0:
                linhas_desc += 1
                descartes_lista.append({
                    'numero_pedido': pedido_id,
                    'sku': sku_tiktok,
                    'status_original': tipo,
                    'motivo_descarte': f"Vendas líquidas <= 0 (valor={vendas_liq})",
                    'receita_estimada': 0,
                    'tarifa_venda_estimada': 0,
                    'tarifa_envio_estimada': 0,
                    'logistica': _LOGISTICA,
                })
                continue

            if not pedido_id or not sku_tiktok or pedido_id == 'nan':
                linhas_desc += 1
                descartes_lista.append({
                    'numero_pedido': pedido_id,
                    'sku': sku_tiktok,
                    'status_original': tipo,
                    'motivo_descarte': "ID do pedido ou ID do SKU ausente",
                    'receita_estimada': max(vendas_liq, 0),
                    'tarifa_venda_estimada': 0,
                    'tarifa_envio_estimada': 0,
                    'logistica': _LOGISTICA,
                })
                continue

            qtd = max(1, int(_limpar_valor(row.get('Quantidade', 1)) or 1))
            data_venda = _parse_data(row.get('Data de criação do pedido'))
            if data_venda:
                datas.append(data_venda)

            # Captura nomes do produto e variante para enriquecer dim_tiktok_skus
            nome_produto = str(row.get('Nome do produto', '') or '').strip()
            nome_sku_variante = str(row.get('Nome do SKU', '') or '').strip()
            if nome_produto or nome_sku_variante:
                nomes_dict[sku_tiktok] = (nome_produto, nome_sku_variante)

            subtotal = _limpar_valor(row.get('Subtotal do item antes dos descontos', 0))
            desc_vendedor = abs(_limpar_valor(row.get('Descontos financiados pelo vendedor', 0)))
            custo_frete_rep = _limpar_valor(row.get('Custo líquido de frete', 0))
            comissao_plat = abs(_limpar_valor(row.get('Tarifa de comissão da plataforma', 0)))
            sfp = abs(_limpar_valor(row.get('Taxa de serviço do SFP', 0)))
            taxa_item = abs(_limpar_valor(row.get('Taxa por item vendido', 0)))
            afiliados = abs(_limpar_valor(row.get('Comissões de afiliados', 0)))
            ads_criadores = abs(_limpar_valor(row.get('Comissão de Anúncios da loja paga aos criadores', 0)))
            ads_agencias = abs(_limpar_valor(row.get('Comissão de Anúncios da loja paga às agências parceiras', 0)))
            gmv_max = abs(_limpar_valor(row.get('Taxa de anúncio de GMV Max', 0)))
            valor_total_rep = _limpar_valor(row.get(col_valor_total, 0))

            # comissao = 6% plataforma + 6% SFP
            comissao_val = round(comissao_plat + sfp, 2)
            # tarifa_fixa = R$4/unit (taxa por item)
            tarifa_fixa_val = round(taxa_item, 2)
            # outros_custos = variáveis (afiliados, ads, GMV Max)
            outros_custos_val = round(afiliados + ads_criadores + ads_agencias + gmv_max, 2)
            # frete: negativo no relatório = custo pro vendedor
            frete_val = round(-custo_frete_rep, 2)

            total_tarifas = round(comissao_val + tarifa_fixa_val + outros_custos_val + frete_val, 2)
            valor_liquido = round(valor_total_rep, 2)
            imposto_val = round(vendas_liq * (imposto_pct / 100), 2)

            # Resolução de SKU via dim_tiktok_skus
            sku_nala = tiktok_sku_map.get(sku_tiktok)
            sku_mapeado = sku_nala is not None

            custo_un = 0.0
            if sku_mapeado:
                custo_un = custos_dict.get(sku_nala, 0.0)
                if custo_un == 0:
                    skus_sem_custo.add(sku_nala)
            else:
                skus_nao_map.add(sku_tiktok)

            custo_total = round(custo_un * qtd, 2)
            margem = round(valor_liquido - custo_total - imposto_val, 2)
            margem_pct = round((margem / vendas_liq * 100) if vendas_liq > 0 else 0, 2)

            # numero_pedido sintético: único por (pedido, variante TikTok)
            numero_pedido = f"TKTK_{pedido_id}_{sku_tiktok}"

            item = {
                'pedido': numero_pedido,
                'pedido_original': pedido_id,
                'sku_tiktok': sku_tiktok,
                'sku': sku_nala if sku_mapeado else sku_tiktok,
                'data': data_venda,
                'qtd': qtd,
                'preco_venda': round(subtotal, 2),
                'receita': round(vendas_liq, 2),
                'desconto_parceiro': round(desc_vendedor, 2),
                'desconto_marketplace': 0.0,
                'comissao': comissao_val,
                'tarifa_fixa': tarifa_fixa_val,
                'outros_custos': outros_custos_val,
                'frete': frete_val,
                'total_tarifas': total_tarifas,
                'valor_liquido': valor_liquido,
                'imposto': imposto_val,
                'custo': round(custo_un, 2),
                'custo_total': custo_total,
                'margem': margem,
                'margem_pct': margem_pct,
                'sku_mapeado': sku_mapeado,
                'fonte': fonte,
            }

            # SKU mapeado -> snapshot (financeiro e em_espera); sem SKU -> pendentes
            if sku_mapeado:
                vendas_ok.append(item)
            else:
                pendentes_lista.append(item)

        except Exception as e:
            linhas_desc += 1
            descartes_lista.append({
                'numero_pedido': str(row.get('ID do pedido/ajuste', '')).strip(),
                'sku': str(row.get('ID do SKU', '')).strip(),
                'status_original': 'Erro',
                'motivo_descarte': f"Erro interno ao processar linha: {str(e)[:200]}",
                'receita_estimada': 0,
                'tarifa_venda_estimada': 0,
                'tarifa_envio_estimada': 0,
                'logistica': _LOGISTICA,
            })
            continue

    return vendas_ok, pendentes_lista, datas, skus_sem_custo, skus_nao_map, linhas_desc, nomes_dict, descartes_lista


# ============================================================
# FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# ============================================================

def processar_arquivo_tiktok(arq_financeiro, arq_emespera, loja, imposto_pct, engine):
    """
    Processa relatório(s) TikTok Shop.

    arq_financeiro : arquivo liquidado (.xlsx, aba 'Detalhes do pedido'), obrigatório.
    arq_emespera   : arquivo em espera (.xlsx), opcional — pode ser None.

    Retorna (df_vendas, info) onde df_vendas contém apenas as linhas do financeiro
    com SKU já mapeado (prontas para snapshot). Pendentes ficam em info['pendentes_sku']
    e info['pendentes_emespera'].
    """
    timestamp = datetime.now().timestamp()
    tiktok_sku_map = _buscar_tiktok_sku_map(engine)
    custos_dict = buscar_custos_skus(engine, force_refresh=timestamp)

    # ── Relatório Financeiro ──────────────────────────────────────────────────
    try:
        try:
            df_fin_raw = pd.read_excel(arq_financeiro, sheet_name='Detalhes do pedido', header=None)
        except Exception:
            arq_financeiro.seek(0)
            df_fin_raw = pd.read_excel(arq_financeiro, sheet_name=0, header=None)
    except Exception as e:
        return None, f"Erro ao ler relatório financeiro: {e}"

    df_fin = _detectar_header(df_fin_raw)
    if df_fin is None:
        return None, "Coluna 'ID do pedido/ajuste' não encontrada no relatório financeiro."

    vendas_fin, pend_fin, datas_fin, sem_custo_fin, nao_map_fin, desc_fin, nomes_fin, descartes_fin = _processar_df(
        df_fin, 'financeiro', loja, imposto_pct, tiktok_sku_map, custos_dict
    )

    # ── Relatório Em Espera (opcional) ───────────────────────────────────────
    # vendas_esp = em_espera com SKU mapeado → vão para snapshot junto com financeiro
    # pend_esp   = em_espera sem SKU mapeado → vão para fact_vendas_pendentes (staging)
    vendas_esp, pend_esp, datas_esp, sem_custo_esp, nao_map_esp, desc_esp = [], [], set(), set(), 0, 0
    nomes_esp = {}
    descartes_esp = []

    if arq_emespera is not None:
        try:
            nome_esp = getattr(arq_emespera, 'name', '').lower()
            if nome_esp.endswith('.csv'):
                df_esp_raw = pd.read_csv(arq_emespera, sep=';', header=None, encoding='latin1', dtype=str)
            else:
                df_esp_raw = pd.read_excel(arq_emespera, sheet_name=0, header=None)
        except Exception as e:
            return None, f"Erro ao ler relatório em espera: {e}"

        df_esp = _detectar_header(df_esp_raw)
        if df_esp is None:
            return None, "Coluna 'ID do pedido/ajuste' não encontrada no relatório em espera."

        vendas_esp, pend_esp, datas_esp, sem_custo_esp, nao_map_esp, desc_esp, nomes_esp, descartes_esp = _processar_df(
            df_esp, 'em_espera', loja, imposto_pct, tiktok_sku_map, custos_dict
        )

    # Upsert nomes produto/variante em dim_tiktok_skus (sem sobrescrever sku_interno)
    _upsert_tiktok_nomes(engine, {**nomes_fin, **nomes_esp})

    # ── Resumo ────────────────────────────────────────────────────────────────
    todas_datas = datas_fin + datas_esp
    if todas_datas:
        periodo_ini = min(todas_datas).strftime('%d/%m/%Y')
        periodo_fim = max(todas_datas).strftime('%d/%m/%Y')
    else:
        periodo_ini = periodo_fim = '-'

    # vendas_fin + vendas_esp → snapshot; ON CONFLICT DO UPDATE garante que quando
    # um pedido em_espera aparecer no financeiro futuro, os dados reais sobrepõem.
    df_vendas = pd.DataFrame(vendas_fin + vendas_esp) if (vendas_fin or vendas_esp) else pd.DataFrame()

    info = {
        'total_linhas': len(vendas_fin) + len(vendas_esp) + len(pend_fin) + len(pend_esp),
        'linhas_descartadas': desc_fin + desc_esp,
        'skus_nao_mapeados': len(nao_map_fin | nao_map_esp),
        'skus_sem_custo': len(sem_custo_fin | sem_custo_esp),
        'periodo_inicio': periodo_ini,
        'periodo_fim': periodo_fim,
        'arquivo_nome': arq_financeiro.name,
        'pendentes_sku': pend_fin,       # financeiro sem SKU mapeado
        'pendentes_emespera': pend_esp,  # em_espera sem SKU mapeado (staging)
        'descartes': descartes_fin + descartes_esp,
        'divergencias': [],
    }

    return df_vendas, info


# ============================================================
# GRAVAÇÃO NO BANCO
# ============================================================

def gravar_vendas_tiktok(df, marketplace, loja, arq_nome, engine, data_ini=None, data_fim=None,
                          pendentes_sku=None, pendentes_emespera=None, descartes=None):
    """
    Grava vendas TikTok no banco seguindo o padrão dos demais processadores.

    Fluxo:
      1. DELETE snapshot no período (evita duplicatas em reupload)
      2. Para cada pedido do financeiro: apaga staging de em_espera nos pendentes
      3. INSERT financeiro resolvidos -> fact_vendas_snapshot  (SAVEPOINT por linha)
      4. INSERT financeiro não mapeados -> fact_vendas_pendentes (SAVEPOINT por linha)
      5. INSERT em_espera -> fact_vendas_pendentes, ON CONFLICT DO NOTHING
      6. INSERT descartes -> fact_vendas_descartadas (SAVEPOINT por linha)
    """
    if pendentes_sku is None:
        pendentes_sku = []
    if pendentes_emespera is None:
        pendentes_emespera = []
    if descartes is None:
        descartes = []

    # Auto-detectar período a partir dos dados do df + pendentes_sku
    todas_datas = []
    if not df.empty and 'data' in df.columns:
        todas_datas += df['data'].dropna().tolist()
    for p in pendentes_sku:
        if p.get('data'):
            todas_datas.append(p['data'])

    if data_ini is None or data_fim is None:
        if todas_datas:
            data_ini = min(todas_datas)
            data_fim = max(todas_datas)

    conn = engine.raw_connection()
    cursor = conn.cursor()

    reg = 0
    err = 0
    skus_invalidos = set()
    dups = 0
    pend = 0
    desc_count = 0
    atualiz = 0

    # IDs dos pedidos no financeiro (para limpar staging de em_espera)
    pedidos_financeiro = set()
    for _, row in df.iterrows():
        pedidos_financeiro.add(str(row.get('pedido_original', '')))
    for p in pendentes_sku:
        pedidos_financeiro.add(str(p.get('pedido_original', '')))

    sql_ins = """
        INSERT INTO fact_vendas_snapshot (
            marketplace_origem, loja_origem, numero_pedido, pedido_original,
            data_venda, sku, codigo_anuncio, quantidade,
            preco_venda, desconto_parceiro, desconto_marketplace,
            valor_venda_efetivo, custo_unitario, custo_total,
            imposto, comissao, frete, tarifa_fixa, outros_custos,
            total_tarifas, valor_liquido, margem_total, margem_percentual,
            data_processamento, arquivo_origem, logistica
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            NOW(), %s, %s
        )
        ON CONFLICT (numero_pedido, sku, loja_origem) DO UPDATE SET
            valor_venda_efetivo = EXCLUDED.valor_venda_efetivo,
            valor_liquido       = EXCLUDED.valor_liquido,
            comissao            = EXCLUDED.comissao,
            frete               = EXCLUDED.frete,
            tarifa_fixa         = EXCLUDED.tarifa_fixa,
            outros_custos       = EXCLUDED.outros_custos,
            total_tarifas       = EXCLUDED.total_tarifas,
            custo_unitario      = EXCLUDED.custo_unitario,
            custo_total         = EXCLUDED.custo_total,
            margem_total        = EXCLUDED.margem_total,
            margem_percentual   = EXCLUDED.margem_percentual,
            arquivo_origem      = EXCLUDED.arquivo_origem,
            data_processamento  = NOW()
    """

    total_itens = len(df) + len(pendentes_sku) + len(pendentes_emespera) + len(descartes)
    if total_itens == 0:
        total_itens = 1
    progress_bar = st.progress(0)
    status_text = st.empty()
    item_atual = 0

    try:
        # 1. DELETE snapshot do arquivo (reupload limpa e reinsere só os próprios registros)
        # NÃO usar range de data: pedidos TikTok de meses anteriores aparecem em relatórios
        # mais recentes (liquidação assíncrona), então overlap por data apaga dados de outros arquivos.
        cursor.execute(
            "DELETE FROM fact_vendas_snapshot WHERE loja_origem = %s AND marketplace_origem = %s AND arquivo_origem = %s",
            (loja, marketplace, arq_nome)
        )
        atualiz = cursor.rowcount

        # 2. Apagar staging de em_espera para pedidos que chegaram no financeiro
        # numero_pedido em pendentes = 'TKTK_{pedido_id}_{sku_tiktok}', então filtramos por prefixo
        if pedidos_financeiro:
            for pid in pedidos_financeiro:
                if pid:
                    cursor.execute(
                        """DELETE FROM fact_vendas_pendentes
                           WHERE loja_origem = %s AND numero_pedido LIKE %s
                             AND motivo LIKE %s AND status = 'Pendente'""",
                        (loja, f'TKTK_{pid}_%', f'{_MOTIVO_EMESPERA}%')
                    )

        # 3. INSERT financeiro resolvidos -> snapshot
        for idx, row in df.iterrows():
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Gravando venda financeiro {item_atual} de {total_itens}...")

                cursor.execute(f"SAVEPOINT tktk_fin_{idx}")
                cursor.execute(sql_ins, (
                    marketplace, loja,
                    str(row['pedido']), str(row.get('pedido_original', '')),
                    row['data'], str(row['sku']), str(row.get('sku_tiktok', '')),
                    int(row['qtd']),
                    float(row['preco_venda']), float(row['desconto_parceiro']), 0.0,
                    float(row['receita']), float(row['custo']), float(row['custo_total']),
                    float(row['imposto']), float(row['comissao']), float(row['frete']),
                    float(row['tarifa_fixa']), float(row['outros_custos']),
                    float(row['total_tarifas']), float(row['valor_liquido']),
                    float(row['margem']), float(row['margem_pct']),
                    arq_nome, _LOGISTICA
                ))
                cursor.execute(f"RELEASE SAVEPOINT tktk_fin_{idx}")
                reg += 1

            except Exception as e:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT tktk_fin_{idx}")
                except Exception:
                    pass
                err += 1
                if err == 1:
                    st.warning(f"Primeiro erro snapshot TikTok: {str(e)[:200]}")

        # 4. INSERT financeiro não mapeados -> pendentes
        for idx, p in enumerate(pendentes_sku):
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Registrando pendente (SKU não mapeado) {idx + 1}...")

                dados_pend = {
                    'marketplace_origem': marketplace,
                    'loja_origem': loja,
                    'numero_pedido': str(p['pedido']),
                    'data_venda': p.get('data'),
                    'sku': str(p['sku']),
                    'codigo_anuncio': str(p.get('sku_tiktok', '')),
                    'quantidade': int(p.get('qtd', 1)),
                    'preco_venda': float(p.get('preco_venda', 0)),
                    'valor_venda_efetivo': float(p.get('receita', 0)),
                    'imposto': float(p.get('imposto', 0)),
                    'comissao': float(p.get('comissao', 0)),
                    'frete': float(p.get('frete', 0)),
                    'tarifa_fixa': float(p.get('tarifa_fixa', 0)),
                    'outros_custos': float(p.get('outros_custos', 0)),
                    'total_tarifas': float(p.get('total_tarifas', 0)),
                    'valor_liquido': float(p.get('valor_liquido', 0)),
                    'arquivo_origem': arq_nome,
                    'motivo': _MOTIVO_SKU_NAO_MAPEADO,
                    'logistica': _LOGISTICA,
                }
                cursor.execute(f"SAVEPOINT tktk_pend_sku_{idx}")
                try:
                    if gravar_venda_pendente(cursor, dados_pend):
                        pend += 1
                    else:
                        err += 1
                    cursor.execute(f"RELEASE SAVEPOINT tktk_pend_sku_{idx}")
                except Exception:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT tktk_pend_sku_{idx}")
                    err += 1
                skus_invalidos.add(str(p.get('sku_tiktok', p['sku'])))

            except Exception:
                err += 1

        # 5. INSERT em_espera -> pendentes (staging, ON CONFLICT DO NOTHING)
        for idx, p in enumerate(pendentes_emespera):
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Registrando em espera {idx + 1} de {len(pendentes_emespera)}...")

                motivo_esp = (f'{_MOTIVO_EMESPERA} - SKU não mapeado'
                              if not p.get('sku_mapeado') else _MOTIVO_EMESPERA)

                dados_pend = {
                    'marketplace_origem': marketplace,
                    'loja_origem': loja,
                    'numero_pedido': str(p['pedido']),
                    'data_venda': p.get('data'),
                    'sku': str(p['sku']),
                    'codigo_anuncio': str(p.get('sku_tiktok', '')),
                    'quantidade': int(p.get('qtd', 1)),
                    'preco_venda': float(p.get('preco_venda', 0)),
                    'valor_venda_efetivo': float(p.get('receita', 0)),
                    'imposto': float(p.get('imposto', 0)),
                    'comissao': float(p.get('comissao', 0)),
                    'frete': float(p.get('frete', 0)),
                    'tarifa_fixa': float(p.get('tarifa_fixa', 0)),
                    'outros_custos': float(p.get('outros_custos', 0)),
                    'total_tarifas': float(p.get('total_tarifas', 0)),
                    'valor_liquido': float(p.get('valor_liquido', 0)),
                    'arquivo_origem': arq_nome,
                    'motivo': motivo_esp,
                    'logistica': _LOGISTICA,
                }
                cursor.execute(f"SAVEPOINT tktk_esp_{idx}")
                try:
                    if gravar_venda_pendente(cursor, dados_pend):
                        pend += 1
                    else:
                        dups += 1  # ON CONFLICT DO NOTHING = duplicata ignorada
                    cursor.execute(f"RELEASE SAVEPOINT tktk_esp_{idx}")
                except Exception:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT tktk_esp_{idx}")
                    err += 1

            except Exception:
                err += 1

        # 6. INSERT descartes -> fact_vendas_descartadas
        from database_utils import gravar_venda_descartada
        for idx, descarte in enumerate(descartes):
            try:
                item_atual += 1
                progress_bar.progress(min(item_atual / total_itens, 1.0))
                status_text.text(f"Registrando descarte {idx + 1} de {len(descartes)}...")

                descarte['marketplace'] = marketplace
                descarte['loja'] = loja
                descarte['arquivo_origem'] = arq_nome

                cursor.execute(f"SAVEPOINT tktk_desc_{idx}")
                if gravar_venda_descartada(cursor, descarte):
                    desc_count += 1
                cursor.execute(f"RELEASE SAVEPOINT tktk_desc_{idx}")
            except Exception:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT tktk_desc_{idx}")
                except Exception:
                    pass
                err += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.error(f"Erro crítico na gravação TikTok: {e}")
        err = len(df) + len(pendentes_sku) + len(pendentes_emespera)

    finally:
        cursor.close()
        conn.close()
        progress_bar.empty()
        status_text.empty()

    return reg, err, skus_invalidos, dups, pend, desc_count, atualiz


# ============================================================
# RE-PROMOÇÃO DE PENDENTES TIKTOK APÓS MAPEAMENTO DE SKU
# ============================================================

def reprocessar_pendentes_tiktok_mapeados(engine, sku_map):
    """
    Promove pendentes TikTok com 'SKU não mapeado' para fact_vendas_snapshot
    usando o sku_map fornecido ({id_sku_tiktok: sku_interno}).

    Retorna (sucesso, erros, detalhes) -- detalhes é uma lista de strings
    com a causa de cada erro, para exibir na UI sem depender do log do servidor.
    """
    if not sku_map:
        return 0, 0, []

    tiktok_ids = list(sku_map.keys())
    custos_dict = buscar_custos_skus(engine)
    skus_validos = buscar_skus_validos(engine)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    sucesso = 0
    erros = 0
    detalhes = []

    try:
        placeholders = ', '.join(['%s'] * len(tiktok_ids))
        cursor.execute(f"""
            SELECT id, marketplace_origem, loja_origem, numero_pedido,
                   data_venda, sku AS sku_tiktok_id, codigo_anuncio,
                   quantidade, preco_venda, valor_venda_efetivo,
                   imposto, comissao, frete, tarifa_fixa, outros_custos,
                   total_tarifas, valor_liquido, arquivo_origem
            FROM fact_vendas_pendentes
            WHERE marketplace_origem = 'TIKTOK'
              AND motivo LIKE %s
              AND status = 'Pendente'
              AND sku IN ({placeholders})
        """, ['%SKU não mapeado%'] + tiktok_ids)
        pendentes = cursor.fetchall()

        sql_ins = """
            INSERT INTO fact_vendas_snapshot (
                marketplace_origem, loja_origem, numero_pedido, pedido_original,
                data_venda, sku, codigo_anuncio, quantidade,
                preco_venda, desconto_parceiro, desconto_marketplace,
                valor_venda_efetivo, custo_unitario, custo_total,
                imposto, comissao, frete, tarifa_fixa, outros_custos,
                total_tarifas, valor_liquido, margem_total, margem_percentual,
                data_processamento, arquivo_origem, logistica
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                NOW(), %s, %s
            )
            ON CONFLICT (numero_pedido, sku, loja_origem) DO NOTHING
        """

        for row in pendentes:
            (pid, marketplace, loja, numero_pedido, data_venda, sku_tiktok_id,
             codigo_anuncio, qtd, preco_venda, receita,
             imposto, comissao, frete, tarifa_fixa, outros_custos,
             total_tarifas, valor_liquido, arquivo_origem) = row

            sku_nala = sku_map.get(sku_tiktok_id)
            if not sku_nala:
                erros += 1
                detalhes.append(f"pedido={numero_pedido} sku_tiktok={sku_tiktok_id}: sem mapeamento em sku_map")
                continue
            if sku_nala not in skus_validos:
                erros += 1
                detalhes.append(f"pedido={numero_pedido} sku_tiktok={sku_tiktok_id}: sku_interno '{sku_nala}' não está Ativo em dim_produtos")
                continue

            custo_un = custos_dict.get(sku_nala, 0.0)
            custo_total = round(custo_un * int(qtd or 1), 2)
            receita_f = float(receita or 0)
            imposto_f = float(imposto or 0)
            margem = round(float(valor_liquido or 0) - custo_total - imposto_f, 2)
            margem_pct = round((margem / receita_f * 100) if receita_f > 0 else 0, 2)

            pedido_original = numero_pedido.replace('TKTK_', '').rsplit('_', 1)[0] if numero_pedido.startswith('TKTK_') else numero_pedido

            try:
                cursor.execute(f"SAVEPOINT tktk_repro_{pid}")
                cursor.execute(sql_ins, (
                    marketplace, loja, numero_pedido, pedido_original,
                    data_venda, sku_nala, codigo_anuncio or sku_tiktok_id,
                    int(qtd or 1),
                    float(preco_venda or 0), 0.0, 0.0,
                    receita_f, custo_un, custo_total,
                    imposto_f, float(comissao or 0), float(frete or 0),
                    float(tarifa_fixa or 0), float(outros_custos or 0),
                    float(total_tarifas or 0), float(valor_liquido or 0),
                    margem, margem_pct,
                    arquivo_origem or '', _LOGISTICA,
                ))
                if cursor.rowcount > 0:
                    cursor.execute(
                        "UPDATE fact_vendas_pendentes SET status = 'Reprocessado' WHERE id = %s",
                        (pid,)
                    )
                    cursor.execute(f"RELEASE SAVEPOINT tktk_repro_{pid}")
                    sucesso += 1
                else:
                    cursor.execute(f"RELEASE SAVEPOINT tktk_repro_{pid}")
                    erros += 1
                    msg = f"pedido={numero_pedido} sku={sku_nala}: INSERT descartado por ON CONFLICT (venda ja existia) -- status mantido 'Pendente'"
                    detalhes.append(msg)
                    print(f"[NALA][TIKTOK] {msg} (id={pid})")
            except Exception as e:
                try:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT tktk_repro_{pid}")
                except Exception:
                    pass
                erros += 1
                msg = f"pedido={numero_pedido} sku={sku_nala}: {e}"
                detalhes.append(msg)
                print(f"[NALA][TIKTOK] Erro ao reprocessar pendente id={pid} {msg}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        detalhes.append(f"erro geral: {e}")
        print(f"[NALA][TIKTOK] Erro geral em reprocessar_pendentes_tiktok_mapeados: {e}")
        erros += len(pendentes) if 'pendentes' in dir() else 1
    finally:
        cursor.close()
        conn.close()

    return sucesso, erros, detalhes
