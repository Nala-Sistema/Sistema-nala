"""
analise_ads_shopee.py — Módulo de Análise de Ads da Shopee
Sistema Nala — Parte 3A (núcleo funcional)

Conteúdo desta entrega:
  - Tab Upload: preview com banner destacado das datas detectadas
  - Tab Dashboard TACOS: query corrigida + tradução loja + dias pago/orgânico
  - Tab Match SKU: data_editor inline com múltiplos SKUs (até 3 por anúncio)
  - Tab Histórico: preservado

Parte 3B (posterior): download/upload xlsx, botão "Gerar Insights com IA".

Regras do projeto:
  - raw_connection() + cursor em TODAS as queries (nunca pd.read_sql com engine)
  - SAVEPOINT por linha em loops de INSERT/UPDATE
  - Formatação BR: R$ 1.234,56 | dd/mm/aaaa | 18,50%
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from processar_ads_shopee import (
    processar_csv_ads_shopee, gravar_ads_shopee, extrair_metadados_csv,
    buscar_skus_match, atualizar_matches_sku, calcular_tacos,
    data_fim_efetiva, LOJA_ADS_PARA_ORIGEM
)


# ============================================================
# CONSTANTES
# ============================================================

LOJAS_SHOPEE = {
    "Shopee Nala/Lithouse": "Nala-Lit",
    "Shopee Litstore (Yanni)": "litstoreshop",
    "Shopee LPT": "LPT Store",
}

MAX_SKUS_POR_ANUNCIO = 3  # número máximo de SKUs que um anúncio pode ter


# ============================================================
# HELPERS DE FORMATAÇÃO (padrão BR)
# ============================================================

def fmt_brl(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"{valor:.2f}%".replace(".", ",")


def fmt_int(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"{int(valor):,}".replace(",", ".")


def fmt_data_br(d):
    if d is None or pd.isna(d):
        return "—"
    if isinstance(d, str):
        return d
    try:
        return d.strftime("%d/%m/%Y")
    except Exception:
        return str(d)


def cor_tacos(tacos):
    if tacos is None:
        return "⚫"
    if tacos <= 3:
        return "✅"
    elif tacos <= 5:
        return "🟡"
    elif tacos <= 10:
        return "🟠"
    return "🔴"


# ============================================================
# HELPERS DE BANCO (raw_connection sempre)
# ============================================================

def _query_df(engine, query, params=None):
    """Executa query e retorna DataFrame — USA raw_connection SEMPRE"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        colunas = [desc[0] for desc in cursor.description]
        dados = cursor.fetchall()
        return pd.DataFrame(dados, columns=colunas)
    except Exception as e:
        st.error(f"Erro na query: {str(e)[:200]}")
        return pd.DataFrame()
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


def _query_scalar(engine, query, params=None):
    """Executa query e retorna valor único — USA raw_connection SEMPRE"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        st.error(f"Erro na query: {str(e)[:200]}")
        return None
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
# ENTRY POINT DO MÓDULO
# ============================================================

def modulo_ads_shopee(engine):
    """Ponto de entrada do módulo Shopee Ads (chamado pelo orquestrador)."""
    subtab_upload, subtab_dashboard, subtab_match, subtab_historico = st.tabs([
        "📤 Upload", "📈 Dashboard TACOS", "🔗 Match SKU", "📋 Histórico"
    ])
    with subtab_upload:
        _shopee_upload(engine)
    with subtab_dashboard:
        _shopee_dashboard(engine)
    with subtab_match:
        _shopee_match_sku(engine)
    with subtab_historico:
        _shopee_historico(engine)


# ============================================================
# TAB 1: UPLOAD
# ============================================================

def _shopee_upload(engine):
    st.subheader("Upload de Relatório de Ads")
    st.caption("Importe CSVs exportados da Central de Marketing da Shopee.")

    col1, col2 = st.columns(2)
    with col1:
        loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="ads_loja")
    with col2:
        tipo = st.selectbox("Tipo de relatório", [
            "Geral (Todos os Anúncios)", "Grupo de Anúncios", "Produto Individual"
        ], key="ads_tipo")

    arquivos = st.file_uploader(
        "Selecione o(s) CSV(s) de ads",
        type=['csv'], accept_multiple_files=True, key="ads_files"
    )

    if arquivos and st.button("🔍 ANALISAR", key="ads_analisar", type="primary"):
        nomes_arquivos = []
        for arquivo in arquivos:
            st.markdown(f"**📄 Arquivo:** `{arquivo.name}`")
            loja_nome = LOJAS_SHOPEE[loja]
            df, meta = processar_csv_ads_shopee(arquivo, loja_override=loja_nome)
            if df is None:
                st.error(f"❌ {meta}")
                continue

            # Banner destacado com o período do relatório
            pi = meta.get('periodo_inicio')
            pf = meta.get('periodo_fim')
            if pi and pf:
                dias = (pf - pi).days + 1
                st.success(
                    f"📅 **Período do relatório: {fmt_data_br(pi)} a {fmt_data_br(pf)}** "
                    f"({dias} dias)  —  Confira antes de gravar!"
                )
            else:
                st.warning("⚠️ Período do relatório não detectado — verifique o CSV antes de gravar.")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Tipo", meta.get('tipo_relatorio', '?'))
            c2.metric("Registros", meta.get('total_registros', 0))
            c3.metric("Investimento", fmt_brl(meta.get('total_despesas', 0)))
            c4.metric("GMV Ads", fmt_brl(meta.get('total_gmv', 0)))
            if meta.get('nome_grupo'):
                st.caption(f"Grupo: {meta['nome_grupo']}")

            # Preview com as datas de vigência de cada anúncio
            preview_cols = [
                'nome_anuncio', 'status_anuncio',
                'data_inicio_anuncio', 'data_fim_anuncio',
                'despesas', 'gmv', 'acos_direto',
                'itens_vendidos_diretos', 'metodo_lance'
            ]
            preview_cols = [c for c in preview_cols if c in df.columns]
            preview = df[preview_cols].copy()
            if 'nome_anuncio' in preview.columns:
                preview['nome_anuncio'] = preview['nome_anuncio'].str[:60]
            if 'data_inicio_anuncio' in preview.columns:
                preview['data_inicio_anuncio'] = preview['data_inicio_anuncio'].apply(fmt_data_br)
            if 'data_fim_anuncio' in preview.columns:
                preview['data_fim_anuncio'] = preview['data_fim_anuncio'].apply(
                    lambda x: fmt_data_br(x) if x is not None and not pd.isna(x) else "Ilimitado"
                )
            if 'despesas' in preview.columns:
                preview['despesas'] = preview['despesas'].apply(fmt_brl)
            if 'gmv' in preview.columns:
                preview['gmv'] = preview['gmv'].apply(fmt_brl)
            if 'acos_direto' in preview.columns:
                preview['acos_direto'] = preview['acos_direto'].apply(fmt_pct)

            preview.columns = [
                {
                    'nome_anuncio': 'Anúncio',
                    'status_anuncio': 'Status',
                    'data_inicio_anuncio': 'Início',
                    'data_fim_anuncio': 'Encerramento',
                    'despesas': 'Investimento',
                    'gmv': 'GMV',
                    'acos_direto': 'ACOS Direto',
                    'itens_vendidos_diretos': 'Itens Diretos',
                    'metodo_lance': 'Método',
                }.get(c, c) for c in preview.columns
            ]

            st.dataframe(preview, use_container_width=True, hide_index=True)

            st.session_state[f'ads_df_{arquivo.name}'] = df
            st.session_state[f'ads_meta_{arquivo.name}'] = meta
            nomes_arquivos.append(arquivo.name)
        st.session_state['ads_arquivos_analisados'] = nomes_arquivos

    # Bloco de gravação
    nomes_pendentes = st.session_state.get('ads_arquivos_analisados', [])
    if nomes_pendentes:
        st.divider()
        st.info(f"📄 {len(nomes_pendentes)} arquivo(s) analisado(s) aguardando gravação.")
        if st.button("💾 GRAVAR NO BANCO", key="ads_gravar", type="primary"):
            for nome_arq in nomes_pendentes:
                key_df = f'ads_df_{nome_arq}'
                key_meta = f'ads_meta_{nome_arq}'
                if key_df not in st.session_state:
                    continue
                df = st.session_state[key_df]
                meta = st.session_state[key_meta]
                with st.spinner(f"Gravando {nome_arq}..."):
                    gravados, erros, duplicatas = gravar_ads_shopee(df, nome_arq, engine)
                    _auto_match_skus(engine, df)
                    _registrar_log_ads(engine, meta, nome_arq, gravados, len(erros))
                if gravados > 0:
                    st.success(f"✅ `{nome_arq}` — {gravados} registros gravados")
                if duplicatas > 0:
                    st.info(f"ℹ️ {duplicatas} registros atualizados (já existentes)")
                if erros:
                    st.warning(f"⚠️ {len(erros)} erros:")
                    for e in erros[:5]:
                        st.caption(e)
            for nome_arq in nomes_pendentes:
                st.session_state.pop(f'ads_df_{nome_arq}', None)
                st.session_state.pop(f'ads_meta_{nome_arq}', None)
            st.session_state.pop('ads_arquivos_analisados', None)


def _auto_match_skus(engine, df_ads):
    """Ao gravar ads, tenta preencher sku_match com o primeiro SKU vigente hoje"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for _, row in df_ads.iterrows():
            lista_skus = buscar_skus_match(engine, row['loja'], row['nome_anuncio'])
            if lista_skus:
                # grava o primeiro SKU (compatibilidade com o schema atual de fact_ads_shopee)
                cursor.execute("""
                    UPDATE fact_ads_shopee SET sku_match = %s, match_confirmado = TRUE
                    WHERE loja = %s AND nome_anuncio = %s AND (sku_match IS NULL OR sku_match = '')
                """, (lista_skus[0], row['loja'], row['nome_anuncio']))
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


def _registrar_log_ads(engine, meta, arquivo_nome, gravados, erros):
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO log_uploads_ads (
                marketplace, loja, tipo_relatorio, arquivo_nome,
                periodo_inicio, periodo_fim,
                total_linhas, linhas_importadas, linhas_erro
            )
            VALUES ('Shopee', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            meta.get('loja', ''), meta.get('tipo_relatorio', ''), arquivo_nome,
            meta.get('periodo_inicio'), meta.get('periodo_fim'),
            meta.get('total_registros', 0), gravados, erros
        ))
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


# ============================================================
# TAB 2: DASHBOARD TACOS
# ============================================================

def _shopee_dashboard(engine):
    st.subheader("Dashboard TACOS")
    st.caption("Meta TACOS: máximo 3% | 🟢 até 3% • 🟡 3–5% • 🟠 5–10% • 🔴 acima de 10%")

    # Dropdown de loja
    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="dash_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    # Dropdown de períodos já importados (mais prático que date_input manual)
    df_periodos = _query_df(engine, """
        SELECT DISTINCT periodo_inicio, periodo_fim,
               COUNT(*) AS anuncios,
               ROUND(SUM(despesas)::numeric, 2) AS invest
        FROM fact_ads_shopee
        WHERE loja = %s
        GROUP BY periodo_inicio, periodo_fim
        ORDER BY periodo_fim DESC, periodo_inicio DESC
    """, [loja_nome])

    if df_periodos.empty:
        st.warning("Nenhum relatório de ads importado para esta loja ainda.")
        st.caption("Importe primeiro um CSV na aba Upload.")
        return

    opcoes_periodos = []
    for _, r in df_periodos.iterrows():
        pi = r['periodo_inicio']
        pf = r['periodo_fim']
        label = (
            f"{fmt_data_br(pi)} a {fmt_data_br(pf)}  "
            f"— {int(r['anuncios'])} anúncios | {fmt_brl(float(r['invest']))}"
        )
        opcoes_periodos.append((label, pi, pf))

    sel = st.selectbox(
        "Período do relatório",
        options=range(len(opcoes_periodos)),
        format_func=lambda i: opcoes_periodos[i][0],
        key="dash_periodo"
    )
    _, dt_inicio, dt_fim = opcoes_periodos[sel]

    if not st.button("📊 CALCULAR", key="dash_calc", type="primary"):
        return

    with st.spinner("Calculando TACOS..."):
        # Ads do período
        df_ads = _query_df(engine, """
            SELECT nome_anuncio, id_produto, status_anuncio,
                   data_inicio_anuncio, data_fim_anuncio,
                   metodo_lance, impressoes, cliques, ctr,
                   conversoes_diretas, taxa_conversao_direta,
                   custo_por_conversao_direta,
                   itens_vendidos_diretos, receita_direta,
                   despesas, gmv, acos_direto, roas_direto,
                   sku_match
            FROM fact_ads_shopee
            WHERE loja = %s
              AND periodo_inicio = %s
              AND periodo_fim = %s
            ORDER BY despesas DESC
        """, [loja_nome, dt_inicio, dt_fim])

        if df_ads.empty:
            st.warning("Nenhum dado de ads encontrado para este período/loja.")
            return

        # Receita total da loja no período — QUERY CORRIGIDA
        # - marketplace_origem = 'SHOPEE' (MAIÚSCULAS, como está no banco)
        # - loja_origem traduzida via LOJA_ADS_PARA_ORIGEM
        loja_origem = LOJA_ADS_PARA_ORIGEM.get(loja_nome, loja_nome)

        vendas_totais = _query_scalar(engine, """
            SELECT COALESCE(SUM(valor_venda_efetivo), 0)
            FROM fact_vendas_snapshot
            WHERE UPPER(marketplace_origem) = 'SHOPEE'
              AND loja_origem = %s
              AND data_venda BETWEEN %s AND %s
        """, [loja_origem, dt_inicio, dt_fim]) or 0
        vendas_totais = float(vendas_totais)

        invest_total = float(df_ads['despesas'].sum())
        gmv_total = float(df_ads['gmv'].sum())
        tacos_loja = (invest_total / vendas_totais * 100) if vendas_totais > 0 else None
        acos_loja = (invest_total / gmv_total * 100) if gmv_total > 0 else None

        st.markdown("### Indicadores da Loja")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Receita Total", fmt_brl(vendas_totais))
        c2.metric("Investimento Ads", fmt_brl(invest_total))
        c3.metric("TACOS Loja", fmt_pct(tacos_loja))
        c4.metric("ACOS Painel", fmt_pct(acos_loja))
        c5.metric("Anúncios", len(df_ads))

        if vendas_totais == 0:
            st.warning(
                f"⚠️ Nenhuma venda encontrada em `fact_vendas_snapshot` para "
                f"`loja_origem = '{loja_origem}'` entre {fmt_data_br(dt_inicio)} e {fmt_data_br(dt_fim)}. "
                f"Confira se os uploads de vendas Shopee cobrem este período."
            )

        st.markdown("### TACOS por Produto")
        st.caption(
            "Produtos com SKU vinculado mostram TACOS real. Sem SKU = apenas ACOS direto do painel.  "
            "**Dias Ads** = até a menor data entre encerramento do anúncio e fim do relatório."
        )

        resultados = []
        for _, ad in df_ads.iterrows():
            # Busca TODOS os SKUs vigentes hoje para este anúncio (match múltiplo)
            lista_skus = buscar_skus_match(engine, loja_nome, ad['nome_anuncio'])

            # Data efetiva de fim do anúncio (MIN entre encerramento e fim do relatório)
            data_fim_anuncio = ad.get('data_fim_anuncio')
            fim_efetivo = data_fim_efetiva(data_fim_anuncio, dt_fim)

            # Dias em que o anúncio rodou dentro do relatório
            inicio_anuncio = ad.get('data_inicio_anuncio')
            if inicio_anuncio is not None and not pd.isna(inicio_anuncio):
                inicio_efetivo = max(inicio_anuncio, dt_inicio)
            else:
                inicio_efetivo = dt_inicio

            if fim_efetivo and inicio_efetivo and fim_efetivo >= inicio_efetivo:
                dias_ads = (fim_efetivo - inicio_efetivo).days + 1
            else:
                dias_ads = 0

            dias_relatorio = (dt_fim - dt_inicio).days + 1
            dias_organico = max(0, dias_relatorio - dias_ads)

            # Cálculo TACOS usando a lista de SKUs (pode ser múltipla)
            tacos_data = None
            if lista_skus:
                tacos_data = calcular_tacos(engine, loja_nome, lista_skus, dt_inicio, dt_fim)
                if tacos_data and 'erro' in tacos_data:
                    tacos_data = None

            sku_display = ", ".join(lista_skus) if lista_skus else "❌ não vinculado"

            resultados.append({
                'Status': cor_tacos(tacos_data['tacos'] if tacos_data else None),
                'Produto': str(ad['nome_anuncio'])[:45],
                'Investimento': float(ad['despesas']),
                'ACOS Direto': float(ad['acos_direto']) if ad['acos_direto'] is not None else 0.0,
                'TACOS': tacos_data['tacos'] if tacos_data else None,
                'Dias Ads': dias_ads,
                'Dias Orgânico': dias_organico,
                'Itens Diretos': int(ad['itens_vendidos_diretos']) if ad['itens_vendidos_diretos'] is not None else 0,
                'Itens Total': tacos_data['qtd_total'] if tacos_data else None,
                '% Orgânico': tacos_data['pct_organico'] if tacos_data else None,
                'Receita Total': tacos_data['receita_total'] if tacos_data else None,
                'Método': str(ad.get('metodo_lance', ''))[:25],
                'SKU(s)': sku_display,
            })

        df_res = pd.DataFrame(resultados).sort_values('Investimento', ascending=False)
        df_exib = df_res.copy()
        df_exib['Investimento'] = df_exib['Investimento'].apply(fmt_brl)
        df_exib['ACOS Direto'] = df_exib['ACOS Direto'].apply(fmt_pct)
        df_exib['TACOS'] = df_exib['TACOS'].apply(fmt_pct)
        df_exib['% Orgânico'] = df_exib['% Orgânico'].apply(fmt_pct)
        df_exib['Receita Total'] = df_exib['Receita Total'].apply(fmt_brl)
        st.dataframe(df_exib, use_container_width=True, hide_index=True)

        # Alertas automáticos
        st.markdown("### ⚠️ Alertas")
        tem_alerta = False
        for _, r in df_res.iterrows():
            if r['TACOS'] is not None and r['TACOS'] > 10:
                st.error(f"🔴 **{r['Produto']}** — TACOS {fmt_pct(r['TACOS'])}. Ação necessária!")
                tem_alerta = True
            elif (r['ACOS Direto'] or 0) > 50 and r['Investimento'] > 10:
                st.warning(
                    f"⚠️ **{r['Produto']}** — ACOS Direto {fmt_pct(r['ACOS Direto'])}. "
                    f"Vincule SKU para calcular TACOS."
                )
                tem_alerta = True
        if not tem_alerta:
            st.success("Nenhum alerta crítico no período.")


# ============================================================
# TAB 3: MATCH SKU (múltiplos SKUs por anúncio, com data_editor)
# ============================================================

def _shopee_match_sku(engine):
    st.subheader("Vincular Produto Ads → SKU")
    st.caption(
        "Cada anúncio pode ter até 3 SKUs vinculados. "
        "Alterações criam um novo registro temporal (SKU antigo é encerrado na data atual, "
        "novo SKU começa hoje) — igual ao snapshot de custos do sistema."
    )

    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="match_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    # 1) Carregar todos os anúncios da loja (com ou sem match)
    df_anuncios = _query_df(engine, """
        SELECT DISTINCT nome_anuncio, id_produto
        FROM fact_ads_shopee
        WHERE loja = %s
        ORDER BY nome_anuncio
    """, [loja_nome])

    if df_anuncios.empty:
        st.info("Nenhum anúncio importado para esta loja ainda. Faça upload de um CSV primeiro.")
        return

    # 2) Carregar SKUs vigentes hoje para cada anúncio
    hoje = date.today()
    matches_atuais = {}  # {nome_anuncio: [sku1, sku2, ...]}
    for _, r in df_anuncios.iterrows():
        lista = buscar_skus_match(engine, loja_nome, r['nome_anuncio'], data_ref=hoje)
        matches_atuais[r['nome_anuncio']] = lista

    # 3) Carregar lista de SKUs disponíveis (para a dica no topo)
    df_skus = _query_df(engine, """
        SELECT sku, nome_produto FROM dim_skus
        WHERE ativo = TRUE ORDER BY sku
    """)
    total_skus = len(df_skus)

    # 4) Métricas de topo
    com_match = sum(1 for skus in matches_atuais.values() if skus)
    sem_match = len(matches_atuais) - com_match

    c1, c2, c3 = st.columns(3)
    c1.metric("Total de anúncios", len(df_anuncios))
    c2.metric("Com SKU vinculado", com_match)
    c3.metric("Sem vínculo", sem_match)

    st.divider()

    # 5) Construir DataFrame editável
    # Colunas: ID, Anúncio, SKU 1, SKU 2, SKU 3
    linhas = []
    for _, r in df_anuncios.iterrows():
        nome = r['nome_anuncio']
        id_prod = r.get('id_produto', '') or ''
        skus = matches_atuais.get(nome, [])
        # Preencher até MAX_SKUS_POR_ANUNCIO
        skus_padded = list(skus) + [""] * (MAX_SKUS_POR_ANUNCIO - len(skus))
        skus_padded = skus_padded[:MAX_SKUS_POR_ANUNCIO]
        linhas.append({
            'ID Produto': str(id_prod),
            'Anúncio': nome,
            'SKU 1': skus_padded[0],
            'SKU 2': skus_padded[1],
            'SKU 3': skus_padded[2],
        })

    df_editor = pd.DataFrame(linhas)

    st.markdown("### 📝 Edite os SKUs abaixo e clique em Salvar")
    st.caption(
        "💡 **Dica:** preencha os SKUs exatamente como cadastrados no sistema "
        f"(exemplos dos seus {total_skus} SKUs ativos disponíveis em `dim_skus`). "
        "Deixe em branco os campos não usados. Para remover um SKU, apague o valor do campo."
    )

    # st.data_editor — edição inline como planilha
    df_editado = st.data_editor(
        df_editor,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=['ID Produto', 'Anúncio'],
        key="match_editor",
        column_config={
            'ID Produto': st.column_config.TextColumn(width="small"),
            'Anúncio': st.column_config.TextColumn(width="large"),
            'SKU 1': st.column_config.TextColumn(width="small", help="SKU principal"),
            'SKU 2': st.column_config.TextColumn(width="small", help="SKU adicional (opcional)"),
            'SKU 3': st.column_config.TextColumn(width="small", help="SKU adicional (opcional)"),
        }
    )

    if st.button("💾 Salvar alterações", key="match_salvar", type="primary"):
        _salvar_matches_em_lote(engine, loja_nome, df_editado, matches_atuais)


def _salvar_matches_em_lote(engine, loja_nome, df_editado, matches_atuais):
    """Processa o DataFrame editado e atualiza a dim_ads_produto_sku com lógica temporal."""
    alterados = 0
    inalterados = 0
    erros = []

    progress = st.progress(0, text="Salvando alterações...")
    total = len(df_editado)

    for idx, row in df_editado.iterrows():
        nome = str(row['Anúncio']).strip()
        id_prod = str(row.get('ID Produto', '')).strip()

        # Coletar SKUs novos (não-vazios, normalizados)
        skus_novos = []
        for col in ['SKU 1', 'SKU 2', 'SKU 3']:
            val = row.get(col, '')
            if val is not None and not pd.isna(val):
                txt = str(val).strip()
                if txt and txt.lower() not in ('nan', 'none'):
                    skus_novos.append(txt)

        # Remover duplicatas preservando ordem
        seen = set()
        skus_novos_unicos = []
        for s in skus_novos:
            if s not in seen:
                seen.add(s)
                skus_novos_unicos.append(s)

        # Comparar com o estado atual
        atuais = matches_atuais.get(nome, [])
        if sorted(atuais) == sorted(skus_novos_unicos):
            inalterados += 1
        else:
            # Aplicar mudanças via função temporal
            sucesso = atualizar_matches_sku(
                engine, loja_nome, nome, id_prod, skus_novos_unicos
            )
            if sucesso:
                # Atualizar sku_match em fact_ads_shopee (pega o primeiro da lista)
                primeiro_sku = skus_novos_unicos[0] if skus_novos_unicos else None
                _sync_fact_sku_match(engine, loja_nome, nome, primeiro_sku)
                alterados += 1
            else:
                erros.append(nome[:40])

        progress.progress((idx + 1) / total, text=f"Processando {idx + 1}/{total}...")

    progress.empty()

    if alterados > 0:
        st.success(f"✅ {alterados} anúncio(s) atualizados com vigência temporal a partir de hoje.")
    if inalterados > 0:
        st.info(f"ℹ️ {inalterados} anúncio(s) sem alteração.")
    if erros:
        st.error(f"❌ {len(erros)} erros: {', '.join(erros[:5])}")

    if alterados > 0:
        st.caption("Clique em **Rerun** ou mude de aba para recarregar a tela.")


def _sync_fact_sku_match(engine, loja, nome_anuncio, primeiro_sku):
    """
    Sincroniza a coluna fact_ads_shopee.sku_match com o primeiro SKU ativo.
    Se primeiro_sku for None, apaga o vínculo em fact_ads_shopee.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        if primeiro_sku:
            cursor.execute("""
                UPDATE fact_ads_shopee
                SET sku_match = %s, match_confirmado = TRUE
                WHERE loja = %s AND nome_anuncio = %s
            """, (primeiro_sku, loja, nome_anuncio))
        else:
            cursor.execute("""
                UPDATE fact_ads_shopee
                SET sku_match = NULL, match_confirmado = FALSE
                WHERE loja = %s AND nome_anuncio = %s
            """, (loja, nome_anuncio))
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


# ============================================================
# TAB 4: HISTÓRICO
# ============================================================

def _shopee_historico(engine):
    st.subheader("Histórico de Uploads")
    df = _query_df(engine, """
        SELECT data_upload, loja, tipo_relatorio, arquivo_nome,
               periodo_inicio, periodo_fim,
               linhas_importadas, linhas_erro, status
        FROM log_uploads_ads
        WHERE marketplace = 'Shopee'
        ORDER BY data_upload DESC LIMIT 50
    """)
    if df.empty:
        st.info("Nenhum upload de ads registrado ainda.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("🔍 Diagnóstico do banco"):
        total = _query_scalar(engine, "SELECT COUNT(*) FROM fact_ads_shopee")
        st.caption(f"Total de registros em `fact_ads_shopee`: {total or 0}")

        total_matches_ativos = _query_scalar(engine, """
            SELECT COUNT(*) FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee' AND data_fim IS NULL
        """)
        st.caption(f"Total de matches ativos (data_fim IS NULL): {total_matches_ativos or 0}")

        total_matches_historico = _query_scalar(engine, """
            SELECT COUNT(*) FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee'
        """)
        st.caption(f"Total histórico em `dim_ads_produto_sku`: {total_matches_historico or 0}")

        df_resumo = _query_df(engine, """
            SELECT loja, periodo_inicio, periodo_fim,
                   COUNT(*) AS registros,
                   ROUND(SUM(despesas)::numeric, 2) AS invest_total
            FROM fact_ads_shopee
            GROUP BY loja, periodo_inicio, periodo_fim
            ORDER BY periodo_fim DESC
        """)
        if not df_resumo.empty:
            st.caption("Dados importados por loja/período:")
            st.dataframe(df_resumo, use_container_width=True, hide_index=True)
        else:
            st.warning("Tabela `fact_ads_shopee` está vazia.")
