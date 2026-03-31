"""
analise_ads.py — Módulo de Análise de Ads
Sistema Nala

Estrutura:
- Tab por marketplace (Shopee primeiro, depois Amazon, ML, etc.)
- Dentro de cada tab: Upload, Dashboard TACOS, Match SKU, Comparativo
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
from processar_ads_shopee import (
    processar_csv_ads_shopee, gravar_ads_shopee, extrair_metadados_csv,
    buscar_match_sku, salvar_match_sku, calcular_tacos
)

# ============================================================
# FORMATAÇÃO BR
# ============================================================

def fmt_brl(valor):
    """Formata valor monetário BR"""
    if valor is None or pd.isna(valor):
        return "N/A"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_pct(valor):
    """Formata percentual BR"""
    if valor is None or pd.isna(valor):
        return "N/A"
    return f"{valor:.2f}%".replace(".", ",")

def fmt_int(valor):
    """Formata inteiro BR"""
    if valor is None or pd.isna(valor):
        return "N/A"
    return f"{int(valor):,}".replace(",", ".")

def cor_tacos(tacos):
    """Retorna emoji de classificação TACOS"""
    if tacos is None:
        return "⚫"
    if tacos <= 3:
        return "✅"
    elif tacos <= 5:
        return "🟡"
    elif tacos <= 10:
        return "🟠"
    else:
        return "🔴"


# ============================================================
# CONFIGURAÇÃO DE LOJAS
# ============================================================

LOJAS_SHOPEE = {
    "Shopee Nala/Lithouse": "Nala-Lit",
    "Shopee Litstore (Yanni)": "litstoreshop",
    "Shopee LPT": "LPT Store",
}


# ============================================================
# MÓDULO PRINCIPAL
# ============================================================

def modulo_ads(engine):
    """Módulo de análise de ads — chamado pelo app.py"""

    st.title("📊 Análise de Ads")
    st.caption("Meta TACOS: máximo 3%")

    # Tabs por marketplace
    tab_shopee, tab_amazon, tab_outros = st.tabs([
        "🟠 Shopee", "📦 Amazon", "🔜 Outros"
    ])

    with tab_shopee:
        _tab_shopee(engine)

    with tab_amazon:
        st.info("Módulo Amazon Ads em desenvolvimento.")

    with tab_outros:
        st.info("Mercado Livre, Shein e Magalu serão adicionados em breve.")


# ============================================================
# TAB SHOPEE
# ============================================================

def _tab_shopee(engine):
    """Conteúdo da tab Shopee Ads"""

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
# UPLOAD DE ADS SHOPEE
# ============================================================

def _shopee_upload(engine):
    """Upload e processamento de CSV de ads"""

    st.subheader("Upload de Relatório de Ads")

    col1, col2 = st.columns(2)
    with col1:
        loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="ads_loja")
    with col2:
        tipo = st.selectbox("Tipo de relatório", [
            "Geral (Todos os Anúncios)",
            "Grupo de Anúncios",
            "Produto Individual"
        ], key="ads_tipo")

    arquivos = st.file_uploader(
        "Selecione o(s) CSV(s) de ads",
        type=['csv'],
        accept_multiple_files=True,
        key="ads_files"
    )

    if arquivos and st.button("🔍 ANALISAR", key="ads_analisar", type="primary"):
        nomes_arquivos = []
        for arquivo in arquivos:
            st.markdown(f"**Arquivo:** {arquivo.name}")

            loja_nome = LOJAS_SHOPEE[loja]
            df, meta = processar_csv_ads_shopee(arquivo, loja_override=loja_nome)

            if df is None:
                st.error(f"❌ {meta}")
                continue

            # Exibir metadados
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Tipo", meta.get('tipo_relatorio', '?'))
            col2.metric("Registros", meta.get('total_registros', 0))
            col3.metric("Investimento", fmt_brl(meta.get('total_despesas', 0)))
            col4.metric("GMV Ads", fmt_brl(meta.get('total_gmv', 0)))

            if meta.get('periodo_inicio') and meta.get('periodo_fim'):
                st.caption(f"Período: {meta['periodo_inicio'].strftime('%d/%m/%Y')} a {meta['periodo_fim'].strftime('%d/%m/%Y')}")

            if meta.get('nome_grupo'):
                st.caption(f"Grupo: {meta['nome_grupo']}")

            # Preview
            st.markdown("**Preview dos dados:**")
            preview = df[['nome_anuncio', 'despesas', 'gmv', 'acos', 'itens_vendidos', 'metodo_lance', 'status_anuncio']].copy()
            preview['nome_anuncio'] = preview['nome_anuncio'].str[:50]
            preview['despesas'] = preview['despesas'].apply(fmt_brl)
            preview['gmv'] = preview['gmv'].apply(fmt_brl)
            preview['acos'] = preview['acos'].apply(fmt_pct)
            st.dataframe(preview, use_container_width=True, hide_index=True)

            # Salvar no session_state para gravar
            st.session_state[f'ads_df_{arquivo.name}'] = df
            st.session_state[f'ads_meta_{arquivo.name}'] = meta
            nomes_arquivos.append(arquivo.name)

        # Salvar lista de nomes para o botão GRAVAR sobreviver ao re-render
        st.session_state['ads_arquivos_analisados'] = nomes_arquivos

    # Botão gravar — FORA do bloco ANALISAR para sobreviver ao re-render
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

                    # Auto-match SKUs conhecidos
                    _auto_match_skus(engine, df)

                    # Log do upload
                    _registrar_log_ads(engine, meta, nome_arq, gravados, len(erros))

                if gravados > 0:
                    st.success(f"✅ {gravados} registros gravados")
                if duplicatas > 0:
                    st.info(f"ℹ️ {duplicatas} registros atualizados (já existiam)")
                if erros:
                    st.warning(f"⚠️ {len(erros)} erros:")
                    for e in erros[:5]:
                        st.caption(e)

            # Limpar session_state após gravação
            for nome_arq in nomes_pendentes:
                st.session_state.pop(f'ads_df_{nome_arq}', None)
                st.session_state.pop(f'ads_meta_{nome_arq}', None)
            st.session_state.pop('ads_arquivos_analisados', None)


def _auto_match_skus(engine, df_ads):
    """Tenta vincular SKUs automaticamente usando dim_ads_produto_sku"""
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        for _, row in df_ads.iterrows():
            nome = row['nome_anuncio']
            loja = row['loja']
            sku = buscar_match_sku(engine, loja, nome)
            if sku:
                cursor.execute("""
                    UPDATE fact_ads_shopee
                    SET sku_match = %s, match_confirmado = TRUE
                    WHERE loja = %s AND nome_anuncio = %s AND sku_match IS NULL
                """, (sku, loja, nome))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def _registrar_log_ads(engine, meta, arquivo_nome, gravados, erros):
    """Registra upload no log"""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO log_uploads_ads (
                marketplace, loja, tipo_relatorio, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro
            ) VALUES ('Shopee', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            meta.get('loja', ''), meta.get('tipo_relatorio', ''),
            arquivo_nome, meta.get('periodo_inicio'), meta.get('periodo_fim'),
            meta.get('total_registros', 0), gravados, erros
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


# ============================================================
# DASHBOARD TACOS
# ============================================================

def _shopee_dashboard(engine):
    """Dashboard de TACOS — cruza ads com vendas"""

    st.subheader("Dashboard TACOS")

    col1, col2, col3 = st.columns(3)
    with col1:
        loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="dash_loja")
    with col2:
        dt_inicio = st.date_input("Início", value=date(2025, 12, 9), key="dash_ini")
    with col3:
        dt_fim = st.date_input("Fim", value=date(2026, 3, 9), key="dash_fim")

    loja_nome = LOJAS_SHOPEE[loja]

    if st.button("📊 CALCULAR", key="dash_calc", type="primary"):
        with st.spinner("Calculando TACOS..."):
            # Buscar dados de ads
            df_ads = _buscar_ads_periodo(engine, loja_nome, dt_inicio, dt_fim)
            if df_ads.empty:
                st.warning("Nenhum dado de ads encontrado para este período/loja.")
                return

            # Buscar vendas totais da loja
            vendas_totais = _buscar_vendas_totais(engine, loja, dt_inicio, dt_fim)

            # Indicadores da loja
            invest_total = df_ads['despesas'].sum()
            gmv_total = df_ads['gmv'].sum()
            tacos_loja = (invest_total / vendas_totais * 100) if vendas_totais > 0 else None
            acos_loja = (invest_total / gmv_total * 100) if gmv_total > 0 else None

            st.markdown("### Indicadores da Loja")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Receita Total", fmt_brl(vendas_totais))
            c2.metric("Investimento Ads", fmt_brl(invest_total))
            c3.metric("TACOS Loja", fmt_pct(tacos_loja))
            c4.metric("ACOS Painel", fmt_pct(acos_loja))
            c5.metric("Anúncios", len(df_ads))

            # Tabela por produto
            st.markdown("### TACOS por Produto")
            st.caption("Produtos com SKU vinculado mostram TACOS real. Sem SKU = apenas ACOS do painel.")

            resultados = []
            for _, ad in df_ads.iterrows():
                sku = ad.get('sku_match')
                tacos_data = None

                if sku:
                    tacos_data = calcular_tacos(engine, loja_nome, sku, dt_inicio, dt_fim)

                resultado = {
                    'Status': cor_tacos(tacos_data['tacos'] if tacos_data and 'tacos' in tacos_data else None),
                    'Produto': ad['nome_anuncio'][:45],
                    'Investimento': ad['despesas'],
                    'ACOS': ad['acos'],
                    'TACOS': tacos_data['tacos'] if tacos_data and 'tacos' in tacos_data else None,
                    'Itens Ads': ad['itens_vendidos'],
                    'Itens Total': tacos_data['qtd_total'] if tacos_data and 'qtd_total' in tacos_data else None,
                    '% Orgânico': tacos_data['pct_organico'] if tacos_data and 'pct_organico' in tacos_data else None,
                    'Receita Total': tacos_data['receita_total'] if tacos_data and 'receita_total' in tacos_data else None,
                    'Método': ad['metodo_lance'][:25] if ad['metodo_lance'] else '',
                    'SKU': sku or '❌ não vinculado',
                }
                resultados.append(resultado)

            df_resultado = pd.DataFrame(resultados)
            df_resultado = df_resultado.sort_values('Investimento', ascending=False)

            # Formatação para exibição
            df_exib = df_resultado.copy()
            df_exib['Investimento'] = df_exib['Investimento'].apply(fmt_brl)
            df_exib['ACOS'] = df_exib['ACOS'].apply(fmt_pct)
            df_exib['TACOS'] = df_exib['TACOS'].apply(fmt_pct)
            df_exib['% Orgânico'] = df_exib['% Orgânico'].apply(fmt_pct)
            df_exib['Receita Total'] = df_exib['Receita Total'].apply(fmt_brl)

            st.dataframe(df_exib, use_container_width=True, hide_index=True)

            # Alertas
            st.markdown("### ⚠️ Alertas")
            for _, r in df_resultado.iterrows():
                tacos = r['TACOS']
                if tacos is not None and tacos > 10:
                    st.error(f"🔴 **{r['Produto']}** — TACOS {fmt_pct(tacos)}. Ação necessária!")
                elif r['ACOS'] > 50 and r['Investimento'] > 10:
                    st.warning(f"⚠️ **{r['Produto']}** — ACOS {fmt_pct(r['ACOS'])}. Sem SKU vinculado para calcular TACOS.")

            # Resumo por método
            st.markdown("### Métodos de Lance")
            df_metodo = df_ads.groupby('metodo_lance').agg(
                invest=('despesas', 'sum'),
                gmv=('gmv', 'sum'),
                conv=('conversoes', 'sum'),
                n=('nome_anuncio', 'count')
            ).reset_index()
            df_metodo['ACOS'] = (df_metodo['invest'] / df_metodo['gmv'] * 100).replace([np.inf], 0)

            for _, m in df_metodo.iterrows():
                met = m['metodo_lance'][:40]
                st.caption(f"**{met}** — {int(m['n'])} anúncios | Invest: {fmt_brl(m['invest'])} | ACOS: {fmt_pct(m['ACOS'])}")


def _buscar_ads_periodo(engine, loja, dt_inicio, dt_fim):
    """Busca ads de uma loja no período"""
    try:
        query = """
            SELECT * FROM fact_ads_shopee
            WHERE loja = %s AND periodo_inicio >= %s AND periodo_fim <= %s
            ORDER BY despesas DESC
        """
        return pd.read_sql(query, engine, params=[loja, dt_inicio, dt_fim])
    except Exception:
        return pd.DataFrame()


def _buscar_vendas_totais(engine, loja_display, dt_inicio, dt_fim):
    """Busca receita total da loja no período via fact_vendas_snapshot"""
    try:
        # Mapear nome de display para nome no banco
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0)
            FROM fact_vendas_snapshot
            WHERE marketplace_origem = 'Shopee'
              AND data_venda BETWEEN %s AND %s
        """, (dt_inicio, dt_fim))
        # Nota: filtrar por loja quando o campo loja_origem tiver valores consistentes
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return float(result[0]) if result else 0
    except Exception:
        return 0


# ============================================================
# MATCH SKU — Vinculação manual
# ============================================================

def _shopee_match_sku(engine):
    """Interface para vincular produtos de ads com SKUs do sistema"""

    st.subheader("Vincular Produto Ads → SKU")
    st.caption("Vincule cada produto do relatório de ads a um SKU do sistema. Feito uma vez, fica salvo.")

    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="match_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    # Buscar produtos sem match
    try:
        df_sem_match = pd.read_sql("""
            SELECT DISTINCT nome_anuncio, id_produto
            FROM fact_ads_shopee
            WHERE loja = %s AND (sku_match IS NULL OR sku_match = '')
              AND despesas > 0
            ORDER BY nome_anuncio
        """, engine, params=[loja_nome])
    except Exception:
        df_sem_match = pd.DataFrame()

    if df_sem_match.empty:
        st.success("✅ Todos os produtos com gasto estão vinculados a um SKU!")
        return

    st.warning(f"⚠️ {len(df_sem_match)} produto(s) sem SKU vinculado:")

    # Buscar SKUs disponíveis
    try:
        df_skus = pd.read_sql("SELECT sku, nome_produto FROM dim_skus WHERE ativo = TRUE ORDER BY sku", engine)
        opcoes_sku = [""] + [f"{r['sku']} — {r['nome_produto'][:40]}" for _, r in df_skus.iterrows()]
    except Exception:
        opcoes_sku = [""]
        st.error("Erro ao carregar SKUs")

    # Formulário de matching
    with st.form("match_form"):
        matches = {}
        for i, (_, row) in enumerate(df_sem_match.iterrows()):
            nome = row['nome_anuncio'][:60]
            id_prod = row.get('id_produto', '')

            col1, col2 = st.columns([3, 2])
            with col1:
                st.text(f"📦 {nome}")
                if id_prod:
                    st.caption(f"ID: {id_prod}")
            with col2:
                sel = st.selectbox(
                    "SKU",
                    opcoes_sku,
                    key=f"match_{i}",
                    label_visibility="collapsed"
                )
                if sel:
                    matches[i] = {
                        'nome': row['nome_anuncio'],
                        'id_produto': id_prod,
                        'sku': sel.split(" — ")[0]
                    }

        submitted = st.form_submit_button("💾 Salvar Vínculos", type="primary")

        if submitted and matches:
            salvos = 0
            for _, match in matches.items():
                ok = salvar_match_sku(engine, loja_nome, match['nome'], match['id_produto'], match['sku'])
                if ok:
                    # Atualizar fact_ads_shopee
                    try:
                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE fact_ads_shopee
                            SET sku_match = %s, match_confirmado = TRUE
                            WHERE loja = %s AND nome_anuncio = %s
                        """, (match['sku'], loja_nome, match['nome']))
                        conn.commit()
                        cursor.close()
                        conn.close()
                        salvos += 1
                    except Exception:
                        pass

            if salvos > 0:
                st.success(f"✅ {salvos} vínculo(s) salvos! O TACOS agora será calculado para estes produtos.")
                st.rerun()


# ============================================================
# HISTÓRICO DE UPLOADS
# ============================================================

def _shopee_historico(engine):
    """Lista uploads de ads anteriores"""

    st.subheader("Histórico de Uploads")

    try:
        df = pd.read_sql("""
            SELECT data_upload, loja, tipo_relatorio, arquivo_nome,
                   periodo_inicio, periodo_fim, linhas_importadas, linhas_erro, status
            FROM log_uploads_ads
            WHERE marketplace = 'Shopee'
            ORDER BY data_upload DESC
            LIMIT 50
        """, engine)

        if df.empty:
            st.info("Nenhum upload de ads registrado ainda.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Erro ao carregar histórico: {str(e)}")
