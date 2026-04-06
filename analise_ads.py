"""
analise_ads.py — Módulo de Análise de Ads
Sistema Nala — v2 corrigido

CORREÇÕES v2:
- Todas queries de leitura usam raw_connection() + cursor (NUNCA pd.read_sql com engine)
- Helper _query_df() e _query_scalar() centralizados
- Erros visíveis (st.error) em vez de falha silenciosa
- Diagnóstico do banco na tab Histórico
- Corrigido fluxo do botão GRAVAR (session_state)
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
from processar_ads_shopee import (
    processar_csv_ads_shopee, gravar_ads_shopee, extrair_metadados_csv,
    buscar_match_sku, salvar_match_sku, calcular_tacos
)


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
            cursor.close()
        if conn:
            conn.close()


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
            cursor.close()
        if conn:
            conn.close()


LOJAS_SHOPEE = {
    "Shopee Nala/Lithouse": "Nala-Lit",
    "Shopee Litstore (Yanni)": "litstoreshop",
    "Shopee LPT": "LPT Store",
}


def modulo_ads(engine):
    st.title("📊 Análise de Ads")
    st.caption("Meta TACOS: máximo 3%")
    tab_shopee, tab_amazon, tab_outros = st.tabs(["🟠 Shopee", "📦 Amazon", "🔜 Outros"])
    with tab_shopee:
        _tab_shopee(engine)
    with tab_amazon:
        st.info("Módulo Amazon Ads em desenvolvimento.")
    with tab_outros:
        st.info("Mercado Livre, Shein e Magalu serão adicionados em breve.")


def _tab_shopee(engine):
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


def _shopee_upload(engine):
    st.subheader("Upload de Relatório de Ads")
    col1, col2 = st.columns(2)
    with col1:
        loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="ads_loja")
    with col2:
        tipo = st.selectbox("Tipo de relatório", [
            "Geral (Todos os Anúncios)", "Grupo de Anúncios", "Produto Individual"
        ], key="ads_tipo")

    arquivos = st.file_uploader("Selecione o(s) CSV(s) de ads", type=['csv'],
                                 accept_multiple_files=True, key="ads_files")

    if arquivos and st.button("🔍 ANALISAR", key="ads_analisar", type="primary"):
        nomes_arquivos = []
        for arquivo in arquivos:
            st.markdown(f"**Arquivo:** {arquivo.name}")
            loja_nome = LOJAS_SHOPEE[loja]
            df, meta = processar_csv_ads_shopee(arquivo, loja_override=loja_nome)
            if df is None:
                st.error(f"❌ {meta}")
                continue

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Tipo", meta.get('tipo_relatorio', '?'))
            c2.metric("Registros", meta.get('total_registros', 0))
            c3.metric("Investimento", fmt_brl(meta.get('total_despesas', 0)))
            c4.metric("GMV Ads", fmt_brl(meta.get('total_gmv', 0)))
            if meta.get('periodo_inicio') and meta.get('periodo_fim'):
                st.caption(f"Período: {meta['periodo_inicio'].strftime('%d/%m/%Y')} a {meta['periodo_fim'].strftime('%d/%m/%Y')}")
            if meta.get('nome_grupo'):
                st.caption(f"Grupo: {meta['nome_grupo']}")

            preview = df[['nome_anuncio', 'despesas', 'gmv', 'acos', 'itens_vendidos', 'metodo_lance', 'status_anuncio']].copy()
            preview['nome_anuncio'] = preview['nome_anuncio'].str[:50]
            preview['despesas'] = preview['despesas'].apply(fmt_brl)
            preview['gmv'] = preview['gmv'].apply(fmt_brl)
            preview['acos'] = preview['acos'].apply(fmt_pct)
            st.dataframe(preview, use_container_width=True, hide_index=True)

            st.session_state[f'ads_df_{arquivo.name}'] = df
            st.session_state[f'ads_meta_{arquivo.name}'] = meta
            nomes_arquivos.append(arquivo.name)
        st.session_state['ads_arquivos_analisados'] = nomes_arquivos

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
                    st.success(f"✅ {gravados} registros gravados")
                if duplicatas > 0:
                    st.info(f"ℹ️ {duplicatas} registros atualizados")
                if erros:
                    st.warning(f"⚠️ {len(erros)} erros:")
                    for e in erros[:5]:
                        st.caption(e)
            for nome_arq in nomes_pendentes:
                st.session_state.pop(f'ads_df_{nome_arq}', None)
                st.session_state.pop(f'ads_meta_{nome_arq}', None)
            st.session_state.pop('ads_arquivos_analisados', None)


def _auto_match_skus(engine, df_ads):
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for _, row in df_ads.iterrows():
            sku = buscar_match_sku(engine, row['loja'], row['nome_anuncio'])
            if sku:
                cursor.execute("""
                    UPDATE fact_ads_shopee SET sku_match = %s, match_confirmado = TRUE
                    WHERE loja = %s AND nome_anuncio = %s AND sku_match IS NULL
                """, (sku, row['loja'], row['nome_anuncio']))
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _registrar_log_ads(engine, meta, arquivo_nome, gravados, erros):
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO log_uploads_ads (marketplace, loja, tipo_relatorio, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro)
            VALUES ('Shopee', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (meta.get('loja', ''), meta.get('tipo_relatorio', ''), arquivo_nome,
              meta.get('periodo_inicio'), meta.get('periodo_fim'),
              meta.get('total_registros', 0), gravados, erros))
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _shopee_dashboard(engine):
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
            df_ads = _query_df(engine, """
                SELECT * FROM fact_ads_shopee
                WHERE loja = %s AND periodo_inicio >= %s AND periodo_fim <= %s
                ORDER BY despesas DESC
            """, [loja_nome, dt_inicio, dt_fim])

            if df_ads.empty:
                st.warning("Nenhum dado de ads encontrado para este período/loja.")
                st.caption(f"Buscando: loja='{loja_nome}', período {dt_inicio} a {dt_fim}")
                df_debug = _query_df(engine, """
                    SELECT DISTINCT loja, periodo_inicio, periodo_fim, COUNT(*) as qtd
                    FROM fact_ads_shopee GROUP BY loja, periodo_inicio, periodo_fim
                    ORDER BY periodo_fim DESC LIMIT 10
                """)
                if not df_debug.empty:
                    st.caption("Dados disponíveis no banco:")
                    st.dataframe(df_debug, hide_index=True)
                else:
                    st.caption("⚠️ Tabela fact_ads_shopee está vazia.")
                return

            vendas_totais = _query_scalar(engine, """
                SELECT COALESCE(SUM(valor_venda_efetivo), 0) FROM fact_vendas_snapshot
                WHERE marketplace_origem = 'Shopee' AND data_venda BETWEEN %s AND %s
            """, [dt_inicio, dt_fim]) or 0
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

            st.markdown("### TACOS por Produto")
            st.caption("Produtos com SKU vinculado mostram TACOS real. Sem SKU = apenas ACOS do painel.")

            resultados = []
            for _, ad in df_ads.iterrows():
                sku = ad.get('sku_match')
                tacos_data = None
                if sku and str(sku).strip():
                    tacos_data = calcular_tacos(engine, loja_nome, sku, dt_inicio, dt_fim)
                    if tacos_data and 'erro' in tacos_data:
                        tacos_data = None

                resultados.append({
                    'Status': cor_tacos(tacos_data['tacos'] if tacos_data else None),
                    'Produto': str(ad['nome_anuncio'])[:45],
                    'Investimento': float(ad['despesas']),
                    'ACOS': float(ad['acos']),
                    'TACOS': tacos_data['tacos'] if tacos_data else None,
                    'Itens Ads': int(ad['itens_vendidos']),
                    'Itens Total': tacos_data['qtd_total'] if tacos_data else None,
                    '% Orgânico': tacos_data['pct_organico'] if tacos_data else None,
                    'Receita Total': tacos_data['receita_total'] if tacos_data else None,
                    'Método': str(ad.get('metodo_lance', ''))[:25],
                    'SKU': sku if sku and str(sku).strip() else '❌ não vinculado',
                })

            df_res = pd.DataFrame(resultados).sort_values('Investimento', ascending=False)
            df_exib = df_res.copy()
            df_exib['Investimento'] = df_exib['Investimento'].apply(fmt_brl)
            df_exib['ACOS'] = df_exib['ACOS'].apply(fmt_pct)
            df_exib['TACOS'] = df_exib['TACOS'].apply(fmt_pct)
            df_exib['% Orgânico'] = df_exib['% Orgânico'].apply(fmt_pct)
            df_exib['Receita Total'] = df_exib['Receita Total'].apply(fmt_brl)
            st.dataframe(df_exib, use_container_width=True, hide_index=True)

            st.markdown("### ⚠️ Alertas")
            tem_alerta = False
            for _, r in df_res.iterrows():
                if r['TACOS'] is not None and r['TACOS'] > 10:
                    st.error(f"🔴 **{r['Produto']}** — TACOS {fmt_pct(r['TACOS'])}. Ação necessária!")
                    tem_alerta = True
                elif r['ACOS'] > 50 and r['Investimento'] > 10:
                    st.warning(f"⚠️ **{r['Produto']}** — ACOS {fmt_pct(r['ACOS'])}. Vincule SKU para calcular TACOS.")
                    tem_alerta = True
            if not tem_alerta:
                st.success("Nenhum alerta crítico no período.")

            st.markdown("### Métodos de Lance")
            metodos = {}
            for _, ad in df_ads.iterrows():
                met = str(ad.get('metodo_lance', 'N/A'))[:40]
                if met not in metodos:
                    metodos[met] = {'invest': 0, 'gmv': 0, 'n': 0}
                metodos[met]['invest'] += float(ad['despesas'])
                metodos[met]['gmv'] += float(ad['gmv'])
                metodos[met]['n'] += 1
            for met, m in metodos.items():
                acos_m = (m['invest'] / m['gmv'] * 100) if m['gmv'] > 0 else 0
                st.caption(f"**{met}** — {m['n']} anúncios | Invest: {fmt_brl(m['invest'])} | ACOS: {fmt_pct(acos_m)}")


def _shopee_match_sku(engine):
    st.subheader("Vincular Produto Ads → SKU")
    st.caption("Vincule cada produto do relatório de ads a um SKU do sistema. Feito uma vez, fica salvo.")

    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="match_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    df_sem_match = _query_df(engine, """
        SELECT DISTINCT nome_anuncio, id_produto FROM fact_ads_shopee
        WHERE loja = %s AND (sku_match IS NULL OR sku_match = '') AND despesas > 0
        ORDER BY nome_anuncio
    """, [loja_nome])

    if df_sem_match.empty:
        st.success("✅ Todos os produtos com gasto estão vinculados a um SKU!")
        df_existentes = _query_df(engine, """
            SELECT DISTINCT nome_anuncio, sku_match FROM fact_ads_shopee
            WHERE loja = %s AND sku_match IS NOT NULL AND sku_match != ''
            ORDER BY nome_anuncio
        """, [loja_nome])
        if not df_existentes.empty:
            st.caption("Vínculos atuais:")
            st.dataframe(df_existentes, use_container_width=True, hide_index=True)
        return

    st.warning(f"⚠️ {len(df_sem_match)} produto(s) sem SKU vinculado:")

    df_skus = _query_df(engine, "SELECT sku, nome_produto FROM dim_skus WHERE ativo = TRUE ORDER BY sku")
    if df_skus.empty:
        opcoes_sku = [""]
        st.error("Nenhum SKU ativo encontrado")
    else:
        opcoes_sku = [""] + [f"{r['sku']} — {str(r['nome_produto'])[:40]}" for _, r in df_skus.iterrows()]

    with st.form("match_form"):
        matches = {}
        for i, (_, row) in enumerate(df_sem_match.iterrows()):
            nome = str(row['nome_anuncio'])[:60]
            id_prod = str(row.get('id_produto', ''))
            col1, col2 = st.columns([3, 2])
            with col1:
                st.text(f"📦 {nome}")
                if id_prod and id_prod not in ('nan', '', 'None'):
                    st.caption(f"ID: {id_prod}")
            with col2:
                sel = st.selectbox("SKU", opcoes_sku, key=f"match_{i}", label_visibility="collapsed")
                if sel:
                    matches[i] = {'nome': row['nome_anuncio'], 'id_produto': id_prod if id_prod not in ('nan', '') else '', 'sku': sel.split(" — ")[0]}

        submitted = st.form_submit_button("💾 Salvar Vínculos", type="primary")
        if submitted and matches:
            salvos = 0
            for _, match in matches.items():
                ok = salvar_match_sku(engine, loja_nome, match['nome'], match['id_produto'], match['sku'])
                if ok:
                    conn = None
                    cursor = None
                    try:
                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE fact_ads_shopee SET sku_match = %s, match_confirmado = TRUE
                            WHERE loja = %s AND nome_anuncio = %s
                        """, (match['sku'], loja_nome, match['nome']))
                        conn.commit()
                        salvos += 1
                    except Exception as e:
                        if conn:
                            conn.rollback()
                        st.error(f"Erro: {str(e)[:100]}")
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            conn.close()
            if salvos > 0:
                st.success(f"✅ {salvos} vínculo(s) salvos!")
                st.rerun()


def _shopee_historico(engine):
    st.subheader("Histórico de Uploads")
    df = _query_df(engine, """
        SELECT data_upload, loja, tipo_relatorio, arquivo_nome,
               periodo_inicio, periodo_fim, linhas_importadas, linhas_erro, status
        FROM log_uploads_ads WHERE marketplace = 'Shopee'
        ORDER BY data_upload DESC LIMIT 50
    """)
    if df.empty:
        st.info("Nenhum upload de ads registrado ainda.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("🔍 Diagnóstico do banco"):
        total = _query_scalar(engine, "SELECT COUNT(*) FROM fact_ads_shopee")
        st.caption(f"Total de registros em fact_ads_shopee: {total or 0}")
        df_resumo = _query_df(engine, """
            SELECT loja, periodo_inicio, periodo_fim, COUNT(*) as registros,
                   ROUND(SUM(despesas)::numeric, 2) as invest_total
            FROM fact_ads_shopee GROUP BY loja, periodo_inicio, periodo_fim
            ORDER BY periodo_fim DESC
        """)
        if not df_resumo.empty:
            st.dataframe(df_resumo, use_container_width=True, hide_index=True)
        else:
            st.warning("Tabela fact_ads_shopee está vazia.")
