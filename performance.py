"""
PERFORMANCE - Sistema Nala
Versão: 1.7 (19/04/2026)

Módulo de acompanhamento de metas mensais por loja e por anúncio.

VERSÃO 1.7 (19/04/2026):
  - FIX: Templates zerado/preenchido agora são iguais à tabela completa
  - Removido botão "Download Tabela" separado (template preenchido faz essa função)
  - Reestruturado fluxo: df_display montado antes da seção de download

VERSÃO 1.6 (16/04/2026):
  - Metas do mês anterior como default (loja + anúncios)
  - Preço médio unitário editável na tabela e template
  - Dois downloads (zerado + preenchido)

VERSÃO 1.5 (14/04/2026):
  - dias_vendas baseado na última venda lançada por loja
  - Caption por loja; help com todos os modelos
"""

import streamlit as st
import pandas as pd
import io
from datetime import date
from database_utils import get_engine
from performance_utils import (
    MODELOS_PROJECAO, get_ano_mes, get_mes_anterior, get_dias_vendas,
    get_primeiro_ultimo_dia, calcular_projecao, calcular_performance,
    buscar_lojas_por_marketplace, buscar_meta_loja, salvar_meta_loja,
    buscar_metas_anuncio, salvar_metas_anuncio_lote,
    buscar_resumo_geral, construir_tabela_performance,
    buscar_opcoes_tags, buscar_realizados_mes,
    buscar_ultimo_dia_vendas, auto_copiar_metas_mes_anterior,
)

# ============================================================
# HELPERS DE FORMATAÇÃO
# ============================================================

def _fmt_brl(val):
    if val is None or val == 0:
        return "R$ 0"
    return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_pct(val):
    if val is None:
        return "—"
    return f"{val:.1f}%"

def _cor_performance(val):
    if val is None:
        return "⚪"
    if val >= 100:
        return "🟢"
    if val >= 70:
        return "🟡"
    return "🔴"

def _bg_performance(val):
    if val is None:
        return ""
    if val >= 100:
        return "background-color: #D1FAE5"
    if val >= 70:
        return "background-color: #FEF3C7"
    return "background-color: #FEE2E2"

def _help_modelos():
    linhas = []
    for nome, info in MODELOS_PROJECAO.items():
        pesos = f"S1={info['sem1']:.0%} | S2={info['sem2']:.0%} | S3={info['sem3']:.0%} | S4={info['sem4']:.0%}"
        linhas.append(f"▸ {nome}: {info['desc']} ({pesos})")
    return "\n".join(linhas)

# ============================================================
# SELETOR DE MÊS
# ============================================================

def _seletor_mes():
    hoje = date.today()
    mes_atual = get_ano_mes(hoje)
    opcoes = []
    for i in range(-1, 4):
        m = get_mes_anterior(mes_atual, i) if i > 0 else mes_atual if i == 0 else get_ano_mes(
            date(hoje.year, hoje.month + 1, 1) if hoje.month < 12 else date(hoje.year + 1, 1, 1))
        opcoes.append(m)
    opcoes = sorted(set(opcoes), reverse=True)

    meses_nomes = {
        '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
        '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
        '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
    }

    def fmt_mes(am):
        nome = meses_nomes.get(am[5:7], am[5:7])
        return f"{nome} {am[:4]}"

    idx_default = opcoes.index(mes_atual) if mes_atual in opcoes else 0
    return st.selectbox("📅 Mês de referência:", opcoes,
                        index=idx_default, format_func=fmt_mes, key="perf_mes")


# ============================================================
# SEÇÃO: META DA LOJA (TOPO)
# ============================================================

def _render_meta_loja(engine, loja, marketplace, ano_mes):
    meta_info = buscar_meta_loja(engine, loja, ano_mes)

    usando_fallback = False
    if meta_info is None:
        mes_ant = get_mes_anterior(ano_mes, 1)
        meta_info_ant = buscar_meta_loja(engine, loja, mes_ant)
        if meta_info_ant:
            meta_atual = float(meta_info_ant['meta_receita'])
            modelo_atual = meta_info_ant.get('modelo_projecao', 'Linear')
            usando_fallback = True
        else:
            meta_atual = 0.0
            modelo_atual = 'Linear'
    else:
        meta_atual = float(meta_info['meta_receita'])
        modelo_atual = meta_info.get('modelo_projecao', 'Linear')

    if usando_fallback:
        st.info("ℹ️ Meta pré-preenchida com o valor do mês anterior. Clique em Salvar Meta para confirmar.")

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        nova_meta = st.number_input(
            f"Meta Receita — {loja}", value=meta_atual, step=1000.0,
            format="%.2f", key=f"meta_{loja}_{ano_mes}")
    with col2:
        modelos = list(MODELOS_PROJECAO.keys())
        idx_mod = modelos.index(modelo_atual) if modelo_atual in modelos else 0
        modelo_sel = st.selectbox(
            "Modelo Projeção", modelos, index=idx_mod,
            help=_help_modelos(),
            key=f"modelo_{loja}_{ano_mes}")
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("💾 Salvar Meta", key=f"btn_meta_{loja}_{ano_mes}"):
            result = salvar_meta_loja(engine, loja, marketplace, ano_mes, nova_meta, modelo_sel)
            if result >= 0:
                st.success("Meta salva!")
                st.rerun()
            else:
                st.error("Erro ao salvar meta.")

    return nova_meta, modelo_sel


def _render_resumo_loja(engine, loja, marketplace, ano_mes, meta_receita, modelo):
    ultima_data = buscar_ultimo_dia_vendas(engine, loja, ano_mes)
    dias_vendas, dias_mes = get_dias_vendas(ano_mes, data_ref=ultima_data)

    if ultima_data:
        dia_fmt = ultima_data.strftime('%d/%m/%Y')
        st.caption(f"📅 Vendas lançadas até {dia_fmt} — Dia {dias_vendas} de {dias_mes} — {dias_mes - dias_vendas} dias restantes")
    else:
        _, dias_mes_total = get_dias_vendas(ano_mes)
        st.caption(f"📅 Nenhuma venda lançada neste mês — {dias_mes_total} dias no mês")

    df_real = buscar_realizados_mes(engine, loja, ano_mes, marketplace)

    fat_realizado = float(df_real['fat_realizado'].sum()) if not df_real.empty else 0
    qtd_realizado = int(df_real['qtd_realizado'].sum()) if not df_real.empty else 0

    if dias_vendas > 0 and fat_realizado > 0:
        proj_fat = calcular_projecao(fat_realizado, dias_vendas, dias_mes, modelo)
    else:
        proj_fat = 0

    perf = calcular_performance(proj_fat, meta_receita)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Meta", _fmt_brl(meta_receita))
    c2.metric("Realizado", _fmt_brl(fat_realizado))
    c3.metric("Projeção", _fmt_brl(proj_fat))
    c4.metric("Unidades", f"{qtd_realizado:,}".replace(",", "."))
    cor = _cor_performance(perf)
    c5.metric("Performance", f"{cor} {_fmt_pct(perf)}")

    if meta_receita > 0:
        progresso = min(fat_realizado / meta_receita, 1.0)
        st.progress(progresso, text=f"Realizado: {progresso*100:.1f}% da meta")

    df_metas = buscar_metas_anuncio(engine, loja, ano_mes)
    if not df_metas.empty and meta_receita > 0:
        from performance_utils import buscar_preco_medio_mes_anterior
        precos = buscar_preco_medio_mes_anterior(engine, loja, ano_mes, marketplace)
        is_amazon = 'AMAZON' in marketplace.upper()
        soma_meta_fat = 0
        for _, m in df_metas.iterrows():
            key = (m['codigo_anuncio'], m.get('logistica') if is_amazon else None)
            preco_manual = None
            if 'preco_medio_manual' in m.index:
                pm_val = m.get('preco_medio_manual')
                if pm_val is not None and not pd.isna(pm_val) and float(pm_val) > 0:
                    preco_manual = float(pm_val)
            pm = preco_manual if preco_manual is not None else precos.get(key, 0)
            soma_meta_fat += int(m['meta_quantidade']) * pm
        diff = meta_receita - soma_meta_fat
        if abs(diff) > 1:
            if diff > 0:
                st.warning(f"⚠️ Distribuição falta **{_fmt_brl(diff)}** para atingir a meta da loja.")
            else:
                st.info(f"ℹ️ Distribuição excede a meta da loja em **{_fmt_brl(abs(diff))}**.")

    return modelo, dias_vendas, dias_mes


# ============================================================
# DOWNLOAD / UPLOAD DE METAS POR ANÚNCIO
# ============================================================

def _gerar_xlsx_tabela(df_display, zerado=False):
    """
    Gera XLSX da tabela completa (mesmas colunas do df_display).
    Se zerado=True, zera Meta Qtd e Observação (template para preenchimento).
    """
    df_out = df_display.copy()
    if zerado:
        if 'Meta Qtd' in df_out.columns:
            df_out['Meta Qtd'] = 0
        if 'Observação' in df_out.columns:
            df_out['Observação'] = ''

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_out.to_excel(writer, index=False, sheet_name='Performance')
        ws = writer.sheets['Performance']
        # Ajustar largura das colunas automaticamente
        for col_idx, col_name in enumerate(df_out.columns, 1):
            max_len = max(len(str(col_name)), 10)
            # Checar primeiras linhas para estimar largura
            for row_idx in range(min(5, len(df_out))):
                cell_val = str(df_out.iloc[row_idx, col_idx - 1])
                max_len = max(max_len, len(cell_val))
            ws.column_dimensions[chr(64 + col_idx) if col_idx <= 26 else 'A'].width = min(max_len + 2, 40)
    buffer.seek(0)
    return buffer


def _processar_upload_metas(arquivo, loja, marketplace, ano_mes, is_amazon, engine):
    """
    Processa upload de planilha XLSX com metas por anúncio.
    Aceita tanto formato template (colunas renomeadas) quanto formato original.
    """
    try:
        df_upload = pd.read_excel(arquivo)
    except Exception as e:
        return -1, f"Erro ao ler arquivo: {e}"

    rename_back = {
        'Código Anúncio': 'codigo_anuncio', 'Codigo Anuncio': 'codigo_anuncio',
        'codigo_anuncio': 'codigo_anuncio', 'Anúncio': 'codigo_anuncio',
        'SKU': 'sku', 'Produto': 'produto',
        'Logística': 'logistica', 'Logistica': 'logistica',
        'Preço Médio': 'preco_medio', 'Preco Medio': 'preco_medio',
        'preco_medio': 'preco_medio',
        'Meta Qtd': 'meta_qtd', 'meta_qtd': 'meta_qtd',
        'Tag': 'tag', 'tag': 'tag',
        'Observação': 'observacao', 'Observacao': 'observacao', 'observacao': 'observacao',
    }
    df_upload = df_upload.rename(columns={
        c: rename_back[c] for c in df_upload.columns if c in rename_back
    })

    if 'codigo_anuncio' not in df_upload.columns:
        return -1, "Coluna 'Código Anúncio' ou 'Anúncio' não encontrada no arquivo."

    metas = []
    for _, row in df_upload.iterrows():
        cod = str(row.get('codigo_anuncio', '')).strip()
        if not cod or cod.lower() in ('nan', 'none', ''):
            continue

        meta_val = row.get('meta_qtd', 0)
        if pd.isna(meta_val):
            meta_val = 0
        try:
            meta_qtd = int(float(meta_val))
        except (ValueError, TypeError):
            meta_qtd = 0

        preco_val = row.get('preco_medio')
        preco_manual = None
        if preco_val is not None and not pd.isna(preco_val):
            try:
                preco_manual = float(preco_val)
                if preco_manual <= 0:
                    preco_manual = None
            except (ValueError, TypeError):
                preco_manual = None

        obs = str(row.get('observacao', '') or '').strip()
        if obs.lower() in ('nan', 'none'):
            obs = ''

        log = None
        if is_amazon:
            log_val = str(row.get('logistica', '') or '').strip()
            if log_val.lower() in ('nan', 'none', ''):
                log = None
            else:
                log = log_val

        metas.append({
            'loja_origem': loja,
            'marketplace': marketplace,
            'codigo_anuncio': cod,
            'logistica': log,
            'ano_mes': ano_mes,
            'meta_quantidade': meta_qtd,
            'observacao': obs,
            'preco_medio_manual': preco_manual,
            'sku': str(row.get('sku', '') or '').strip(),
            'tag': str(row.get('tag', '') or '').strip() if 'tag' in df_upload.columns else None,
        })

    if not metas:
        return 0, "Nenhuma meta válida encontrada no arquivo."

    result = salvar_metas_anuncio_lote(engine, metas)

    # Salvar tags (se coluna Tag estava no upload)
    tags_para_salvar = []
    for m in metas:
        tag_val = m.get('tag')
        if tag_val is not None and tag_val.lower() not in ('nan', 'none', ''):
            tags_para_salvar.append({
                'marketplace': marketplace,
                'codigo_anuncio': m['codigo_anuncio'],
                'sku': m.get('sku', ''),
                'tag_status': tag_val if tag_val else None,
            })
    if tags_para_salvar:
        _salvar_tags_editadas(engine, tags_para_salvar)

    if result > 0:
        return result, f"{result} metas gravadas com sucesso."
    else:
        return result, "Erro ao gravar metas no banco."


def _render_download_upload_metas(engine, df_display, loja, marketplace, ano_mes, is_amazon):
    """
    Renderiza seção de Download e Upload de metas.
    df_display = tabela completa com colunas renomeadas (mesma exibida na tela).
    Template Zerado = tabela completa com Meta Qtd=0.
    Template Preenchido = tabela completa como está.
    """
    st.markdown("##### 📋 Metas por Anúncio — Planilha")

    col_dl_z, col_dl_p, col_ul = st.columns(3)

    nome_safe = loja.replace(' ', '_').replace('/', '-')

    with col_dl_z:
        if not df_display.empty:
            buffer_z = _gerar_xlsx_tabela(df_display, zerado=True)
            st.download_button(
                label="⬇️ Template Zerado",
                data=buffer_z,
                file_name=f"metas_zerado_{nome_safe}_{ano_mes}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_metas_z_{loja}_{ano_mes}",
                use_container_width=True,
            )
        else:
            st.info("Sem anúncios.")

    with col_dl_p:
        if not df_display.empty:
            buffer_p = _gerar_xlsx_tabela(df_display, zerado=False)
            st.download_button(
                label="⬇️ Tabela Completa",
                data=buffer_p,
                file_name=f"performance_{nome_safe}_{ano_mes}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_metas_p_{loja}_{ano_mes}",
                use_container_width=True,
            )
        else:
            st.info("Sem anúncios.")

    with col_ul:
        arquivo_up = st.file_uploader(
            "⬆️ Upload Metas (XLSX)",
            type=['xlsx'],
            key=f"ul_metas_{loja}_{ano_mes}",
            label_visibility="collapsed",
        )

    if arquivo_up is not None:
        if st.button("📤 Processar Upload de Metas", key=f"btn_ul_{loja}_{ano_mes}",
                      use_container_width=True):
            with st.spinner("Processando metas..."):
                result, msg = _processar_upload_metas(
                    arquivo_up, loja, marketplace, ano_mes, is_amazon, engine
                )
            if result > 0:
                st.success(f"✅ {msg}")
                st.rerun()
            elif result == 0:
                st.warning(f"⚠️ {msg}")
            else:
                st.error(f"❌ {msg}")

    st.divider()


# ============================================================
# TABELA DE ANÚNCIOS (EDITÁVEL)
# ============================================================

def _render_tabela_anuncios(engine, loja, marketplace, ano_mes, modelo, dias_vendas, dias_mes):
    df = construir_tabela_performance(engine, loja, marketplace, ano_mes, modelo,
                                      dias_vendas_override=dias_vendas,
                                      dias_mes_override=dias_mes)

    is_amazon = 'AMAZON' in marketplace.upper()

    if df.empty:
        # Renderiza download/upload com df vazio para manter o upload disponível
        _render_download_upload_metas(engine, pd.DataFrame(), loja, marketplace, ano_mes, is_amazon)
        st.info("Nenhum anúncio com vendas nos últimos 3 meses para esta loja.")
        return

    # Toggle histórico (M-2 e M-3)
    mostrar_hist = st.toggle("📊 Mostrar colunas de histórico (M-2 e M-3)", value=False,
                             key=f"hist_{loja}_{ano_mes}")

    # Buscar opções de tags
    opcoes_tags = buscar_opcoes_tags(engine, 'anuncio')
    opcoes_tags_display = [''] + opcoes_tags

    # ── MONTAR df_display (ANTES do download) ──
    cols_principais = ['codigo_anuncio', 'sku', 'produto']
    if is_amazon:
        cols_principais.append('logistica')
    cols_principais += ['curva', 'tag', 'margem_ant', 'margem_atual']

    cols_mes_ant = []
    if 'hist_1_qtd' in df.columns:
        cols_mes_ant = ['hist_1_qtd', 'hist_1_fat']

    cols_principais += cols_mes_ant
    cols_principais += ['preco_medio', 'meta_qtd', 'meta_fat', 'qtd_realizado', 'fat_realizado',
                        'performance', 'proj_qtd', 'proj_fat', 'observacao']

    cols_hist = []
    if mostrar_hist:
        for i in range(2, 4):
            if f'hist_{i}_qtd' in df.columns:
                cols_hist += [f'hist_{i}_qtd', f'hist_{i}_fat']

    df_display = df[cols_principais + cols_hist].copy()

    # Renomear colunas
    rename_map = {
        'codigo_anuncio': 'Anúncio', 'sku': 'SKU', 'produto': 'Produto',
        'logistica': 'Logística', 'curva': 'Curva', 'tag': 'Tag',
        'margem_ant': 'Margem Ant.%', 'margem_atual': 'Margem Atual%',
        'preco_medio': 'Preço Médio',
        'meta_qtd': 'Meta Qtd', 'meta_fat': 'Meta Fat.',
        'qtd_realizado': 'Real. Qtd', 'fat_realizado': 'Real. Fat.',
        'performance': '⭐ Perf.%', 'proj_qtd': 'Proj. Qtd', 'proj_fat': 'Proj. Fat.',
        'observacao': 'Observação',
    }
    for i in range(1, 4):
        if f'hist_{i}_mes' in df.columns and len(df) > 0:
            mes_val = df[f'hist_{i}_mes'].iloc[0] if not df.empty else f'M-{i}'
            rename_map[f'hist_{i}_qtd'] = f'{mes_val} Qtd'
            rename_map[f'hist_{i}_fat'] = f'{mes_val} Fat.'

    df_display = df_display.rename(columns=rename_map)

    # ── DOWNLOAD / UPLOAD (usando df_display completo) ──
    _render_download_upload_metas(engine, df_display, loja, marketplace, ano_mes, is_amazon)

    # ── CONFIGURAR COLUNAS ──
    col_config = {
        'Anúncio': st.column_config.TextColumn(width="medium", disabled=True),
        'SKU': st.column_config.TextColumn(width="small", disabled=True),
        'Produto': st.column_config.TextColumn(width="medium", disabled=True),
        'Curva': st.column_config.TextColumn(width="small", disabled=True),
        'Tag': st.column_config.SelectboxColumn(options=opcoes_tags_display, width="small"),
        'Margem Ant.%': st.column_config.NumberColumn(format="%.1f%%", disabled=True),
        'Margem Atual%': st.column_config.NumberColumn(format="%.1f%%", disabled=True),
        'Preço Médio': st.column_config.NumberColumn(format="R$ %.2f", min_value=0, step=0.01),
        'Meta Qtd': st.column_config.NumberColumn(min_value=0, step=1, format="%d"),
        'Meta Fat.': st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
        'Real. Qtd': st.column_config.NumberColumn(format="%d", disabled=True),
        'Real. Fat.': st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
        '⭐ Perf.%': st.column_config.NumberColumn(format="%.1f%%", disabled=True),
        'Proj. Qtd': st.column_config.NumberColumn(format="%d", disabled=True),
        'Proj. Fat.': st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
        'Observação': st.column_config.TextColumn(width="medium"),
    }
    if is_amazon:
        col_config['Logística'] = st.column_config.TextColumn(width="small", disabled=True)

    for i in range(1, 4):
        qtd_col = rename_map.get(f'hist_{i}_qtd', f'M-{i} Qtd')
        fat_col = rename_map.get(f'hist_{i}_fat', f'M-{i} Fat.')
        if qtd_col in df_display.columns:
            col_config[qtd_col] = st.column_config.NumberColumn(format="%d", disabled=True)
        if fat_col in df_display.columns:
            col_config[fat_col] = st.column_config.NumberColumn(format="R$ %.2f", disabled=True)

    # ── DATA EDITOR ──
    df_editado = st.data_editor(
        df_display,
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        height=600,
        key=f"editor_{loja}_{ano_mes}",
        num_rows="fixed",
    )

    # ── BOTÃO SALVAR ──
    if st.button("💾 Salvar Metas e Observações", key=f"btn_salvar_{loja}_{ano_mes}",
                 type="primary", use_container_width=True):
        _salvar_edicoes(engine, df, df_editado, loja, marketplace, ano_mes, is_amazon, rename_map)


def _salvar_edicoes(engine, df_original, df_editado, loja, marketplace, ano_mes, is_amazon, rename_map):
    metas_para_salvar = []
    tags_para_salvar = []

    for idx in range(len(df_editado)):
        cod = df_original.iloc[idx]['codigo_anuncio']
        logistica = df_original.iloc[idx].get('logistica') if is_amazon else None

        meta_qtd = int(df_editado.iloc[idx].get('Meta Qtd', 0) or 0)

        preco_editado = df_editado.iloc[idx].get('Preço Médio')
        preco_manual = None
        if preco_editado is not None and not pd.isna(preco_editado):
            try:
                preco_manual = float(preco_editado)
                if preco_manual <= 0:
                    preco_manual = None
            except (ValueError, TypeError):
                preco_manual = None

        obs = str(df_editado.iloc[idx].get('Observação', '') or '')

        metas_para_salvar.append({
            'loja_origem': loja,
            'marketplace': marketplace,
            'codigo_anuncio': cod,
            'logistica': logistica,
            'ano_mes': ano_mes,
            'meta_quantidade': meta_qtd,
            'observacao': obs,
            'preco_medio_manual': preco_manual,
        })

        tag_nova = str(df_editado.iloc[idx].get('Tag', '') or '')
        tag_original = str(df_original.iloc[idx].get('tag', '') or '')
        if tag_nova != tag_original:
            tags_para_salvar.append({
                'marketplace': marketplace,
                'codigo_anuncio': cod,
                'sku': df_original.iloc[idx].get('sku', ''),
                'tag_status': tag_nova if tag_nova else None,
            })

    result = salvar_metas_anuncio_lote(engine, metas_para_salvar)
    if result < 0:
        st.error("Erro ao salvar metas.")
        return

    if tags_para_salvar:
        _salvar_tags_editadas(engine, tags_para_salvar)

    st.success(f"✅ {len(metas_para_salvar)} metas salvas com sucesso!")
    st.rerun()


def _salvar_tags_editadas(engine, tags_list):
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for t in tags_list:
            cursor.execute("""
                INSERT INTO dim_tags_anuncio (marketplace, codigo_anuncio, sku, tag_status, data_atualizacao)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (marketplace, codigo_anuncio)
                DO UPDATE SET tag_status = EXCLUDED.tag_status, data_atualizacao = NOW()
            """, (t['marketplace'], t['codigo_anuncio'], t['sku'], t['tag_status']))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Erro ao salvar tags: {e}")


# ============================================================
# TAB MARKETPLACE
# ============================================================

def _render_tab_marketplace(engine, marketplace, ano_mes):
    lojas = buscar_lojas_por_marketplace(engine, marketplace)
    if not lojas:
        st.warning(f"Nenhuma loja cadastrada para {marketplace}.")
        return

    loja = st.selectbox("🏪 Selecione a loja:", lojas, key=f"sel_loja_{marketplace}")

    st.markdown(f"### 🏪 {loja}")

    copiou = auto_copiar_metas_mes_anterior(engine, loja, marketplace, ano_mes)
    if copiou:
        st.toast(f"📋 Metas do mês anterior copiadas para {loja}", icon="📋")

    meta_receita, modelo = _render_meta_loja(engine, loja, marketplace, ano_mes)

    modelo, dias_vendas, dias_mes = _render_resumo_loja(engine, loja, marketplace, ano_mes, meta_receita, modelo)

    st.divider()

    _render_tabela_anuncios(engine, loja, marketplace, ano_mes, modelo, dias_vendas, dias_mes)


# ============================================================
# TAB GERAL
# ============================================================

def _render_tab_geral(engine, ano_mes):
    st.subheader("📊 Visão Geral — Todas as Lojas")

    df_vendas, df_metas, df_dev = buscar_resumo_geral(engine, ano_mes)

    from performance_utils import buscar_todas_lojas
    df_lojas = buscar_todas_lojas(engine)
    if df_lojas.empty:
        st.info("Nenhuma loja cadastrada.")
        return

    rows = []
    for _, loja_row in df_lojas.iterrows():
        loja = loja_row['loja']
        mktp = loja_row['marketplace']

        ultima_data = buscar_ultimo_dia_vendas(engine, loja, ano_mes)
        dias_vendas, dias_mes = get_dias_vendas(ano_mes, data_ref=ultima_data)

        meta_receita = 0
        modelo = 'Linear'
        if not df_metas.empty:
            m = df_metas[df_metas['loja_origem'] == loja]
            if not m.empty:
                meta_receita = float(m.iloc[0]['meta_receita'])
                modelo = m.iloc[0].get('modelo_projecao', 'Linear')

        fat_real = 0
        if not df_vendas.empty:
            v = df_vendas[df_vendas['loja_origem'] == loja]
            if not v.empty:
                fat_real = float(v['fat_realizado'].sum())

        fat_dev = 0
        if not df_dev.empty:
            d = df_dev[df_dev['loja_origem'] == loja]
            if not d.empty:
                fat_dev = float(d['fat_dev'].sum())

        fat_liquido = fat_real - fat_dev

        if dias_vendas > 0 and fat_liquido > 0:
            proj = calcular_projecao(fat_liquido, dias_vendas, dias_mes, modelo)
        else:
            proj = 0

        perf = calcular_performance(proj, meta_receita)

        ult_venda_fmt = ultima_data.strftime('%d/%m') if ultima_data else '—'

        rows.append({
            'Loja': loja,
            'Marketplace': mktp,
            'Últ. Venda': ult_venda_fmt,
            'Dia': dias_vendas,
            'Meta': meta_receita,
            'Realizado': round(fat_liquido, 2),
            'Projeção': round(proj, 2),
            'Modelo': modelo,
            '⭐ Performance': round(perf, 1) if perf else None,
        })

    df_geral = pd.DataFrame(rows)

    if not df_geral.empty:
        col_config = {
            'Meta': st.column_config.NumberColumn(format="R$ %.2f"),
            'Realizado': st.column_config.NumberColumn(format="R$ %.2f"),
            'Projeção': st.column_config.NumberColumn(format="R$ %.2f"),
            '⭐ Performance': st.column_config.NumberColumn(format="%.1f%%"),
            'Dia': st.column_config.NumberColumn(format="%d"),
        }
        st.dataframe(df_geral, column_config=col_config,
                      hide_index=True, use_container_width=True)

        total_meta = df_geral['Meta'].sum()
        total_real = df_geral['Realizado'].sum()
        total_proj = df_geral['Projeção'].sum()
        total_perf = calcular_performance(total_proj, total_meta)

        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Meta", _fmt_brl(total_meta))
        c2.metric("Total Realizado", _fmt_brl(total_real))
        c3.metric("Total Projeção", _fmt_brl(total_proj))
        cor = _cor_performance(total_perf)
        c4.metric("Performance Geral", f"{cor} {_fmt_pct(total_perf)}")


# ============================================================
# MAIN
# ============================================================

MARKETPLACES = [
    ("🛒 Mercado Livre", "MERCADO LIVRE"),
    ("📦 Amazon", "AMAZON"),
    ("🛍️ Shopee", "SHOPEE"),
    ("👗 Shein", "SHEIN"),
    ("🏬 Magalu", "MAGALU"),
    ("📊 Geral", "GERAL"),
]

def main():
    st.title("📊 Performance Mensal")
    st.caption("v1.7 — template = tabela completa")

    engine = get_engine()
    ano_mes = _seletor_mes()

    tab_names = [m[0] for m in MARKETPLACES]
    tabs = st.tabs(tab_names)

    for i, (label, mktp_code) in enumerate(MARKETPLACES):
        with tabs[i]:
            if mktp_code == "GERAL":
                _render_tab_geral(engine, ano_mes)
            else:
                _render_tab_marketplace(engine, mktp_code, ano_mes)


if __name__ == "__main__":
    main()
