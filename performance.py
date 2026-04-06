"""
PERFORMANCE - Sistema Nala
Versão: 1.3 (06/04/2026)

Módulo de acompanhamento de metas mensais por loja e por anúncio.
- Tabs por marketplace + tab Geral
- Meta de loja (admin) + distribuição por anúncio (gestor)
- Projeção com 4 modelos + indicadores visuais de performance
- Histórico de 3 meses como colunas expandíveis
- Integração com tags (Curva ABC + Status manual)

VERSÃO 1.4 (06/04/2026):
  - NOVO: Template XLSX e tabela visual agora mostram vendas do mês anterior
    (Qtd e Fat) como referência para preenchimento de metas
  - Mês anterior sempre visível na tabela; toggle cobre apenas M-2 e M-3

VERSÃO 1.3 (06/04/2026):
  - NOVO: Download de template XLSX com metas por anúncio (pré-preenchido)
  - NOVO: Upload de planilha XLSX para UPSERT de metas em lote
  - Regra dos 3 Meses: template e tela listam todos os anúncios com vendas
    nos últimos 3 meses, garantindo preenchimento preventivo no início do mês

VERSÃO 1.2:
  - fix duplicatas + dict mapping
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
    buscar_opcoes_tags, buscar_realizados_mes
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

# ============================================================
# SELETOR DE MÊS
# ============================================================

def _seletor_mes():
    hoje = date.today()
    mes_atual = get_ano_mes(hoje)
    # Gerar opções: mês atual + 3 anteriores + 1 próximo
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
    meta_atual = float(meta_info['meta_receita']) if meta_info else 0.0
    modelo_atual = meta_info.get('modelo_projecao', 'Linear') if meta_info else 'Linear'

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
            help=MODELOS_PROJECAO[modelos[idx_mod]]['desc'],
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
    """Barra de progresso e indicadores da loja."""
    dias_vendas, dias_mes = get_dias_vendas(ano_mes)
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

    # Verificar distribuição das metas de anúncio
    df_metas = buscar_metas_anuncio(engine, loja, ano_mes)
    if not df_metas.empty and meta_receita > 0:
        precos = {}
        from performance_utils import buscar_preco_medio_mes_anterior
        precos = buscar_preco_medio_mes_anterior(engine, loja, ano_mes, marketplace)
        is_amazon = 'AMAZON' in marketplace.upper()
        soma_meta_fat = 0
        for _, m in df_metas.iterrows():
            key = (m['codigo_anuncio'], m.get('logistica') if is_amazon else None)
            pm = precos.get(key, 0)
            soma_meta_fat += int(m['meta_quantidade']) * pm
        diff = meta_receita - soma_meta_fat
        if abs(diff) > 1:
            if diff > 0:
                st.warning(f"⚠️ Distribuição falta **{_fmt_brl(diff)}** para atingir a meta da loja.")
            else:
                st.info(f"ℹ️ Distribuição excede a meta da loja em **{_fmt_brl(abs(diff))}**.")

    return modelo


# ============================================================
# DOWNLOAD / UPLOAD DE METAS POR ANÚNCIO (v1.3)
# ============================================================

def _gerar_template_metas(df, is_amazon):
    """
    Gera XLSX template para download de metas por anúncio.
    Inclui todos os anúncios do universo (regra 3 meses) com metas
    pré-preenchidas quando existentes.
    v1.4: Inclui vendas do mês anterior (Qtd e Fat) como referência.
    """
    cols_template = ['codigo_anuncio', 'sku', 'produto']
    if is_amazon:
        cols_template.append('logistica')

    # Vendas do mês anterior como referência (hist_1 = mês mais recente)
    has_hist = 'hist_1_qtd' in df.columns
    if has_hist:
        cols_template += ['hist_1_qtd', 'hist_1_fat']

    cols_template += ['meta_qtd', 'observacao']

    df_template = df[cols_template].copy()

    # Descobrir label do mês anterior para nome das colunas
    mes_ant_label = 'Mês Ant.'
    if has_hist and 'hist_1_mes' in df.columns and not df.empty:
        mes_ant_label = df['hist_1_mes'].iloc[0]

    rename_map = {
        'codigo_anuncio': 'Código Anúncio',
        'sku': 'SKU',
        'produto': 'Produto',
        'logistica': 'Logística',
        'hist_1_qtd': f'{mes_ant_label} Qtd',
        'hist_1_fat': f'{mes_ant_label} Fat.',
        'meta_qtd': 'Meta Qtd',
        'observacao': 'Observação',
    }
    df_template = df_template.rename(columns=rename_map)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Metas')
        # Ajustar largura das colunas
        ws = writer.sheets['Metas']
        if is_amazon:
            col_widths = {
                'A': 22,  # Código Anúncio
                'B': 15,  # SKU
                'C': 35,  # Produto
                'D': 12,  # Logística
                'E': 14,  # Mês Ant. Qtd
                'F': 16,  # Mês Ant. Fat.
                'G': 12,  # Meta Qtd
                'H': 30,  # Observação
            }
        else:
            col_widths = {
                'A': 22,  # Código Anúncio
                'B': 15,  # SKU
                'C': 35,  # Produto
                'D': 14,  # Mês Ant. Qtd
                'E': 16,  # Mês Ant. Fat.
                'F': 12,  # Meta Qtd
                'G': 30,  # Observação
            }
        if not has_hist:
            # Sem histórico: remover colunas de mês anterior do mapa
            if is_amazon:
                col_widths = {'A': 22, 'B': 15, 'C': 35, 'D': 12, 'E': 12, 'F': 30}
            else:
                col_widths = {'A': 22, 'B': 15, 'C': 35, 'D': 12, 'E': 30}
        for col_letter, width in col_widths.items():
            ws.column_dimensions[col_letter].width = width
    buffer.seek(0)
    return buffer


def _processar_upload_metas(arquivo, loja, marketplace, ano_mes, is_amazon, engine):
    """
    Processa upload de planilha XLSX com metas por anúncio.
    Faz UPSERT em dim_metas_anuncio via salvar_metas_anuncio_lote.
    Grava qualquer codigo_anuncio enviado, mesmo fora do universo de 3 meses.
    """
    try:
        df_upload = pd.read_excel(arquivo)
    except Exception as e:
        return -1, f"Erro ao ler arquivo: {e}"

    # Normalizar nomes de colunas
    rename_back = {
        'Código Anúncio': 'codigo_anuncio',
        'Codigo Anuncio': 'codigo_anuncio',
        'codigo_anuncio': 'codigo_anuncio',
        'SKU': 'sku',
        'Produto': 'produto',
        'Logística': 'logistica',
        'Logistica': 'logistica',
        'Meta Qtd': 'meta_qtd',
        'meta_qtd': 'meta_qtd',
        'Observação': 'observacao',
        'Observacao': 'observacao',
        'observacao': 'observacao',
    }
    df_upload = df_upload.rename(columns={
        c: rename_back[c] for c in df_upload.columns if c in rename_back
    })

    if 'codigo_anuncio' not in df_upload.columns:
        return -1, "Coluna 'Código Anúncio' não encontrada no arquivo."

    metas = []
    for _, row in df_upload.iterrows():
        cod = str(row.get('codigo_anuncio', '')).strip()
        if not cod or cod.lower() in ('nan', 'none', ''):
            continue

        # Meta quantidade — tratar NaN e valores não numéricos
        meta_val = row.get('meta_qtd', 0)
        if pd.isna(meta_val):
            meta_val = 0
        try:
            meta_qtd = int(float(meta_val))
        except (ValueError, TypeError):
            meta_qtd = 0

        # Observação
        obs = str(row.get('observacao', '') or '').strip()
        if obs.lower() in ('nan', 'none'):
            obs = ''

        # Logística (apenas Amazon)
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
        })

    if not metas:
        return 0, "Nenhuma meta válida encontrada no arquivo."

    result = salvar_metas_anuncio_lote(engine, metas)
    if result > 0:
        return result, f"{result} metas gravadas com sucesso."
    else:
        return result, "Erro ao gravar metas no banco."


def _render_download_upload_metas(engine, df, loja, marketplace, ano_mes, is_amazon):
    """
    Renderiza seção de Download template e Upload de metas por anúncio.
    Posicionada acima da tabela editável, dentro de cada tab marketplace/loja.
    """
    st.markdown("##### 📋 Metas por Anúncio — Planilha")
    col_dl, col_ul = st.columns(2)

    # ── DOWNLOAD TEMPLATE ──
    with col_dl:
        if not df.empty:
            buffer = _gerar_template_metas(df, is_amazon)
            nome_safe = loja.replace(' ', '_').replace('/', '-')
            nome_arquivo = f"metas_{nome_safe}_{ano_mes}.xlsx"
            st.download_button(
                label="⬇️ Download Template Metas",
                data=buffer,
                file_name=nome_arquivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_metas_{loja}_{ano_mes}",
                use_container_width=True,
            )
        else:
            st.info("Sem anúncios para gerar template.")

    # ── UPLOAD METAS ──
    with col_ul:
        arquivo_up = st.file_uploader(
            "⬆️ Upload Metas (XLSX)",
            type=['xlsx'],
            key=f"ul_metas_{loja}_{ano_mes}",
            label_visibility="collapsed",
        )

    # Botão de processar FORA do file_uploader (evita re-render bug do Streamlit)
    if arquivo_up is not None:
        # Guardar nome no session_state para evitar reprocessamento
        ss_key = f"upload_processado_{loja}_{ano_mes}"
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

def _render_tabela_anuncios(engine, loja, marketplace, ano_mes, modelo):
    df = construir_tabela_performance(engine, loja, marketplace, ano_mes, modelo)

    is_amazon = 'AMAZON' in marketplace.upper()

    # ── Download / Upload de Metas (v1.3) ──
    _render_download_upload_metas(engine, df, loja, marketplace, ano_mes, is_amazon)

    if df.empty:
        st.info("Nenhum anúncio com vendas nos últimos 3 meses para esta loja.")
        return

    # Toggle histórico (M-2 e M-3 — mês anterior já aparece sempre)
    mostrar_hist = st.toggle("📊 Mostrar colunas de histórico (M-2 e M-3)", value=False,
                             key=f"hist_{loja}_{ano_mes}")

    # Buscar opções de tags
    opcoes_tags = buscar_opcoes_tags(engine, 'anuncio')
    opcoes_tags_display = [''] + opcoes_tags

    # Preparar colunas para exibição
    cols_principais = ['codigo_anuncio', 'sku', 'produto']
    if is_amazon:
        cols_principais.append('logistica')
    cols_principais += ['curva', 'tag', 'margem_ant', 'margem_atual']

    # Vendas do mês anterior sempre visíveis (v1.4)
    cols_mes_ant = []
    if 'hist_1_qtd' in df.columns:
        cols_mes_ant = ['hist_1_qtd', 'hist_1_fat']

    cols_principais += cols_mes_ant
    cols_principais += ['meta_qtd', 'meta_fat', 'qtd_realizado', 'fat_realizado',
                        'performance', 'proj_qtd', 'proj_fat', 'observacao']

    # Histórico M-2 e M-3 (toggle)
    cols_hist = []
    if mostrar_hist:
        for i in range(2, 4):
            if f'hist_{i}_qtd' in df.columns:
                mes_label = df[f'hist_{i}_mes'].iloc[0] if f'hist_{i}_mes' in df.columns and len(df) > 0 else f'M-{i}'
                cols_hist += [f'hist_{i}_qtd', f'hist_{i}_fat']

    df_display = df[cols_principais + cols_hist].copy()

    # Renomear colunas para exibição
    rename_map = {
        'codigo_anuncio': 'Anúncio', 'sku': 'SKU', 'produto': 'Produto',
        'logistica': 'Logística', 'curva': 'Curva', 'tag': 'Tag',
        'margem_ant': 'Margem Ant.%', 'margem_atual': 'Margem Atual%',
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

    # Configurar colunas editáveis e formatos
    col_config = {
        'Anúncio': st.column_config.TextColumn(width="medium", disabled=True),
        'SKU': st.column_config.TextColumn(width="small", disabled=True),
        'Produto': st.column_config.TextColumn(width="medium", disabled=True),
        'Curva': st.column_config.TextColumn(width="small", disabled=True),
        'Tag': st.column_config.SelectboxColumn(options=opcoes_tags_display, width="small"),
        'Margem Ant.%': st.column_config.NumberColumn(format="%.1f%%", disabled=True),
        'Margem Atual%': st.column_config.NumberColumn(format="%.1f%%", disabled=True),
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

    # Histórico
    for i in range(1, 4):
        qtd_col = rename_map.get(f'hist_{i}_qtd', f'M-{i} Qtd')
        fat_col = rename_map.get(f'hist_{i}_fat', f'M-{i} Fat.')
        if qtd_col in df_display.columns:
            col_config[qtd_col] = st.column_config.NumberColumn(format="%d", disabled=True)
        if fat_col in df_display.columns:
            col_config[fat_col] = st.column_config.NumberColumn(format="R$ %.2f", disabled=True)

    # Aplicar cores na coluna performance via styling
    def style_perf(val):
        if pd.isna(val) or val is None:
            return ''
        if val >= 100:
            return 'background-color: #D1FAE5; font-weight: bold; color: #065F46'
        if val >= 70:
            return 'background-color: #FEF3C7; font-weight: bold; color: #92400E'
        return 'background-color: #FEE2E2; font-weight: bold; color: #991B1B'

    # Data editor
    df_editado = st.data_editor(
        df_display,
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        key=f"editor_{loja}_{ano_mes}",
        num_rows="fixed",
    )

    # Botão salvar
    if st.button("💾 Salvar Metas e Observações", key=f"btn_salvar_{loja}_{ano_mes}",
                 type="primary", use_container_width=True):
        _salvar_edicoes(engine, df, df_editado, loja, marketplace, ano_mes, is_amazon, rename_map)


def _salvar_edicoes(engine, df_original, df_editado, loja, marketplace, ano_mes, is_amazon, rename_map):
    """Salva meta_qtd, observacao e tag editados."""
    metas_para_salvar = []
    tags_para_salvar = []

    # Mapear colunas de volta
    inv_rename = {v: k for k, v in rename_map.items()}

    for idx in range(len(df_editado)):
        cod = df_original.iloc[idx]['codigo_anuncio']
        logistica = df_original.iloc[idx].get('logistica') if is_amazon else None

        # Meta quantidade
        meta_col = 'Meta Qtd'
        meta_qtd = int(df_editado.iloc[idx].get(meta_col, 0) or 0)

        # Observação
        obs = str(df_editado.iloc[idx].get('Observação', '') or '')

        metas_para_salvar.append({
            'loja_origem': loja,
            'marketplace': marketplace,
            'codigo_anuncio': cod,
            'logistica': logistica,
            'ano_mes': ano_mes,
            'meta_quantidade': meta_qtd,
            'observacao': obs,
        })

        # Tag (salvar em dim_tags_anuncio)
        tag_nova = str(df_editado.iloc[idx].get('Tag', '') or '')
        tag_original = str(df_original.iloc[idx].get('tag', '') or '')
        if tag_nova != tag_original:
            tags_para_salvar.append({
                'marketplace': marketplace,
                'codigo_anuncio': cod,
                'sku': df_original.iloc[idx].get('sku', ''),
                'tag_status': tag_nova if tag_nova else None,
            })

    # Salvar metas
    result = salvar_metas_anuncio_lote(engine, metas_para_salvar)
    if result < 0:
        st.error("Erro ao salvar metas.")
        return

    # Salvar tags alteradas
    if tags_para_salvar:
        _salvar_tags_editadas(engine, tags_para_salvar)

    st.success(f"✅ {len(metas_para_salvar)} metas salvas com sucesso!")
    st.rerun()


def _salvar_tags_editadas(engine, tags_list):
    """Salva tags editadas na dim_tags_anuncio."""
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

    # Seletor de loja (em vez de mostrar todas de uma vez)
    loja = st.selectbox("🏪 Selecione a loja:", lojas, key=f"sel_loja_{marketplace}")

    st.markdown(f"### 🏪 {loja}")

    # Meta da loja
    meta_receita, modelo = _render_meta_loja(engine, loja, marketplace, ano_mes)

    # Resumo
    _render_resumo_loja(engine, loja, marketplace, ano_mes, meta_receita, modelo)

    st.divider()

    # Tabela de anúncios
    _render_tabela_anuncios(engine, loja, marketplace, ano_mes, modelo)


# ============================================================
# TAB GERAL
# ============================================================

def _render_tab_geral(engine, ano_mes):
    st.subheader("📊 Visão Geral — Todas as Lojas")

    df_vendas, df_metas, df_dev = buscar_resumo_geral(engine, ano_mes)
    dias_vendas, dias_mes = get_dias_vendas(ano_mes)

    # Montar tabela consolidada
    from performance_utils import buscar_todas_lojas
    df_lojas = buscar_todas_lojas(engine)
    if df_lojas.empty:
        st.info("Nenhuma loja cadastrada.")
        return

    rows = []
    for _, loja_row in df_lojas.iterrows():
        loja = loja_row['loja']
        mktp = loja_row['marketplace']

        # Meta
        meta_receita = 0
        modelo = 'Linear'
        if not df_metas.empty:
            m = df_metas[df_metas['loja_origem'] == loja]
            if not m.empty:
                meta_receita = float(m.iloc[0]['meta_receita'])
                modelo = m.iloc[0].get('modelo_projecao', 'Linear')

        # Realizado
        fat_real = 0
        if not df_vendas.empty:
            v = df_vendas[df_vendas['loja_origem'] == loja]
            if not v.empty:
                fat_real = float(v['fat_realizado'].sum())

        # Devoluções
        fat_dev = 0
        if not df_dev.empty:
            d = df_dev[df_dev['loja_origem'] == loja]
            if not d.empty:
                fat_dev = float(d['fat_dev'].sum())

        fat_liquido = fat_real - fat_dev

        # Projeção
        if dias_vendas > 0 and fat_liquido > 0:
            proj = calcular_projecao(fat_liquido, dias_vendas, dias_mes, modelo)
        else:
            proj = 0

        perf = calcular_performance(proj, meta_receita)

        rows.append({
            'Loja': loja,
            'Marketplace': mktp,
            'Meta': meta_receita,
            'Realizado': round(fat_liquido, 2),
            'Projeção': round(proj, 2),
            'Modelo': modelo,
            '⭐ Performance': round(perf, 1) if perf else None,
        })

    df_geral = pd.DataFrame(rows)

    if not df_geral.empty:
        # Estilizar
        col_config = {
            'Meta': st.column_config.NumberColumn(format="R$ %.2f"),
            'Realizado': st.column_config.NumberColumn(format="R$ %.2f"),
            'Projeção': st.column_config.NumberColumn(format="R$ %.2f"),
            '⭐ Performance': st.column_config.NumberColumn(format="%.1f%%"),
        }
        st.dataframe(df_geral, column_config=col_config,
                      hide_index=True, use_container_width=True)

        # Totais
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
    st.caption("v1.4 — vendas mês anterior no template + tabela")

    engine = get_engine()
    ano_mes = _seletor_mes()

    # Info de dias
    dias_vendas, dias_mes = get_dias_vendas(ano_mes)
    st.caption(f"📅 Dia {dias_vendas} de {dias_mes} — {dias_mes - dias_vendas} dias restantes")

    # Tabs
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
