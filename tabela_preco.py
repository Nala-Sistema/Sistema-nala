"""
tabela_preco.py — Módulo Tabela de Preço (Projeto Nala) v3.0
Grade de precificação estratégica por marketplace.
Tabs: ML | Shopee | Amazon | Shein | Magalu | B2B
"""

import streamlit as st
import pandas as pd
import numpy as np
import io
from sqlalchemy import text
from datetime import datetime, timedelta
from database_utils import get_engine

# ============================================================
# CONSTANTES
# ============================================================

TABS_ORDER = ["Mercado Livre", "Shopee", "Amazon", "Shein", "Magalu", "B2B"]
ML_LOJA_ORDER = ["ML-Nala", "ML-LPT", "ML-YanniRJ", "ML-YanniSP"]
PERFIS_COM_CUSTO = ["ADMIN", "CONTROLADORIA", "DIRETOR", "COMPRAS"]
EDITOR_HEIGHT = 595  # ~16 rows

# ============================================================
# CARGA DE DADOS
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def carregar_produtos(_engine):
    query = text("""
        SELECT p.sku, p.nome, p.categoria, p.status,
               COALESCE(p.preco_a_ser_considerado, 0) AS custo_sku,
               p.margem_minima, p.margem_desejavel,
               p.largura, p.comprimento, p.altura, p.peso_bruto
        FROM dim_produtos p
        WHERE p.status = 'Ativo'
        ORDER BY p.nome
    """)
    with _engine.connect() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=300, show_spinner=False)
def carregar_lojas(_engine):
    query = text("SELECT id, marketplace, loja, imposto, custo_flex FROM dim_lojas")
    with _engine.connect() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=300, show_spinner=False)
def carregar_precos_salvos(_engine, marketplace):
    query = text("""
        SELECT sku, loja, logistica, preco_venda,
               comissao_percentual_override, frete_override, taxa_fixa_override
        FROM dim_precos_marketplace
        WHERE LOWER(marketplace) = LOWER(:mkt)
    """)
    with _engine.connect() as conn:
        return pd.read_sql(query, conn, params={"mkt": marketplace})


@st.cache_data(ttl=300, show_spinner=False)
def carregar_frete_ml(_engine):
    query = text("SELECT * FROM dim_frete_ml ORDER BY tipo, faixa_peso_min_kg, faixa_preco_min")
    with _engine.connect() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=120, show_spinner=False)
def carregar_vendas_30d(_engine, lojas_tuple):
    """Vendas 30d por SKU+loja, filtrando pelas lojas passadas."""
    if not lojas_tuple:
        return pd.DataFrame(columns=['sku', 'loja', 'qtd_vendas_30d', 'margem_real_30d'])
    data_corte = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    params = {"data_corte": data_corte}
    placeholders = []
    for i, loja in enumerate(lojas_tuple):
        params[f"lj{i}"] = loja
        placeholders.append(f":lj{i}")
    ph = ", ".join(placeholders)
    query = text(f"""
        SELECT sku, loja_origem AS loja,
               COUNT(*) AS qtd_vendas_30d,
               AVG(margem_percentual) AS margem_real_30d
        FROM fact_vendas_snapshot
        WHERE loja_origem IN ({ph})
          AND data_venda >= :data_corte
        GROUP BY sku, loja_origem
    """)
    with _engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


@st.cache_data(ttl=300, show_spinner=False)
def carregar_lojas_gestor(_engine, username):
    query = text("""
        SELECT dl.loja, dl.marketplace
        FROM dim_usuario_lojas dul
        JOIN dim_lojas dl ON dul.id_loja = dl.id
        JOIN dim_usuarios du ON dul.id_usuario = du.id_usuario
        WHERE LOWER(du.username) = LOWER(:usr)
    """)
    with _engine.connect() as conn:
        return pd.read_sql(query, conn, params={"usr": username})


# ============================================================
# CÁLCULOS
# ============================================================

def peso_efetivo(largura, comprimento, altura, peso_bruto):
    if pd.isna(largura) or pd.isna(comprimento) or pd.isna(altura):
        return peso_bruto if not pd.isna(peso_bruto) else None
    cubado = (largura * comprimento * altura) / 6000
    real = peso_bruto if not pd.isna(peso_bruto) else 0
    return max(real, cubado)


def buscar_frete_ml(df_frete, peso_kg, preco_venda, tipo='envio_padrao'):
    if peso_kg is None or preco_venda is None or preco_venda <= 0 or df_frete.empty:
        return None
    f = df_frete[
        (df_frete['tipo'] == tipo) &
        (df_frete['faixa_peso_min_kg'] <= peso_kg) &
        (df_frete['faixa_peso_max_kg'] > peso_kg) &
        (df_frete['faixa_preco_min'] <= preco_venda) &
        (df_frete['faixa_preco_max'] >= preco_venda)
    ]
    if len(f) > 0:
        custo = float(f.iloc[0]['custo_envio'])
        if preco_venda < 19:
            custo = min(custo, preco_venda * 0.5)
        return custo
    return None


def calcular_margem(preco, custo_sku, comissao_pct, taxa_frete, imposto_pct, extra=0):
    """
    preco: valor de venda
    comissao_pct: em decimal (0.165 = 16.5%)
    imposto_pct: em decimal (0.10 = 10%)
    """
    if not preco or preco <= 0:
        return None, None
    comissao = preco * comissao_pct
    imposto = preco * imposto_pct
    subtotal = comissao + imposto + (taxa_frete or 0) + (custo_sku or 0) + extra
    margem_abs = preco - subtotal
    margem_pct = (margem_abs / preco) * 100
    return round(margem_abs, 2), round(margem_pct, 2)


def preco_sugerido(custo_sku, comissao_pct, taxa_frete, imposto_pct, margem_alvo_pct, extra=0):
    divisor = 1 - comissao_pct - imposto_pct - (margem_alvo_pct / 100)
    if divisor <= 0:
        return None
    num = (taxa_frete or 0) + (custo_sku or 0) + extra
    return round(num / divisor, 2)


def classificar_tag(qtd):
    vendas = qtd or 0
    if vendas >= 200:
        return "⭐ Top Seller"
    elif vendas >= 100:
        return "🔥 Escala"
    elif vendas >= 50:
        return "📈 Tração"
    else:
        return "⚠️ Atenção"


def semaforo(margem_pct, mc_min, mc_des):
    if margem_pct is None:
        return "⚪"
    mm = (mc_min or 0) * 100 if mc_min and mc_min < 1 else (mc_min or 0)
    md = (mc_des or 0) * 100 if mc_des and mc_des < 1 else (mc_des or 0)
    if margem_pct < mm:
        return "🔴"
    elif margem_pct < md:
        return "🟡"
    else:
        return "🟢"


def normalizar_margem(val):
    """Converte margem stored (pode ser 0.18 ou 18) para pct inteiro."""
    if val is None or pd.isna(val):
        return 0
    return val * 100 if val < 1 else val


# ============================================================
# SALVAR PREÇOS
# ============================================================

def salvar_precos(engine, rows_list, marketplace, loja, logistica, usuario):
    count = 0
    with engine.connect() as conn:
        for row in rows_list:
            preco = row.get('preco_venda')
            if preco is None or pd.isna(preco) or preco <= 0:
                continue
            conn.execute(text("""
                INSERT INTO dim_precos_marketplace
                    (sku, marketplace, loja, logistica, preco_venda,
                     comissao_percentual_override, frete_override, taxa_fixa_override,
                     updated_at, updated_by)
                VALUES (:sku, :mkt, :loja, :log, :preco, :com, :frete, :taxa, NOW(), :usr)
                ON CONFLICT (sku, marketplace, loja, logistica)
                DO UPDATE SET preco_venda=EXCLUDED.preco_venda,
                    comissao_percentual_override=EXCLUDED.comissao_percentual_override,
                    frete_override=EXCLUDED.frete_override,
                    taxa_fixa_override=EXCLUDED.taxa_fixa_override,
                    updated_at=NOW(), updated_by=EXCLUDED.updated_by
            """), {
                "sku": row['sku'], "mkt": marketplace, "loja": loja,
                "log": logistica, "preco": float(preco),
                "com": float(row['comissao']) / 100 if row.get('comissao') else None,
                "frete": float(row['frete']) if row.get('frete') else None,
                "taxa": float(row['taxa']) if row.get('taxa') else None,
                "usr": usuario,
            })
            count += 1
        conn.commit()
    return count


# ============================================================
# DOWNLOAD / UPLOAD XLSX
# ============================================================

def gerar_template_xlsx(df_produtos, colunas_preco, titulo):
    """Gera template XLSX para download com SKUs ativos."""
    df = df_produtos[['sku', 'nome', 'categoria']].copy()
    for col in colunas_preco:
        df[col] = None
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name=titulo[:31])
    return buf.getvalue()


def processar_upload_xlsx(arquivo, colunas_esperadas):
    """Lê XLSX uploadado e valida colunas."""
    try:
        df = pd.read_excel(arquivo)
        faltantes = [c for c in colunas_esperadas if c not in df.columns]
        if faltantes:
            return None, f"Colunas faltantes: {', '.join(faltantes)}"
        return df, None
    except Exception as e:
        return None, str(e)


def botao_download_xlsx(df_display, key, filename):
    """Adiciona botão de download XLSX para a tabela exibida."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df_display.to_excel(w, index=False, sheet_name='Dados')
    st.download_button(
        "📥 Baixar XLSX", data=buf.getvalue(),
        file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key
    )


# ============================================================
# TAB MERCADO LIVRE — Tabela Unificada Premium + Clássico
# ============================================================

def render_tab_ml(engine, perfil, usuario):
    st.subheader("📦 Mercado Livre")

    df_lojas = carregar_lojas(engine)
    ml_lojas = df_lojas[df_lojas['marketplace'].str.upper().str.contains('MERCADO')].copy()

    if perfil == "GESTOR":
        gestor_lojas = carregar_lojas_gestor(engine, usuario)
        permitidas = gestor_lojas[gestor_lojas['marketplace'].str.upper().str.contains('MERCADO')]['loja'].tolist()
        ml_lojas = ml_lojas[ml_lojas['loja'].isin(permitidas)]

    if ml_lojas.empty:
        st.warning("Nenhuma loja ML encontrada ou sem permissão.")
        return

    # Ordenar lojas
    ml_lojas['_order'] = ml_lojas['loja'].apply(
        lambda x: ML_LOJA_ORDER.index(x) if x in ML_LOJA_ORDER else 99
    )
    ml_lojas = ml_lojas.sort_values('_order')
    lojas_list = ml_lojas['loja'].tolist()

    # Frete ML
    try:
        df_frete = carregar_frete_ml(engine)
    except Exception:
        df_frete = pd.DataFrame()

    # Dados
    df_prod = carregar_produtos(engine)
    df_vendas = carregar_vendas_30d(engine, tuple(lojas_list))
    df_precos = carregar_precos_salvos(engine, "Mercado Livre")

    sub_tabs = st.tabs(lojas_list)

    for idx, loja in enumerate(lojas_list):
        with sub_tabs[idx]:
            loja_info = ml_lojas[ml_lojas['loja'] == loja].iloc[0]
            imposto_raw = float(loja_info.get('imposto', 0) or 0)
            imposto_dec = imposto_raw / 100 if imposto_raw > 1 else imposto_raw
            extra = 6.0 if 'yannisp' in loja.lower().replace('-', '').replace(' ', '') else 0.0

            # Toggles
            c1, c2, c3 = st.columns(3)
            show_class = c1.checkbox("Clássico (11,5%)", True, key=f"ml_cl_{loja}")
            show_prem = c2.checkbox("Premium (16,5%)", True, key=f"ml_pr_{loja}")
            show_sug = c3.checkbox("💡 Sugeridos", False, key=f"ml_sg_{loja}")

            st.caption(
                f"Imposto {imposto_raw:.0f}% | Taxa fixa padrão R$6,75 (≤R$78,99) | "
                f"≥R$79 = frete obrigatório pela tabela ML"
                f"{' | +R$6,00 Yanni SP' if extra > 0 else ''}"
            )

            # Montar dados
            rows = []
            for _, p in df_prod.iterrows():
                sku = p['sku']
                custo = p.get('custo_sku', 0) or 0
                mc_min = normalizar_margem(p.get('margem_minima'))
                mc_des = normalizar_margem(p.get('margem_desejavel'))
                peso = peso_efetivo(p.get('largura'), p.get('comprimento'), p.get('altura'), p.get('peso_bruto'))

                # Vendas 30d
                v = df_vendas[(df_vendas['sku'] == sku) & (df_vendas['loja'] == loja)]
                qtd_v = int(v['qtd_vendas_30d'].sum()) if not v.empty else 0
                mr = round(float(v['margem_real_30d'].mean()), 1) if not v.empty and v['margem_real_30d'].notna().any() else None

                # Preços salvos
                def get_salvo(log):
                    s = df_precos[(df_precos['sku'] == sku) & (df_precos['loja'] == loja) & (df_precos['logistica'] == log)]
                    if s.empty:
                        return {}
                    r = s.iloc[0]
                    return {
                        'preco': r.get('preco_venda'),
                        'com': r.get('comissao_percentual_override'),
                        'frete': r.get('frete_override'),
                        'taxa': r.get('taxa_fixa_override'),
                    }

                sv_cl = get_salvo("Clássico")
                sv_pr = get_salvo("Premium")

                row = {
                    'sku': sku, 'produto': p['nome'], 'categoria': p['categoria'],
                    'tag': classificar_tag(qtd_v),
                    'real_30d': mr,
                    'custo': custo, 'mc_esp': mc_des, 'mc_min': mc_min,
                    'peso_kg': round(peso, 2) if peso else None,
                }

                # Clássico
                pc = sv_cl.get('preco')
                com_cl = (sv_cl.get('com') or 0) * 100 if sv_cl.get('com') else 11.5
                # Taxa/Frete: se preço > 78.99 → frete, senão → 6.75
                if pc and pc > 78.99:
                    tf_cl = sv_cl.get('frete') or sv_cl.get('taxa')
                    if tf_cl is None and peso:
                        tf_cl = buscar_frete_ml(df_frete, peso, pc)
                    tf_cl = tf_cl or 0
                else:
                    tf_cl = sv_cl.get('taxa') or 6.75

                ma_cl, mp_cl = calcular_margem(pc, custo, com_cl / 100, tf_cl, imposto_dec, extra)
                row['preco_cl'] = pc
                row['com_cl'] = round(com_cl, 1)
                row['tf_cl'] = round(tf_cl, 2) if tf_cl else 6.75
                row['mg_cl'] = mp_cl
                row['mg_abs_cl'] = ma_cl

                # Premium
                pp = sv_pr.get('preco')
                com_pr = (sv_pr.get('com') or 0) * 100 if sv_pr.get('com') else 16.5
                if pp and pp > 78.99:
                    tf_pr = sv_pr.get('frete') or sv_pr.get('taxa')
                    if tf_pr is None and peso:
                        tf_pr = buscar_frete_ml(df_frete, peso, pp)
                    tf_pr = tf_pr or 0
                else:
                    tf_pr = sv_pr.get('taxa') or 6.75

                ma_pr, mp_pr = calcular_margem(pp, custo, com_pr / 100, tf_pr, imposto_dec, extra)
                row['preco_pr'] = pp
                row['com_pr'] = round(com_pr, 1)
                row['tf_pr'] = round(tf_pr, 2) if tf_pr else 6.75
                row['mg_pr'] = mp_pr
                row['mg_abs_pr'] = ma_pr

                # Semáforo baseado no Clássico
                row['sinal'] = semaforo(mp_cl, p.get('margem_minima'), p.get('margem_desejavel'))

                # Sugeridos (baseados no Clássico)
                if show_sug:
                    row['sug_min'] = preco_sugerido(custo, 0.115, 6.75, imposto_dec, mc_min, extra)
                    row['sug_esp'] = preco_sugerido(custo, 0.115, 6.75, imposto_dec, mc_des, extra)

                rows.append(row)

            df = pd.DataFrame(rows)
            if df.empty:
                st.info("Nenhum produto ativo.")
                continue

            # Com preço: pelo menos um dos dois preenchido
            com_preco = ((df['preco_cl'].notna() & (df['preco_cl'] > 0)) |
                         (df['preco_pr'].notna() & (df['preco_pr'] > 0))).sum()

            # Column config
            cc = {
                'sinal': st.column_config.TextColumn("🚦", width="tiny", disabled=True,
                         help="Semáforo baseado na margem Clássico"),
                'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
                'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                'tag': st.column_config.TextColumn("🏷️", disabled=True, width="small"),
                'real_30d': st.column_config.NumberColumn("Real 30d", format="%.1f%%", disabled=True, width="small"),
                'peso_kg': st.column_config.NumberColumn("Peso kg", format="%.2f", disabled=True, width="small"),
            }

            col_order = ['sinal', 'sku', 'produto', 'categoria', 'tag', 'real_30d']

            if perfil in PERFIS_COM_CUSTO:
                cc['custo'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
                cc['mc_esp'] = st.column_config.NumberColumn("MC Esp%", format="%.0f%%", disabled=True, width="tiny")
                cc['mc_min'] = st.column_config.NumberColumn("MC Mín%", format="%.0f%%", disabled=True, width="tiny")
                col_order += ['custo', 'mc_esp', 'mc_min']

            col_order.append('peso_kg')

            if show_sug:
                cc['sug_min'] = st.column_config.NumberColumn("💡 Mínimo", format="R$ %.2f", disabled=True, width="small")
                cc['sug_esp'] = st.column_config.NumberColumn("💡 Esperado", format="R$ %.2f", disabled=True, width="small")
                col_order += ['sug_min', 'sug_esp']

            if show_class:
                cc['preco_cl'] = st.column_config.NumberColumn("🟠 Preço Class.", format="R$ %.2f", min_value=0, width="small")
                cc['com_cl'] = st.column_config.NumberColumn("Com.Cl %", format="%.1f%%", min_value=0, width="tiny")
                cc['tf_cl'] = st.column_config.NumberColumn("Taxa/Frete Cl", format="R$ %.2f", min_value=0, width="small")
                cc['mg_cl'] = st.column_config.NumberColumn("Mg.Cl %", format="%.1f%%", disabled=True, width="small")
                col_order += ['preco_cl', 'com_cl', 'tf_cl', 'mg_cl']
                if perfil in PERFIS_COM_CUSTO:
                    cc['mg_abs_cl'] = st.column_config.NumberColumn("Mg.Cl R$", format="R$ %.2f", disabled=True, width="small")
                    col_order.append('mg_abs_cl')

            if show_prem:
                cc['preco_pr'] = st.column_config.NumberColumn("🟠 Preço Prem.", format="R$ %.2f", min_value=0, width="small")
                cc['com_pr'] = st.column_config.NumberColumn("Com.Pr %", format="%.1f%%", min_value=0, width="tiny")
                cc['tf_pr'] = st.column_config.NumberColumn("Taxa/Frete Pr", format="R$ %.2f", min_value=0, width="small")
                cc['mg_pr'] = st.column_config.NumberColumn("Mg.Pr %", format="%.1f%%", disabled=True, width="small")
                col_order += ['preco_pr', 'com_pr', 'tf_pr', 'mg_pr']
                if perfil in PERFIS_COM_CUSTO:
                    cc['mg_abs_pr'] = st.column_config.NumberColumn("Mg.Pr R$", format="R$ %.2f", disabled=True, width="small")
                    col_order.append('mg_abs_pr')

            # Ocultar colunas não usadas
            for c in df.columns:
                if c not in col_order and c not in cc:
                    cc[c] = None

            edited = st.data_editor(
                df, column_config=cc, column_order=col_order,
                use_container_width=True, hide_index=True, num_rows="fixed",
                height=EDITOR_HEIGHT, key=f"ed_ml_{loja}"
            )

            # Métricas
            mg_media = edited['mg_cl'].mean() if edited['mg_cl'].notna().any() else 0
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("SKUs", len(edited))
            c2.metric("Com preço", int(com_preco))
            c3.metric("Margem média", f"{mg_media:.1f}%")
            c4.metric("🟢", (edited['sinal'] == '🟢').sum())
            c5.metric("🟡", (edited['sinal'] == '🟡').sum())
            c6.metric("🔴", (edited['sinal'] == '🔴').sum())

            # Botões
            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                if st.button(f"💾 Salvar preços — {loja}", key=f"sv_ml_{loja}"):
                    saves = []
                    for _, r in edited.iterrows():
                        if show_class and r.get('preco_cl') and r['preco_cl'] > 0:
                            saves.append({'sku': r['sku'], 'preco_venda': r['preco_cl'],
                                         'comissao': r.get('com_cl', 11.5), 'frete': r.get('tf_cl'),
                                         'taxa': r.get('tf_cl')})
                        # Salvar Clássico
                    cnt_cl = salvar_precos(engine, saves, "Mercado Livre", loja, "Clássico", usuario) if saves else 0
                    saves_pr = []
                    for _, r in edited.iterrows():
                        if show_prem and r.get('preco_pr') and r['preco_pr'] > 0:
                            saves_pr.append({'sku': r['sku'], 'preco_venda': r['preco_pr'],
                                            'comissao': r.get('com_pr', 16.5), 'frete': r.get('tf_pr'),
                                            'taxa': r.get('tf_pr')})
                    cnt_pr = salvar_precos(engine, saves_pr, "Mercado Livre", loja, "Premium", usuario) if saves_pr else 0
                    total = cnt_cl + cnt_pr
                    if total > 0:
                        st.success(f"✅ {total} preços salvos!")
                        carregar_precos_salvos.clear()
                    else:
                        st.warning("Nenhum preço válido para salvar.")

            with bc2:
                botao_download_xlsx(edited[col_order], f"dl_ml_{loja}", f"tabela_preco_ML_{loja}.xlsx")

            with bc3:
                _upload_precos_widget(engine, df_prod, "Mercado Livre", loja,
                                     ["preco_venda", "comissao_pct", "taxa_fixa", "frete"],
                                     f"up_ml_{loja}", usuario)


# ============================================================
# TAB SHOPEE — Sub-tabs por loja, regra escalonada
# ============================================================

def render_tab_shopee(engine, perfil, usuario):
    st.subheader("🛒 Shopee")

    df_lojas = carregar_lojas(engine)
    sp_lojas = df_lojas[df_lojas['marketplace'].str.upper().str.contains('SHOPEE')]

    if perfil == "GESTOR":
        gestor_lojas = carregar_lojas_gestor(engine, usuario)
        permitidas = gestor_lojas[gestor_lojas['marketplace'].str.upper().str.contains('SHOPEE')]['loja'].tolist()
        sp_lojas = sp_lojas[sp_lojas['loja'].isin(permitidas)]

    if sp_lojas.empty:
        st.warning("Nenhuma loja Shopee ou sem permissão.")
        return

    st.caption("Regras: ≤R$79,99 → 20%+R$4 | R$80-99,99 → 14%+R$16 | R$100-199,99 → 14%+R$20 | >R$199,99 → 14%+R$26")

    lojas_list = sp_lojas['loja'].tolist()
    df_prod = carregar_produtos(engine)
    df_vendas = carregar_vendas_30d(engine, tuple(lojas_list))
    df_precos = carregar_precos_salvos(engine, "Shopee")

    sub_tabs = st.tabs(lojas_list)
    for idx, loja in enumerate(lojas_list):
        with sub_tabs[idx]:
            loja_info = sp_lojas[sp_lojas['loja'] == loja].iloc[0]
            imp_raw = float(loja_info.get('imposto', 0) or 0)
            imp_dec = imp_raw / 100 if imp_raw > 1 else imp_raw

            show_sug = st.checkbox("💡 Sugeridos", False, key=f"sp_sg_{loja}")

            rows = []
            for _, p in df_prod.iterrows():
                sku = p['sku']
                custo = p.get('custo_sku', 0) or 0
                mc_min = normalizar_margem(p.get('margem_minima'))
                mc_des = normalizar_margem(p.get('margem_desejavel'))

                v = df_vendas[(df_vendas['sku'] == sku) & (df_vendas['loja'] == loja)]
                qtd_v = int(v['qtd_vendas_30d'].sum()) if not v.empty else 0
                mr = round(float(v['margem_real_30d'].mean()), 1) if not v.empty and v['margem_real_30d'].notna().any() else None

                sv = df_precos[(df_precos['sku'] == sku) & (df_precos['loja'] == loja)]
                preco = float(sv.iloc[0]['preco_venda']) if not sv.empty and pd.notna(sv.iloc[0]['preco_venda']) else None

                # Regra escalonada
                if preco and preco > 0:
                    if preco <= 79.99:
                        com, taxa = 20.0, 4.0
                    elif preco <= 99.99:
                        com, taxa = 14.0, 16.0
                    elif preco <= 199.99:
                        com, taxa = 14.0, 20.0
                    else:
                        com, taxa = 14.0, 26.0
                else:
                    com, taxa = 20.0, 4.0

                ma, mp = calcular_margem(preco, custo, com / 100, taxa, imp_dec)
                sinal = semaforo(mp, p.get('margem_minima'), p.get('margem_desejavel'))

                row = {
                    'sinal': sinal, 'sku': sku, 'produto': p['nome'], 'categoria': p['categoria'],
                    'tag': classificar_tag(qtd_v), 'real_30d': mr,
                    'custo': custo, 'mc_esp': mc_des, 'mc_min': mc_min,
                    'preco_venda': preco, 'com_pct': com, 'taxa_fixa': taxa,
                    'mg_pct': mp, 'mg_abs': ma,
                }
                if show_sug:
                    row['sug_min'] = preco_sugerido(custo, 0.20, 4.0, imp_dec, mc_min)
                    row['sug_esp'] = preco_sugerido(custo, 0.20, 4.0, imp_dec, mc_des)
                rows.append(row)

            df = pd.DataFrame(rows)
            cc = {
                'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
                'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
                'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                'tag': st.column_config.TextColumn("🏷️", disabled=True, width="small"),
                'real_30d': st.column_config.NumberColumn("Real 30d", format="%.1f%%", disabled=True, width="small"),
                'preco_venda': st.column_config.NumberColumn("🟠 Preço", format="R$ %.2f", min_value=0, width="small"),
                'com_pct': st.column_config.NumberColumn("Com. %", format="%.0f%%", disabled=True, width="tiny"),
                'taxa_fixa': st.column_config.NumberColumn("Taxa R$", format="R$ %.2f", disabled=True, width="tiny"),
                'mg_pct': st.column_config.NumberColumn("Margem %", format="%.1f%%", disabled=True, width="small"),
                'mg_abs': st.column_config.NumberColumn("Margem R$", format="R$ %.2f", disabled=True, width="small"),
            }
            col_order = ['sinal', 'sku', 'produto', 'categoria', 'tag', 'real_30d']
            if perfil in PERFIS_COM_CUSTO:
                cc['custo'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
                cc['mc_esp'] = st.column_config.NumberColumn("MC Esp%", format="%.0f%%", disabled=True, width="tiny")
                cc['mc_min'] = st.column_config.NumberColumn("MC Mín%", format="%.0f%%", disabled=True, width="tiny")
                col_order += ['custo', 'mc_esp', 'mc_min']
            if show_sug:
                cc['sug_min'] = st.column_config.NumberColumn("💡 Mín", format="R$ %.2f", disabled=True, width="small")
                cc['sug_esp'] = st.column_config.NumberColumn("💡 Esp", format="R$ %.2f", disabled=True, width="small")
                col_order += ['sug_min', 'sug_esp']
            col_order += ['preco_venda', 'com_pct', 'taxa_fixa', 'mg_pct']
            if perfil in PERFIS_COM_CUSTO:
                col_order.append('mg_abs')

            edited = st.data_editor(df, column_config=cc, column_order=col_order,
                                     use_container_width=True, hide_index=True,
                                     num_rows="fixed", height=EDITOR_HEIGHT, key=f"ed_sp_{loja}")

            bc1, bc2 = st.columns(2)
            with bc1:
                if st.button(f"💾 Salvar — {loja}", key=f"sv_sp_{loja}"):
                    saves = [{'sku': r['sku'], 'preco_venda': r['preco_venda'],
                              'comissao': None, 'frete': None, 'taxa': None}
                             for _, r in edited.iterrows() if r.get('preco_venda') and r['preco_venda'] > 0]
                    cnt = salvar_precos(engine, saves, "Shopee", loja, "Shopee", usuario)
                    if cnt > 0:
                        st.success(f"✅ {cnt} preços salvos!")
                        carregar_precos_salvos.clear()
            with bc2:
                botao_download_xlsx(edited[col_order], f"dl_sp_{loja}", f"tabela_preco_Shopee_{loja}.xlsx")


# ============================================================
# TAB AMAZON — Placeholder (adiada)
# ============================================================

def render_tab_amazon(engine, perfil, usuario):
    st.subheader("📦 Amazon")
    st.info("🔧 Módulo Amazon em desenvolvimento. A tabela Amazon usa ASINs (não SKUs) "
            "e requer estrutura específica. Será implementado em fase posterior.")


# ============================================================
# TAB GENÉRICA — Shein, Magalu
# ============================================================

def render_tab_generica(engine, perfil, usuario, marketplace, logisticas_config):
    """
    Renderiza tab para Shein ou Magalu.
    logisticas_config: list of dicts com {nome, comissao_default, taxa_default, label}
    """
    df_lojas = carregar_lojas(engine)
    mkt_lojas = df_lojas[df_lojas['marketplace'].str.upper().str.contains(marketplace.upper())]

    if perfil == "GESTOR":
        gestor_lojas = carregar_lojas_gestor(engine, usuario)
        permitidas = gestor_lojas[gestor_lojas['marketplace'].str.upper().str.contains(marketplace.upper())]['loja'].tolist()
        mkt_lojas = mkt_lojas[mkt_lojas['loja'].isin(permitidas)]

    if mkt_lojas.empty:
        st.warning(f"Nenhuma loja {marketplace} ou sem permissão.")
        return

    lojas_list = mkt_lojas['loja'].tolist()
    df_prod = carregar_produtos(engine)
    df_vendas = carregar_vendas_30d(engine, tuple(lojas_list))
    df_precos = carregar_precos_salvos(engine, marketplace)

    sub_tabs = st.tabs(lojas_list)
    for idx, loja in enumerate(lojas_list):
        with sub_tabs[idx]:
            loja_info = mkt_lojas[mkt_lojas['loja'] == loja].iloc[0]
            imp_raw = float(loja_info.get('imposto', 0) or 0)
            imp_dec = imp_raw / 100 if imp_raw > 1 else imp_raw

            # Logísticas como toggles
            for lconf in logisticas_config:
                show = st.checkbox(lconf['label'], lconf.get('default_on', True),
                                   key=f"{marketplace}_{lconf['nome']}_{loja}")
                if not show:
                    continue

                st.markdown(f"##### {lconf['label']}")
                com_def = lconf['comissao_default']
                taxa_def = lconf['taxa_default']

                rows = []
                for _, p in df_prod.iterrows():
                    sku = p['sku']
                    custo = p.get('custo_sku', 0) or 0
                    mc_min = normalizar_margem(p.get('margem_minima'))
                    mc_des = normalizar_margem(p.get('margem_desejavel'))

                    v = df_vendas[(df_vendas['sku'] == sku) & (df_vendas['loja'] == loja)]
                    qtd_v = int(v['qtd_vendas_30d'].sum()) if not v.empty else 0
                    mr = round(float(v['margem_real_30d'].mean()), 1) if not v.empty and v['margem_real_30d'].notna().any() else None

                    sv = df_precos[(df_precos['sku'] == sku) & (df_precos['loja'] == loja) &
                                   (df_precos['logistica'] == lconf['nome'])]
                    preco = float(sv.iloc[0]['preco_venda']) if not sv.empty and pd.notna(sv.iloc[0]['preco_venda']) else None
                    com_ov = sv.iloc[0].get('comissao_percentual_override') if not sv.empty else None
                    com = (com_ov * 100) if com_ov and pd.notna(com_ov) else com_def
                    taxa = sv.iloc[0].get('taxa_fixa_override') if not sv.empty and pd.notna(sv.iloc[0].get('taxa_fixa_override')) else taxa_def

                    ma, mp = calcular_margem(preco, custo, com / 100, taxa, imp_dec)
                    sinal = semaforo(mp, p.get('margem_minima'), p.get('margem_desejavel'))

                    rows.append({
                        'sinal': sinal, 'sku': sku, 'produto': p['nome'], 'categoria': p['categoria'],
                        'tag': classificar_tag(qtd_v), 'real_30d': mr,
                        'custo': custo, 'mc_esp': mc_des, 'mc_min': mc_min,
                        'preco_venda': preco, 'com_pct': round(com, 1), 'taxa_fixa': round(taxa, 2),
                        'mg_pct': mp, 'mg_abs': ma,
                    })

                df = pd.DataFrame(rows)
                cc = {
                    'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
                    'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
                    'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                    'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                    'tag': st.column_config.TextColumn("🏷️", disabled=True, width="small"),
                    'real_30d': st.column_config.NumberColumn("Real 30d", format="%.1f%%", disabled=True, width="small"),
                    'preco_venda': st.column_config.NumberColumn("🟠 Preço", format="R$ %.2f", min_value=0, width="small"),
                    'com_pct': st.column_config.NumberColumn("Com. %", format="%.1f%%", min_value=0, width="tiny"),
                    'taxa_fixa': st.column_config.NumberColumn("Taxa R$", format="R$ %.2f", min_value=0, width="small"),
                    'mg_pct': st.column_config.NumberColumn("Margem %", format="%.1f%%", disabled=True, width="small"),
                    'mg_abs': st.column_config.NumberColumn("Margem R$", format="R$ %.2f", disabled=True, width="small"),
                }
                col_order = ['sinal', 'sku', 'produto', 'categoria', 'tag', 'real_30d']
                if perfil in PERFIS_COM_CUSTO:
                    cc['custo'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
                    cc['mc_esp'] = st.column_config.NumberColumn("MC Esp%", format="%.0f%%", disabled=True, width="tiny")
                    cc['mc_min'] = st.column_config.NumberColumn("MC Mín%", format="%.0f%%", disabled=True, width="tiny")
                    col_order += ['custo', 'mc_esp', 'mc_min']
                col_order += ['preco_venda', 'com_pct', 'taxa_fixa', 'mg_pct']
                if perfil in PERFIS_COM_CUSTO:
                    col_order.append('mg_abs')

                edited = st.data_editor(df, column_config=cc, column_order=col_order,
                                         use_container_width=True, hide_index=True,
                                         num_rows="fixed", height=EDITOR_HEIGHT,
                                         key=f"ed_{marketplace}_{lconf['nome']}_{loja}")

                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button(f"💾 Salvar — {loja}/{lconf['nome']}", key=f"sv_{marketplace}_{lconf['nome']}_{loja}"):
                        saves = [{'sku': r['sku'], 'preco_venda': r['preco_venda'],
                                  'comissao': r.get('com_pct'), 'frete': None, 'taxa': r.get('taxa_fixa')}
                                 for _, r in edited.iterrows() if r.get('preco_venda') and r['preco_venda'] > 0]
                        cnt = salvar_precos(engine, saves, marketplace, loja, lconf['nome'], usuario)
                        if cnt > 0:
                            st.success(f"✅ {cnt} preços salvos!")
                            carregar_precos_salvos.clear()
                with bc2:
                    botao_download_xlsx(edited[col_order], f"dl_{marketplace}_{lconf['nome']}_{loja}",
                                       f"tabela_{marketplace}_{loja}_{lconf['nome']}.xlsx")


def render_tab_shein(engine, perfil, usuario):
    st.subheader("👗 Shein")
    st.caption("Comissão 16% | Taxa fixa editável (R$5 a R$15)")
    render_tab_generica(engine, perfil, usuario, "Shein", [
        {"nome": "Normal", "comissao_default": 16.0, "taxa_default": 5.0, "label": "Normal (16%+R$5)", "default_on": True},
        {"nome": "FULL", "comissao_default": 16.0, "taxa_default": 6.0, "label": "FULL (16%+R$6)", "default_on": False},
    ])


def render_tab_magalu(engine, perfil, usuario):
    st.subheader("🟦 Magalu")
    st.caption("Comissão 14,8% | Taxa fixa R$5 | Frete >R$79 → manual")
    render_tab_generica(engine, perfil, usuario, "Magalu", [
        {"nome": "Loja", "comissao_default": 14.8, "taxa_default": 5.0, "label": "Expedição Própria (14,8%+R$5)", "default_on": True},
        {"nome": "Fulfillment", "comissao_default": 14.8, "taxa_default": 5.0, "label": "Fulfillment (14,8%+R$5)", "default_on": False},
    ])


# ============================================================
# TAB B2B
# ============================================================

def render_tab_b2b(engine, perfil, usuario):
    st.subheader("🏢 B2B — Venda Direta")
    st.caption("Desconto máx: >R$300 → 4% | R$301-1000 → 7% | >R$1000 → 10%")

    df_prod = carregar_produtos(engine)
    df_precos = carregar_precos_salvos(engine, "B2B")

    cenarios = [
        {"nome": "PIX sem NF", "markup": 30, "comissao": 2.0, "maquina": 0.0, "imposto": 0.0},
        {"nome": "PIX com NF", "markup": 48, "comissao": 2.0, "maquina": 0.0, "imposto": 2.0},
        {"nome": "Cartão 3x sem NF", "markup": 48, "comissao": 2.0, "maquina": 10.0, "imposto": 0.0},
        {"nome": "Cartão 3x com NF", "markup": 70, "comissao": 2.0, "maquina": 10.0, "imposto": 10.0},
    ]

    cenario_tabs = st.tabs([c["nome"] for c in cenarios])
    for ci, cen in enumerate(cenarios):
        with cenario_tabs[ci]:
            simular = st.toggle("🔄 Simular outro preço", key=f"b2b_sim_{ci}")

            rows = []
            for _, p in df_prod.iterrows():
                sku = p['sku']
                custo = p.get('custo_sku', 0) or 0
                mc_min = normalizar_margem(p.get('margem_minima'))
                mc_des = normalizar_margem(p.get('margem_desejavel'))
                preco_base = round(custo + (custo * cen['markup'] / 100), 2)

                sv = df_precos[(df_precos['sku'] == sku) & (df_precos['logistica'] == cen['nome'])]
                preco_salvo = float(sv.iloc[0]['preco_venda']) if not sv.empty and pd.notna(sv.iloc[0]['preco_venda']) else None
                preco = preco_salvo if (simular and preco_salvo) else preco_base

                com_total = (cen['comissao'] + cen['maquina']) / 100
                imp_dec = cen['imposto'] / 100
                ma, mp = calcular_margem(preco, custo, com_total, 0, imp_dec)
                sinal = semaforo(mp, p.get('margem_minima'), p.get('margem_desejavel'))

                rows.append({
                    'sinal': sinal, 'sku': sku, 'produto': p['nome'], 'categoria': p['categoria'],
                    'custo': custo, 'mc_esp': mc_des, 'mc_min': mc_min,
                    'preco_base': preco_base,
                    'preco_venda': preco if simular else preco_base,
                    'com_pct': cen['comissao'], 'maquina_pct': cen['maquina'],
                    'imp_pct': cen['imposto'],
                    'mg_pct': mp, 'mg_abs': ma,
                })

            df = pd.DataFrame(rows)
            cc = {
                'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
                'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
                'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                'preco_base': st.column_config.NumberColumn(f"Base (+{cen['markup']}%)", format="R$ %.2f", disabled=True, width="small"),
                'preco_venda': st.column_config.NumberColumn(
                    "🟠 Preço" if simular else "Preço",
                    format="R$ %.2f", min_value=0, disabled=not simular, width="small"),
                'com_pct': st.column_config.NumberColumn("Com. %", format="%.1f%%", width="tiny"),
                'maquina_pct': st.column_config.NumberColumn("Máq. %", format="%.1f%%", width="tiny"),
                'imp_pct': st.column_config.NumberColumn("Imp. %", format="%.1f%%", width="tiny"),
                'mg_pct': st.column_config.NumberColumn("Margem %", format="%.1f%%", disabled=True, width="small"),
                'mg_abs': st.column_config.NumberColumn("Margem R$", format="R$ %.2f", disabled=True, width="small"),
            }
            col_order = ['sinal', 'sku', 'produto', 'categoria']
            if perfil in PERFIS_COM_CUSTO:
                cc['custo'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
                cc['mc_esp'] = st.column_config.NumberColumn("MC Esp%", format="%.0f%%", disabled=True, width="tiny")
                cc['mc_min'] = st.column_config.NumberColumn("MC Mín%", format="%.0f%%", disabled=True, width="tiny")
                col_order += ['custo', 'mc_esp', 'mc_min']
            col_order += ['preco_base', 'preco_venda', 'com_pct', 'maquina_pct', 'imp_pct', 'mg_pct']
            if perfil in PERFIS_COM_CUSTO:
                col_order.append('mg_abs')

            edited = st.data_editor(df, column_config=cc, column_order=col_order,
                                     use_container_width=True, hide_index=True,
                                     num_rows="fixed", height=EDITOR_HEIGHT, key=f"ed_b2b_{ci}")

            bc1, bc2 = st.columns(2)
            with bc1:
                if simular and st.button(f"💾 Salvar simulação — {cen['nome']}", key=f"sv_b2b_{ci}"):
                    saves = [{'sku': r['sku'], 'preco_venda': r['preco_venda'],
                              'comissao': r.get('com_pct'), 'frete': None, 'taxa': None}
                             for _, r in edited.iterrows() if r.get('preco_venda') and r['preco_venda'] > 0]
                    cnt = salvar_precos(engine, saves, "B2B", "B2B", cen['nome'], usuario)
                    if cnt > 0:
                        st.success(f"✅ {cnt} preços salvos!")
                        carregar_precos_salvos.clear()
            with bc2:
                botao_download_xlsx(edited[col_order], f"dl_b2b_{ci}", f"tabela_B2B_{cen['nome']}.xlsx")


# ============================================================
# UPLOAD DE PREÇOS (widget reutilizável)
# ============================================================

def _upload_precos_widget(engine, df_prod, marketplace, loja, colunas_extra, key_prefix, usuario):
    """Widget de upload de preços em massa via XLSX."""
    with st.expander("📤 Upload em massa", expanded=False):
        # Template
        cols_template = ["preco_venda", "comissao_pct", "taxa_fixa", "frete"]
        xlsx_data = gerar_template_xlsx(df_prod, cols_template, f"{marketplace}_{loja}")
        st.download_button("📄 Baixar template", data=xlsx_data,
                          file_name=f"template_{marketplace}_{loja}.xlsx",
                          key=f"tmpl_{key_prefix}")

        logistica = st.text_input("Logística", value="Clássico", key=f"log_{key_prefix}")
        arquivo = st.file_uploader("Subir XLSX preenchido", type=["xlsx"], key=f"file_{key_prefix}")

        if arquivo and st.button("📥 Processar upload", key=f"proc_{key_prefix}"):
            df_up, err = processar_upload_xlsx(arquivo, ["sku", "preco_venda"])
            if err:
                st.error(f"❌ {err}")
            else:
                saves = []
                for _, r in df_up.iterrows():
                    pv = r.get('preco_venda')
                    if pd.isna(pv) or not pv or float(pv) <= 0:
                        continue
                    saves.append({
                        'sku': str(r['sku']).strip(),
                        'preco_venda': float(pv),
                        'comissao': float(r['comissao_pct']) if pd.notna(r.get('comissao_pct')) else None,
                        'frete': float(r['frete']) if pd.notna(r.get('frete')) else None,
                        'taxa': float(r['taxa_fixa']) if pd.notna(r.get('taxa_fixa')) else None,
                    })
                cnt = salvar_precos(engine, saves, marketplace, loja, logistica, usuario)
                if cnt > 0:
                    st.success(f"✅ {cnt} preços importados!")
                    carregar_precos_salvos.clear()
                else:
                    st.warning("Nenhum preço válido no arquivo.")


# ============================================================
# PÁGINA PRINCIPAL
# ============================================================

def tabela_preco_page():
    st.title("📊 Tabela de Preço")
    st.caption("Grade de precificação estratégica — simule preços e veja margens por marketplace • v3.0")

    usuario_dict = st.session_state.get('usuario', {})
    if not usuario_dict or not usuario_dict.get('role'):
        st.error("Sessão não encontrada. Faça login novamente.")
        return

    usuario = usuario_dict.get('username', '')
    perfil = usuario_dict.get('role', '')
    engine = get_engine()

    # Tabs acessíveis
    tabs_disp = TABS_ORDER.copy()
    if perfil == "GESTOR":
        gestor_lojas = carregar_lojas_gestor(engine, usuario)
        if gestor_lojas.empty:
            st.warning("Sem lojas atribuídas. Contate o administrador.")
            return
        mkts = gestor_lojas['marketplace'].str.upper().unique().tolist()
        tabs_disp = [t for t in TABS_ORDER
                     if t.upper() == "B2B" or any(t.upper() in m or m in t.upper() for m in mkts)]

    if not tabs_disp:
        st.warning("Nenhum marketplace disponível.")
        return

    tab_objs = st.tabs(tabs_disp)
    for i, tn in enumerate(tabs_disp):
        with tab_objs[i]:
            try:
                if tn == "Mercado Livre":
                    render_tab_ml(engine, perfil, usuario)
                elif tn == "Shopee":
                    render_tab_shopee(engine, perfil, usuario)
                elif tn == "Amazon":
                    render_tab_amazon(engine, perfil, usuario)
                elif tn == "Shein":
                    render_tab_shein(engine, perfil, usuario)
                elif tn == "Magalu":
                    render_tab_magalu(engine, perfil, usuario)
                elif tn == "B2B":
                    render_tab_b2b(engine, perfil, usuario)
            except Exception as e:
                st.error(f"Erro ao carregar {tn}: {str(e)}")
                st.exception(e)
