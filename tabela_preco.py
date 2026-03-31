"""
tabela_preco.py — Módulo Tabela de Preço (Projeto Nala)
Grade de precificação estratégica por marketplace.
Tabs: ML | Shopee | Amazon | Shein | Magalu | B2B
"""

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import text
from datetime import datetime, timedelta
from database_utils import get_engine

# ============================================================
# CONSTANTES
# ============================================================

TABS_ORDER = ["Mercado Livre", "Shopee", "Amazon", "Shein", "Magalu", "B2B"]

# Perfis que veem custos detalhados
PERFIS_COM_CUSTO = ["ADMIN", "CONTROLADORIA", "DIRETOR", "COMPRAS"]

# ============================================================
# FUNÇÕES DE CARGA DE DADOS
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def carregar_produtos_ativos(_engine):
    """Carrega todos os produtos ativos com custos e dimensões."""
    query = text("""
        SELECT
            p.sku, p.nome, p.categoria, p.status,
            p.preco_a_ser_considerado,
            COALESCE(p.preco_a_ser_considerado, 0) AS custo_sku,
            p.margem_minima, p.margem_desejavel,
            p.largura, p.comprimento, p.altura, p.peso_bruto
        FROM dim_produtos p
        WHERE p.status = 'Ativo'
        ORDER BY p.nome
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_lojas(_engine):
    """Carrega todas as lojas com imposto e custo_flex."""
    query = text("""
        SELECT marketplace, loja, imposto, custo_flex
        FROM dim_lojas
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_config_marketplace(_engine, marketplace):
    """Carrega configs de marketplace (comissão, taxa fixa, frete)."""
    query = text("""
        SELECT sku, marketplace, loja, logistica,
               comissao_percentual, taxa_fixa, frete_estimado
        FROM dim_config_marketplace
        WHERE LOWER(marketplace) = LOWER(:mkt)
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"mkt": marketplace})
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_precos_salvos(_engine, marketplace):
    """Carrega preços salvos na dim_precos_marketplace."""
    query = text("""
        SELECT sku, marketplace, loja, logistica,
               preco_venda, comissao_percentual_override,
               frete_override, taxa_fixa_override,
               updated_at, updated_by
        FROM dim_precos_marketplace
        WHERE LOWER(marketplace) = LOWER(:mkt)
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"mkt": marketplace})
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_frete_ml(_engine):
    """Carrega tabela de frete do ML."""
    query = text("SELECT * FROM dim_frete_ml ORDER BY tipo, faixa_peso_min_kg, faixa_preco_min")
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_frete_amazon(_engine, tipo):
    """Carrega tabela de frete Amazon por tipo (FBA/DBA)."""
    query = text("""
        SELECT * FROM dim_frete_amazon
        WHERE tipo = :tipo
        ORDER BY regiao, faixa_peso_min_kg, faixa_preco_min
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"tipo": tipo})
    return df


@st.cache_data(ttl=120, show_spinner=False)
def carregar_vendas_30d(_engine, marketplace):
    """Carrega contagem de vendas e margem real dos últimos 30 dias."""
    data_corte = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    query = text("""
        SELECT
            sku,
            marketplace,
            loja,
            COUNT(*) AS qtd_vendas_30d,
            AVG(margem_percentual) AS margem_real_30d
        FROM fact_vendas_snapshot
        WHERE LOWER(marketplace) = LOWER(:mkt)
          AND data_venda >= :data_corte
        GROUP BY sku, marketplace, loja
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"mkt": marketplace, "data_corte": data_corte})
    return df


@st.cache_data(ttl=300, show_spinner=False)
def carregar_lojas_gestor(_engine, usuario):
    """Carrega lojas autorizadas para um gestor."""
    query = text("""
        SELECT loja, marketplace
        FROM dim_usuario_lojas
        WHERE LOWER(usuario) = LOWER(:usr)
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"usr": usuario})
    return df


# ============================================================
# FUNÇÕES DE CÁLCULO
# ============================================================

def calcular_peso_efetivo(largura, comprimento, altura, peso_bruto):
    """Retorna max(peso_bruto, peso_cubado). Cubado = L×C×A / 6000."""
    if pd.isna(largura) or pd.isna(comprimento) or pd.isna(altura):
        return peso_bruto if not pd.isna(peso_bruto) else None
    peso_cubado = (largura * comprimento * altura) / 6000
    peso_real = peso_bruto if not pd.isna(peso_bruto) else 0
    return max(peso_real, peso_cubado)


def buscar_frete_ml(df_frete, peso_kg, preco_venda, tipo='envio_padrao'):
    """Busca custo de envio ML na tabela de frete."""
    if peso_kg is None or preco_venda is None or preco_venda <= 0:
        return None
    filtro = df_frete[
        (df_frete['tipo'] == tipo) &
        (df_frete['faixa_peso_min_kg'] <= peso_kg) &
        (df_frete['faixa_peso_max_kg'] > peso_kg) &
        (df_frete['faixa_preco_min'] <= preco_venda) &
        (df_frete['faixa_preco_max'] >= preco_venda)
    ]
    if len(filtro) > 0:
        custo = float(filtro.iloc[0]['custo_envio'])
        # Regra: produtos <R$19 pagam no máximo 50% do preço
        if preco_venda < 19:
            custo = min(custo, preco_venda * 0.5)
        return custo
    return None


def buscar_frete_amazon(df_frete, peso_kg, preco_venda):
    """Busca tarifa de frete Amazon na tabela."""
    if peso_kg is None or preco_venda is None or preco_venda <= 0:
        return None
    # Para peso acima de 10kg, calcular kg adicional
    peso_base = min(peso_kg, 10)
    kg_extra = max(0, peso_kg - 10)

    filtro = df_frete[
        (df_frete['faixa_peso_min_kg'] <= peso_base) &
        (df_frete['faixa_peso_max_kg'] > peso_base) &
        (df_frete['faixa_preco_min'] <= preco_venda) &
        (df_frete['faixa_preco_max'] >= preco_venda)
    ]
    if len(filtro) > 0:
        tarifa = float(filtro.iloc[0]['tarifa'])
        kg_add = float(filtro.iloc[0].get('kg_adicional', 0) or 0)
        if kg_extra > 0 and kg_add > 0:
            tarifa += np.ceil(kg_extra) * kg_add
        return tarifa
    return None


def calcular_margem(preco_venda, custo_sku, comissao_pct, frete, taxa_fixa,
                    imposto_pct, custo_extra=0):
    """
    Calcula margem absoluta e percentual.
    Retorna (margem_abs, margem_pct) ou (None, None) se preço inválido.
    """
    if not preco_venda or preco_venda <= 0:
        return None, None
    comissao_valor = preco_venda * (comissao_pct or 0)
    imposto_valor = preco_venda * (imposto_pct or 0)
    frete_val = frete or 0
    taxa_val = taxa_fixa or 0
    custo_val = custo_sku or 0

    subtotal = comissao_valor + imposto_valor + frete_val + taxa_val + custo_val + custo_extra
    margem_abs = preco_venda - subtotal
    margem_pct = (margem_abs / preco_venda) * 100
    return round(margem_abs, 2), round(margem_pct, 2)


def calcular_preco_sugerido(custo_sku, comissao_pct, frete, taxa_fixa,
                            imposto_pct, margem_alvo_pct, custo_extra=0):
    """
    Calcula o preço de venda necessário para atingir a margem alvo.
    Preço = (frete + taxa + extra + custo) / (1 - comissão - imposto - margem_alvo)
    """
    divisor = 1 - (comissao_pct or 0) - (imposto_pct or 0) - (margem_alvo_pct / 100)
    if divisor <= 0:
        return None
    numerador = (frete or 0) + (taxa_fixa or 0) + (custo_extra or 0) + (custo_sku or 0)
    preco = numerador / divisor
    return round(preco, 2)


def classificar_tag(qtd_vendas_30d, data_cadastro):
    """Classifica tag de performance do SKU."""
    if data_cadastro:
        dias = (datetime.now() - pd.Timestamp(data_cadastro).to_pydatetime().replace(tzinfo=None)).days
        if dias < 30:
            return "🚀 Lançamento"
    vendas = qtd_vendas_30d or 0
    if vendas >= 200:
        return "⭐ Top Seller"
    elif vendas >= 100:
        return "🔥 Escala"
    elif vendas >= 50:
        return "📈 Tração"
    else:
        return "⚠️ Atenção"


def semaforo(margem_pct, margem_minima, margem_desejavel):
    """Retorna emoji de semáforo baseado nas margens."""
    if margem_pct is None:
        return "⚪"
    mm = (margem_minima or 0) * 100 if margem_minima and margem_minima < 1 else (margem_minima or 0)
    md = (margem_desejavel or 0) * 100 if margem_desejavel and margem_desejavel < 1 else (margem_desejavel or 0)
    if margem_pct < mm:
        return "🔴"
    elif margem_pct < md:
        return "🟡"
    else:
        return "🟢"


def fmt_brl(valor):
    """Formata valor em R$ brasileiro."""
    if valor is None or pd.isna(valor):
        return ""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(valor):
    """Formata percentual brasileiro."""
    if valor is None or pd.isna(valor):
        return ""
    return f"{valor:,.2f}%".replace(".", ",")


# ============================================================
# FUNÇÕES DE SALVAMENTO
# ============================================================

def salvar_precos(engine, df_salvar, marketplace, loja, logistica, usuario):
    """Salva/atualiza preços na dim_precos_marketplace."""
    if df_salvar.empty:
        return 0
    count = 0
    with engine.connect() as conn:
        for _, row in df_salvar.iterrows():
            preco = row.get('preco_venda')
            if preco is None or pd.isna(preco) or preco <= 0:
                continue
            conn.execute(text("""
                INSERT INTO dim_precos_marketplace
                    (sku, marketplace, loja, logistica, preco_venda,
                     comissao_percentual_override, frete_override,
                     taxa_fixa_override, updated_at, updated_by)
                VALUES
                    (:sku, :mkt, :loja, :log, :preco,
                     :comissao, :frete, :taxa,
                     NOW(), :usr)
                ON CONFLICT (sku, marketplace, loja, logistica)
                DO UPDATE SET
                    preco_venda = EXCLUDED.preco_venda,
                    comissao_percentual_override = EXCLUDED.comissao_percentual_override,
                    frete_override = EXCLUDED.frete_override,
                    taxa_fixa_override = EXCLUDED.taxa_fixa_override,
                    updated_at = NOW(),
                    updated_by = EXCLUDED.updated_by
            """), {
                "sku": row['sku'],
                "mkt": marketplace,
                "loja": loja,
                "log": logistica,
                "preco": float(preco),
                "comissao": float(row.get('comissao_override')) if pd.notna(row.get('comissao_override')) else None,
                "frete": float(row.get('frete_override')) if pd.notna(row.get('frete_override')) else None,
                "taxa": float(row.get('taxa_fixa_override')) if pd.notna(row.get('taxa_fixa_override')) else None,
                "usr": usuario,
            })
            count += 1
        conn.commit()
    return count


# ============================================================
# FUNÇÃO GENÉRICA DE MONTAGEM DO DATAFRAME
# ============================================================

def montar_df_base(df_produtos, df_vendas, df_precos_salvos, loja, logistica):
    """
    Monta o DataFrame base para uma combinação loja/logística.
    Inclui: dados do produto, tag, margem real 30d, preço salvo.
    """
    df = df_produtos.copy()

    # Peso efetivo
    df['peso_efetivo'] = df.apply(
        lambda r: calcular_peso_efetivo(r['largura'], r['comprimento'], r['altura'], r['peso_bruto']),
        axis=1
    )

    # Merge vendas 30d (por SKU + loja)
    if not df_vendas.empty:
        vendas_loja = df_vendas[df_vendas['loja'].str.lower() == loja.lower()] if 'loja' in df_vendas.columns else df_vendas
        vendas_agg = vendas_loja.groupby('sku').agg(
            qtd_vendas_30d=('qtd_vendas_30d', 'sum'),
            margem_real_30d=('margem_real_30d', 'mean')
        ).reset_index()
        df = df.merge(vendas_agg, on='sku', how='left')
    else:
        df['qtd_vendas_30d'] = 0
        df['margem_real_30d'] = None

    # Tag de performance
    df['tag'] = df.apply(
        lambda r: classificar_tag(r.get('qtd_vendas_30d', 0), r.get('data_cadastro')),
        axis=1
    )

    # Merge preços salvos
    if not df_precos_salvos.empty:
        salvos = df_precos_salvos[
            (df_precos_salvos['loja'].str.lower() == loja.lower()) &
            (df_precos_salvos['logistica'].str.lower() == logistica.lower())
        ][['sku', 'preco_venda', 'comissao_percentual_override', 'frete_override', 'taxa_fixa_override']]
        df = df.merge(salvos, on='sku', how='left', suffixes=('', '_salvo'))
    else:
        df['preco_venda'] = None
        df['comissao_percentual_override'] = None
        df['frete_override'] = None
        df['taxa_fixa_override'] = None

    return df


# ============================================================
# RENDERIZAÇÃO GENÉRICA DE TAB
# ============================================================

def render_tab_marketplace(engine, marketplace, loja, logistica,
                           comissao_default, taxa_fixa_default,
                           imposto_pct, custo_extra, perfil, usuario,
                           fn_calcular_frete=None, df_frete=None,
                           mostrar_toggle_sugerido=True,
                           descricao_cenario=""):
    """
    Renderiza uma tab genérica de marketplace com data_editor.

    Args:
        fn_calcular_frete: função(df_frete, peso_kg, preco) -> frete calculado
        df_frete: DataFrame da tabela de frete para lookup
        custo_extra: custo fixo extra (ex: R$6 Yanni SP)
    """
    # Carregar dados
    df_produtos = carregar_produtos_ativos(engine)
    df_vendas = carregar_vendas_30d(engine, marketplace)
    df_precos = carregar_precos_salvos(engine, marketplace)

    if df_produtos.empty:
        st.warning("Nenhum produto ativo encontrado.")
        return

    # Montar base
    df = montar_df_base(df_produtos, df_vendas, df_precos, loja, logistica)

    if descricao_cenario:
        st.caption(descricao_cenario)

    # Toggle preço sugerido
    mostrar_sugerido = False
    if mostrar_toggle_sugerido:
        mostrar_sugerido = st.toggle("📊 Mostrar preços sugeridos (Mínimo / Esperado)",
                                      key=f"sug_{marketplace}_{loja}_{logistica}")

    # ---- Preparar colunas do editor ----
    rows = []
    for _, r in df.iterrows():
        peso = r.get('peso_efetivo')
        preco_salvo = r.get('preco_venda')

        # Comissão: override > config > default
        comissao = r.get('comissao_percentual_override')
        if pd.isna(comissao) or comissao is None:
            comissao = comissao_default

        # Frete auto-calculado
        frete_auto = None
        if fn_calcular_frete and df_frete is not None and peso and preco_salvo and preco_salvo > 0:
            frete_auto = fn_calcular_frete(df_frete, peso, preco_salvo)

        # Frete: override > auto > default None
        frete_override = r.get('frete_override')
        frete = frete_override if pd.notna(frete_override) else frete_auto

        # Taxa fixa: override > default
        taxa_override = r.get('taxa_fixa_override')
        taxa = taxa_override if pd.notna(taxa_override) else taxa_fixa_default

        # Calcular margem
        custo = r.get('custo_sku', 0)
        margem_abs, margem_pct = calcular_margem(
            preco_salvo, custo, comissao, frete, taxa, imposto_pct, custo_extra
        )

        # Semáforo
        sinal = semaforo(margem_pct, r.get('margem_minima'), r.get('margem_desejavel'))

        row_data = {
            'sku': r['sku'],
            'produto': r['nome'],
            'categoria': r['categoria'],
            'tag': r.get('tag', ''),
            'margem_real_30d': r.get('margem_real_30d'),
            'custo_sku': custo,
            'mc_esperada': r.get('margem_desejavel'),
            'mc_minima': r.get('margem_minima'),
            'peso_kg': peso,
            'preco_venda': preco_salvo,
            'comissao_pct': comissao,
            'frete_auto': frete_auto,
            'frete_override': frete_override if pd.notna(frete_override) else None,
            'taxa_fixa': taxa,
            'imposto_pct': imposto_pct,
            'custo_extra': custo_extra,
            'margem_abs': margem_abs,
            'margem_pct': margem_pct,
            'sinal': sinal,
        }

        # Preços sugeridos
        if mostrar_sugerido:
            preco_min = calcular_preco_sugerido(
                custo, comissao, frete, taxa, imposto_pct,
                (r.get('margem_minima', 0) or 0) * 100 if (r.get('margem_minima') or 0) < 1 else (r.get('margem_minima', 0) or 0),
                custo_extra
            )
            preco_esp = calcular_preco_sugerido(
                custo, comissao, frete, taxa, imposto_pct,
                (r.get('margem_desejavel', 0) or 0) * 100 if (r.get('margem_desejavel') or 0) < 1 else (r.get('margem_desejavel', 0) or 0),
                custo_extra
            )
            row_data['preco_minimo'] = preco_min
            row_data['preco_esperado'] = preco_esp

        rows.append(row_data)

    df_display = pd.DataFrame(rows)

    if df_display.empty:
        st.info("Nenhum dado para exibir.")
        return

    # ---- Configurar colunas ----
    # Colunas base visíveis
    col_config = {
        'sku': st.column_config.TextColumn("SKU", width="small", disabled=True),
        'produto': st.column_config.TextColumn("Produto", width="medium", disabled=True),
        'categoria': st.column_config.TextColumn("Cat.", width="small", disabled=True),
        'tag': st.column_config.TextColumn("🏷️ Tag", width="small", disabled=True),
        'margem_real_30d': st.column_config.NumberColumn(
            "Real 30d %", format="%.1f%%", disabled=True, width="small"
        ),
        'preco_venda': st.column_config.NumberColumn(
            "🟠 Preço Venda", format="R$ %.2f", min_value=0, width="small",
            help="Preço de venda anunciado (editável)"
        ),
        'comissao_pct': st.column_config.NumberColumn(
            "Comissão %", format="%.1f%%", min_value=0, max_value=1, width="small",
            help="Editável. Deixe o valor padrão ou altere para simular."
        ),
        'frete_override': st.column_config.NumberColumn(
            "Frete Manual", format="R$ %.2f", min_value=0, width="small",
            help="Override manual do frete (deixe vazio para auto-cálculo)"
        ),
        'taxa_fixa': st.column_config.NumberColumn(
            "Taxa Fixa", format="R$ %.2f", min_value=0, width="small",
            help="Editável"
        ),
        'margem_pct': st.column_config.NumberColumn(
            "Margem %", format="%.1f%%", disabled=True, width="small"
        ),
        'margem_abs': st.column_config.NumberColumn(
            "Margem R$", format="R$ %.2f", disabled=True, width="small"
        ),
        'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
    }

    # Colunas ocultas
    hidden = ['custo_extra', 'imposto_pct', 'frete_auto', 'peso_kg', 'data_cadastro',
              'largura', 'comprimento', 'altura', 'peso_bruto', 'preco_a_ser_considerado',
              'qtd_vendas_30d', 'status']

    # GESTOR: ocultar custo e margens detalhadas
    if perfil == "GESTOR":
        hidden.extend(['custo_sku', 'mc_esperada', 'mc_minima', 'margem_abs'])
    else:
        col_config['custo_sku'] = st.column_config.NumberColumn(
            "Custo", format="R$ %.2f", disabled=True, width="small"
        )
        col_config['mc_esperada'] = st.column_config.NumberColumn(
            "MC Esp.", format="%.0f%%", disabled=True, width="tiny"
        )
        col_config['mc_minima'] = st.column_config.NumberColumn(
            "MC Mín.", format="%.0f%%", disabled=True, width="tiny"
        )

    # Preços sugeridos
    if mostrar_sugerido:
        col_config['preco_minimo'] = st.column_config.NumberColumn(
            "💡 Mínimo", format="R$ %.2f", disabled=True, width="small"
        )
        col_config['preco_esperado'] = st.column_config.NumberColumn(
            "💡 Esperado", format="R$ %.2f", disabled=True, width="small"
        )

    # Definir ordem de colunas
    col_order = ['sinal', 'sku', 'produto', 'categoria', 'tag', 'margem_real_30d']
    if perfil != "GESTOR":
        col_order.extend(['custo_sku', 'mc_esperada', 'mc_minima'])
    if mostrar_sugerido:
        col_order.extend(['preco_minimo', 'preco_esperado'])
    col_order.extend(['preco_venda', 'comissao_pct', 'frete_override', 'taxa_fixa',
                      'margem_pct'])
    if perfil != "GESTOR":
        col_order.append('margem_abs')

    # Ocultar colunas não desejadas
    for col in df_display.columns:
        if col not in col_order and col not in col_config:
            hidden.append(col)

    for h in hidden:
        if h in df_display.columns:
            col_config[h] = None

    # ---- Exibir editor ----
    key_editor = f"editor_{marketplace}_{loja}_{logistica}"
    edited_df = st.data_editor(
        df_display,
        column_config=col_config,
        column_order=col_order,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key=key_editor,
    )

    # ---- Métricas resumo ----
    total_skus = len(edited_df)
    com_preco = edited_df['preco_venda'].notna().sum()
    margem_media = edited_df['margem_pct'].mean() if edited_df['margem_pct'].notna().any() else 0
    verdes = (edited_df['sinal'] == '🟢').sum()
    amarelos = (edited_df['sinal'] == '🟡').sum()
    vermelhos = (edited_df['sinal'] == '🔴').sum()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("SKUs", total_skus)
    c2.metric("Com preço", com_preco)
    c3.metric("Margem média", f"{margem_media:.1f}%")
    c4.metric("🟢", verdes)
    c5.metric("🟡", amarelos)
    c6.metric("🔴", vermelhos)

    # ---- Botão salvar ----
    if st.button(f"💾 Salvar preços — {loja} / {logistica}", key=f"save_{marketplace}_{loja}_{logistica}"):
        df_para_salvar = edited_df[['sku', 'preco_venda', 'comissao_pct', 'frete_override', 'taxa_fixa']].copy()
        df_para_salvar.rename(columns={'comissao_pct': 'comissao_override'}, inplace=True)
        df_para_salvar.rename(columns={'taxa_fixa': 'taxa_fixa_override'}, inplace=True)
        count = salvar_precos(engine, df_para_salvar, marketplace, loja, logistica, usuario)
        if count > 0:
            st.success(f"✅ {count} preços salvos com sucesso!")
            # Limpar cache para recarregar
            carregar_precos_salvos.clear()
        else:
            st.warning("Nenhum preço válido para salvar. Preencha a coluna 🟠 Preço Venda.")

    # Aviso se comissão não cadastrada
    if comissao_default is None or comissao_default == 0:
        st.info(
            "ℹ️ Comissão padrão não encontrada no módulo Configurações para esta loja/logística. "
            "Você pode simular editando a coluna 'Comissão %', mas cadastre no módulo Configurações "
            "para que o valor seja permanente."
        )


# ============================================================
# TABS POR MARKETPLACE
# ============================================================

def render_tab_ml(engine, perfil, usuario):
    """Tab Mercado Livre com sub-cenários."""
    st.subheader("📦 Mercado Livre")

    df_lojas = carregar_lojas(engine)
    ml_lojas = df_lojas[df_lojas['marketplace'].str.lower().str.contains('mercado')]

    # Filtrar por GESTOR
    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        ml_permitidas = lojas_gestor[lojas_gestor['marketplace'].str.lower().str.contains('mercado')]['loja'].tolist()
        ml_lojas = ml_lojas[ml_lojas['loja'].isin(ml_permitidas)]

    if ml_lojas.empty:
        st.warning("Nenhuma loja ML encontrada ou sem permissão.")
        return

    # Logísticas ML
    logisticas = ["Premium", "Clássico"]
    lojas_list = ml_lojas['loja'].unique().tolist()

    # Carregar frete
    try:
        df_frete_ml = carregar_frete_ml(engine)
    except Exception:
        df_frete_ml = pd.DataFrame()
        st.warning("⚠️ Tabela de frete ML não encontrada. Frete será manual.")

    # Criar sub-tabs por loja
    sub_tabs = st.tabs([f"{loja}" for loja in lojas_list])

    for idx, loja in enumerate(lojas_list):
        with sub_tabs[idx]:
            loja_info = ml_lojas[ml_lojas['loja'] == loja].iloc[0]
            imposto = float(loja_info.get('imposto', 0) or 0)

            # Custo extra Yanni SP
            custo_extra = 6.0 if 'yanni' in loja.lower() else 0.0

            # Toggle Premium / Clássico
            col_a, col_b = st.columns(2)
            with col_a:
                show_premium = st.checkbox("Premium (16,5%)", value=True,
                                           key=f"ml_prem_{loja}")
            with col_b:
                show_classico = st.checkbox("Clássico (11,5%)", value=True,
                                            key=f"ml_class_{loja}")

            if show_premium:
                st.markdown("#### Premium")
                # Frete ML: para >=R$79 usa tabela por peso, para <R$79 usa tabela também
                def fn_frete_ml_premium(df_fr, peso, preco):
                    if preco >= 79:
                        return buscar_frete_ml(df_fr, peso, preco, 'envio_padrao')
                    else:
                        return buscar_frete_ml(df_fr, peso, preco, 'envio_padrao')

                render_tab_marketplace(
                    engine=engine,
                    marketplace="Mercado Livre",
                    loja=loja,
                    logistica="Premium",
                    comissao_default=0.165,
                    taxa_fixa_default=0,  # taxa fixa agora está no frete (tabela ML)
                    imposto_pct=imposto,
                    custo_extra=custo_extra,
                    perfil=perfil,
                    usuario=usuario,
                    fn_calcular_frete=fn_frete_ml_premium if not df_frete_ml.empty else None,
                    df_frete=df_frete_ml if not df_frete_ml.empty else None,
                    descricao_cenario=f"Comissão 16,5% | Imposto {imposto*100:.0f}% | "
                                     f"Frete pela tabela ML 2026{' | +R$6,00 Yanni' if custo_extra > 0 else ''}"
                )

            if show_classico:
                st.markdown("#### Clássico")

                def fn_frete_ml_classico(df_fr, peso, preco):
                    return buscar_frete_ml(df_fr, peso, preco, 'envio_padrao')

                render_tab_marketplace(
                    engine=engine,
                    marketplace="Mercado Livre",
                    loja=loja,
                    logistica="Clássico",
                    comissao_default=0.115,
                    taxa_fixa_default=0,
                    imposto_pct=imposto,
                    custo_extra=custo_extra,
                    perfil=perfil,
                    usuario=usuario,
                    fn_calcular_frete=fn_frete_ml_classico if not df_frete_ml.empty else None,
                    df_frete=df_frete_ml if not df_frete_ml.empty else None,
                    descricao_cenario=f"Comissão 11,5% | Imposto {imposto*100:.0f}% | "
                                     f"Frete pela tabela ML 2026{' | +R$6,00 Yanni' if custo_extra > 0 else ''}"
                )


def render_tab_shopee(engine, perfil, usuario):
    """Tab Shopee com regras escalonadas."""
    st.subheader("🛒 Shopee")

    df_lojas = carregar_lojas(engine)
    shopee_lojas = df_lojas[df_lojas['marketplace'].str.lower().str.contains('shopee')]

    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        permitidas = lojas_gestor[lojas_gestor['marketplace'].str.lower().str.contains('shopee')]['loja'].tolist()
        shopee_lojas = shopee_lojas[shopee_lojas['loja'].isin(permitidas)]

    if shopee_lojas.empty:
        st.warning("Nenhuma loja Shopee encontrada ou sem permissão.")
        return

    st.caption(
        "Regras escalonadas: ≤R$79,99 → 20% + R$4 | R$80-99,99 → 14% + R$16 | "
        "R$100-199,99 → 14% + R$20 | >R$199,99 → 14% + R$26"
    )

    for _, loja_row in shopee_lojas.iterrows():
        loja = loja_row['loja']
        imposto = float(loja_row.get('imposto', 0) or 0)

        st.markdown(f"#### {loja}")

        # Shopee: comissão e taxa fixa dependem do preço (escalonada)
        # Passamos defaults que serão recalculados dentro do render
        # Para Shopee, precisamos de tratamento especial
        render_tab_shopee_especial(engine, loja, imposto, perfil, usuario)


def render_tab_shopee_especial(engine, loja, imposto_pct, perfil, usuario):
    """Renderização especial para Shopee com regras escalonadas."""
    df_produtos = carregar_produtos_ativos(engine)
    df_vendas = carregar_vendas_30d(engine, "Shopee")
    df_precos = carregar_precos_salvos(engine, "Shopee")

    if df_produtos.empty:
        st.warning("Nenhum produto ativo.")
        return

    df = montar_df_base(df_produtos, df_vendas, df_precos, loja, "Shopee")

    mostrar_sugerido = st.toggle("📊 Preços sugeridos", key=f"sug_shopee_{loja}")

    rows = []
    for _, r in df.iterrows():
        preco = r.get('preco_venda')
        custo = r.get('custo_sku', 0)

        # Regras escalonadas Shopee
        if preco and preco > 0:
            if preco <= 79.99:
                comissao, taxa = 0.20, 4.0
            elif preco <= 99.99:
                comissao, taxa = 0.14, 16.0
            elif preco <= 199.99:
                comissao, taxa = 0.14, 20.0
            else:
                comissao, taxa = 0.14, 26.0
        else:
            comissao, taxa = 0.20, 4.0  # default

        margem_abs, margem_pct = calcular_margem(preco, custo, comissao, 0, taxa, imposto_pct)
        sinal = semaforo(margem_pct, r.get('margem_minima'), r.get('margem_desejavel'))

        row_data = {
            'sku': r['sku'], 'produto': r['nome'], 'categoria': r['categoria'],
            'tag': r.get('tag', ''), 'margem_real_30d': r.get('margem_real_30d'),
            'custo_sku': custo,
            'mc_esperada': r.get('margem_desejavel'), 'mc_minima': r.get('margem_minima'),
            'preco_venda': preco,
            'comissao_pct': comissao, 'taxa_fixa': taxa,
            'margem_pct': margem_pct, 'margem_abs': margem_abs, 'sinal': sinal,
        }

        if mostrar_sugerido:
            # Para sugerido, usar a faixa de comissão que resultaria
            mc_min = (r.get('margem_minima') or 0)
            mc_min = mc_min * 100 if mc_min < 1 else mc_min
            mc_des = (r.get('margem_desejavel') or 0)
            mc_des = mc_des * 100 if mc_des < 1 else mc_des
            row_data['preco_minimo'] = calcular_preco_sugerido(custo, 0.20, 0, 4, imposto_pct, mc_min)
            row_data['preco_esperado'] = calcular_preco_sugerido(custo, 0.20, 0, 4, imposto_pct, mc_des)

        rows.append(row_data)

    df_display = pd.DataFrame(rows)

    col_config = {
        'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
        'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
        'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
        'tag': st.column_config.TextColumn("🏷️", disabled=True, width="small"),
        'margem_real_30d': st.column_config.NumberColumn("Real 30d", format="%.1f%%", disabled=True, width="small"),
        'preco_venda': st.column_config.NumberColumn("🟠 Preço", format="R$ %.2f", min_value=0, width="small"),
        'comissao_pct': st.column_config.NumberColumn("Comissão", format="%.0f%%", disabled=True, width="tiny"),
        'taxa_fixa': st.column_config.NumberColumn("Taxa", format="R$ %.2f", disabled=True, width="tiny"),
        'margem_pct': st.column_config.NumberColumn("Margem %", format="%.1f%%", disabled=True, width="small"),
        'margem_abs': st.column_config.NumberColumn("Margem R$", format="R$ %.2f", disabled=True, width="small"),
        'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
    }

    col_order = ['sinal', 'sku', 'produto', 'categoria', 'tag', 'margem_real_30d']
    if perfil != "GESTOR":
        col_order.extend(['custo_sku', 'mc_esperada', 'mc_minima'])
        col_config['custo_sku'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
        col_config['mc_esperada'] = st.column_config.NumberColumn("MC Esp.", format="%.0f%%", disabled=True, width="tiny")
        col_config['mc_minima'] = st.column_config.NumberColumn("MC Mín.", format="%.0f%%", disabled=True, width="tiny")
    if mostrar_sugerido:
        col_order.extend(['preco_minimo', 'preco_esperado'])
        col_config['preco_minimo'] = st.column_config.NumberColumn("💡 Mínimo", format="R$ %.2f", disabled=True, width="small")
        col_config['preco_esperado'] = st.column_config.NumberColumn("💡 Esperado", format="R$ %.2f", disabled=True, width="small")
    col_order.extend(['preco_venda', 'comissao_pct', 'taxa_fixa', 'margem_pct'])
    if perfil != "GESTOR":
        col_order.append('margem_abs')

    edited_df = st.data_editor(df_display, column_config=col_config, column_order=col_order,
                                use_container_width=True, hide_index=True, num_rows="fixed",
                                key=f"editor_shopee_{loja}")

    # Salvar
    if st.button(f"💾 Salvar — Shopee / {loja}", key=f"save_shopee_{loja}"):
        df_s = edited_df[['sku', 'preco_venda']].copy()
        df_s['comissao_override'] = None
        df_s['frete_override'] = None
        df_s['taxa_fixa_override'] = None
        count = salvar_precos(engine, df_s, "Shopee", loja, "Shopee", usuario)
        if count > 0:
            st.success(f"✅ {count} preços salvos!")
            carregar_precos_salvos.clear()


def render_tab_amazon(engine, perfil, usuario):
    """Tab Amazon com DBA e FBA."""
    st.subheader("📦 Amazon")

    df_lojas = carregar_lojas(engine)
    amz_lojas = df_lojas[df_lojas['marketplace'].str.lower().str.contains('amazon')]

    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        permitidas = lojas_gestor[lojas_gestor['marketplace'].str.lower().str.contains('amazon')]['loja'].tolist()
        amz_lojas = amz_lojas[amz_lojas['loja'].isin(permitidas)]

    if amz_lojas.empty:
        st.warning("Nenhuma loja Amazon encontrada ou sem permissão.")
        return

    # Carregar tabelas de frete
    try:
        df_frete_dba = carregar_frete_amazon(engine, 'DBA')
    except Exception:
        df_frete_dba = pd.DataFrame()

    try:
        df_frete_fba = carregar_frete_amazon(engine, 'FBA')
    except Exception:
        df_frete_fba = pd.DataFrame()

    lojas_list = amz_lojas['loja'].unique().tolist()
    sub_tabs = st.tabs([f"{loja}" for loja in lojas_list])

    for idx, loja in enumerate(lojas_list):
        with sub_tabs[idx]:
            loja_info = amz_lojas[amz_lojas['loja'] == loja].iloc[0]
            imposto = float(loja_info.get('imposto', 0) or 0)

            col_a, col_b = st.columns(2)
            with col_a:
                show_dba = st.checkbox("DBA", value=True, key=f"amz_dba_{loja}")
            with col_b:
                show_fba = st.checkbox("FBA", value=True, key=f"amz_fba_{loja}")

            if show_dba:
                st.markdown("#### DBA")
                st.caption(
                    "Frete DBA: <R$30 → R$4,50 | R$30-49,99 → R$6,50 | "
                    "R$50-78,99 → R$6,75 | ≥R$79 → tabela por peso/preço"
                )
                render_tab_marketplace(
                    engine=engine, marketplace="Amazon", loja=loja, logistica="DBA",
                    comissao_default=0.12, taxa_fixa_default=0,
                    imposto_pct=imposto, custo_extra=0,
                    perfil=perfil, usuario=usuario,
                    fn_calcular_frete=buscar_frete_amazon if not df_frete_dba.empty else None,
                    df_frete=df_frete_dba if not df_frete_dba.empty else None,
                    descricao_cenario=f"Comissão 12% (editável) | Imposto {imposto*100:.0f}% | "
                                     "Região: Outras capitais Sul/SE"
                )

            if show_fba:
                st.markdown("#### FBA")
                st.caption("Frete FBA: manual ou auto-cálculo pela tabela de frete Amazon")
                render_tab_marketplace(
                    engine=engine, marketplace="Amazon", loja=loja, logistica="FBA",
                    comissao_default=0.12, taxa_fixa_default=0,
                    imposto_pct=imposto, custo_extra=0,
                    perfil=perfil, usuario=usuario,
                    fn_calcular_frete=buscar_frete_amazon if not df_frete_fba.empty else None,
                    df_frete=df_frete_fba if not df_frete_fba.empty else None,
                    descricao_cenario=f"Comissão 12% (editável) | Imposto {imposto*100:.0f}%"
                )


def render_tab_shein(engine, perfil, usuario):
    """Tab Shein com FULL e Normal."""
    st.subheader("👗 Shein")

    df_lojas = carregar_lojas(engine)
    shein_lojas = df_lojas[df_lojas['marketplace'].str.lower().str.contains('shein')]

    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        permitidas = lojas_gestor[lojas_gestor['marketplace'].str.lower().str.contains('shein')]['loja'].tolist()
        shein_lojas = shein_lojas[shein_lojas['loja'].isin(permitidas)]

    if shein_lojas.empty:
        st.warning("Nenhuma loja Shein encontrada ou sem permissão.")
        return

    for _, loja_row in shein_lojas.iterrows():
        loja = loja_row['loja']
        imposto = float(loja_row.get('imposto', 0) or 0)

        st.markdown(f"#### {loja}")

        col_a, col_b = st.columns(2)
        with col_a:
            show_full = st.checkbox("FULL (16% + R$6)", value=False, key=f"shein_full_{loja}")
        with col_b:
            show_normal = st.checkbox("Normal (16% + R$5)", value=True, key=f"shein_normal_{loja}")

        if show_full:
            st.markdown("##### Shein FULL")
            render_tab_marketplace(
                engine=engine, marketplace="Shein", loja=loja, logistica="FULL",
                comissao_default=0.16, taxa_fixa_default=6.0,
                imposto_pct=imposto, custo_extra=0,
                perfil=perfil, usuario=usuario,
                descricao_cenario=f"Comissão 16% | Taxa fixa R$6 (editável R$5-15) | Imposto {imposto*100:.0f}%"
            )

        if show_normal:
            st.markdown("##### Shein Normal")
            render_tab_marketplace(
                engine=engine, marketplace="Shein", loja=loja, logistica="Normal",
                comissao_default=0.16, taxa_fixa_default=5.0,
                imposto_pct=imposto, custo_extra=0,
                perfil=perfil, usuario=usuario,
                descricao_cenario=f"Comissão 16% | Taxa fixa R$5 (editável R$5-15) | Imposto {imposto*100:.0f}%"
            )


def render_tab_magalu(engine, perfil, usuario):
    """Tab Magalu com Loja e FBA."""
    st.subheader("🟦 Magalu")

    df_lojas = carregar_lojas(engine)
    mag_lojas = df_lojas[df_lojas['marketplace'].str.lower().str.contains('magalu')]

    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        permitidas = lojas_gestor[lojas_gestor['marketplace'].str.lower().str.contains('magalu')]['loja'].tolist()
        mag_lojas = mag_lojas[mag_lojas['loja'].isin(permitidas)]

    if mag_lojas.empty:
        st.warning("Nenhuma loja Magalu encontrada ou sem permissão.")
        return

    for _, loja_row in mag_lojas.iterrows():
        loja = loja_row['loja']
        imposto = float(loja_row.get('imposto', 0) or 0)

        st.markdown(f"#### {loja}")

        col_a, col_b = st.columns(2)
        with col_a:
            show_loja = st.checkbox("Expedição Própria", value=True, key=f"mag_loja_{loja}")
        with col_b:
            show_fba = st.checkbox("Fulfillment", value=False, key=f"mag_fba_{loja}")

        if show_loja:
            st.markdown("##### Expedição Própria")

            def fn_frete_magalu(df_fr, peso, preco):
                """Magalu: frete = 0 abaixo de R$79, frete da tabela acima."""
                return 0 if preco and preco <= 79 else None

            render_tab_marketplace(
                engine=engine, marketplace="Magalu", loja=loja, logistica="Loja",
                comissao_default=0.148, taxa_fixa_default=5.0,
                imposto_pct=imposto, custo_extra=0,
                perfil=perfil, usuario=usuario,
                descricao_cenario=f"Comissão 14,8% | Taxa fixa R$5 | Imposto {imposto*100:.0f}% | "
                                 "Frete: >R$79 → manual"
            )

        if show_fba:
            st.markdown("##### Fulfillment")
            render_tab_marketplace(
                engine=engine, marketplace="Magalu", loja=loja, logistica="Fulfillment",
                comissao_default=0.148, taxa_fixa_default=5.0,
                imposto_pct=imposto, custo_extra=0,
                perfil=perfil, usuario=usuario,
                descricao_cenario=f"Comissão 14,8% (editável) | Taxa fixa R$5 (editável) | Imposto {imposto*100:.0f}%"
            )


def render_tab_b2b(engine, perfil, usuario):
    """Tab B2B com 4 cenários de venda direta."""
    st.subheader("🏢 B2B — Venda Direta")

    st.caption(
        "Desconto máximo: vendas >R$300 → até 4% | R$301-1000 → até 7% | >R$1000 → até 10%"
    )

    df_produtos = carregar_produtos_ativos(engine)
    df_precos = carregar_precos_salvos(engine, "B2B")

    if df_produtos.empty:
        st.warning("Nenhum produto ativo.")
        return

    # Cenários B2B
    cenarios = [
        {"nome": "PIX sem NF", "markup": 0.30, "comissao": 0.02, "maquina": 0, "imposto": 0},
        {"nome": "PIX com NF", "markup": 0.48, "comissao": 0.02, "maquina": 0, "imposto": 0.02},
        {"nome": "Cartão 3x sem NF", "markup": 0.48, "comissao": 0.02, "maquina": 0.10, "imposto": 0},
        {"nome": "Cartão 3x com NF", "markup": 0.70, "comissao": 0.02, "maquina": 0.10, "imposto": 0.10},
    ]

    cenario_tabs = st.tabs([c["nome"] for c in cenarios])

    for ci, cenario in enumerate(cenarios):
        with cenario_tabs[ci]:
            df = montar_df_base(df_produtos, pd.DataFrame(), df_precos,
                                "B2B", cenario["nome"])

            simular = st.toggle("🔄 Simular outro preço", key=f"b2b_sim_{ci}")

            rows = []
            for _, r in df.iterrows():
                custo = r.get('custo_sku', 0) or 0
                preco_formula = round(custo + (custo * cenario['markup']), 2) if custo > 0 else 0
                preco_salvo = r.get('preco_venda')

                # Se simulação ativa e tem preço salvo, usar salvo; senão usar fórmula
                preco = preco_salvo if (simular and pd.notna(preco_salvo) and preco_salvo > 0) else preco_formula

                # Cálculo B2B: comissão + máquina de cartão + imposto
                comissao_total = cenario['comissao'] + cenario['maquina']
                margem_abs, margem_pct = calcular_margem(
                    preco, custo, comissao_total, 0, 0, cenario['imposto']
                )
                sinal = semaforo(margem_pct, r.get('margem_minima'), r.get('margem_desejavel'))

                row_data = {
                    'sku': r['sku'], 'produto': r['nome'], 'categoria': r['categoria'],
                    'custo_sku': custo,
                    'mc_esperada': r.get('margem_desejavel'), 'mc_minima': r.get('margem_minima'),
                    'preco_formula': preco_formula,
                    'preco_venda': preco if simular else preco_formula,
                    'comissao_pct': cenario['comissao'],
                    'maquina_pct': cenario['maquina'],
                    'imposto_pct': cenario['imposto'],
                    'margem_pct': margem_pct, 'margem_abs': margem_abs, 'sinal': sinal,
                }
                rows.append(row_data)

            df_display = pd.DataFrame(rows)

            col_config_b2b = {
                'sku': st.column_config.TextColumn("SKU", disabled=True, width="small"),
                'produto': st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                'categoria': st.column_config.TextColumn("Cat.", disabled=True, width="small"),
                'preco_formula': st.column_config.NumberColumn(
                    "Preço Base", format="R$ %.2f", disabled=True, width="small",
                    help=f"Custo + {cenario['markup']*100:.0f}%"
                ),
                'preco_venda': st.column_config.NumberColumn(
                    "🟠 Preço" if simular else "Preço",
                    format="R$ %.2f", min_value=0, width="small",
                    disabled=not simular
                ),
                'comissao_pct': st.column_config.NumberColumn("Comissão", format="%.0f%%", disabled=True, width="tiny"),
                'maquina_pct': st.column_config.NumberColumn("Máquina", format="%.0f%%", disabled=True, width="tiny"),
                'imposto_pct': st.column_config.NumberColumn("Imposto", format="%.0f%%", disabled=True, width="tiny"),
                'margem_pct': st.column_config.NumberColumn("Margem %", format="%.1f%%", disabled=True, width="small"),
                'margem_abs': st.column_config.NumberColumn("Margem R$", format="R$ %.2f", disabled=True, width="small"),
                'sinal': st.column_config.TextColumn("🚦", disabled=True, width="tiny"),
            }

            col_order_b2b = ['sinal', 'sku', 'produto', 'categoria']
            if perfil != "GESTOR":
                col_order_b2b.extend(['custo_sku', 'mc_esperada', 'mc_minima'])
                col_config_b2b['custo_sku'] = st.column_config.NumberColumn("Custo", format="R$ %.2f", disabled=True, width="small")
                col_config_b2b['mc_esperada'] = st.column_config.NumberColumn("MC Esp.", format="%.0f%%", disabled=True, width="tiny")
                col_config_b2b['mc_minima'] = st.column_config.NumberColumn("MC Mín.", format="%.0f%%", disabled=True, width="tiny")
            col_order_b2b.extend(['preco_formula', 'preco_venda', 'comissao_pct',
                                  'maquina_pct', 'imposto_pct', 'margem_pct'])
            if perfil != "GESTOR":
                col_order_b2b.append('margem_abs')

            edited = st.data_editor(df_display, column_config=col_config_b2b,
                                     column_order=col_order_b2b, use_container_width=True,
                                     hide_index=True, num_rows="fixed",
                                     key=f"editor_b2b_{ci}")

            if simular:
                if st.button(f"💾 Salvar preços simulados — {cenario['nome']}",
                             key=f"save_b2b_{ci}"):
                    df_s = edited[['sku', 'preco_venda']].copy()
                    df_s['comissao_override'] = None
                    df_s['frete_override'] = None
                    df_s['taxa_fixa_override'] = None
                    count = salvar_precos(engine, df_s, "B2B", "B2B", cenario['nome'], usuario)
                    if count > 0:
                        st.success(f"✅ {count} preços salvos!")
                        carregar_precos_salvos.clear()


# ============================================================
# PÁGINA PRINCIPAL
# ============================================================

def tabela_preco_page():
    """Função principal do módulo Tabela de Preço."""
    st.title("📊 Tabela de Preço")
    st.caption("Grade de precificação estratégica — simule preços e veja margens por marketplace • v2.1")

    # Verificar sessão
    usuario_dict = st.session_state.get('usuario', {})
    if not usuario_dict or not usuario_dict.get('role'):
        st.error("Sessão não encontrada. Faça login novamente.")
        return

    usuario = usuario_dict.get('username', '')
    perfil = usuario_dict.get('role', '')
    engine = get_engine()

    # Determinar tabs acessíveis
    tabs_disponiveis = TABS_ORDER.copy()

    if perfil == "GESTOR":
        lojas_gestor = carregar_lojas_gestor(engine, usuario)
        if lojas_gestor.empty:
            st.warning("Você não tem lojas atribuídas. Contate o administrador.")
            return
        # Filtrar tabs pelos marketplaces do gestor
        mkts_gestor = lojas_gestor['marketplace'].str.lower().unique().tolist()
        tabs_filtradas = []
        for tab in TABS_ORDER:
            tab_lower = tab.lower()
            # B2B acessível a todos
            if tab_lower == "b2b":
                tabs_filtradas.append(tab)
                continue
            for mkt in mkts_gestor:
                if tab_lower in mkt or mkt in tab_lower:
                    tabs_filtradas.append(tab)
                    break
        tabs_disponiveis = tabs_filtradas

    if not tabs_disponiveis:
        st.warning("Nenhum marketplace disponível para o seu perfil.")
        return

    # Criar tabs
    tab_objects = st.tabs(tabs_disponiveis)

    for i, tab_name in enumerate(tabs_disponiveis):
        with tab_objects[i]:
            try:
                if tab_name == "Mercado Livre":
                    render_tab_ml(engine, perfil, usuario)
                elif tab_name == "Shopee":
                    render_tab_shopee(engine, perfil, usuario)
                elif tab_name == "Amazon":
                    render_tab_amazon(engine, perfil, usuario)
                elif tab_name == "Shein":
                    render_tab_shein(engine, perfil, usuario)
                elif tab_name == "Magalu":
                    render_tab_magalu(engine, perfil, usuario)
                elif tab_name == "B2B":
                    render_tab_b2b(engine, perfil, usuario)
            except Exception as e:
                st.error(f"Erro ao carregar {tab_name}: {str(e)}")
                st.exception(e)
