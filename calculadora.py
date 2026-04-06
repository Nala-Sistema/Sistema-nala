"""
MÓDULO: Calculadora de Preços e Simulador de Viabilidade
Sistema Nala v3.1

Duas abas:
  1. Gestão de Promoções — SKU existente, simula desconto e margem
  2. Simulador de Viabilidade — produto novo, calcula preço alvo por marketplace

Regras de taxas (editáveis):
  ML Clássico:  R$6,50 fixo + 12% | Acima R$78,99: fixo → frete
  ML Premium:   R$6,50 fixo + 17% | Acima R$78,99: fixo → frete
  Amazon DBA:   R$6,50 fixo + 12% | Acima R$78,99: fixo → frete
  Amazon FBA:   R$5,50 fixo + 12% | Acima R$78,99: fixo → frete
  Magalu:       R$5,00 fixo + 14,8% | Acima R$78,99: fixo → frete
  Shein:        R$5,00 fixo + 16% | Sem regra de frete
  Shopee:       Tabela escalonada (20%+R$4 / 14%+R$16 / 14%+R$20 / 14%+R$26)

Preço Fake: Preço inflado para suportar cupom de desconto.
  Fórmula: Preço_Fake = Preço_Alvo / (1 - %_Cupom)
"""

import streamlit as st
import pandas as pd
from database_utils import get_engine


# ============================================================
# FORMATADORES BR
# ============================================================

def _fmt_brl(valor):
    """Formata número para R$ 1.234,56"""
    if valor is None:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(valor):
    """Formata número para 12,50%"""
    if valor is None:
        return "0,00%"
    return f"{valor:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def _parse_numero(valor_str, default=0.0):
    """Converte string BR (vírgula) para float."""
    if valor_str is None:
        return default
    try:
        s = str(valor_str).strip().replace("R$", "").replace("%", "").strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


# ============================================================
# REGRAS DE MARKETPLACE — DEFAULTS EDITÁVEIS
# ============================================================

MARKETPLACES_CONFIG = {
    'ML Clássico': {
        'comissao_pct': 12.0,
        'taxa_fixa': 6.50,
        'frete_acima_79': 6.50,
        'tem_regra_frete': True,
        'limite_frete': 78.99,
    },
    'ML Premium': {
        'comissao_pct': 17.0,
        'taxa_fixa': 6.50,
        'frete_acima_79': 6.50,
        'tem_regra_frete': True,
        'limite_frete': 78.99,
    },
    'Amazon DBA': {
        'comissao_pct': 12.0,
        'taxa_fixa': 6.50,
        'frete_acima_79': 6.50,
        'tem_regra_frete': True,
        'limite_frete': 78.99,
    },
    'Amazon FBA': {
        'comissao_pct': 12.0,
        'taxa_fixa': 5.50,
        'frete_acima_79': 5.50,
        'tem_regra_frete': True,
        'limite_frete': 78.99,
    },
    'Magalu': {
        'comissao_pct': 14.8,
        'taxa_fixa': 5.00,
        'frete_acima_79': 5.00,
        'tem_regra_frete': True,
        'limite_frete': 78.99,
    },
    'Shein': {
        'comissao_pct': 16.0,
        'taxa_fixa': 5.00,
        'frete_acima_79': 0.0,
        'tem_regra_frete': False,
        'limite_frete': 0,
    },
}

# Shopee: tabela escalonada (taxa %, fixo R$, limite superior)
SHOPEE_FAIXAS = [
    (79.99,       20.0, 4.00),
    (99.99,       14.0, 16.00),
    (199.99,      14.0, 20.00),
    (499.99,      14.0, 26.00),
    (float('inf'), 14.0, 26.00),
]


# ============================================================
# FUNÇÕES DE CÁLCULO
# ============================================================

def _calcular_comissao_shopee(preco_unitario):
    """Calcula comissão Shopee pela tabela escalonada (por unidade)."""
    for limite, taxa_pct, fixo in SHOPEE_FAIXAS:
        if preco_unitario <= limite:
            return preco_unitario * (taxa_pct / 100) + fixo
    return preco_unitario * 0.14 + 26.00


def _calcular_taxas_marketplace(nome_mkt, preco_venda, config_editada):
    """
    Calcula taxas totais de um marketplace para um preço de venda.

    Args:
        nome_mkt: nome do marketplace (chave do dict)
        preco_venda: preço de venda unitário
        config_editada: dict com valores editados pelo usuário

    Returns:
        dict com {comissao, taxa_fixa_ou_frete, total_taxas}
    """
    if nome_mkt == 'Shopee':
        total = _calcular_comissao_shopee(preco_venda)
        return {
            'comissao': preco_venda * 0.20 if preco_venda <= 79.99 else preco_venda * 0.14,
            'taxa_fixa_ou_frete': total - (preco_venda * 0.20 if preco_venda <= 79.99 else preco_venda * 0.14),
            'total_taxas': total,
            'label_fixo': 'Taxa fixa',
        }

    cfg = config_editada.get(nome_mkt, MARKETPLACES_CONFIG.get(nome_mkt, {}))
    comissao_pct = cfg.get('comissao_pct', 12.0)
    taxa_fixa = cfg.get('taxa_fixa', 5.0)
    frete_valor = cfg.get('frete_acima_79', taxa_fixa)
    tem_regra = cfg.get('tem_regra_frete', False)
    limite = cfg.get('limite_frete', 78.99)

    comissao = preco_venda * (comissao_pct / 100)

    if tem_regra and preco_venda > limite:
        # Acima do limite: taxa fixa não cobra, cobra frete
        fixo_ou_frete = frete_valor
        label = 'Frete'
    else:
        # Abaixo do limite: cobra taxa fixa normal
        fixo_ou_frete = taxa_fixa
        label = 'Taxa fixa'

    return {
        'comissao': comissao,
        'taxa_fixa_ou_frete': fixo_ou_frete,
        'total_taxas': comissao + fixo_ou_frete,
        'label_fixo': label,
    }


def _calcular_margem(preco_venda, custo_merc, imposto_pct, taxas_mkt, frete_manual=0, outros_custos=0):
    """
    Calcula margem líquida.

    Returns:
        dict com {margem_rs, margem_pct, valor_imposto, receita_liquida}
    """
    if preco_venda <= 0:
        return {'margem_rs': 0, 'margem_pct': 0, 'valor_imposto': 0, 'receita_liquida': 0}

    valor_imposto = preco_venda * (imposto_pct / 100)
    total_deducoes = taxas_mkt + valor_imposto + frete_manual + outros_custos
    receita_liquida = preco_venda - total_deducoes
    margem_rs = receita_liquida - custo_merc
    margem_pct = (margem_rs / preco_venda) * 100

    return {
        'margem_rs': margem_rs,
        'margem_pct': margem_pct,
        'valor_imposto': valor_imposto,
        'receita_liquida': receita_liquida,
    }


def _calcular_preco_alvo(custo_merc, imposto_pct, comissao_pct, taxa_fixa, frete_manual, outros_custos, margem_desejada_pct):
    """
    Calcula o preço de venda necessário para atingir uma margem desejada.

    Fórmula derivada:
      margem = preco - (preco * comissao%) - (preco * imposto%) - taxa_fixa - frete - outros - custo
      margem = preco * margem_desejada%
      Resolvendo para preco:
        preco * margem% = preco - preco*(comissao% + imposto%) - taxa_fixa - frete - outros - custo
        preco * margem% - preco + preco*(comissao% + imposto%) = -(taxa_fixa + frete + outros + custo)
        preco * (margem% - 1 + comissao% + imposto%) = -(custos_fixos)
        preco = custos_fixos / (1 - comissao% - imposto% - margem%)
    """
    custos_fixos = custo_merc + taxa_fixa + frete_manual + outros_custos
    denominador = 1 - (comissao_pct / 100) - (imposto_pct / 100) - (margem_desejada_pct / 100)

    if denominador <= 0:
        return None  # Impossível atingir essa margem

    return custos_fixos / denominador


def _calcular_preco_fake(preco_alvo, cupom_pct):
    """Calcula preço fake para suportar cupom."""
    if cupom_pct >= 100 or cupom_pct < 0:
        return preco_alvo
    return preco_alvo / (1 - cupom_pct / 100)


# ============================================================
# COMPONENTES VISUAIS
# ============================================================

def _cor_margem(margem_pct):
    """Retorna cor baseada na margem."""
    if margem_pct < 5:
        return "#DC2626"  # Vermelho
    elif margem_pct < 10:
        return "#F59E0B"  # Laranja
    else:
        return "#10B981"  # Verde


def _mostrar_margem_colorida(label, margem_rs, margem_pct):
    """Exibe margem com cor de alerta."""
    cor = _cor_margem(margem_pct)
    st.markdown(f"""
    <div style="padding:12px;border-radius:8px;border-left:4px solid {cor};background:var(--color-background-secondary);margin-bottom:8px;">
        <div style="font-size:12px;color:var(--color-text-secondary);">{label}</div>
        <div style="font-size:20px;font-weight:600;color:{cor};">{_fmt_brl(margem_rs)}</div>
        <div style="font-size:14px;color:{cor};">{_fmt_pct(margem_pct)}</div>
    </div>
    """, unsafe_allow_html=True)


def _mostrar_preco_fake(preco_alvo, cupom_pct):
    """Exibe preço fake abaixo do preço alvo."""
    if cupom_pct > 0:
        preco_fake = _calcular_preco_fake(preco_alvo, cupom_pct)
        st.caption(
            f"Preço Fake ({_fmt_pct(cupom_pct)} cupom): **{_fmt_brl(preco_fake)}** "
            f"→ com desconto cai para {_fmt_brl(preco_alvo)}"
        )


# ============================================================
# BUSCA DE DADOS DO BANCO
# ============================================================

def _buscar_skus_para_calculadora(engine):
    """Busca SKUs ativos com custo para o selectbox."""
    query = """
        SELECT sku, nome, preco_a_ser_considerado
        FROM dim_produtos
        WHERE status = 'Ativo'
        ORDER BY sku
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        colunas = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception:
        return pd.DataFrame()


# ============================================================
# SIDEBAR — INPUTS DE TAXAS EDITÁVEIS
# ============================================================

def _sidebar_config_taxas():
    """
    Renderiza no sidebar os inputs editáveis de taxas por marketplace.
    Retorna dict com configs editadas.
    """
    config_editada = {}

    with st.sidebar:
        st.markdown("---")
        st.subheader("⚙️ Taxas por Marketplace")
        st.caption("Valores editáveis (pré-preenchidos com padrão)")

        for nome_mkt, defaults in MARKETPLACES_CONFIG.items():
            with st.expander(f"📌 {nome_mkt}", expanded=False):
                comissao = st.number_input(
                    "Comissão (%)", value=defaults['comissao_pct'],
                    min_value=0.0, max_value=100.0, step=0.1,
                    key=f"cfg_com_{nome_mkt}"
                )
                taxa_fixa = st.number_input(
                    "Taxa Fixa (R$)", value=defaults['taxa_fixa'],
                    min_value=0.0, step=0.50,
                    key=f"cfg_fix_{nome_mkt}"
                )

                frete_val = defaults['frete_acima_79']
                tem_regra = defaults['tem_regra_frete']

                if tem_regra:
                    frete_acima = st.number_input(
                        "Frete (acima R$78,99)", value=frete_val,
                        min_value=0.0, step=0.50,
                        key=f"cfg_frt_{nome_mkt}"
                    )
                else:
                    frete_acima = 0.0

                config_editada[nome_mkt] = {
                    'comissao_pct': comissao,
                    'taxa_fixa': taxa_fixa,
                    'frete_acima_79': frete_acima,
                    'tem_regra_frete': tem_regra,
                    'limite_frete': defaults['limite_frete'],
                }

        # Shopee à parte (tabela escalonada)
        with st.expander("📌 Shopee (escalonada)", expanded=False):
            st.caption("Tabela fixa — edite no código se necessário")
            for i, (limite, taxa, fixo) in enumerate(SHOPEE_FAIXAS):
                lim_txt = f"Até R${limite:.2f}" if limite < 9999 else "Acima R$500"
                st.text(f"{lim_txt}: {taxa}% + R${fixo:.2f}")

    return config_editada


# ============================================================
# ABA 1: GESTÃO DE PROMOÇÕES (SKU EXISTENTE)
# ============================================================

def _aba_gestao_promocoes(engine, config_taxas):
    """Aba 1: Simula desconto e margem para um SKU existente."""

    st.subheader("🏷️ Gestão de Promoções")
    st.caption("Selecione um SKU e simule descontos para ver o impacto na margem.")

    # ---- Buscar SKUs ----
    df_skus = _buscar_skus_para_calculadora(engine)

    if df_skus.empty:
        st.warning("Nenhum SKU ativo encontrado no banco.")
        return

    # ---- Seleção de SKU ----
    df_skus['_label'] = df_skus['sku'] + ' — ' + df_skus['nome'].fillna('')
    opcoes = df_skus['_label'].tolist()
    escolha = st.selectbox("🔍 Selecione o SKU:", opcoes, key="calc_sku_sel")

    if not escolha:
        return

    idx = opcoes.index(escolha)
    sku_row = df_skus.iloc[idx]
    custo_banco = float(sku_row['preco_a_ser_considerado'] or 0)

    st.info(f"**SKU:** {sku_row['sku']} | **Custo Mercadoria (banco):** {_fmt_brl(custo_banco)}")

    # ---- Inputs manuais ----
    st.markdown("---")
    st.markdown("**Parâmetros da Simulação**")

    col1, col2, col3, col4 = st.columns(4)

    imposto_pct = col1.number_input(
        "Imposto (%)", value=10.0, min_value=0.0, max_value=100.0, step=0.5,
        key="promo_imposto"
    )
    frete_manual = col2.number_input(
        "Frete Manual (R$)", value=0.0, min_value=0.0, step=0.50,
        key="promo_frete"
    )
    outros_custos = col3.number_input(
        "Outros Custos (R$)", value=0.0, min_value=0.0, step=0.50,
        key="promo_outros"
    )
    cupom_pct = col4.number_input(
        "Cupom Desconto (%)", value=10.0, min_value=0.0, max_value=99.0, step=1.0,
        key="promo_cupom"
    )

    st.markdown("---")
    st.markdown("**Preço de Venda e Desconto**")

    col_a, col_b = st.columns(2)
    preco_venda = col_a.number_input(
        "Preço de Venda (R$)", value=0.0, min_value=0.0, step=1.0,
        key="promo_preco"
    )
    desconto_pct = col_b.number_input(
        "Desconto (%)", value=0.0, min_value=0.0, max_value=99.0, step=1.0,
        key="promo_desconto"
    )

    if preco_venda <= 0:
        st.info("Insira o preço de venda para ver a simulação.")
        return

    # Aplicar desconto
    preco_com_desconto = preco_venda * (1 - desconto_pct / 100)

    st.markdown("---")
    st.markdown(f"**Resultado: Preço com desconto = {_fmt_brl(preco_com_desconto)}**")

    # ---- Calcular margem para cada marketplace ----
    # Adicionar Shopee à lista
    todos_mkts = list(MARKETPLACES_CONFIG.keys()) + ['Shopee']

    cols = st.columns(4)
    col_idx = 0

    for nome_mkt in todos_mkts:
        taxas = _calcular_taxas_marketplace(nome_mkt, preco_com_desconto, config_taxas)
        margem = _calcular_margem(
            preco_com_desconto, custo_banco, imposto_pct,
            taxas['total_taxas'], frete_manual, outros_custos
        )

        with cols[col_idx % 4]:
            _mostrar_margem_colorida(nome_mkt, margem['margem_rs'], margem['margem_pct'])

            # Breakdown
            with st.expander("Detalhes", expanded=False):
                st.text(f"Preço:      {_fmt_brl(preco_com_desconto)}")
                st.text(f"Comissão:   {_fmt_brl(taxas['comissao'])}")
                st.text(f"{taxas['label_fixo']}:  {_fmt_brl(taxas['taxa_fixa_ou_frete'])}")
                st.text(f"Imposto:    {_fmt_brl(margem['valor_imposto'])}")
                st.text(f"Frete Man:  {_fmt_brl(frete_manual)}")
                st.text(f"Outros:     {_fmt_brl(outros_custos)}")
                st.text(f"Custo Merc: {_fmt_brl(custo_banco)}")
                st.text(f"─────────────────")
                st.text(f"Margem:     {_fmt_brl(margem['margem_rs'])}")

        col_idx += 1

    # ---- Preço Fake ----
    if cupom_pct > 0:
        st.markdown("---")
        st.markdown("**Preço Fake (para suportar cupom)**")
        _mostrar_preco_fake(preco_com_desconto, cupom_pct)


# ============================================================
# ABA 2: SIMULADOR DE VIABILIDADE (PRODUTO NOVO)
# ============================================================

def _aba_simulador_viabilidade(config_taxas):
    """Aba 2: Calcula preço alvo por marketplace para margens desejadas."""

    st.subheader("🚀 Simulador de Viabilidade")
    st.caption("Insira os custos do produto e veja qual preço praticar em cada marketplace.")

    # ---- Inputs de custo ----
    st.markdown("**1. Custos do Produto**")

    col1, col2, col3, col4 = st.columns(4)

    custo_merc = col1.number_input(
        "Custo Mercadoria (R$)", value=0.0, min_value=0.0, step=1.0,
        key="viab_custo"
    )
    imposto_pct = col2.number_input(
        "Imposto (%)", value=10.0, min_value=0.0, max_value=100.0, step=0.5,
        key="viab_imposto"
    )
    frete_estimado = col3.number_input(
        "Frete Estimado (R$)", value=0.0, min_value=0.0, step=0.50,
        key="viab_frete"
    )
    outros_custos = col4.number_input(
        "Outros Custos (R$)", value=0.0, min_value=0.0, step=0.50,
        key="viab_outros"
    )

    if custo_merc <= 0:
        st.info("Insira o custo da mercadoria para ver a simulação.")
        return

    # ---- Metas de margem editáveis ----
    st.markdown("---")
    st.markdown("**2. Metas de Margem (editáveis)**")

    col_m1, col_m2, col_m3, col_cupom = st.columns(4)
    margem_1 = col_m1.number_input("Margem M1 (%)", value=5.0, min_value=0.0, max_value=90.0, step=1.0, key="viab_m1")
    margem_2 = col_m2.number_input("Margem M2 (%)", value=10.0, min_value=0.0, max_value=90.0, step=1.0, key="viab_m2")
    margem_3 = col_m3.number_input("Margem M3 (%)", value=20.0, min_value=0.0, max_value=90.0, step=1.0, key="viab_m3")
    cupom_pct = col_cupom.number_input("Cupom (%)", value=10.0, min_value=0.0, max_value=99.0, step=1.0, key="viab_cupom")

    margens = [
        ('M1', margem_1),
        ('M2', margem_2),
        ('M3', margem_3),
    ]

    # ---- Tabela de resultados ----
    st.markdown("---")
    st.markdown("**3. Preço de Venda Necessário por Marketplace**")

    # Montar lista de todos os marketplaces
    todos_mkts = list(MARKETPLACES_CONFIG.keys()) + ['Shopee']

    # Construir tabela
    linhas = []

    for nome_mkt in todos_mkts:
        cfg = config_taxas.get(nome_mkt, MARKETPLACES_CONFIG.get(nome_mkt, {}))
        comissao_pct = cfg.get('comissao_pct', 12.0) if nome_mkt != 'Shopee' else 0
        taxa_fixa_val = cfg.get('taxa_fixa', 5.0) if nome_mkt != 'Shopee' else 0

        linha = {'Marketplace': nome_mkt}

        for label_m, margem_pct in margens:
            if nome_mkt == 'Shopee':
                # Shopee: precisa iterar porque a taxa depende do preço
                preco = _resolver_preco_shopee(
                    custo_merc, imposto_pct, frete_estimado, outros_custos, margem_pct
                )
            else:
                preco = _calcular_preco_alvo(
                    custo_merc, imposto_pct, comissao_pct,
                    taxa_fixa_val, frete_estimado, outros_custos, margem_pct
                )

            if preco and preco > 0:
                # Verificar regra de frete (acima de 78,99)
                if nome_mkt != 'Shopee' and cfg.get('tem_regra_frete', False):
                    if preco > cfg.get('limite_frete', 78.99):
                        # Recalcular com frete em vez de taxa fixa
                        frete_mkt = cfg.get('frete_acima_79', taxa_fixa_val)
                        preco = _calcular_preco_alvo(
                            custo_merc, imposto_pct, comissao_pct,
                            frete_mkt, frete_estimado, outros_custos, margem_pct
                        )

                linha[f'Preço {label_m}'] = _fmt_brl(preco) if preco else '—'
                linha[f'Fake {label_m}'] = _fmt_brl(_calcular_preco_fake(preco, cupom_pct)) if preco and cupom_pct > 0 else '—'
            else:
                linha[f'Preço {label_m}'] = 'Inviável'
                linha[f'Fake {label_m}'] = '—'

        linhas.append(linha)

    df_resultado = pd.DataFrame(linhas)

    # Exibir tabela
    st.dataframe(df_resultado, use_container_width=True, hide_index=True)

    # ---- Legenda ----
    st.caption(
        f"M1 = {_fmt_pct(margem_1)} | M2 = {_fmt_pct(margem_2)} | M3 = {_fmt_pct(margem_3)} | "
        f"Cupom = {_fmt_pct(cupom_pct)}"
    )
    if cupom_pct > 0:
        st.caption("'Fake' = preço inflado para que, com o cupom, o valor caia no preço alvo e a margem seja protegida.")

    # ---- Cards visuais por marketplace ----
    st.markdown("---")
    st.markdown("**Detalhamento por Marketplace**")

    for nome_mkt in todos_mkts:
        with st.expander(f"📊 {nome_mkt}", expanded=False):
            cfg = config_taxas.get(nome_mkt, MARKETPLACES_CONFIG.get(nome_mkt, {}))

            for label_m, margem_pct in margens:
                if nome_mkt == 'Shopee':
                    preco = _resolver_preco_shopee(
                        custo_merc, imposto_pct, frete_estimado, outros_custos, margem_pct
                    )
                    if preco:
                        taxas = _calcular_taxas_marketplace('Shopee', preco, config_taxas)
                else:
                    comissao_pct_val = cfg.get('comissao_pct', 12.0)
                    taxa_fixa_val = cfg.get('taxa_fixa', 5.0)

                    preco = _calcular_preco_alvo(
                        custo_merc, imposto_pct, comissao_pct_val,
                        taxa_fixa_val, frete_estimado, outros_custos, margem_pct
                    )

                    # Regra de frete
                    if preco and cfg.get('tem_regra_frete', False) and preco > cfg.get('limite_frete', 78.99):
                        frete_mkt = cfg.get('frete_acima_79', taxa_fixa_val)
                        preco = _calcular_preco_alvo(
                            custo_merc, imposto_pct, comissao_pct_val,
                            frete_mkt, frete_estimado, outros_custos, margem_pct
                        )

                    if preco:
                        taxas = _calcular_taxas_marketplace(nome_mkt, preco, config_taxas)

                if preco and preco > 0:
                    margem_calc = _calcular_margem(
                        preco, custo_merc, imposto_pct,
                        taxas['total_taxas'], frete_estimado, outros_custos
                    )
                    c1, c2, c3 = st.columns(3)
                    c1.metric(f"Preço Alvo ({label_m})", _fmt_brl(preco))
                    c2.metric("Margem R$", _fmt_brl(margem_calc['margem_rs']))
                    c3.metric("Margem %", _fmt_pct(margem_calc['margem_pct']))

                    if cupom_pct > 0:
                        _mostrar_preco_fake(preco, cupom_pct)
                else:
                    st.warning(f"{label_m}: Margem de {_fmt_pct(margem_pct)} é inviável neste marketplace.")


def _resolver_preco_shopee(custo_merc, imposto_pct, frete_manual, outros_custos, margem_pct):
    """
    Resolve preço alvo para Shopee por iteração (taxa depende do preço).
    Tenta cada faixa e verifica se o preço resultante cai nela.
    """
    for limite, taxa_pct, fixo in SHOPEE_FAIXAS:
        preco = _calcular_preco_alvo(
            custo_merc, imposto_pct, taxa_pct, fixo, frete_manual, outros_custos, margem_pct
        )
        if preco is None:
            continue

        # Verificar se o preço cai nesta faixa
        if limite == float('inf'):
            if preco > 499.99:
                return preco
        else:
            faixa_anterior = 0
            for lim, _, _ in SHOPEE_FAIXAS:
                if lim == limite:
                    break
                faixa_anterior = lim + 0.01

            if faixa_anterior <= preco <= limite:
                return preco

    # Fallback: usar última faixa
    return _calcular_preco_alvo(
        custo_merc, imposto_pct, 14.0, 26.0, frete_manual, outros_custos, margem_pct
    )


# ============================================================
# MAIN
# ============================================================

def main():
    st.header("🧮 Calculadora de Preços")

    engine = get_engine()

    # Sidebar com taxas editáveis
    config_taxas = _sidebar_config_taxas()

    # Tabs
    tab1, tab2 = st.tabs([
        "🏷️ Gestão de Promoções (SKU Existente)",
        "🚀 Simulador de Viabilidade (Produto Novo)"
    ])

    with tab1:
        _aba_gestao_promocoes(engine, config_taxas)

    with tab2:
        _aba_simulador_viabilidade(config_taxas)


if __name__ == "__main__":
    main()
