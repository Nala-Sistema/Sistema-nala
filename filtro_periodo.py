"""
filtro_periodo.py — Filtro de data padrão do sistema
Sistema Nala

Um só seletor de período para todas as telas: Hoje, Ontem, Últimos 7, 15 e
30 dias e Personalizado (de/até). Cada tela tinha o seu, com contas
diferentes — em alguns "Últimos 7 dias" ia de hoje−7 a hoje, que são oito
dias. Aqui a regra é uma e fica num lugar só.

REGRAS
  - "Últimos N dias" são N dias exatos, contando o último dia.
  - A data de referência é a de São Paulo, não a do servidor: o Streamlit
    Cloud roda em UTC, e às 22h no escritório o servidor já está no dia
    seguinte.
  - Quando a tela informa até quando o dado existe (`dado_ate`), os atalhos
    "Últimos N dias" terminam nesse dia, e não hoje. Ads do ML chega com um
    dia de atraso: sem isso, "Últimos 7 dias" teria só seis dias de gasto
    contra sete de venda, e o TACOS sairia menor do que é.
  - Hoje, Ontem e Personalizado respeitam o calendário, mas o fim é cortado
    em `dado_ate` e a tela avisa. Se nada do período tem dado, o componente
    devolve (None, None) e a tela não mostra número nenhum.

USO
    from filtro_periodo import filtro_periodo
    ini, fim = filtro_periodo("ads_ml", dado_ate=ultima_data)
    if ini is None:
        return
"""

from datetime import date, datetime, timedelta, timezone

import streamlit as st

OPCOES = ["Hoje", "Ontem", "Últimos 7 dias", "Últimos 15 dias",
          "Últimos 30 dias", "Personalizado"]

_DIAS = {"Últimos 7 dias": 7, "Últimos 15 dias": 15, "Últimos 30 dias": 30}


def hoje_brasil():
    """Data de hoje em São Paulo (fallback UTC−3: sem horário de verão desde 2019)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo('America/Sao_Paulo')).date()
    except Exception:
        return datetime.now(timezone(timedelta(hours=-3))).date()


def _como_data(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        import pandas as pd
        if pd.isna(v):
            return None
        return pd.to_datetime(v).date()
    except Exception:
        return None


def resolver_periodo(opcao, hoje=None, dado_ate=None):
    """
    Datas (início, fim) de um atalho, sem desenhar nada. Personalizado
    devolve (None, None): as datas vêm da tela.
    """
    hoje = hoje or hoje_brasil()
    if opcao == "Hoje":
        return hoje, hoje
    if opcao == "Ontem":
        d = hoje - timedelta(days=1)
        return d, d
    if opcao in _DIAS:
        fim = min(hoje, dado_ate) if dado_ate else hoje
        return fim - timedelta(days=_DIAS[opcao] - 1), fim
    return None, None


def filtro_periodo(key, dado_ate=None, padrao="Últimos 7 dias",
                   rotulo="📅 Período"):
    """
    Desenha o seletor e devolve (início, fim) já cortados em `dado_ate`.

    `key` precisa ser único por tela: é o prefixo das chaves do Streamlit.
    Devolve (None, None) quando o período escolhido não tem dado nenhum.
    """
    dado_ate = _como_data(dado_ate)
    hoje = hoje_brasil()

    c1, c2, c3 = st.columns([1.2, 1, 1])
    with c1:
        opcao = st.selectbox(rotulo, OPCOES, index=OPCOES.index(padrao),
                             key=f"{key}_periodo")

    if opcao == "Personalizado":
        ref = dado_ate or hoje
        with c2:
            ini = st.date_input("De", value=ref - timedelta(days=6),
                                format="DD/MM/YYYY", key=f"{key}_de")
        with c3:
            fim = st.date_input("Até", value=ref, format="DD/MM/YYYY",
                                key=f"{key}_ate")
        if ini > fim:
            st.error("A data **De** está depois da data **Até**.")
            return None, None
    else:
        ini, fim = resolver_periodo(opcao, hoje, dado_ate)
        with c2:
            st.caption(f"De **{ini:%d/%m/%Y}** até **{fim:%d/%m/%Y}** "
                       f"({(fim - ini).days + 1} dia(s))")

    if dado_ate is not None:
        with c3 if opcao != "Personalizado" else c1:
            st.caption(f"Dado disponível até **{dado_ate:%d/%m/%Y}**")
        if ini > dado_ate:
            st.warning(
                f"Ainda não há dado para {ini:%d/%m/%Y} — o último dia "
                f"disponível é **{dado_ate:%d/%m/%Y}**.")
            return None, None
        if fim > dado_ate:
            st.info(
                f"O dado vai só até **{dado_ate:%d/%m/%Y}**; o período foi "
                f"cortado para **{ini:%d/%m/%Y} a {dado_ate:%d/%m/%Y}**.")
            fim = dado_ate

    if (ini.year, ini.month) != (fim.year, fim.month):
        st.caption(
            f"⚠️ O período junta dois meses ({ini:%m/%Y} e {fim:%m/%Y}). "
            "Para fechamento, use Personalizado dentro de um mês só.")

    return ini, fim
