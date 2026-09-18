"""
analise_ads_ml.py — Aba de Ads do Mercado Livre
Sistema Nala

Tela de leitura do gasto de mídia do ML. Nada entra por aqui: desde 14/09
quem alimenta `fact_ads_performance` é o coletor da API, com uma linha por
dia e por anúncio.

POR QUE NÃO EXISTE MAIS ABA DE UPLOAD
  O upload gravava UMA linha cobrindo o intervalo inteiro do relatório (uma
  semana, por exemplo), e o índice `ux_ads_perf_periodo` inclui
  periodo_inicio e periodo_fim — então a linha semanal e as sete linhas
  diárias da API têm chaves diferentes e convivem na tabela sem conflito.
  O cruzamento soma tudo o que cai no intervalo, e o mesmo gasto entraria
  duas vezes: TACOS perto do dobro do real, sem nenhum aviso na tela. A
  porta de entrada foi fechada; o histórico não foi tocado.

Duas leituras convivem de propósito no resumo:
  - ACOS   — investimento sobre a receita ATRIBUÍDA a ads, que é a conta do
             próprio ML e mede a eficiência da campanha.
  - TACOS  — investimento sobre a receita TOTAL da loja, que é a conta que
             chega na margem.
Uma campanha pode ter ACOS ótimo e ainda assim pesar na margem; olhar só um
dos dois esconde metade do problema.
"""

import pandas as pd
import streamlit as st


def _fmt_brl(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{float(v):.2f}%"
    except (TypeError, ValueError):
        return "—"


def _q(engine, sql, params=()):
    conn = engine.raw_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()
        conn.close()


def _pct(num, den):
    try:
        num, den = float(num or 0), float(den or 0)
    except (TypeError, ValueError):
        return None
    return 100.0 * num / den if den else None


def _cruzamento_tacos_ml(engine):
    """
    Cruza o gasto de ads (por anúncio/MLB) com a venda real do produto no
    sistema, no mesmo intervalo de datas — MATCH EXATO POR MLB (o ML ads traz
    o MLB em codigo_anuncio, e as vendas ML guardam o mesmo MLB → SKU).

    Desde 14/09 o coletor grava uma linha por dia (periodo_inicio =
    periodo_fim). Por isso o período aqui é um intervalo livre e o gasto é
    SOMADO dentro dele; listar os pares (início, fim) do banco virava uma
    opção por dia e ninguém conseguia ver a semana.

    ACOS, TACOS e margem pós-ads aparecem juntos: ACOS mede a campanha,
    TACOS e margem mostram quanto do lucro o anúncio consumiu.
    """
    from filtro_periodo import filtro_periodo

    st.subheader("🔗 Cruzamento Ads ↔ Vendas (TACOS real)")
    st.caption(
        "Cruzamento exato por **MLB**: liga cada anúncio ao SKU pela venda "
        "real do sistema, somando gasto e venda no intervalo escolhido."
    )

    lojas = _q(engine, "SELECT DISTINCT loja FROM fact_ads_performance "
                       "WHERE marketplace='MERCADO LIVRE' ORDER BY loja")['loja'].tolist()
    if not lojas:
        st.info("Nenhum dado de ads de ML gravado ainda.")
        return

    loja = st.selectbox("Loja", lojas, key="cruz_ml_loja")
    lim = _q(engine, """
        SELECT (SELECT MAX(periodo_fim) FROM fact_ads_performance
                 WHERE marketplace='MERCADO LIVRE' AND loja=%s) AS ads_ate,
               (SELECT MAX(data_venda) FROM fact_vendas_snapshot
                 WHERE UPPER(marketplace_origem)='MERCADO LIVRE'
                   AND loja_origem=%s) AS vendas_ate
    """, (loja, loja))
    ads_ate, vendas_ate = lim['ads_ate'][0], lim['vendas_ate'][0]

    ini, fim = filtro_periodo("cruz_ml", dado_ate=ads_ate)
    if ini is None:
        return
    if vendas_ate is None or vendas_ate < fim:
        ate = vendas_ate.strftime('%d/%m/%Y') if vendas_ate else 'nunca'
        st.warning(
            f"As vendas de **{loja}** estão gravadas só até **{ate}**. Os "
            "dias sem venda deixam o TACOS maior e a margem menor do que são "
            "— suba as vendas antes de tirar conclusão.")

    df = _q(engine, """
        WITH ads AS (
            SELECT codigo_anuncio AS mlb, MAX(titulo) AS titulo,
                   SUM(gasto_ads) AS gasto, SUM(receita_ads) AS receita_ads
            FROM fact_ads_performance
            WHERE marketplace='MERCADO LIVRE' AND loja=%s
              AND periodo_inicio >= %s AND periodo_fim <= %s
            GROUP BY codigo_anuncio
            HAVING SUM(gasto_ads) > 0
        ), v AS (
            SELECT codigo_anuncio AS mlb,
                   string_agg(DISTINCT sku, ', ') AS skus,
                   SUM(valor_venda_efetivo) AS venda,
                   SUM(margem_total) AS margem
            FROM fact_vendas_snapshot
            WHERE UPPER(marketplace_origem)='MERCADO LIVRE' AND loja_origem=%s
              AND data_venda BETWEEN %s AND %s
            GROUP BY codigo_anuncio
        )
        SELECT ads.mlb, ads.titulo, v.skus, ads.gasto, ads.receita_ads,
               v.venda, v.margem
        FROM ads LEFT JOIN v ON v.mlb = ads.mlb
        ORDER BY ads.gasto DESC
    """, (loja, ini, fim, loja, ini, fim))

    if df.empty:
        st.info("Nenhum anúncio com gasto neste período.")
        return

    tot = _q(engine, """
        SELECT SUM(valor_venda_efetivo) AS venda, SUM(margem_total) AS margem
        FROM fact_vendas_snapshot
        WHERE UPPER(marketplace_origem)='MERCADO LIVRE' AND loja_origem=%s
          AND data_venda BETWEEN %s AND %s
    """, (loja, ini, fim))
    venda_loja = float(tot['venda'][0] or 0)
    margem_loja = float(tot['margem'][0] or 0)

    for c in ('gasto', 'receita_ads', 'venda', 'margem'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
    gasto_tot = df['gasto'].sum()
    receita_ads_tot = df['receita_ads'].fillna(0).sum()
    casaram = int((df['venda'].fillna(0) > 0).sum())

    st.markdown(f"**{loja}** — {ini:%d/%m/%Y} a {fim:%d/%m/%Y} "
                f"({(fim - ini).days + 1} dia(s))")
    c1, c2, c3 = st.columns(3)
    c1.metric("Investimento em ads", _fmt_brl(gasto_tot))
    c2.metric("ACOS (investido ÷ receita de ads)",
              _fmt_pct(_pct(gasto_tot, receita_ads_tot)))
    c3.metric("TACOS da loja (investido ÷ venda total)",
              _fmt_pct(_pct(gasto_tot, venda_loja)))
    c1, c2, c3 = st.columns(3)
    c1.metric("Venda total da loja", _fmt_brl(venda_loja))
    c2.metric("Margem antes de ads",
              _fmt_pct(_pct(margem_loja, venda_loja)),
              help=_fmt_brl(margem_loja))
    c3.metric("Margem pós-ads",
              _fmt_pct(_pct(margem_loja - gasto_tot, venda_loja)),
              help=_fmt_brl(margem_loja - gasto_tot))
    st.caption(f"Anúncios com gasto: **{len(df)}** • com venda no período: "
               f"**{casaram} de {len(df)}** • referência: TACOS até 2% na "
               "loja e 3% por anúncio.")

    show = pd.DataFrame({
        'MLB': df['mlb'],
        'Título': df['titulo'].astype(str).str[:45],
        'SKU(s)': df['skus'].fillna('❌ não vendeu no período'),
        'Investido': df['gasto'].apply(_fmt_brl),
        'Receita de ads': df['receita_ads'].apply(_fmt_brl),
        'ACOS': [_fmt_pct(_pct(g, r)) for g, r in zip(df['gasto'], df['receita_ads'])],
        'Venda no período': df['venda'].apply(
            lambda v: _fmt_brl(v) if pd.notna(v) and v > 0 else '— sem venda —'),
        'TACOS real': [_fmt_pct(_pct(g, v)) for g, v in zip(df['gasto'], df['venda'])],
        'Margem antes de ads': [_fmt_pct(_pct(m, v)) for m, v in zip(df['margem'], df['venda'])],
        'Margem pós-ads': [_fmt_pct(_pct((m if pd.notna(m) else 0) - g, v))
                           for m, g, v in zip(df['margem'], df['gasto'], df['venda'])],
    })
    st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption(
        "**ACOS** = investido ÷ receita que o ML atribui a ads (eficiência da "
        "campanha). **TACOS real** = investido ÷ venda total do produto no "
        "período. **Margem pós-ads** = (margem do produto − investido) ÷ "
        "venda; ainda **não desconta o Full**, então a margem real é menor. "
        "*'sem venda'* = anúncio rodou e o produto não vendeu (candidato a "
        "revisão)."
    )


def modulo_ads_ml(engine):
    """
    Ponto de entrada da aba, chamado pelo roteador `analise_ads.py`.
    Duas sub-abas, as duas de leitura: Cruzamento (ACOS/TACOS/margem por
    intervalo de datas) e ROAS objetivo (histórico das campanhas).

    Cada uma roda em try/except próprio pelo mesmo motivo da tab de Despesas
    de Full: esta aba divide a tela com a de Shopee, que já está em uso, e uma
    exceção aqui não pode derrubar a outra.
    """
    sub_cruz, sub_cfg = st.tabs(
        ["🔗 Cruzamento — TACOS real", "🎯 ROAS objetivo"])
    with sub_cruz:
        try:
            _cruzamento_tacos_ml(engine)
        except Exception as e:
            st.error("O cruzamento encontrou um erro e foi isolado.")
            st.caption(f"Detalhe técnico: {type(e).__name__}: {e}")
    with sub_cfg:
        try:
            _historico_campanhas(engine)
        except Exception as e:
            st.error("O histórico de campanhas encontrou um erro e foi isolado.")
            st.caption(f"Detalhe técnico: {type(e).__name__}: {e}")


def _historico_campanhas(engine):
    """
    Histórico do ROAS objetivo e do orçamento por campanha — só leitura.

    O coletor da API grava uma foto de `fact_ads_campanha_config` várias
    vezes por dia desde 14/09; a captura manual (bookmarklet / HTML salvo)
    saiu da tela porque duas fontes para o mesmo dado só geram conflito. As
    fotos manuais antigas continuam no banco e entram no histórico.

    A comparação usa a ÚLTIMA foto de cada dia: comparar foto com foto
    repetiria o mesmo evento a cada coleta. O ROAS objetivo é a alavanca que
    se opera no ML, então a pergunta desta tela é "quem mexeu, quando e de
    quanto para quanto" — o efeito aparece no Cruzamento, no período depois
    da data.
    """
    from filtro_periodo import filtro_periodo

    st.subheader("🎯 ROAS objetivo e orçamento por campanha")
    st.caption(
        "Leitura do que a API do ML captura todo dia. O relatório de ads traz "
        "o **resultado**; aqui fica a **alavanca** — e quando ela mudou."
    )

    lojas = _q(engine, "SELECT DISTINCT loja FROM fact_ads_campanha_config "
                       "WHERE marketplace='MERCADO LIVRE' ORDER BY loja")['loja'].tolist()
    if not lojas:
        st.info("Nenhuma foto de campanha gravada ainda.")
        return
    loja = st.selectbox("Loja", lojas, key="cfg_ml_loja")

    diario = _q(engine, """
        WITH d AS (
            SELECT DISTINCT ON (campanha, data_captura::date)
                   campanha, data_captura::date AS dia, data_captura,
                   roas_objetivo, orcamento_diario, status_campanha,
                   diagnostico_ml, arquivo_origem
            FROM fact_ads_campanha_config
            WHERE marketplace='MERCADO LIVRE' AND loja=%s
            ORDER BY campanha, data_captura::date, data_captura DESC
        )
        SELECT d.*,
               LAG(dia)              OVER w AS dia_ant,
               LAG(roas_objetivo)    OVER w AS roas_ant,
               LAG(orcamento_diario) OVER w AS orc_ant,
               LAG(status_campanha)  OVER w AS status_ant
        FROM d
        WINDOW w AS (PARTITION BY campanha ORDER BY dia)
    """, (loja,))
    if diario.empty:
        st.info("Nenhuma foto desta loja.")
        return

    ultima = pd.to_datetime(diario['data_captura']).max()
    st.caption(f"Última foto de **{loja}**: {ultima:%d/%m/%Y %H:%M}")

    # ── Mudanças no período ────────────────────────────────────────────
    st.markdown("### 🔔 O que mudou")
    # Linha de base = primeira foto COMPLETA. A captura manual de HTML pegava
    # só a página na tela (10 campanhas); usá-la como base faria as demais
    # aparecerem como "novas" no primeiro dia da API.
    api = diario[diario['arquivo_origem'].fillna('').str.startswith('API')]
    primeiro_dia = (api if not api.empty else diario)['dia'].min()
    ini, fim = filtro_periodo("cfg_ml", dado_ate=diario['dia'].max(),
                              padrao="Últimos 30 dias")
    if ini is not None:
        eventos = []
        for r in diario.itertuples(index=False):
            if not (ini <= r.dia <= fim):
                continue
            quando = r.dia.strftime('%d/%m/%Y')
            desde = (None if r.dia_ant is None or pd.isna(r.dia_ant)
                     else r.dia_ant.strftime('%d/%m/%Y'))
            if desde is None:
                if r.dia <= primeiro_dia:
                    continue
                eventos.append((r.dia, quando, r.campanha, 'Campanha nova',
                                '—', _fmt_roas(r.roas_objetivo), None))
                continue
            if _mudou(r.roas_ant, r.roas_objetivo):
                eventos.append((r.dia, quando, r.campanha,
                                'ROAS objetivo' + _seta(r.roas_ant, r.roas_objetivo),
                                _fmt_roas(r.roas_ant), _fmt_roas(r.roas_objetivo), desde))
            if _mudou(r.orc_ant, r.orcamento_diario):
                eventos.append((r.dia, quando, r.campanha,
                                'Orçamento diário' + _seta(r.orc_ant, r.orcamento_diario),
                                _fmt_brl(r.orc_ant), _fmt_brl(r.orcamento_diario), desde))
            if (r.status_ant and r.status_campanha
                    and r.status_ant != r.status_campanha):
                eventos.append((r.dia, quando, r.campanha, 'Status',
                                r.status_ant, r.status_campanha, desde))
        if eventos:
            ev = pd.DataFrame(eventos, columns=[
                'dia', 'Visto em', 'Campanha', 'Mudança', 'Antes', 'Depois',
                'Foto anterior'])
            ev = ev.sort_values(['dia', 'Campanha'], ascending=[False, True])
            st.dataframe(ev.drop(columns='dia').fillna('—'),
                         use_container_width=True, hide_index=True)
            st.caption(
                "**Visto em** é o dia da foto que mostrou a mudança; ela "
                "aconteceu entre a **foto anterior** e esse dia. Com foto "
                "diária, é o próprio dia (ou o anterior).")
        else:
            st.info("Nenhuma mudança de ROAS objetivo, orçamento ou status "
                    "neste período.")
        st.caption(f"Primeira foto completa desta loja: "
                   f"{primeiro_dia:%d/%m/%Y} — campanhas que já estavam nela "
                   "não contam como novas.")

    # ── Situação atual ─────────────────────────────────────────────────
    st.markdown("### Como está agora")
    dia_max = diario['dia'].max()
    atual = diario[diario['dia'] == dia_max].sort_values('campanha')
    st.dataframe(pd.DataFrame({
        'Campanha': atual['campanha'],
        'ROAS objetivo': atual['roas_objetivo'].apply(_fmt_roas),
        'Orçamento diário': atual['orcamento_diario'].apply(_fmt_brl),
        'Status': atual['status_campanha'].fillna('—'),
        'Sinal do ML': atual['diagnostico_ml'].fillna('—'),
    }), use_container_width=True, hide_index=True)
    st.caption(
        f"Foto de {dia_max:%d/%m/%Y}. O **orçamento** é teto, não gasto — o "
        "gasto real está no Cruzamento. **Sinal do ML** é a opinião da "
        "plataforma, que ganha quando você gasta mais: baliza, não veredito."
    )


def _mudou(antes, depois):
    if antes is None or depois is None or pd.isna(antes) or pd.isna(depois):
        return False
    return abs(float(antes) - float(depois)) > 1e-6


def _seta(antes, depois):
    return ' ↓' if float(depois) < float(antes) else ' ↑'


def _fmt_roas(v):
    if v is None or pd.isna(v):
        return '—'
    return f"{float(v):.1f}x".replace('.', ',')
