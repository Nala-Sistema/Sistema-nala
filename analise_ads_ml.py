"""
analise_ads_ml.py — Aba de Ads do Mercado Livre
Sistema Nala

Tela de entrada do gasto de mídia do ML, que até aqui não existia: a aba
"Outros" do módulo de Ads dizia "em breve" enquanto o ML respondia por mais
da metade da receita e a `fact_ads_performance` ficava vazia.

O arquivo aceito é o export "Relatório de anúncios" do painel de
Publicidade. A leitura e a gravação ficam em `processar_ads_ml.py`; aqui só
há tela.

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


def _fmt_int(v):
    try:
        return f"{int(v):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def _fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{float(v):.2f}%"
    except (TypeError, ValueError):
        return "—"


def _cruzamento_tacos_ml(engine):
    """
    Cruza o gasto de ads (por anúncio/MLB) com a venda real do produto no
    sistema, no mesmo período — MATCH EXATO POR MLB (o ML ads traz o MLB em
    codigo_anuncio, e as vendas ML guardam o mesmo MLB → SKU). Não precisa de
    título nem de match manual como o Shopee. Mostra o TACOS real (gasto ÷
    venda total do produto no período), que o painel do ML não dá.
    """
    st.subheader("🔗 Cruzamento Ads ↔ Vendas (TACOS real)")
    st.caption(
        "Cruzamento exato por **MLB**: liga cada anúncio ao SKU pela venda "
        "real do sistema. **TACOS real = investido ÷ venda total do produto "
        "no período** (o que o painel do ML não mostra)."
    )

    def _q(sql, params=()):
        conn = engine.raw_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            cur.close()
            conn.close()

    lojas = _q("SELECT DISTINCT loja FROM fact_ads_performance "
               "WHERE marketplace='MERCADO LIVRE' ORDER BY loja")['loja'].tolist()
    if not lojas:
        st.info("Nenhum relatório de ads de ML gravado ainda. Suba um na aba **Upload**.")
        return

    loja = st.selectbox("Loja", lojas, key="cruz_ml_loja")
    per = _q("SELECT DISTINCT periodo_inicio, periodo_fim FROM fact_ads_performance "
             "WHERE marketplace='MERCADO LIVRE' AND loja=%s "
             "ORDER BY periodo_fim DESC, periodo_inicio DESC", (loja,))
    if per.empty:
        st.info("Sem períodos para esta loja.")
        return
    opts = [f"{pi.strftime('%d/%m/%Y')} a {pf.strftime('%d/%m/%Y')}"
            for pi, pf in zip(per['periodo_inicio'], per['periodo_fim'])]
    i = st.selectbox("Período", range(len(opts)), format_func=lambda i: opts[i], key="cruz_ml_per")
    ini, fim = per['periodo_inicio'][i], per['periodo_fim'][i]

    df = _q("""
        SELECT a.codigo_anuncio AS mlb, a.titulo AS titulo, v.skus AS skus,
               a.gasto_ads AS gasto, v.receita_total AS venda_total,
               CASE WHEN v.receita_total>0 THEN a.gasto_ads/v.receita_total*100 END AS tacos_real
        FROM fact_ads_performance a
        LEFT JOIN LATERAL (
            SELECT string_agg(DISTINCT s.sku, ', ') AS skus,
                   SUM(s.valor_venda_efetivo) AS receita_total
            FROM fact_vendas_snapshot s
            WHERE UPPER(s.marketplace_origem)='MERCADO LIVRE' AND s.loja_origem=%s
              AND s.codigo_anuncio=a.codigo_anuncio
              AND s.data_venda BETWEEN %s AND %s
        ) v ON true
        WHERE a.marketplace='MERCADO LIVRE' AND a.loja=%s
          AND a.periodo_inicio=%s AND a.periodo_fim=%s AND a.gasto_ads>0
        ORDER BY a.gasto_ads DESC
    """, (loja, ini, fim, loja, ini, fim))

    if df.empty:
        st.info("Nenhum anúncio com gasto neste período.")
        return

    gasto_tot = float(df['gasto'].fillna(0).sum())
    venda_tot = float(df['venda_total'].fillna(0).sum())
    casaram = int((df['venda_total'].fillna(0) > 0).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Investimento", _fmt_brl(gasto_tot))
    c2.metric("TACOS da conta", _fmt_pct(100.0 * gasto_tot / venda_tot) if venda_tot else "—")
    c3.metric("Anúncios com gasto", _fmt_int(len(df)))
    c4.metric("Casaram (têm venda)", f"{casaram}/{len(df)}")

    show = df.copy()
    show['gasto'] = show['gasto'].apply(_fmt_brl)
    show['venda_total'] = show['venda_total'].apply(
        lambda v: _fmt_brl(v) if v and float(v) > 0 else '— sem venda —')
    show['tacos_real'] = show['tacos_real'].apply(
        lambda v: _fmt_pct(float(v)) if v is not None else 'N/A')
    show['skus'] = show['skus'].fillna('❌ não vendeu no período')
    show['titulo'] = show['titulo'].astype(str).str[:45]
    show = show[['mlb', 'titulo', 'skus', 'gasto', 'venda_total', 'tacos_real']]
    show.columns = ['MLB', 'Título', 'SKU(s)', 'Investido', 'Venda no período', 'TACOS real']
    st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption(
        "**TACOS real** = investido ÷ venda total do produto no período. "
        "*'sem venda'* = anúncio rodou mas o produto não vendeu (candidato a revisão). "
        "TACOS acima de ~3% no nível produto é sinal de atenção."
    )


def modulo_ads_ml(engine):
    """
    Ponto de entrada da aba, chamado pelo roteador `analise_ads.py`.
    Duas sub-abas: Upload (ingestão) e Cruzamento (TACOS real ads↔vendas).

    Cada uma roda em try/except próprio pelo mesmo motivo da tab de Despesas
    de Full: esta aba divide a tela com a de Shopee, que já está em uso, e uma
    exceção aqui não pode derrubar a outra.
    """
    sub_up, sub_cruz = st.tabs(["📤 Upload", "🔗 Cruzamento — TACOS real"])
    with sub_up:
        try:
            _render(engine)
        except Exception as e:
            st.error(
                "Esta aba encontrou um erro e foi isolada — as demais continuam "
                "funcionando normalmente."
            )
            st.caption(f"Detalhe técnico: {type(e).__name__}: {e}")
    with sub_cruz:
        try:
            _cruzamento_tacos_ml(engine)
        except Exception as e:
            st.error("O cruzamento encontrou um erro e foi isolado.")
            st.caption(f"Detalhe técnico: {type(e).__name__}: {e}")


def _render(engine):
    from processar_ads_ml import (
        ler_relatorio_ads_ml, gravar_ads_ml,
        garantir_schema_ads_performance, garantir_tabela_desempenho_anuncios,
    )

    st.subheader("🟡 Ads — Mercado Livre")

    ok, erro = garantir_schema_ads_performance(engine)
    if not ok:
        st.error(f"Não consegui preparar a tabela de ads: {erro}")
        return
    # Criada vazia aqui só para não custar depois. Nada nesta aba lê ou
    # escreve nela — o módulo de Ads não depende do relatório de desempenho.
    garantir_tabela_desempenho_anuncios(engine)

    st.caption(
        "Suba o **Relatório de anúncios** do painel de Publicidade do ML "
        "(o arquivo `report-pads_...xlsx`). O período vem de dentro do "
        "arquivo. Subir o mesmo período de novo **substitui** o anterior — "
        "pode corrigir e resubir à vontade."
    )

    try:
        lojas = pd.read_sql(
            "SELECT loja FROM dim_lojas WHERE marketplace = 'MERCADO LIVRE' "
            "AND COALESCE(visivel_no_painel, TRUE) ORDER BY loja", engine
        )['loja'].tolist()
    except Exception:
        lojas = []
    if not lojas:
        st.error("Não consegui carregar as lojas de Mercado Livre.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        loja = st.selectbox("Loja", lojas, key="ads_ml_loja")
    with col2:
        arquivo = st.file_uploader(
            "Relatório de anúncios (.xlsx)", type=["xlsx"], key="ads_ml_upl"
        )

    if not arquivo:
        _resumo_gravado(engine)
        return

    df, meta = ler_relatorio_ads_ml(
        arquivo, loja=loja, nome_arquivo=getattr(arquivo, 'name', ''))

    for aviso in meta['avisos']:
        st.warning(aviso)
    if df.empty:
        return

    # O nome do arquivo manda quando segue o padrão do Drive, porque é mais
    # confiável que a seleção manual — mas o gestor precisa saber disso.
    loja_final = meta['loja'] or loja
    if meta['loja'] and meta['loja'] != loja:
        st.warning(
            f"O nome do arquivo indica **{meta['loja']}**, mas você "
            f"selecionou **{loja}**. Vou gravar como {meta['loja']} — "
            "renomeie o arquivo se não for isso."
        )

    ini, fim = meta['periodo_inicio'], meta['periodo_fim']
    st.success(
        f"Lido: **{loja_final}** — período de "
        f"**{ini.strftime('%d/%m/%Y') if ini else '?'}** a "
        f"**{fim.strftime('%d/%m/%Y') if fim else '?'}**"
    )

    com_gasto = df[df['gasto_ads'].fillna(0) > 0]
    gasto = float(com_gasto['gasto_ads'].sum())
    receita_ads = float(com_gasto['receita_ads'].fillna(0).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Investimento", _fmt_brl(gasto))
    c2.metric("Receita de ads", _fmt_brl(receita_ads))
    c3.metric("Impressões", _fmt_int(com_gasto['impressoes'].fillna(0).sum()))
    c4.metric("Cliques", _fmt_int(com_gasto['cliques'].fillna(0).sum()))

    c1, c2, c3 = st.columns(3)
    c1.metric("Anúncios com gasto", _fmt_int(len(com_gasto)))
    c2.metric("Anúncios no arquivo", _fmt_int(len(df)))
    c3.metric("ACOS do período",
              _fmt_pct(100.0 * gasto / receita_ads) if receita_ads else "—")

    st.markdown("**Maiores investimentos do período**")
    top = (com_gasto.sort_values('gasto_ads', ascending=False)
           .head(15)
           [['codigo_anuncio', 'campanha', 'titulo', 'impressoes', 'cliques',
             'gasto_ads', 'receita_ads', 'vendas_diretas', 'vendas_indiretas']]
           .copy())
    top['gasto_ads'] = top['gasto_ads'].apply(_fmt_brl)
    top['receita_ads'] = top['receita_ads'].apply(_fmt_brl)
    top.columns = ['Anúncio', 'Campanha', 'Título', 'Impressões', 'Cliques',
                   'Investido', 'Receita ads', 'Vendas diretas',
                   'Vendas indiretas']
    st.dataframe(top, use_container_width=True, hide_index=True)

    incluir = st.checkbox(
        "Guardar também os anúncios sem gasto e sem impressão",
        value=False, key="ads_ml_todos",
        help="O relatório traz todo anúncio já cadastrado na campanha, e a "
             "maioria vem zerada. Por padrão eles não são gravados."
    )

    if st.button("💾 Gravar no sistema", type="primary", key="ads_ml_grava"):
        res = gravar_ads_ml(
            engine, df, loja_final, getattr(arquivo, 'name', ''),
            incluir_sem_gasto=incluir)
        if res['inseridos']:
            st.success(res['mensagem'])
        else:
            st.error(res['mensagem'])
        _resumo_gravado(engine)


def _resumo_gravado(engine):
    """Resumo do que já está no banco, por loja."""
    from processar_ads_ml import resumo_por_loja

    st.divider()
    st.markdown("### O que já está no sistema")
    try:
        df = resumo_por_loja(engine)
    except Exception as e:
        st.caption(f"Resumo indisponível: {type(e).__name__}: {e}")
        return

    if df is None or df.empty:
        st.info(
            "Nenhum relatório de ads do Mercado Livre foi carregado ainda. "
            "Suba o primeiro arquivo acima."
        )
        return

    d = df.copy()
    d['Período'] = (pd.to_datetime(d['de']).dt.strftime('%d/%m/%Y') + ' a '
                    + pd.to_datetime(d['ate']).dt.strftime('%d/%m/%Y'))
    d['Investido'] = d['gasto'].apply(_fmt_brl)
    d['Receita ads'] = d['receita_ads'].apply(_fmt_brl)
    d['Receita loja'] = d['receita_total'].apply(_fmt_brl)
    d['TACOS'] = d['tacos'].apply(_fmt_pct)
    d['Impressões'] = d['impressoes'].apply(_fmt_int)
    d['Cliques'] = d['cliques'].apply(_fmt_int)

    st.dataframe(
        d[['loja', 'Período', 'Investido', 'Receita ads', 'Receita loja',
           'TACOS', 'Impressões', 'Cliques']].rename(columns={'loja': 'Loja'}),
        use_container_width=True, hide_index=True
    )
    st.caption(
        "**TACOS** compara o investimento com a receita **total** da loja no "
        "período — é a conta que chega na margem. O **ACOS**, que aparece no "
        "arquivo, compara com a receita atribuída a ads e mede a eficiência "
        "da campanha. Os dois são úteis e não se substituem."
    )
