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


def modulo_ads_ml(engine):
    """
    Ponto de entrada da aba, chamado pelo roteador `analise_ads.py`.

    Todo o corpo roda dentro de try/except pelo mesmo motivo da tab de
    Despesas de Full: esta aba divide a tela com a de Shopee, que já está em
    uso, e uma exceção aqui não pode derrubar a outra.
    """
    try:
        _render(engine)
    except Exception as e:
        st.error(
            "Esta aba encontrou um erro e foi isolada — as demais continuam "
            "funcionando normalmente."
        )
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
