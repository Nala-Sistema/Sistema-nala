"""
analise_ads.py — Orquestrador de Análise de Ads
Sistema Nala — TAREFA 4

Papel deste arquivo: roteador de abas por marketplace.
A lógica completa de cada marketplace fica em arquivos separados:
  - analise_ads_shopee.py   (Shopee — implementado nas Tarefas 3A + 3B)
  - analise_ads_amazon.py   (Amazon — futuro)
  - analise_ads_<...>.py    (demais — futuros)

Mantém a função modulo_ads(engine) com o mesmo nome que app.py já chama,
portanto NÃO requer nenhuma alteração no app.py.
"""

import streamlit as st


def modulo_ads(engine):
    """Ponto de entrada do módulo de Análise de Ads (chamado pelo app.py)."""
    st.title("📊 Análise de Ads")
    st.caption("Meta TACOS: máximo 3% | Análise multi-marketplace de campanhas pagas")

    tab_shopee, tab_amazon, tab_outros = st.tabs([
        "🟠 Shopee", "📦 Amazon", "🔜 Outros"
    ])

    with tab_shopee:
        try:
            from analise_ads_shopee import modulo_ads_shopee
            modulo_ads_shopee(engine)
        except ImportError as e:
            st.error(
                "❌ Módulo `analise_ads_shopee.py` não encontrado ou com erro de import. "
                f"Detalhe: {str(e)[:200]}"
            )
        except Exception as e:
            st.error(f"❌ Erro ao carregar módulo Shopee: {str(e)[:300]}")

    with tab_amazon:
        st.info("📦 Módulo Amazon Ads em desenvolvimento.")
        st.caption(
            "Em breve: análise de campanhas Sponsored Products, Sponsored Brands e "
            "Sponsored Display, com TACOS e cruzamento por ASIN."
        )

    with tab_outros:
        st.info("🔜 Em breve.")
        st.caption(
            "Mercado Livre Ads, Magalu Ads e Shein Ads serão adicionados nas próximas releases."
        )
