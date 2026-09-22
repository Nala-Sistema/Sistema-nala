"""
FONTE DE DADOS POR LOJA E ASSUNTO — Sistema Nala

Lê `dim_fonte_dados` (criada em sql/fase1_vendas_shopee.sql, no repositório
nala-coletor-ml): uma linha por (loja, assunto) dizendo se a fonte daquele
assunto, para aquela loja, é 'api' ou 'upload'. Sem linha = 'upload' (é o
padrão de toda loja e todo assunto que ainda não foi migrado).

FASE 2 (22/09/2026): Shopee-Nala e Shopee-LPT viram 'api' para o assunto
'vendas' a partir de 01/09/2026. A Shopee-Yanni continua 'upload' — este
módulo nunca esconde uma loja sem que dim_fonte_dados diga isso.

Genérico de propósito (regra: o que se constrói para Full/ads/vendas serve ML
E Shopee — [[project_desenho_multimarketplace]]): quando outro assunto ou
outro marketplace migrar, nenhuma tela precisa de código novo, só uma linha
nova em dim_fonte_dados.

CACHE CURTO (60s, não os 5 min do resto do Sistema Nala): a troca de fonte é
rara mas tem que valer quase na hora — é o que impede alguém de subir upload
pra uma loja que acabou de virar 'api' só porque a tela ainda está com cache
velho.
"""

import pandas as pd
import streamlit as st


@st.cache_data(ttl=60, show_spinner=False)
def _cached_fonte_dados():
    """DataFrame cru de dim_fonte_dados. Cache 60s. Vazio se a tabela não
    existir ainda (Sistema Nala num ambiente sem a Fase 1/2 aplicada)."""
    from database_utils import get_engine
    engine = get_engine()
    try:
        return pd.read_sql(
            "SELECT marketplace, loja, assunto, fonte, api_desde FROM dim_fonte_dados",
            engine)
    except Exception:
        return pd.DataFrame(columns=['marketplace', 'loja', 'assunto', 'fonte', 'api_desde'])


def lojas_por_fonte(assunto: str, fonte: str) -> set:
    """Lojas com LINHA EXPLÍCITA em dim_fonte_dados dizendo fonte=`fonte`,
    neste assunto — leitura literal da tabela, sem interpretar ausência.

    Correção do auditor-tecnico (22/09/2026): a versão anterior, para
    fonte='upload', devolvia o CONJUNTO 'api' (o oposto do nome), porque
    tentava representar "loja sem linha também é upload" aqui dentro. Isso
    confundia quem lesse a chamada. Agora é literal: loja sem linha nenhuma
    NÃO aparece nem em 'api' nem em 'upload' por aqui. Quem precisa do
    "upload por padrão, incluindo quem não tem linha" usa
    `lojas_upload_permitidas`, que é onde essa regra pertence.
    """
    df = _cached_fonte_dados()
    se_assunto = df[df['assunto'] == assunto]
    return set(se_assunto.loc[se_assunto['fonte'] == fonte, 'loja'])


def lojas_upload_permitidas(todas_as_lojas, assunto: str = 'vendas') -> list:
    """De uma lista/Series de lojas, devolve só as que ainda podem subir
    upload neste assunto — ou seja, tira as que dim_fonte_dados marcou 'api'.
    Loja sem linha nenhuma continua permitida (upload é o padrão).

    `todas_as_lojas` pode ser lista, tupla ou coluna de DataFrame; a ordem
    original é preservada.
    """
    api = lojas_por_fonte(assunto, 'api')
    return [l for l in todas_as_lojas if l not in api]


def fonte_da_loja(loja: str, assunto: str = 'vendas') -> str:
    """'api' ou 'upload' para uma loja específica. Sem linha = 'upload'."""
    df = _cached_fonte_dados()
    linha = df[(df['loja'] == loja) & (df['assunto'] == assunto)]
    if linha.empty:
        return 'upload'
    return linha.iloc[0]['fonte']


def origem_da_venda(loja: str) -> str:
    """Texto curto para avisar na tela de onde vêm as vendas desta loja."""
    df = _cached_fonte_dados()
    linha = df[(df['loja'] == loja) & (df['assunto'] == 'vendas') & (df['fonte'] == 'api')]
    if linha.empty:
        return ''
    desde = linha.iloc[0]['api_desde']
    desde_str = pd.to_datetime(desde).strftime('%d/%m/%Y') if pd.notna(desde) else '?'
    return (f"As vendas de **{loja}** vêm da API da Shopee desde {desde_str}. "
           f"Períodos antes disso continuam do upload.")
