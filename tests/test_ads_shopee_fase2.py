"""Fase 2 dos ads da Shopee: uma fonte por loja, e nunca as duas.

O CENÁRIO QUE ESTE ARQUIVO GUARDA
  Na Shopee-Nala, o upload tem um relatório cobrindo 27/07 a 02/08/2026 e a API
  tem dia a dia desde 23/03/2026. Ou seja: existe um período em que as duas
  fontes descrevem o MESMO dinheiro, em tabelas diferentes, sem nenhuma chave
  em comum que faça o banco reclamar.

  Foi exatamente essa forma de erro que dobrou o TACOS em 17/09/2026. Por isso
  o teste central aqui não verifica um total: verifica que a tela, para uma
  loja 'api', NÃO ENCOSTA em fact_ads_shopee — nem para completar, nem para
  "mostrar o histórico junto".

COMO TESTA SEM BANCO
  `_query_df` e `_query_scalar` são trocados por um espião que guarda todo SQL
  executado e devolve valores combinados. O que se afirma é sobre o SQL que a
  tela decide emitir, que é onde a regra vive.
"""

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analise_ads_shopee as tela  # noqa: E402
import processar_ads_shopee as proc  # noqa: E402


NALA = 'Shopee Lithouse(Nala)'

# O período do teste cruza 02/08: começa dentro do relatório do upload
# (27/07 a 02/08) e termina depois dele.
INI, FIM = date(2026, 7, 28), date(2026, 8, 5)

# O que fact_ads_diario_loja tem nesse período. É o número que a tela precisa
# mostrar como "Gasto da Loja".
GASTO_DA_LOJA = 431.77
GASTO_NOS_ANUNCIOS = 300.55
RECEITA_DA_LOJA = 9000.00


class Espiao:
    """Guarda o SQL executado e devolve valores combinados por ordem."""

    def __init__(self, escalares):
        self.sqls = []
        self._escalares = list(escalares)

    def escalar(self, _engine, sql, params=None):
        self.sqls.append(sql)
        return self._escalares.pop(0) if self._escalares else None

    def df(self, _engine, sql, params=None):
        self.sqls.append(sql)
        return pd.DataFrame(columns=[
            'codigo_anuncio', 'titulo', 'skus', 'impressoes', 'cliques',
            'pedidos', 'gasto', 'receita_ads', 'venda', 'margem'])

    def tocou(self, tabela):
        return any(tabela in s for s in self.sqls)


def _streamlit_falso():
    """MagicMock de st, com `columns(n)` devolvendo n colunas de verdade."""
    st = mock.MagicMock()
    st.columns.side_effect = lambda n, *a, **k: [
        mock.MagicMock() for _ in range(n if isinstance(n, int) else len(n))]
    return st


def _rodar_dashboard_api(espiao):
    with mock.patch.object(tela, '_query_scalar', side_effect=espiao.escalar), \
            mock.patch.object(tela, '_query_df', side_effect=espiao.df), \
            mock.patch.object(tela, 'st', _streamlit_falso()), \
            mock.patch.dict(sys.modules, {'filtro_periodo': mock.MagicMock(
                filtro_periodo=mock.Mock(return_value=(INI, FIM)))}):
        tela._shopee_dashboard_api(object(), NALA)


class PeriodoQueCruzaAsDuasFontes(unittest.TestCase):
    def setUp(self):
        # ordem dos escalares: dado_ate, gasto da loja, gasto nos anúncios, receita
        self.espiao = Espiao([date(2026, 9, 23), GASTO_DA_LOJA,
                              GASTO_NOS_ANUNCIOS, RECEITA_DA_LOJA])
        _rodar_dashboard_api(self.espiao)

    def test_nao_encosta_em_fact_ads_shopee(self):
        # A regra inteira em uma linha.
        self.assertFalse(self.espiao.tocou('fact_ads_shopee'),
                         'a tela de uma loja "api" consultou a tabela do upload')

    def test_o_gasto_da_loja_vem_de_fact_ads_diario_loja(self):
        gasto = [s for s in self.espiao.sqls
                 if 'fact_ads_diario_loja' in s and 'SUM(gasto_ads)' in s]
        self.assertTrue(gasto, 'o gasto da loja não veio de fact_ads_diario_loja')

    def test_o_gasto_por_anuncio_vem_de_fact_ads_performance(self):
        self.assertTrue(self.espiao.tocou('fact_ads_performance'))

    def test_cruza_venda_pela_ponte_do_item_id(self):
        # Na Shopee o join NÃO é por fact_vendas_snapshot.codigo_anuncio (que
        # guarda o SKU); é por fact_pedidos_itens_marketplace.id_anuncio_plataforma.
        ponte = [s for s in self.espiao.sqls if 'id_anuncio_plataforma' in s]
        self.assertTrue(ponte, 'o cruzamento não usou a ponte do item_id')

    def test_a_linha_de_ads_nao_traz_sku_da_tabela_de_ads(self):
        # O SKU é derivado da venda, nunca lido de uma coluna de ads.
        for sql in self.espiao.sqls:
            if 'fact_ads_performance' in sql:
                self.assertNotIn('sku_match', sql)


class FonteUnicaPorLoja(unittest.TestCase):
    def _com_fontes(self, mapa):
        def fonte_da_loja(loja, assunto='vendas'):
            self.assertEqual(assunto, 'ads')
            return mapa.get(loja, 'upload')
        return mock.patch.dict(
            sys.modules, {'fonte_dados': mock.MagicMock(fonte_da_loja=fonte_da_loja)})

    def test_loja_api_some_das_abas_de_upload_e_match(self):
        with self._com_fontes({NALA: 'api', 'Shopee-LPT': 'api'}):
            disponiveis = tela._lojas_de_upload()
        nomes = {linha[2] for linha in disponiveis}
        self.assertNotIn(NALA, nomes)
        self.assertNotIn('Shopee-LPT', nomes)

    def test_a_yanni_continua_no_upload_e_nao_perde_a_tela(self):
        # A Shopee-Yanni não tem acesso à API. Ela não pode sumir da tela junto
        # com as outras duas.
        with self._com_fontes({NALA: 'api', 'Shopee-LPT': 'api'}):
            disponiveis = tela._lojas_de_upload()
        self.assertEqual([l[2] for l in disponiveis], ['Shopee Litstore(Yanni)'])

    def test_antes_da_virada_as_tres_continuam_no_upload(self):
        with self._com_fontes({}):
            self.assertEqual(len(tela._lojas_de_upload()), 3)


class TravaNoGravador(unittest.TestCase):
    """Esconder a loja do seletor não basta: quem chega por outro caminho passa
    pela tela, não pelo gravador."""

    def _df(self, lojas):
        return pd.DataFrame({'loja': lojas})

    def test_recusa_upload_de_loja_que_ja_e_api(self):
        with mock.patch.dict(sys.modules, {'fonte_dados': mock.MagicMock(
                fonte_da_loja=lambda loja, assunto='vendas': 'api')}):
            gravados, erros, dup = proc.gravar_ads_shopee(
                self._df(['Nala-Lit']), 'relatorio.csv', engine=None)
        self.assertEqual((gravados, dup), (0, 0))
        self.assertIn('já é a API', erros[0])
        self.assertIn('Shopee Lithouse(Nala)', erros[0])

    def test_traduz_o_nome_curto_para_o_nome_do_sistema(self):
        vistos = []

        def fonte_da_loja(loja, assunto='vendas'):
            vistos.append(loja)
            return 'upload'

        with mock.patch.dict(sys.modules, {'fonte_dados': mock.MagicMock(
                fonte_da_loja=fonte_da_loja)}):
            proc.lojas_bloqueadas_para_upload(self._df(['Nala-Lit', 'LPT Store']))
        # dim_fonte_dados guarda o nome do sistema, não o do relatório de ads
        self.assertEqual(sorted(vistos), ['Shopee Lithouse(Nala)', 'Shopee-LPT'])

    def test_loja_em_upload_passa_direto(self):
        with mock.patch.dict(sys.modules, {'fonte_dados': mock.MagicMock(
                fonte_da_loja=lambda loja, assunto='vendas': 'upload')}):
            self.assertEqual(
                proc.lojas_bloqueadas_para_upload(self._df(['litstoreshop'])), [])


if __name__ == '__main__':
    unittest.main()
