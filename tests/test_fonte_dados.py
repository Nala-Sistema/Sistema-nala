"""
Testa fonte_dados.py — a regra de fonte única por loja e assunto (Fase 2 das
vendas da Shopee pela API, 22/09/2026).

Não abre banco nenhum: `_cached_fonte_dados()` é trocada por um DataFrame fixo
em cada teste.

Rodar:  python tests/test_fonte_dados.py   (ou: pytest tests/test_fonte_dados.py)
"""

import os
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fonte_dados as fd  # noqa: E402

DF = pd.DataFrame([
    {'marketplace': 'SHOPEE', 'loja': 'Shopee Lithouse(Nala)', 'assunto': 'vendas',
     'fonte': 'api', 'api_desde': pd.Timestamp('2026-09-01')},
    {'marketplace': 'SHOPEE', 'loja': 'Shopee-LPT', 'assunto': 'vendas',
     'fonte': 'api', 'api_desde': pd.Timestamp('2026-09-01')},
    {'marketplace': 'SHOPEE', 'loja': 'Shopee Litstore(Yanni)', 'assunto': 'vendas',
     'fonte': 'upload', 'api_desde': pd.NaT},
    {'marketplace': 'MERCADO LIVRE', 'loja': 'ML-Nala', 'assunto': 'ads',
     'fonte': 'api', 'api_desde': pd.Timestamp('2026-09-14')},
])

# Como a tabela fica DEPOIS da troca de fonte do ML (corte 01/10/2026). Serve
# para provar que nada aqui é específico da Shopee.
DF_COM_ML = pd.concat([DF, pd.DataFrame([
    {'marketplace': 'MERCADO LIVRE', 'loja': 'ML-LPT', 'assunto': 'vendas',
     'fonte': 'api', 'api_desde': pd.Timestamp('2026-10-01')},
    {'marketplace': 'MERCADO LIVRE', 'loja': 'ML-YanniSP', 'assunto': 'vendas',
     'fonte': 'upload', 'api_desde': pd.NaT},
])], ignore_index=True)


def _com_df(df=DF):
    return mock.patch.object(fd, '_cached_fonte_dados', return_value=df)


class LojasPorFonte(unittest.TestCase):
    def test_api_devolve_so_quem_tem_fonte_api_no_assunto(self):
        with _com_df():
            r = fd.lojas_por_fonte('vendas', 'api')
        self.assertEqual(r, {'Shopee Lithouse(Nala)', 'Shopee-LPT'})

    def test_upload_e_leitura_literal_so_quem_tem_linha_explicita(self):
        # Shopee-Yanni TEM linha explícita fonte='upload' no fixture: aparece.
        # Loja sem linha nenhuma NÃO aparece aqui (isso é trabalho de
        # lojas_upload_permitidas, não deste).
        with _com_df():
            r = fd.lojas_por_fonte('vendas', 'upload')
        self.assertEqual(r, {'Shopee Litstore(Yanni)'})

    def test_assunto_diferente_nao_mistura(self):
        with _com_df():
            r = fd.lojas_por_fonte('ads', 'api')
        self.assertEqual(r, {'ML-Nala'})

    def test_tabela_vazia_nao_quebra(self):
        vazio = pd.DataFrame(columns=['marketplace', 'loja', 'assunto', 'fonte', 'api_desde'])
        with _com_df(vazio):
            self.assertEqual(fd.lojas_por_fonte('vendas', 'api'), set())


class LojasUploadPermitidas(unittest.TestCase):
    def test_tira_so_as_que_sao_api_preserva_a_ordem(self):
        todas = ['Shopee-LPT', 'Shopee Litstore(Yanni)', 'Shopee Lithouse(Nala)']
        with _com_df():
            r = fd.lojas_upload_permitidas(todas)
        self.assertEqual(r, ['Shopee Litstore(Yanni)'])

    def test_shopee_yanni_nunca_e_escondida(self):
        with _com_df():
            r = fd.lojas_upload_permitidas(['Shopee Litstore(Yanni)'])
        self.assertEqual(r, ['Shopee Litstore(Yanni)'])

    def test_loja_sem_linha_nenhuma_continua_upload(self):
        with _com_df():
            r = fd.lojas_upload_permitidas(['Uma Loja Nova Sem Linha'])
        self.assertEqual(r, ['Uma Loja Nova Sem Linha'])

    def test_sem_dim_fonte_dados_nenhuma_loja_e_escondida(self):
        vazio = pd.DataFrame(columns=['marketplace', 'loja', 'assunto', 'fonte', 'api_desde'])
        with _com_df(vazio):
            r = fd.lojas_upload_permitidas(['Shopee Lithouse(Nala)', 'Shopee-LPT'])
        self.assertEqual(r, ['Shopee Lithouse(Nala)', 'Shopee-LPT'])


class FonteDaLoja(unittest.TestCase):
    def test_api_e_upload(self):
        with _com_df():
            self.assertEqual(fd.fonte_da_loja('Shopee-LPT'), 'api')
            self.assertEqual(fd.fonte_da_loja('Shopee Litstore(Yanni)'), 'upload')

    def test_sem_linha_e_upload(self):
        with _com_df():
            self.assertEqual(fd.fonte_da_loja('Loja Que Nao Existe'), 'upload')


class OrigemDaVenda(unittest.TestCase):
    def test_loja_api_traz_a_data(self):
        with _com_df():
            texto = fd.origem_da_venda('Shopee-LPT')
        self.assertIn('01/09/2026', texto)
        self.assertIn('Shopee-LPT', texto)

    def test_loja_upload_nao_traz_nada(self):
        with _com_df():
            self.assertEqual(fd.origem_da_venda('Shopee Litstore(Yanni)'), '')


class TextoDaOrigemPorMarketplace(unittest.TestCase):
    """O texto de `origem_da_venda` dizia "API da Shopee" fixo. Depois da troca
    de fonte do ML isso faria a tela mentir o nome do marketplace."""

    def test_shopee_diz_shopee(self):
        with _com_df(DF_COM_ML):
            texto = fd.origem_da_venda('Shopee-LPT')
        self.assertIn('API da Shopee', texto)
        self.assertIn('01/09/2026', texto)

    def test_mercado_livre_diz_mercado_livre(self):
        with _com_df(DF_COM_ML):
            texto = fd.origem_da_venda('ML-LPT')
        self.assertIn('API do Mercado Livre', texto)
        self.assertIn('01/10/2026', texto)
        self.assertNotIn('Shopee', texto)

    def test_loja_de_ml_ainda_no_upload_nao_traz_nada(self):
        with _com_df(DF_COM_ML):
            self.assertEqual(fd.origem_da_venda('ML-YanniSP'), '')

    def test_o_nome_do_marketplace_sai_de_maiusculas(self):
        self.assertEqual(fd.nome_do_marketplace('SHOPEE'), 'Shopee')
        self.assertEqual(fd.nome_do_marketplace('MERCADO LIVRE'), 'Mercado Livre')
        self.assertEqual(fd.nome_do_marketplace(None), '')

    def test_sem_marketplace_na_linha_nao_inventa_nome(self):
        df = pd.DataFrame([{'marketplace': None, 'loja': 'X', 'assunto': 'vendas',
                           'fonte': 'api', 'api_desde': pd.Timestamp('2026-10-01')}])
        with _com_df(df):
            texto = fd.origem_da_venda('X')
        self.assertIn('vêm da API desde', texto)

    def test_as_lojas_de_upload_do_ml_seguem_liberadas_na_tela(self):
        # A regra de fonte única, já genérica: só a loja marcada 'api' some.
        with _com_df(DF_COM_ML):
            permitidas = fd.lojas_upload_permitidas(
                ['ML-LPT', 'ML-YanniSP', 'Shopee-LPT', 'Shopee Litstore(Yanni)'])
        self.assertEqual(permitidas, ['ML-YanniSP', 'Shopee Litstore(Yanni)'])


if __name__ == '__main__':
    unittest.main()
