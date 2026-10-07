"""
Testa o Ads na Margem Real (performance.py). Sem banco: a leitura e simulada.

Regras do Thiago (07/10/2026), num lugar so (ADS_SEM_GASTO):
  - ML e Shopee na API (dim_fonte_dados): gasto das tabelas da API;
  - Amazon e Magalu "ads pausado", TikTok "sem ads no mes", Shein "nao faz
    ads": R$ 0, nao bloqueiam;
  - Shopee Litstore (faz ads, sem fonte): continua "falta Ads";
  - mes antes da API: "falta", nunca soma com upload;
  - mes corrente: "Ads depois de dd/mm".

Rodar:  pytest tests/test_margem_real_ads.py
"""

import os
import sys
import types
import unittest
from datetime import date
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import performance as pf  # noqa: E402

SET_INI, SET_FIM = date(2026, 9, 1), date(2026, 9, 30)


class Regra(unittest.TestCase):
    def test_loja_na_api_com_mes_inteiro_fecha_e_mostra_centavos(self):
        v = pf.situacao_ads('MERCADO LIVRE', True, date(2026, 8, 1), 0.32,
                            SET_FIM, SET_INI, SET_FIM)
        self.assertEqual(v, (0.32, SET_FIM, None, None))

    def test_mes_antes_da_api_e_falta_sem_somar_upload(self):
        v = pf.situacao_ads('SHOPEE', True, date(2026, 3, 23), 500.0,
                            date(2026, 3, 31), date(2026, 3, 1), date(2026, 3, 31))
        self.assertEqual(v[0], 0.0)
        self.assertIn('API só desde 23/03/2026', v[2])

    def test_mes_corrente_e_parcial(self):
        v = pf.situacao_ads('MERCADO LIVRE', True, date(2026, 8, 1), 700.0,
                            date(2026, 10, 6), date(2026, 10, 1), date(2026, 10, 31))
        self.assertEqual(v[2], 'Ads depois de 06/10')
        self.assertEqual(v[0], 700.0)

    def test_marketplaces_sem_ads_nao_bloqueiam(self):
        for mkt, nota in (('AMAZON', 'ads pausado'), ('MAGALU', 'ads pausado'),
                          ('TIKTOK', 'sem ads no mês'), ('SHEIN', 'não faz ads')):
            self.assertEqual(pf.situacao_ads(mkt, False, None, None, None, SET_INI, SET_FIM),
                             (0.0, None, None, nota))

    def test_loja_que_faz_ads_sem_fonte_continua_falta(self):
        v = pf.situacao_ads('SHOPEE', False, None, None, None, SET_INI, SET_FIM)
        self.assertEqual(v, (0.0, None, 'Ads', None))

    def test_amazon_voltando_a_anunciar_vira_falta(self):
        with mock.patch.dict(pf.ADS_SEM_GASTO, clear=False):
            del pf.ADS_SEM_GASTO['AMAZON']
            self.assertEqual(pf.situacao_ads('AMAZON', False, None, None, None,
                                             SET_INI, SET_FIM)[2], 'Ads')


class Tela(unittest.TestCase):
    """Setembro/2026 com os numeros reais de gasto da API."""

    def setUp(self):
        self.saida = []
        s = self.saida
        self.st = types.SimpleNamespace(
            error=lambda m: s.append(('error', m)), warning=lambda m: s.append(('warning', m)),
            success=lambda m: s.append(('success', m)), info=lambda m: s.append(('info', m)),
            caption=lambda m: None,
            columns=lambda n: [types.SimpleNamespace(metric=lambda *a, **k: None)] * n,
            dataframe=lambda df, **k: s.append(('tabela', df)))
        perm = types.ModuleType('permissoes')
        perm.ve_todas_lojas = lambda: True
        perm.get_lojas_usuario = lambda e: None
        self.perm = mock.patch.dict(sys.modules, {'permissoes': perm})
        self.perm.start()

    def tearDown(self):
        self.perm.stop()

    def leitor(self, sql, engine, params=None):
        if 'fact_vendas_snapshot' in sql:
            return pd.DataFrame({
                'loja': ['ML-LPT', 'ML-YanniRJ', 'Shopee-LPT', 'Shopee Litstore(Yanni)',
                         'AMZ-LPT', 'TikTok-Nala'],
                'marketplace': ['MERCADO LIVRE', 'MERCADO LIVRE', 'SHOPEE', 'SHOPEE',
                                'AMAZON', 'TIKTOK'],
                'receita': [141912.0, 6172.0, 84586.0, 16542.0, 25039.0, 6441.0],
                'margem_contabil': [15000.0, 600.0, 9000.0, 1500.0, 2500.0, 500.0]})
        if 'fact_custos_extras' in sql:
            self.assertNotIn("tipo = 'ADS'), 0)", sql)  # ads nunca vem daqui
            return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ'],
                                 'armazenagem': [1363.58, 74.98], 'coleta': [839.06, 75.09],
                                 'antigo': [43.0, 70.0], 'full_ate': [SET_FIM, SET_FIM],
                                 'full_nao_atribuido': [17.16, 0.48], 'outros': [0.0, 1.38]})
        if 'dim_fonte_dados' in sql:
            return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ', 'Shopee-LPT']})
        self.assertNotIn('fact_ads_shopee', sql)  # upload antigo nunca e lido
        if 'GROUP BY loja' in sql:
            if params['m'] == 'MERCADO LIVRE':
                return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ'], 'gasto': [4016.0, 0.32],
                                     'ate': [SET_FIM, date(2026, 9, 20)]})
            return pd.DataFrame({'loja': ['Shopee-LPT'], 'gasto': [2305.25], 'ate': [SET_FIM]})
        ini = date(2026, 8, 1) if params['m'] == 'MERCADO LIVRE' else date(2026, 3, 23)
        return pd.DataFrame({'ini': [ini], 'ate': [SET_FIM]})

    def rodar(self):
        with mock.patch.object(pf, 'st', self.st), \
                mock.patch.object(pf.pd, 'read_sql', side_effect=self.leitor):
            pf._render_tab_margem_real(None, '2026-09')
        return next(df for t, df in self.saida if t == 'tabela').set_index('Loja')

    def test_setembro(self):
        t = self.rodar()
        self.assertEqual(t.loc['ML-LPT', 'Ads'], 'R$ 4.016,00')
        self.assertEqual(t.loc['ML-YanniRJ', 'Ads'], 'R$ 0,32')
        # ML: o "ate" e o da coleta do marketplace, nao o ultimo dia com gasto
        self.assertEqual(t.loc['ML-YanniRJ', 'Ads até'], '30/09')
        self.assertEqual(t.loc['Shopee-LPT', 'Ads'], 'R$ 2.305,25')
        self.assertTrue(t.loc['Shopee Litstore(Yanni)', 'Situação'].endswith(', Ads'))
        self.assertEqual(t.loc['AMZ-LPT', 'Ads'], 'ads pausado')
        self.assertNotIn('Ads', t.loc['AMZ-LPT', 'Situação'].replace('ads pausado', ''))
        self.assertEqual(t.loc['TikTok-Nala', 'Ads'], 'sem ads no mês')
        self.assertNotIn('Ads', t.loc['ML-LPT', 'Situação'])
        # o ads entra no custo lancado da loja
        self.assertIn('6.261,64', t.loc['ML-LPT', 'Custos lançados'])


if __name__ == '__main__':
    unittest.main()
