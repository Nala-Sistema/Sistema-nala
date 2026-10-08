"""
Testa o Ads na Margem Real (performance.py). Sem banco: a leitura e simulada.

Regras do Thiago (07-08/10/2026):
  - ML e Shopee na API (dim_fonte_dados): gasto das tabelas da API, so se a
    API cobre a LOJA desde o dia 1 do mes (mes antes = "falta", nunca soma
    com upload);
  - Shopee sem linha no mes = "falta Ads do mes"; dia faltando = buraco;
  - loja do ML sem linha no mes, coletor do ML rodando o mes todo (ML-YanniSP)
    = "sem ads no mes", R$ 0, nao bloqueia;
  - ADS_SEM_GASTO_LOJA por NOME EXATO (AMZ-*, Magalu-Nala, TikTok-Nala, Shein):
    R$ 0 com nota; loja nao listada = "falta Ads" (Shopee Litstore);
  - Full: ML, Shopee, Amazon e Magalu bloqueiam sem custo; TikTok e Shein nao;
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
    def test_loja_coberta_fecha_e_mostra_centavos(self):
        v = pf.situacao_ads('ML-YanniRJ', True, date(2026, 8, 1), 0.32,
                            SET_FIM, SET_INI, SET_FIM)
        self.assertEqual(v, (0.32, SET_FIM, None, None))

    def test_inicio_da_api_e_da_loja(self):
        v = pf.situacao_ads('ML-YanniRJ', True, date(2026, 9, 16), 0.32,
                            SET_FIM, SET_INI, SET_FIM)
        self.assertEqual(v[2], 'Ads (API só desde 16/09/2026)')

    def test_mes_corrente_e_parcial(self):
        v = pf.situacao_ads('ML-LPT', True, date(2026, 8, 1), 700.0,
                            date(2026, 10, 6), date(2026, 10, 1), date(2026, 10, 31))
        self.assertEqual((v[0], v[2]), (700.0, 'Ads depois de 06/10'))

    def test_buraco(self):
        v = pf.situacao_ads('Shopee-LPT', True, date(2026, 3, 23), 100.0, SET_FIM,
                            SET_INI, SET_FIM, buraco=date(2026, 9, 12))
        self.assertEqual(v[2], 'Ads (buraco em 12/09)')

    def test_sem_linha_coberta_nao_bloqueia(self):
        v = pf.situacao_ads('ML-YanniSP', True, date(2026, 9, 14), None,
                            date(2026, 10, 31), date(2026, 10, 1), date(2026, 10, 31),
                            sem_linha_coberta=True)
        self.assertEqual(v, (0.0, date(2026, 10, 31), None, 'sem ads no mês'))

    def test_lojas_sem_ads_pelo_nome_exato(self):
        for loja, nota in (('AMZ-LPT', 'ads pausado'), ('Magalu-Nala', 'ads pausado'),
                           ('TikTok-Nala', 'sem ads no mês'), ('Shein LPT', 'não faz ads')):
            self.assertEqual(pf.situacao_ads(loja, False, None, None, None, SET_INI, SET_FIM),
                             (0.0, None, None, nota))

    def test_loja_nao_listada_e_falta(self):
        for loja in ('Shopee Litstore(Yanni)', 'AMZ-Nova'):
            self.assertEqual(pf.situacao_ads(loja, False, None, None, None, SET_INI, SET_FIM),
                             (0.0, None, 'Ads', None))

    def test_primeiro_buraco(self):
        dias = {date(2026, 9, d) for d in range(1, 31) if d != 12}
        self.assertEqual(pf.primeiro_buraco(dias, SET_INI, SET_FIM), date(2026, 9, 12))
        self.assertIsNone(pf.primeiro_buraco(dias | {date(2026, 9, 12)}, SET_INI, SET_FIM))


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
        # Shopee-LPT: um buraco em 12/09; Shopee-Nala: sem nenhuma linha no mes
        self.dias_shopee = [date(2026, 9, d) for d in range(1, 31) if d != 12]
        self.ate_mkt = SET_FIM

    def tearDown(self):
        self.perm.stop()

    def leitor(self, sql, engine, params=None):
        if 'fact_vendas_snapshot' in sql:
            lojas = [('ML-LPT', 'MERCADO LIVRE', 141912.0), ('ML-YanniRJ', 'MERCADO LIVRE', 6172.0),
                     ('ML-YanniSP', 'MERCADO LIVRE', 1118.0), ('Shopee-LPT', 'SHOPEE', 84586.0),
                     ('Shopee Lithouse(Nala)', 'SHOPEE', 42750.0),
                     ('Shopee Litstore(Yanni)', 'SHOPEE', 16542.0), ('AMZ-LPT', 'AMAZON', 25039.0),
                     ('TikTok-Nala', 'TIKTOK', 6441.0)]
            return pd.DataFrame({'loja': [l for l, _, _ in lojas], 'marketplace': [m for _, m, _ in lojas],
                                 'receita': [r for _, _, r in lojas],
                                 'margem_contabil': [r * 0.1 for _, _, r in lojas]})
        if 'fact_custos_extras' in sql:
            self.assertNotIn("tipo = 'ADS'), 0)", sql)  # ads nunca vem daqui
            return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ', 'ML-YanniSP'],
                                 'armazenagem': [1363.58, 74.98, 31.04], 'coleta': [839.06, 75.09, 70.36],
                                 'antigo': [43.0, 70.0, 0.0], 'full_ate': [SET_FIM] * 3,
                                 'full_nao_atribuido': [17.16, 0.48, 0.09], 'outros': [0.0, 1.38, 0.0]})
        if 'dim_fonte_dados' in sql:
            return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ', 'ML-YanniSP', 'Shopee-LPT',
                                          'Shopee Lithouse(Nala)'],
                                 'api_desde': [date(2026, 9, 14)] * 3 + [date(2026, 3, 23)] * 2})
        self.assertNotIn('fact_ads_shopee', sql)  # upload antigo nunca e lido
        ml = params['m'] == 'MERCADO LIVRE'
        if 'GROUP BY loja, data' in sql:
            if ml:
                return pd.DataFrame({'loja': ['ML-LPT', 'ML-LPT', 'ML-YanniRJ'],
                                     'data': [date(2026, 9, 1), SET_FIM, date(2026, 9, 20)],
                                     'gasto': [2000.0, 2016.0, 0.32]})
            return pd.DataFrame({'loja': ['Shopee-LPT'] * len(self.dias_shopee),
                                 'data': self.dias_shopee,
                                 'gasto': [2305.25 / len(self.dias_shopee)] * len(self.dias_shopee)})
        if 'MIN(data)' in sql:
            if ml:
                return pd.DataFrame({'loja': ['ML-LPT', 'ML-YanniRJ'],
                                     'ini': [date(2026, 8, 1), date(2026, 9, 16)]})
            return pd.DataFrame({'loja': ['Shopee-LPT', 'Shopee Lithouse(Nala)'],
                                 'ini': [date(2026, 3, 23)] * 2})
        return pd.DataFrame({'ate': [self.ate_mkt]})

    def rodar(self, mes='2026-09'):
        with mock.patch.object(pf, 'st', self.st), \
                mock.patch.object(pf.pd, 'read_sql', side_effect=self.leitor):
            pf._render_tab_margem_real(None, mes)
        return next(df for t, df in self.saida if t == 'tabela').set_index('Loja')

    def test_setembro(self):
        t = self.rodar()
        self.assertEqual(t.loc['ML-LPT', 'Ads'], 'R$ 4.016,00')
        self.assertEqual(t.loc['ML-LPT', 'Ads até'], '30/09')
        self.assertNotIn('Ads', t.loc['ML-LPT', 'Situação'])
        # R1: YanniRJ so tem API desde 16/09 -> falta, mas mostra os R$ 0,32
        self.assertEqual(t.loc['ML-YanniRJ', 'Ads'], 'R$ 0,32')
        self.assertIn('Ads (API só desde 16/09/2026)', t.loc['ML-YanniRJ', 'Situação'])
        # R3: YanniSP na API desde 14/09 -> setembro nao coberto inteiro: falta
        self.assertIn('Ads (API só desde 14/09/2026)', t.loc['ML-YanniSP', 'Situação'])
        # S1: buraco na Shopee-LPT
        self.assertIn('Ads (buraco em 12/09)', t.loc['Shopee-LPT', 'Situação'])
        # R2: Shopee sem linha no mes -> falta, nunca "fechada" com R$ 0
        self.assertIn('Ads do mês', t.loc['Shopee Lithouse(Nala)', 'Situação'])
        self.assertTrue(t.loc['Shopee Litstore(Yanni)', 'Situação'].endswith(', Ads'))
        self.assertEqual(t.loc['AMZ-LPT', 'Ads'], 'ads pausado')
        self.assertEqual(t.loc['TikTok-Nala', 'Ads'], 'sem ads no mês')
        # Full: TikTok nao usa e fecha; Amazon e Shopee Litstore usam e faltam
        self.assertTrue(t.loc['TikTok-Nala', 'Situação'].startswith('✅ fechado'))
        self.assertEqual(t.loc['TikTok-Nala', 'Full até'], 'não usa Full')
        self.assertIn('armazenagem de Full', t.loc['AMZ-LPT', 'Situação'])
        # o ads entra no custo lancado da loja
        self.assertIn('6.261,64', t.loc['ML-LPT', 'Custos lançados'])

    def test_outubro_yannisp_sem_ads_nao_bloqueia(self):
        self.ate_mkt = date(2026, 10, 31)
        t = self.rodar('2026-10')
        self.assertEqual(t.loc['ML-YanniSP', 'Ads'], 'sem ads no mês')
        self.assertNotIn('Ads', t.loc['ML-YanniSP', 'Situação'].replace('sem ads no mês', ''))


if __name__ == '__main__':
    unittest.main()
