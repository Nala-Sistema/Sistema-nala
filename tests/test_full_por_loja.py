"""
Testa a aba Full por loja (full_por_loja.py, frente [FULL POR LOJA], 12/10/2026)
e o filtro por texto do Sinais do Dia (A3).

Sem banco: prazo com e sem data liberada, Full por estoque_id (caso
K-L-0421-A: 6 un. no anúncio que vende e 89 no parado, nunca somados), regra
do "parado" (6 semanas ou sem venda em 30 dias), "parado" que não acende com
"acabando" no mesmo anúncio, quanto enviar limitado ao galpão, conflito de
galpão entre lojas (ML e Shopee, sem dividir) com a ML-YanniSP fora, gravação
da data (validação e permissão) e % solto nas SQL.

Rodar:  python -m pytest tests/test_full_por_loja.py
"""

import os
import re
import sys
import unittest
from datetime import date, datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import full_por_loja as fpl  # noqa: E402
import sinais_dia as sd  # noqa: E402

HOJE = date(2026, 10, 12)
FOTO_DIA = date(2026, 10, 11)
ML, SHP = sd.MARKETPLACE, sd.SHOPEE
LPT, SP, SHP_LPT = 'ML-LPT', 'ML-YanniSP', 'Shopee-LPT'


def _venda(q7, q30):
    return {'qtd_ritmo': float(q7), 'qtd_30': float(q30), 'rec_30': 30.0 * q30}


class Prazo(unittest.TestCase):
    def test_sem_data_vale_21(self):
        self.assertEqual(fpl.prazo_da_loja(None, HOJE)[0], 21)

    def test_data_futura_soma_chegada(self):
        p, txt, d = fpl.prazo_da_loja({'data_envio': date(2026, 10, 20)}, HOJE)
        self.assertEqual(p, 8 + fpl.PRAZO_CHEGADA_DIAS)
        self.assertEqual(d, date(2026, 10, 20))
        self.assertIn('20/10', txt)

    def test_data_vencida_ou_hoje_vale_21(self):
        for d in (date(2026, 10, 1), HOJE):
            p, txt, vigor = fpl.prazo_da_loja({'data_envio': d}, HOJE)
            self.assertEqual(p, 21)
            self.assertIsNone(vigor)
            self.assertIn('já passou', txt)

    def test_data_limpa_vale_21(self):
        self.assertEqual(fpl.prazo_da_loja({'data_envio': None}, HOJE)[0], 21)


class Avaliar(unittest.TestCase):
    # K-L-0421-A na ML-LPT: dois anúncios, dois estoques
    PONTE = [(LPT, 'MLB_VENDE', 'E_VENDE', 'K-L-0421-A', True),
             (LPT, 'MLB_PARADO', 'E_PARADO', 'K-L-0421-A', True)]
    FOTO = [(LPT, 'E_VENDE', FOTO_DIA, 6, 0, 0, 300),
            (LPT, 'E_PARADO', FOTO_DIA, 89, 0, 0, 300)]
    POR_SKU = {(LPT, 'MLB_VENDE', 'K-L-0421-A'): _venda(14, 30)}

    def _itens(self, prazos=None, custos=None, foto=None, ponte=None, por_sku=None):
        return fpl.avaliar(ML, foto or self.FOTO, ponte or self.PONTE,
                           self.POR_SKU if por_sku is None else por_sku,
                           {'K-L-0421-A': 10} if custos is None else custos,
                           prazos or {LPT: 21})

    def test_por_estoque_nunca_somado(self):
        it = {i['estoque_id']: i for i in self._itens()}
        vende, parado = it['E_VENDE'], it['E_PARADO']
        self.assertEqual(vende['venda_dia'], 2.0)            # max(14/7, 30/30)
        self.assertEqual(vende['dias_full'], 3.0)
        self.assertTrue(vende['acabando'])
        self.assertEqual(vende['necessidade'], 2 * fpl.DIAS_ALVO - 6)
        self.assertFalse(vende['parado'])
        self.assertFalse(parado['acabando'])
        self.assertTrue(parado['parado'])                     # sem venda em 30 dias
        self.assertEqual(parado['rs_parado'], 890.0)

    def test_parado_por_seis_semanas(self):
        por_sku = {(LPT, 'MLB_VENDE', 'K-L-0421-A'): _venda(0, 30)}   # 1/dia
        foto = [(LPT, 'E_VENDE', FOTO_DIA, 42, 0, 0, 0)]
        i = self._itens(foto=foto, por_sku=por_sku)[0]
        self.assertTrue(i['parado'])
        foto = [(LPT, 'E_VENDE', FOTO_DIA, 41, 0, 0, 0)]
        self.assertFalse(self._itens(foto=foto, por_sku=por_sku)[0]['parado'])

    def test_full_zero_nao_e_parado(self):
        foto = [(LPT, 'E_PARADO', FOTO_DIA, 0, 0, 0, 0)]
        self.assertFalse(self._itens(foto=foto)[0]['parado'])

    def test_transferencia_conta_na_cobertura(self):
        foto = [(LPT, 'E_VENDE', FOTO_DIA, 6, 40, 0, 300)]
        i = self._itens(foto=foto)[0]
        self.assertEqual(i['dias_full'], 23.0)
        self.assertFalse(i['acabando'])

    def test_data_liberada_muda_o_alerta(self):
        foto = [(LPT, 'E_VENDE', FOTO_DIA, 30, 0, 0, 300)]      # 15 dias de Full
        self.assertTrue(self._itens(foto=foto)[0]['acabando'])                  # < 21
        self.assertFalse(self._itens(foto=foto, prazos={LPT: 12})[0]['acabando'])

    def test_sem_custo(self):
        it = {i['estoque_id']: i for i in self._itens(custos={})}
        self.assertTrue(it['E_PARADO']['parado'])
        self.assertIsNone(it['E_PARADO']['rs_parado'])
        s = fpl.sinais_parado(list(it.values()))
        self.assertIn('sem custo', s[0]['numero'])

    def test_estoque_fora_da_ponte_sai(self):
        foto = [(LPT, 'OUTRO', FOTO_DIA, 50, 0, 0, 0)]
        self.assertEqual(self._itens(foto=foto), [])


class ParadoEAcabando(unittest.TestCase):
    def test_mesmo_anuncio_nao_acende_os_dois(self):
        # um anúncio, duas variações (dois estoques): uma acabando, outra parada
        ponte = [(LPT, 'MLB1', 'E_A', 'L-0320', True), (LPT, 'MLB1', 'E_B', 'L-0321', True)]
        foto = [(LPT, 'E_A', FOTO_DIA, 2, 0, 0, 50), (LPT, 'E_B', FOTO_DIA, 80, 0, 0, 50)]
        por_sku = {(LPT, 'MLB1', 'L-0320'): _venda(14, 30)}
        itens = fpl.avaliar(ML, foto, ponte, por_sku, {'L-0321': 5, 'L-0320': 5}, {LPT: 21})
        fpl.suprimir_parado_com_acabando(itens)
        self.assertEqual(fpl.sinais_parado(itens), [])
        # o R$ parado do topo continua contando o estoque
        r = fpl.resumo_por_loja(itens, [(ML, LPT)])[0]
        self.assertEqual(r['Estoques parados'], 1)

    def test_sinal_parado_chave_do_ciente(self):
        ponte = [(LPT, 'MLB9', 'E9', 'L-9', True)]
        itens = fpl.avaliar(ML, [(LPT, 'E9', FOTO_DIA, 10, 0, 0, 0)], ponte, {}, {'L-9': 3},
                            {LPT: 21})
        s = fpl.sinais_parado(itens)[0]
        self.assertEqual((s['regra'], s['objeto'], s['medida']), ('full_parado', 'E9', 30.0))
        self.assertIn('full_parado', sd.REGRAS)
        # Ciente aberto cala; R$ 50% maior volta
        ciente = (1, LPT, 'full_parado', 'E9', 'proposital', None, date(2026, 10, 30), 30.0,
                  'x', 'ana', datetime(2026, 10, 12, 9))
        ativos, sil = sd.aplicar_cientes([s], [ciente], HOJE)
        self.assertEqual((len(ativos), len(sil)), (0, 1))
        self.assertTrue(sd.piorou('full_parado', 45.0, 30.0))
        self.assertFalse(sd.piorou('full_parado', 40.0, 30.0))


class Galpao(unittest.TestCase):
    def _item(self, mkt, loja, eid, nec, galpao, sku='L-0303'):
        return {'mkt': mkt, 'loja': loja, 'estoque_id': eid, 'skus': (sku,),
                'necessidade': nec, 'galpao': galpao, 'enviar': None, 'obs': '',
                'acabando': nec > 0}

    def test_limita_ao_galpao(self):
        i = self._item(ML, LPT, 'E1', 50, 20)
        self.assertEqual(fpl.aplicar_galpao([i]), [])
        self.assertEqual(i['enviar'], 20)
        self.assertIn('faltam 30', i['obs'])

    def test_galpao_desconhecido_e_zerado(self):
        a, b = self._item(ML, LPT, 'E1', 10, None), self._item(ML, LPT, 'E2', 10, 0, 'X')
        fpl.aplicar_galpao([a, b])
        self.assertEqual((a['enviar'], b['enviar']), (10, 0))

    def test_conflito_ml_e_shopee_nao_divide(self):
        a = self._item(ML, LPT, 'E1', 60, 80)
        b = self._item(SHP, SHP_LPT, '1:2', 40, 80)
        c = fpl.aplicar_galpao([a, b])
        self.assertEqual(len(c), 1)
        self.assertEqual((c[0]['galpao'], c[0]['soma']), (80, 100))
        self.assertIsNone(a['enviar'])
        self.assertIsNone(b['enviar'])
        self.assertIn('decisão do Thiago', a['obs'])

    def test_galpao_nunca_somado_vale_o_maior(self):
        # o galpão se repete nos anúncios: 2 estoques com 80 não viram 160
        a = self._item(ML, LPT, 'E1', 50, 80)
        b = self._item(SHP, SHP_LPT, '1:2', 50, 70)
        self.assertEqual(fpl.aplicar_galpao([a, b])[0]['galpao'], 80)

    def test_cabe_para_todos(self):
        a, b = self._item(ML, LPT, 'E1', 30, 80), self._item(SHP, SHP_LPT, '1:2', 40, 80)
        self.assertEqual(fpl.aplicar_galpao([a, b]), [])
        self.assertEqual((a['enviar'], b['enviar']), (30, 40))

    def test_prep_center_fora_do_conflito(self):
        a = self._item(ML, LPT, 'E1', 60, 80)
        sp = self._item(ML, SP, 'E2', 40, 15)
        self.assertEqual(fpl.aplicar_galpao([a, sp]), [])
        self.assertEqual(a['enviar'], 60)
        self.assertEqual(sp['enviar'], 15)
        self.assertIn('prep center', sp['obs'])


class _Cur:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, p):
        self.log.append((sql, p))

    def close(self):
        pass


class _Conn:
    def __init__(self):
        self.log, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return _Cur(self.log)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class GravarData(unittest.TestCase):
    def test_grava(self):
        c = _Conn()
        fpl.gravar_data(c, ML, LPT, date(2026, 10, 20), 'ana', HOJE, [LPT])
        self.assertEqual(c.commits, 1)
        self.assertEqual(c.log[0][1]['data'], date(2026, 10, 20))
        self.assertEqual(c.log[0][1]['usuario'], 'ana')

    def test_limpar_grava_null(self):
        c = _Conn()
        fpl.gravar_data(c, ML, LPT, None, 'ana', HOJE, [LPT])
        self.assertIsNone(c.log[0][1]['data'])

    def test_recusa(self):
        casos = [(HOJE, 'ana', [LPT], ValueError),
                 (date(2027, 2, 1), 'ana', [LPT], ValueError),
                 (date(2026, 10, 20), '', [LPT], ValueError),
                 (date(2026, 10, 20), 'ana', ['ML-Nala'], PermissionError)]
        for d, u, lojas, erro in casos:
            c = _Conn()
            with self.assertRaises(erro):
                fpl.gravar_data(c, ML, LPT, d, u, HOJE, lojas)
            self.assertEqual(c.log, [])

    def test_check_do_sql_bate_com_o_codigo(self):
        texto = open(os.path.join(RAIZ, 'sql', 'full_por_loja.sql'), encoding='utf-8').read()
        self.assertIn('informado_em::date + 90', texto)
        self.assertEqual(fpl.DIAS_DATA_MAX, 90)
        for nome in ('sinal_ciente_regra_valida', 'sinal_historico_regra_valida'):
            bloco = re.search(nome + r' CHECK \(regra IN \((.*?)\)\)', texto, re.S).group(1)
            self.assertEqual(set(re.findall(r"'(\w+)'", bloco)), set(sd.REGRAS))


class Sql(unittest.TestCase):
    def test_sem_porcento_solto(self):
        for sql in fpl.TODAS_AS_SQL:
            self.assertIsNone(re.search(r'%(?!\()', sql), sql)


class FiltroTexto(unittest.TestCase):
    S = [sd._sinal(LPT, 'FULL', 'MLB1', ['L-0320'], '5 dias de Full', 'Repor', 10,
                   regra='full_cobertura', objeto='E1'),
         sd._sinal(LPT, 'VENDAS', 'MLB2', ['K-L-0421-A'], 'Venda caiu 30%', 'Ver', 20,
                   regra='vendas_queda')]
    NOMES = {'L-0320': 'Escova Secadora Rosa', 'K-L-0421-A': 'Kit Alicate'}

    def test_vazio_mostra_tudo(self):
        self.assertEqual(len(sd.filtrar_sinais(self.S, '  ', self.NOMES)), 2)

    def test_por_nome_sku_e_texto(self):
        self.assertEqual(sd.filtrar_sinais(self.S, 'escova', self.NOMES)[0]['anuncio'], 'MLB1')
        self.assertEqual(sd.filtrar_sinais(self.S, 'k-l-0421', self.NOMES)[0]['anuncio'], 'MLB2')
        self.assertEqual(len(sd.filtrar_sinais(self.S, 'venda caiu', self.NOMES)), 1)
        self.assertEqual(sd.filtrar_sinais(self.S, 'escova caiu', self.NOMES), [])


if __name__ == '__main__':
    unittest.main()
