"""
Testa a aba Semana do Sinais do Dia (performance_semana.py, [PERFORMANCE
SEMANAL] E1, 26/10/2026) e o módulo comum da Top 30 (top_anuncios.py).

Sem banco: semana seg–dom fechada a partir de hoje, prazo D+2 útil, G8 (T6/T7)
com a mediana do Top 30 da loja e os cortes de ruído e de nota (casos do teste
da v2: L-0405, L-0421, L-0413), T1 × T2 pela conta do Full, T5 só sugere,
T8/T15/T16, silêncio de 4 semanas e a volta quando piora, validação da
marcação, carga incompleta, % solto nas SQL e a CHECK do SQL igual ao código.

Rodar:  python -m pytest tests/test_performance_semana.py
"""

import os
import re
import sys
import unittest
from datetime import date, datetime, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import performance_semana as ps  # noqa: E402
import sinais_dia as sd  # noqa: E402
import top_anuncios as ta  # noqa: E402

HOJE = date(2026, 10, 20)            # terça: a semana fechada é 12–18/10
ML, SHP = sd.MARKETPLACE, sd.SHOPEE


def linha(chave, loja='ML-LPT', mkt=ML, pos=1, **kw):
    r = {'loja': loja, 'mkt': mkt, 'chave': chave, 'skus': kw.pop('skus', 'L-0001'),
         'sku_top': None, 'nome': 'Produto', 'pos': pos, 'rec': 3000.0, 'ped': 60,
         'views_30d': None, 'rec_sem': 700.0, 'ped_sem': 10, 'rec_base': 2800.0,
         'vis_sem': 150.0, 'imp_sem': 0, 'cli_sem': 0, 'gasto_sem': 0, 'imp_base': 0,
         'cli_base': 0, 'gasto_base': 0}
    r.update(kw)
    return r


def est(dias=7, em_full=True, travado=0, tem=True):
    return {'em_full': em_full, 'dias_full': dias, 'dias_galpao': dias, 'dias_estoque': dias,
            'dias_semana': 7, 'foto': date(2026, 10, 19), 'travado': travado,
            'tem_estoque_hoje': tem, 'galpao': 100, 'envio_parado': []}


def ctx_base(**kw):
    c = {'full': [], 'parados': {}, 'estoque': {}, 'campanhas': [], 'roas': [], 'nota': {},
         'recl': {}, 'frete': {}, 'status': {}}
    c.update(kw)
    return c


def item_full(anuncio, loja='ML-LPT', full=10, transf=0, galpao=50, vd=2.0, prazo=21,
              parado=False, enviar=60, skus=('L-0001',)):
    dias = (full + transf) / vd if vd else None
    return {'mkt': ML, 'loja': loja, 'estoque_id': 'E' + anuncio, 'data': date(2026, 10, 19),
            'anuncios': (anuncio,), 'anuncio': anuncio, 'skus': skus, 'em_full': True,
            'full': full, 'transf': transf, 'galpao': galpao, 'qtd_7': 14, 'qtd_30': 60,
            'rec_30': 1800.0, 'venda_dia': vd, 'dias_full': dias, 'prazo': prazo,
            'acabando': bool(vd) and dias < prazo, 'necessidade': 0, 'parado': parado,
            'parado_suprimido': False, 'custo': 10.0, 'rs_parado': full * 10.0 if parado else None,
            'semanas_full': (full / vd / 7) if vd else None, 'enviar': enviar, 'obs': ''}


class Janelas(unittest.TestCase):
    def test_semana_fechada_de_terca(self):
        j = ps.semana_fechada(HOJE)
        self.assertEqual((j['sem_ini'], j['sem_fim']), (date(2026, 10, 12), date(2026, 10, 18)))
        self.assertEqual((j['base_ini'], j['base_fim']), (date(2026, 9, 14), date(2026, 10, 11)))
        self.assertEqual(j['ini30'], date(2026, 9, 19))      # o teste da v2: 19/09–18/10

    def test_segunda_pega_a_semana_que_acabou_ontem(self):
        j = ps.semana_fechada(date(2026, 10, 26))
        self.assertEqual((j['sem_ini'], j['sem_fim']), (date(2026, 10, 19), date(2026, 10, 25)))

    def test_domingo_nao_e_semana_fechada(self):
        j = ps.semana_fechada(date(2026, 10, 25))
        self.assertEqual(j['sem_fim'], date(2026, 10, 18))

    def test_prazo_d2_util(self):
        self.assertEqual(ps.prazo_minimo(date(2026, 10, 26)), date(2026, 10, 28))  # seg -> qua
        self.assertEqual(ps.prazo_minimo(date(2026, 10, 23)), date(2026, 10, 27))  # sex -> ter


class G8(unittest.TestCase):
    """Mediana ML-LPT ~147 visitas / 8,7%: L-0405 (564 vis., 23 ped.) = T7;
    L-0421 (78 vis., 11 ped., nota 100) = T6; L-0413 (13%) sai pela nota 30."""

    def top(self):
        outros = [linha(f'MLB{k}', pos=k + 3, vis_sem=147.0, ped_sem=13) for k in range(7)]
        return outros + [
            linha('MLB4631526853', pos=1, vis_sem=564.0, ped_sem=23, rec_sem=577.51),
            linha('MLB6203090906', pos=2, vis_sem=78.0, ped_sem=11, rec_sem=282.64),
            linha('MLB0413', pos=3, vis_sem=100.0, ped_sem=13),
            linha('MLB_ZERO', pos=20, vis_sem=0.0, ped_sem=0)]   # 0 visita: fora da mediana

    def rodar(self, **ctx):
        top = self.top()
        estq = {('ML-LPT', r['chave']): est() for r in top}
        nota = {('ML-LPT', 'MLB6203090906'): (100, None, HOJE),
                ('ML-LPT', 'MLB0413'): (30, 'Evite enviar unidades com defeito.', HOJE)}
        c = ctx_base(estoque=estq, nota=nota, **ctx)
        return ps.regras_ataque(top, c, HOJE)

    def test_mediana_ignora_zero_visita(self):
        med = ps.medianas_por_loja(self.top())['ML-LPT']
        self.assertEqual(med[0], 147.0)

    def test_t7_e_t6_acendem(self):
        itens = self.rodar()
        t7 = [i['chave'] for i in itens if i['regra'] == 'T7']
        t6 = [i['chave'] for i in itens if i['regra'] == 'T6']
        self.assertEqual(t7, ['MLB4631526853'])
        self.assertEqual(t6, ['MLB6203090906'])

    def test_l0413_sai_pela_nota_e_vira_t9(self):
        itens = self.rodar()
        self.assertFalse([i for i in itens if i['chave'] == 'MLB0413' and i['regra'] == 'T6'])
        t9 = [i for i in itens if i['chave'] == 'MLB0413' and i['regra'] == 'T9']
        self.assertEqual(len(t9), 1)
        self.assertIn('lote', t9[0]['sugestao'])
        self.assertIn('Não ligar ads', t9[0]['sugestao'])

    def test_t7_em_jogo_venda_perdida_contra_a_mediana(self):
        i = [i for i in self.rodar() if i['regra'] == 'T7'][0]
        med = ps.medianas_por_loja(self.top())['ML-LPT'][1]
        esperado = 564 * (med - 23 / 564) * 577.51 / 23
        self.assertAlmostEqual(i['em_jogo'], round(esperado, 2), places=1)

    def test_ruido_corta_t7(self):
        top = self.top()
        top[-4]['ped_sem'] = 4                                # < 5 pedidos
        c = ctx_base(estoque={('ML-LPT', r['chave']): est() for r in top})
        self.assertFalse([i for i in ps.regras_ataque(top, c, HOJE) if i['regra'] == 'T7'])

    def test_sem_estoque_nos_7_dias_nao_acende(self):
        top = self.top()
        estq = {('ML-LPT', r['chave']): est() for r in top}
        estq[('ML-LPT', 'MLB4631526853')] = est(dias=5)
        c = ctx_base(estoque=estq)
        self.assertFalse([i for i in ps.regras_ataque(top, c, HOJE) if i['regra'] == 'T7'])

    def test_t6_nao_acende_com_full_acabando(self):
        itens = self.rodar(full=[item_full('MLB6203090906', full=10, vd=2.0)])
        regras = {i['regra'] for i in itens if i['chave'] == 'MLB6203090906'}
        self.assertIn('T1', regras)
        self.assertNotIn('T6', regras)

    def test_shopee_usa_30_dias_e_views(self):
        top = [linha(f'I{k}', loja='Shopee-LPT', mkt=SHP, pos=k + 2, views_30d=553.0, ped=17)
               for k in range(5)]
        top.append(linha('IK3', loja='Shopee-LPT', mkt=SHP, pos=1, views_30d=646.0, ped=4,
                         vis_sem=None))
        c = ctx_base(estoque={('Shopee-LPT', r['chave']): est() for r in top})
        t7 = [i for i in ps.regras_ataque(top, c, HOJE) if i['regra'] == 'T7']
        self.assertEqual([i['chave'] for i in t7], [])   # 4 pedidos em 30 dias: ruído
        top[-1]['ped'] = 8                               # 1,2% ≤ 0,5 × 3,1% (K3-LKE-3104 da v2)
        t7 = [i for i in ps.regras_ataque(top, c, HOJE) if i['regra'] == 'T7']
        self.assertEqual([i['chave'] for i in t7], ['IK3'])
        self.assertIn('views 30d', t7[0]['evidencia'])


class Full(unittest.TestCase):
    def test_t1_com_galpao_e_o_que_sai(self):
        r = linha('MLB1')
        parado = item_full('MLB9', full=300, vd=1.0, parado=True)
        c = ctx_base(full=[item_full('MLB1', full=10, vd=2.0, enviar=60), parado],
                     parados={'ML-LPT': [parado]})
        itens = ps.regras_full(r, c, HOJE)
        t1 = [i for i in itens if i['regra'] == 'T1'][0]
        self.assertIn('Enviar 60 un.', t1['sugestao'])
        self.assertIn('abrir espaço', t1['sugestao'])
        self.assertGreaterEqual(t1['prazo'], ps.prazo_minimo(HOJE))
        # 5 dias de Full contra 21: 7 dias sem estoque na semana (teto), 2 un./dia × R$ 30
        self.assertAlmostEqual(t1['em_jogo'], 2.0 * 30.0 * 7)
        t5 = [i for i in itens if i['regra'] == 'T5']
        self.assertEqual(len(t5), 1)
        self.assertTrue(t5[0]['sugestao'].startswith('SUGESTÃO, decisão caso a caso'))

    def test_t2_full_e_transf_zerados(self):
        itens = ps.regras_full(linha('MLB1'), ctx_base(full=[item_full('MLB1', full=0)]), HOJE)
        self.assertEqual([i['regra'] for i in itens], ['T2'])
        self.assertIn('não pausar', itens[0]['sugestao'])

    def test_sem_galpao_nao_e_ataque(self):
        itens = ps.regras_full(linha('MLB1'),
                               ctx_base(full=[item_full('MLB1', full=0, galpao=0)]), HOJE)
        self.assertEqual(itens, [])

    def test_t3_travado(self):
        c = ctx_base(full=[item_full('MLB1', full=100, vd=1.0)],
                     estoque={('ML-LPT', 'MLB1'): est(travado=4)})
        itens = ps.regras_full(linha('MLB1'), c, HOJE)
        self.assertEqual([i['regra'] for i in itens], ['T3'])
        self.assertIn('4 un. travadas', itens[0]['sugestao'])

    @staticmethod
    def _hist(transf_por_dia, loja='ML-LPT', an='MLB1'):
        """transf_por_dia: do dia mais antigo ao mais novo (19/10)."""
        n = len(transf_por_dia)
        return [(loja, an, 'E1', True, date(2026, 10, 19) - timedelta(days=n - 1 - k),
                 5, t, 0, 0, 0, 10) for k, t in enumerate(transf_por_dia)]

    def _t4(self, transf_por_dia, loja='ML-LPT', mkt=ML, an='MLB1'):
        e = ps.resumir_estoque(self._hist(transf_por_dia, loja, an), ps.semana_fechada(HOJE))
        c = ctx_base(full=[item_full(an, loja=loja, full=100, vd=1.0)], estoque=e)
        return [i for i in ps.regras_full(linha(an, loja=loja, mkt=mkt), c, HOJE)
                if i['regra'] == 'T4']

    def test_t4_ml_transferencia_parada_pede_conferir(self):
        t4 = self._t4([3] * 10)
        self.assertEqual(len(t4), 1)
        self.assertTrue(t4[0]['sugestao'].startswith('Transferência parada há 7 dias — conferir'))
        self.assertNotIn('Cobrar', t4[0]['sugestao'])
        self.assertIn('desde 10/10', t4[0]['sugestao'])         # os 10 dias parados

    def test_t4_transferencia_que_diminuiu_nao_acende(self):
        self.assertEqual(self._t4([9, 9, 9, 9, 9, 9, 3, 3, 3, 3]), [])  # entrou no 7º dia antes
        self.assertEqual(self._t4([3] * 6), [])                 # menos de 7 fotos: não julga

    def test_t4_desligada_na_shopee(self):
        self.assertEqual(self._t4([3] * 10, loja='Shopee-LPT', mkt=SHP, an='I1'), [])


class Ads(unittest.TestCase):
    def test_t8_orcamento_travando(self):
        r = linha('MLB1')
        c = ctx_base(campanhas=[{'loja': 'ML-LPT', 'id_campanha': 'C1', 'campanha': 'C1',
                                 'gasto_sem': 64.0, 'anuncios': ['MLB1'], 'orc_medio': 10.0,
                                 'dias_orc': 7}])
        self.assertEqual([i['regra'] for i in ps.regras_ads(r, c, HOJE, False)], ['T8'])
        self.assertEqual(ps.regras_ads(r, c, HOJE, True), [])      # T1 acesa: não escala

    def test_t15_voltar_roas(self):
        r = linha('MLB1', rec_sem=400.0, rec_base=2800.0, gasto_sem=30.0, gasto_base=120.0)
        c = ctx_base(roas=[{'loja': 'ML-LPT', 'dia': date(2026, 10, 14), 'antes': 6,
                            'depois': 9, 'anuncios': {'MLB1'}}])
        itens = ps.regras_ads(r, c, HOJE, False)
        self.assertEqual([i['regra'] for i in itens], ['T15'])
        self.assertAlmostEqual(itens[0]['em_jogo'], 300.0)

    def test_t15_manter_quando_tacos_caiu(self):
        r = linha('MLB1', rec_sem=650.0, rec_base=2800.0, gasto_sem=10.0, gasto_base=120.0)
        c = ctx_base(roas=[{'loja': 'ML-LPT', 'dia': date(2026, 10, 14), 'antes': 6,
                            'depois': 9, 'anuncios': {'MLB1'}}])
        self.assertEqual(ps.regras_ads(r, c, HOJE, False), [])

    def test_t16_espiral(self):
        r = linha('MLB1', rec_sem=400.0, rec_base=2800.0, gasto_sem=10.0, gasto_base=120.0)
        c = ctx_base(estoque={('ML-LPT', 'MLB1'): est()})
        self.assertEqual([i['regra'] for i in ps.regras_ads(r, c, HOJE, False)], ['T16'])

    def test_loja_sem_ads(self):
        r = linha('MLB1', loja='ML-YanniSP', rec_sem=400.0, gasto_base=120.0)
        self.assertEqual(ps.regras_ads(r, ctx_base(), HOJE, False), [])


class Marcacao(unittest.TestCase):
    SEM = date(2026, 10, 12)

    def item(self, em_jogo=100.0):
        return {**ps._item(linha('MLB4067169783', loja='ML-Nala'), 'T7', 'rever', 'ev',
                           em_jogo, HOJE), 'marcacao': None}

    def marca(self, motivo, semana, medida=100.0):
        return {'marketplace': ML, 'loja': 'ML-Nala', 'semana_ini': semana,
                'codigo_anuncio': 'MLB4067169783', 'regra': 'T7', 'motivo': motivo,
                'nota': None, 'prazo': None, 'medida': medida, 'decidido_por': 'Larissa',
                'decidido_em': datetime(2026, 10, 6, 9, 0)}

    def test_ja_resolvido_cala_4_semanas(self):
        a, s = ps.aplicar_marcacoes([self.item()], [self.marca('ja_resolvido', date(2026, 9, 14))],
                                    self.SEM)
        self.assertEqual((len(a), len(s)), (0, 1))
        a, s = ps.aplicar_marcacoes([self.item()], [self.marca('ja_resolvido', date(2026, 9, 7))],
                                    self.SEM)
        self.assertEqual((len(a), len(s)), (1, 0))        # 5 semanas atrás: voltou

    def test_volta_se_piorar(self):
        a, s = ps.aplicar_marcacoes([self.item(em_jogo=151.0)],
                                    [self.marca('ruido', date(2026, 10, 5))], self.SEM)
        self.assertEqual(len(a), 1)
        self.assertTrue(a[0]['voltou'].startswith('voltou'))

    def test_vou_agir_nao_cala(self):
        a, s = ps.aplicar_marcacoes([self.item()], [self.marca('vou_agir', date(2026, 10, 5))],
                                    self.SEM)
        self.assertEqual((len(a), len(s)), (1, 0))

    def test_marcacao_desta_semana_fica_no_item_e_tira_da_sugestao(self):
        a, _s = ps.aplicar_marcacoes([self.item()], [self.marca('ja_resolvido', self.SEM)],
                                     self.SEM)
        self.assertEqual(a[0]['marcacao']['motivo'], 'ja_resolvido')
        self.assertEqual(ps.sugestoes_de(a), {})

    def test_validacao(self):
        it = self.item()
        self.assertTrue(ps.validar_marcacao(it, 'vou_agir', '', None, HOJE))
        self.assertTrue(ps.validar_marcacao(it, 'outro', ' ', None, HOJE))
        self.assertTrue(ps.validar_marcacao(it, 'vou_agir', '', HOJE + timedelta(days=1), HOJE))
        self.assertFalse(ps.validar_marcacao(it, 'vou_agir', '', ps.prazo_minimo(HOJE), HOJE))
        self.assertFalse(ps.validar_marcacao(it, 'ruido', '', None, HOJE))

    def test_gravar_so_responsavel(self):
        with self.assertRaises(PermissionError):
            ps.gravar_marcacoes(None, [(self.item(), 'ruido', '', None)], 'Patricia', HOJE,
                                self.SEM, set())

    def test_sugestao_com_origem(self):
        a, _s = ps.aplicar_marcacoes([self.item()], [], self.SEM)
        sug = ps.sugestoes_de(a)
        self.assertEqual(sug[('ML-Nala', 'MLB4067169783')][0]['origem'], 'performance')
        self.assertIn('[performance] T7:', ta.fmt_sugestoes(sug[('ML-Nala', 'MLB4067169783')]))


class Carga(unittest.TestCase):
    def test_falta_domingo(self):
        j = ps.semana_fechada(date(2026, 10, 26))      # domingo 25/10
        linhas = [('vendas', 'ML-LPT', datetime(2026, 10, 26, 5, 0)),
                  ('estoque', 'ML-LPT', datetime(2026, 10, 25)),
                  ('visitas', 'ML-LPT', datetime(2026, 10, 24)),
                  ('ads', 'ML-LPT', datetime(2026, 10, 25)),
                  ('vendas', 'ML-YanniSP', datetime(2026, 10, 26, 5, 0)),
                  ('estoque', 'ML-YanniSP', datetime(2026, 10, 25)),
                  ('visitas', 'ML-YanniSP', datetime(2026, 10, 25))]
        out = ps.carga_incompleta(linhas, {'ML-LPT': ML, 'ML-YanniSP': ML}, j)
        self.assertEqual(out, {'ML-LPT': ['visitas']})  # YanniSP não tem ads


class SQL(unittest.TestCase):
    def test_sem_porcento_solto(self):
        for sql in ps.TODAS_AS_SQL + (ta.SQL_ATAQUE,):
            self.assertIsNone(re.search(r'%(?!\()', sql), sql)

    def test_check_do_sql_bate_com_o_codigo(self):
        ddl = open(os.path.join(RAIZ, 'sql', 'performance_semana_marcacao.sql'),
                   encoding='utf-8').read()
        m = re.search(r"perf_semana_marcacao_motivo CHECK \(motivo IN \((.*?)\)\)", ddl, re.S)
        self.assertEqual(set(re.findall(r"'(\w+)'", m.group(1))), set(ps.MOTIVOS))
        regra = re.compile(r'^(T([1-9]|1[0-6])|D([1-9]|1[0-7]))$')
        self.assertTrue(all(regra.match(r) for r in ps.REGRAS))

    def test_mais_vendidos_sem_semana_nao_le_a_mais(self):
        p = ta.params_ataque(['ML-LPT'], date(2026, 10, 1), date(2026, 10, 30), 30)
        self.assertEqual((p['sem_ini'], p['sem_fim']), (p['ini'], p['fim']))
        self.assertEqual((p['base_ini'], p['base_fim']), (p['ini_ant'], p['fim_ant']))
        self.assertEqual((p['ini_le'], p['fim_le']), (p['ini_ant'], p['fim']))
        self.assertFalse(p['por_loja'])


if __name__ == '__main__':
    unittest.main()
