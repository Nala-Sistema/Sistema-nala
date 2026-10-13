"""
Testa o Plano de escala (plano_escala.py, frente [PLANO ESCALA], Etapa 1).

Números da planilha "Plano de escala" do Drive
(1R12xedUR4VqLCYNY8UwstDwmaOESLssws0ksvEYiFdM, aba "Planos de Escala"),
com o preço de margem zero FIXO da planilha (margem_unit = preço − PMZ), que
é como ela calcula. No sistema a margem sai da conta da Tabela de Preço no
preço de cada rodada (decisão do Thiago, 12/10); isso é testado à parte.

ONDE O NÚMERO DIFERE DA PLANILHA, E POR QUÊ (vale a ESPECIFICAÇÃO):
  - Cabo de bateria, 8ª rodada: a planilha sobe para R$ 53,18, acima do preço
    meta (R$ 52,90). Pela especificação o último degrau é o preço meta: a 8ª
    roda a R$ 52,90 (resultado R$ 513,94, não R$ 533,01). Rodadas 1 a 7 e o
    TETO (−R$ 707,58) batem.
  - Chave de roda e Kit chave de fenda L-0421: as vendas das rodadas 2+ da
    planilha apontam para a célula do Cabo de bateria ($D$3 = 350), erro de
    cópia; e a rodada 1 tem 35 digitado. Pela especificação a meta é 20% de
    200 = 40 → rodadas de 20, 80, 40... A conta POR LINHA da planilha (com as
    vendas e preços dela) é conferida igual; o plano pela especificação tem
    números próprios.
  - "Meta de Vendas" do pé da planilha usa 50% da venda do concorrente; a
    especificação usa 20% (a meta das rodadas da própria planilha é 20%).
  - Preços da planilha são mostrados com 2 casas (42,42) mas a conta usa o
    valor cheio (49,9 × 0,85 = 42,415); aqui também.
"""
import io
import os
import re
import sys
import unittest
from datetime import date, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import plano_escala as pe  # noqa: E402


def _plano(conc_v, conc_p, pmz, preco_meta, mao=None, meta_propria=None):
    meta = pe.meta_mensal(conc_v, meta_propria)
    rod = pe.prever(pe.montar_rodadas(meta, conc_p, preco_meta, mao), lambda p: p - pmz)
    return rod, pe.teto_e_virada(rod)


# Cabo de bateria (planilha, linhas 5–12): vendas, preço, gasto ads, resultado
CABO_PLANILHA = [
    (35, 42.42, 111.34, -184.31), (140, 44.54, 436.45, -431.45), (70, 45.87, 187.85, -91.82),
    (70, 47.25, 158.75, 33.61), (70, 48.67, 131.15, 160.43), (70, 50.13, 105.26, 288.51),
    (70, 51.63, 81.32, 417.72),
]


class TestPrevistoPlanilha(unittest.TestCase):

    def test_cabo_de_bateria_rodadas_1_a_7_e_teto(self):
        rod, (teto, virada) = _plano(350, 49.9, 44.5, 52.9)
        for r, (v, p, g, res) in zip(rod, CABO_PLANILHA):
            self.assertEqual(r['vendas'], v)
            self.assertAlmostEqual(r['preco'], p, delta=0.006)
            self.assertAlmostEqual(r['gasto_ads'], g, delta=0.01)
            self.assertAlmostEqual(r['resultado'], res, delta=0.01)
        self.assertAlmostEqual(teto, -707.58, delta=0.01)       # TETO = buraco máximo
        # virada: 455 vendas até a 6ª, faltam 225,03 a 5,967/un. na 7ª -> 492,7
        self.assertEqual(virada, 493)

    def test_cabo_ultimo_degrau_travado_no_preco_meta(self):
        rod, _ = _plano(350, 49.9, 44.5, 52.9)
        self.assertEqual(len(rod), 8)
        self.assertEqual(rod[-1]['preco'], 52.9)                 # planilha: 53,18
        self.assertAlmostEqual(rod[-1]['resultado'], 513.94, delta=0.01)

    def test_acos_parcela_tacos_da_planilha(self):
        rod, _ = _plano(350, 49.9, 44.5, 52.9)
        self.assertEqual([round(r['acos'], 2) for r in rod],
                         [0.10, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05, 0.05])
        self.assertEqual([round(r['parcela_ads'], 2) for r in rod],
                         [0.75, 0.70, 0.65, 0.60, 0.55, 0.50, 0.45, 0.40])
        self.assertEqual([round(r['tacos'] * 100, 2) for r in rod],
                         [7.5, 7.0, 5.85, 4.8, 3.85, 3.0, 2.25, 2.0])

    def test_pisos_de_acos_e_parcela(self):
        self.assertEqual(pe.acos_da_rodada(20), 0.05)
        self.assertEqual(pe.parcela_da_rodada(20), 0.40)

    def test_chave_de_roda_conta_por_linha_igual_a_planilha(self):
        # com as vendas e os preços da própria planilha (linhas 25–32)
        linhas = [(35, 35.9, 10.76), (140, 36.9, 198.38), (70, 37.9, 194.80),
                  (70, 38.9, 289.30), (70, 39.9, 382.47), (70, 40.9, 474.11),
                  (70, 42.9, 632.43), (70, 44.9, 777.14)]
        for n, (v, p, res) in enumerate(linhas, start=1):
            r = pe.prever([{'rodada': n, 'vendas': v, 'preco': p,
                            'tacos': pe.acos_da_rodada(n) * pe.parcela_da_rodada(n)}],
                          lambda x: x - 32.9)[0]
            self.assertAlmostEqual(r['resultado'], res, delta=0.01)

    def test_kit_chave_de_fenda_l0421_conta_por_linha(self):
        linhas = [(35, 20.9, -19.86), (140, 21.9, 65.38), (70, 22.9, 116.22),
                  (70, 23.9, 199.70), (70, 24.9, 282.89), (70, 25.9, 365.61),
                  (70, 26.9, 447.63)]
        for n, (v, p, res) in enumerate(linhas, start=1):
            r = pe.prever([{'rodada': n, 'vendas': v, 'preco': p,
                            'tacos': pe.acos_da_rodada(n) * pe.parcela_da_rodada(n)}],
                          lambda x: x - 19.9)[0]
            self.assertAlmostEqual(r['resultado'], res, delta=0.01)

    def test_chave_de_roda_pela_especificacao_com_precos_a_mao(self):
        mao = {1: 35.9, 2: 36.9, 3: 37.9, 4: 38.9, 5: 39.9, 6: 40.9, 7: 42.9, 8: 44.9}
        rod, (teto, virada) = _plano(200, 45.9, 32.9, 44.9, mao)
        self.assertEqual([r['vendas'] for r in rod], [20, 80, 40, 40, 40, 40, 40, 40])
        self.assertEqual(len(rod), 8)                            # 44,90 = preço meta
        self.assertTrue(all(r['preco_a_mao'] for r in rod))
        self.assertEqual((teto, virada), (0.0, 0))               # nunca fica negativo

    def test_l0421_pela_especificacao(self):
        mao = {1: 20.9, 2: 21.9, 3: 22.9, 4: 23.9, 5: 24.9, 6: 25.9, 7: 26.9}
        rod, (teto, virada) = _plano(200, 28.5, 19.9, 26.9, mao)
        self.assertEqual(len(rod), 7)
        self.assertAlmostEqual(teto, -11.35, delta=0.01)
        self.assertEqual(virada, 45)          # 20 da 1ª + 11,35 ÷ 0,467/un. na 2ª

    def test_preco_a_mao_no_meio_e_os_degraus_seguintes_partem_dele(self):
        rod = pe.montar_rodadas(70, 49.9, 52.9, {2: 46.0})
        self.assertAlmostEqual(rod[1]['preco'], 46.0)
        self.assertTrue(rod[1]['preco_a_mao'])
        self.assertAlmostEqual(rod[2]['preco'], 46.0 * 1.03)
        self.assertFalse(rod[2]['preco_a_mao'])

    def test_meta_propria_vence_o_concorrente(self):
        self.assertEqual(pe.meta_mensal(350, 100), 100.0)
        self.assertEqual(pe.meta_mensal(350, None), 70.0)

    def test_preco_meta_abaixo_da_rodada_1_vira_rodada_unica(self):
        rod = pe.montar_rodadas(70, 100.0, 80.0)
        self.assertEqual(len(rod), 1)
        self.assertEqual(rod[0]['preco'], 80.0)

    def test_virada_none_quando_nao_volta(self):
        rod, (teto, virada) = _plano(350, 49.9, 60.0, 52.9)
        self.assertLess(teto, 0)
        self.assertIsNone(virada)

    def test_guarda_de_rodadas(self):
        with self.assertRaises(ValueError):
            pe.montar_rodadas(70, 10.0, 10000.0)


class TestMargemDoSistema(unittest.TestCase):

    def test_barreira_79_taxa_fixa_abaixo_e_frete_acima(self):
        f = pe.margem_ml(custo=30, comissao=0.115, imposto=0.11, frete_abaixo=None,
                         frete_acima=20.0)
        # abaixo de 79: taxa fixa da Tabela de Preço (R$ 6,75 entre 50 e 79)
        self.assertAlmostEqual(f(78.0), round(78 - 78 * 0.225 - 6.75 - 30, 2))
        # a partir de 79: frete do vendedor
        self.assertAlmostEqual(f(79.0), round(79 - 79 * 0.225 - 20 - 30, 2))
        self.assertLess(f(79.0), f(78.0))                        # a barreira morde

    def test_sem_frete_acima_devolve_none(self):
        f = pe.margem_ml(30, 0.115, 0.11, None, None)
        self.assertIsNone(f(80.0))
        with self.assertRaises(ValueError):
            pe.prever(pe.montar_rodadas(10, 100.0, 120.0), f)

    def test_frete_observado_acompanha_a_faixa_da_taxa_fixa(self):
        # ressalva 1 do auditor: vende hoje a R$ 25 (faixa R$ 4,50) com frete
        # observado R$ 4,80; planejado a R$ 45 (faixa R$ 6,50) -> R$ 6,80
        f = pe.margem_ml(10, 0.1, 0.1, 4.8, None, preco_ref_abaixo=25.0)
        self.assertAlmostEqual(f(45.0), round(45 - 45 * 0.2 - 6.8 - 10, 2))
        self.assertAlmostEqual(f(25.0), round(25 - 25 * 0.2 - 4.8 - 10, 2))
        self.assertAlmostEqual(f(60.0), round(60 - 60 * 0.2 - 7.05 - 10, 2))

    def test_insumos_guardam_o_preco_de_referencia_do_frete(self):
        obs = pe.agregar_observado([('ML-Nala', 'MLB1', 'S', 'UD', False, 10, 250.0, 25.0, 48.0)])
        i = pe.resolver_insumos('ML-Nala', 'MLB1', 'S', 'UD', obs)
        self.assertAlmostEqual(i['preco_ref_abaixo'], 25.0)
        rod, _ = pe.calcular_plano({'loja': 'ML-Nala', 'venda_concorrente': 100,
                                    'meta_propria': None, 'preco_concorrente': 52.94,
                                    'preco_meta': 50.0, 'precos_mao': {}},
                                   {'custo': 10}, 0.1, i)
        # rodada 1 a R$ 45 (52,94 x 0,85): frete 4,80 + (6,50 - 4,50)
        self.assertAlmostEqual(rod[0]['margem_unit'],
                               round(rod[0]['preco'] * 0.8 - 6.8 - 10, 2), delta=0.01)

    def test_extra_da_loja(self):
        a = pe.margem_ml(10, 0.1, 0.1, 6.0, None)(40.0)
        b = pe.margem_ml(10, 0.1, 0.1, 6.0, None, extra=6.0)(40.0)
        self.assertAlmostEqual(a - b, 6.0)

    def test_preco_margem_zero(self):
        f = pe.margem_ml(20, 0.115, 0.11, 6.5, 25.0)
        p = pe.preco_margem_zero(f)
        self.assertGreaterEqual(f(p), 0)
        self.assertLess(f(round(p - 0.02, 2)), 0)
        self.assertAlmostEqual(p, (20 + 6.5) / (1 - 0.225), delta=0.02)

    def test_preco_margem_zero_acima_da_barreira(self):
        f = pe.margem_ml(66.71, 0.107, 0.11, None, 27.5)       # LKE-9945 tem custo alto
        p = pe.preco_margem_zero(f)
        self.assertGreaterEqual(p, 79)
        self.assertAlmostEqual(p, (66.71 + 27.5) / (1 - 0.217), delta=0.02)


def _obs(*linhas):
    return pe.agregar_observado(linhas)


class TestInsumos(unittest.TestCase):
    """Fallback da comissão (armadilha do Mestre, 12/10): anúncio novo ou
    parado (L-0298 revivido) não tem venda própria."""

    def setUp(self):
        self.obs = _obs(
            # loja, anúncio, sku, categoria, acima, qtd, valor, comissão, frete
            ('ML-Nala', 'MLB1', 'L-0298', 'UD', False, 10, 400.0, 46.0, 68.0),
            ('ML-Nala', 'MLB2', 'L-0298', 'UD', False, 5, 200.0, 25.0, 34.0),
            ('ML-Nala', 'MLB9', 'X-1', 'UD', True, 20, 3000.0, 330.0, 540.0),
            ('ML-Nala', 'MLB3', 'L-0308', 'UD', False, 2, 70.0, 9.0, 13.6),
        )

    def test_comissao_do_proprio_anuncio(self):
        i = pe.resolver_insumos('ML-Nala', 'MLB1', 'L-0298', 'UD', self.obs)
        self.assertAlmostEqual(i['comissao'], 0.115)
        self.assertIn('vendas do anúncio', i['comissao_origem'])
        self.assertAlmostEqual(i['frete_abaixo'], 6.8)

    def test_anuncio_novo_cai_para_o_sku_em_outro_anuncio(self):
        i = pe.resolver_insumos('ML-Nala', 'MLB-NOVO', 'L-0298', 'UD', self.obs)
        self.assertAlmostEqual(i['comissao'], 71.0 / 600.0)
        self.assertIn('SKU em outros anúncios', i['comissao_origem'])

    def test_sku_sem_venda_cai_para_a_categoria(self):
        # MLB3 tem só 2 un. (< MIN_UNID_INSUMO): nem anúncio nem SKU servem
        i = pe.resolver_insumos('ML-Nala', 'MLB3', 'L-0308', 'UD', self.obs)
        self.assertIn('categoria', i['comissao_origem'])
        self.assertAlmostEqual(i['comissao'], (46 + 25 + 330 + 9) / (400 + 200 + 3000 + 70))
        self.assertIsNone(i['frete_abaixo'])                     # -> taxa fixa
        self.assertIn('taxa fixa', i['frete_abaixo_origem'])
        self.assertIsNone(i['frete_acima'])
        self.assertIn('informe', i['frete_acima_origem'])

    def test_nada_achado_pede_o_campo(self):
        i = pe.resolver_insumos('ML-LPT', 'MLB7', 'Z', 'OUTRA', self.obs)
        self.assertIsNone(i['comissao'])
        self.assertIn('informe a Comissão', i['comissao_origem'])
        with self.assertRaises(ValueError):
            pe.calcular_plano({'loja': 'ML-LPT', 'venda_concorrente': 100, 'meta_propria': None,
                               'preco_concorrente': 40, 'preco_meta': 45, 'precos_mao': {}},
                              {'custo': 10}, 0.11, i)

    def test_informado_vence_o_observado(self):
        i = pe.resolver_insumos('ML-Nala', 'MLB1', 'L-0298', 'UD', self.obs,
                                comissao_informada=0.165, frete_acima_informado=22.0)
        self.assertEqual((i['comissao'], i['frete_acima']), (0.165, 22.0))
        self.assertEqual(i['comissao_origem'], 'informada no cadastro')

    def test_frete_acima_observado(self):
        i = pe.resolver_insumos('ML-Nala', 'MLB9', 'X-1', 'UD', self.obs)
        self.assertAlmostEqual(i['frete_acima'], 27.0)

    def test_calcular_plano_completo(self):
        i = pe.resolver_insumos('ML-Nala', 'MLB1', 'L-0298', 'UD', self.obs)
        cad = {'loja': 'ML-Nala', 'venda_concorrente': 300, 'meta_propria': None,
               'preco_concorrente': 49.9, 'preco_meta': 52.9, 'precos_mao': {}}
        rod, res = pe.calcular_plano(cad, {'custo': 20.4}, 0.11, i)
        self.assertEqual(res['meta_mensal'], 60.0)
        self.assertEqual(rod[-1]['preco'], 52.9)
        self.assertLessEqual(res['teto'], 0)
        self.assertEqual(res['insumos']['comissao_origem'], i['comissao_origem'])
        # margem unitária = conta da Tabela de Preço no preço de CADA rodada
        r1 = rod[0]
        esperado = round(r1['preco'] - r1['preco'] * (0.115 + 0.11) - 6.8 - 20.4, 2)
        self.assertAlmostEqual(r1['margem_unit'], esperado)


def _dias(inicio, unidades, margem_un=5.0, gasto=1.0, ads_un=0):
    return [{'data': inicio + timedelta(days=i), 'unid': u, 'receita': u * 40.0,
             'margem': u * margem_un, 'gasto': gasto, 'unid_ads': min(u, ads_un)}
            for i, u in enumerate(unidades)]


class TestReal(unittest.TestCase):
    HOJE = date(2026, 10, 12)

    def _prev(self):
        rod = pe.prever(pe.montar_rodadas(20, 49.9, 52.9), lambda p: p - 44.5)
        return rod                       # vendas 10, 40, 20, ...; teto negativo

    def test_fim_fechado_usa_a_constante_do_sinais(self):
        from sinais_dia import DIAS_ATRASO_API
        self.assertEqual(pe.fim_fechado(self.HOJE),
                         self.HOJE - timedelta(days=DIAS_ATRASO_API + 1))

    def test_serie_diaria_preenche_dias_sem_venda(self):
        ini = date(2026, 10, 1)
        s = pe.serie_diaria(ini, date(2026, 10, 3), {date(2026, 10, 2): (3, 120.0, 15.0)},
                            {date(2026, 10, 1): (2.5, 1)})
        self.assertEqual([d['unid'] for d in s], [0, 3, 0])
        self.assertEqual(s[0]['gasto'], 2.5)

    def test_rodada_fecha_no_dia_que_atinge_e_a_sobra_fica_nela(self):
        prev = self._prev()
        reais = pe.alocar_rodadas(prev, _dias(date(2026, 10, 1), [4, 4, 4, 5]))
        self.assertTrue(reais[0]['fechada'])
        self.assertEqual(reais[0]['unid'], 12)                    # previstas 10
        self.assertEqual(reais[0]['fim'], date(2026, 10, 3))
        self.assertEqual(reais[1]['inicio'], date(2026, 10, 4))
        self.assertFalse(reais[1]['fechada'])

    def test_margem_real_subtrai_ads_uma_vez(self):
        reais = pe.alocar_rodadas(self._prev(), _dias(date(2026, 10, 1), [10], 5.0, 7.0))
        self.assertAlmostEqual(reais[0]['margem_real'], 10 * 5.0 - 7.0)

    def test_parcela_do_ads_real(self):
        reais = pe.alocar_rodadas(self._prev(), _dias(date(2026, 10, 1), [10], ads_un=9))
        self.assertAlmostEqual(reais[0]['parcela_ads'], 0.9)

    def test_alerta_parcela_em_2_rodadas_fechadas_seguidas(self):
        prev = self._prev()                      # parcela prevista 75%, 70%
        reais = pe.alocar_rodadas(prev, _dias(date(2026, 10, 1), [10, 40], ads_un=40))
        self.assertTrue(pe.alerta_parcela(prev, reais))         # 100% e 100%
        reais = pe.alocar_rodadas(prev, _dias(date(2026, 10, 1), [10, 40], ads_un=8))
        self.assertFalse(pe.alerta_parcela(prev, reais))        # 80% (+5) e 20%

    def test_ritmo_lenta_depois_de_7_dias(self):
        prev = self._prev()[0]                   # 10 vendas em 15 dias = 0,67/dia
        real = pe.alocar_rodadas([prev], _dias(date(2026, 10, 1), [0] * 5 + [1]))[0]
        self.assertFalse(pe.ritmo(prev, real)['lenta'])         # 6 dias: ainda não julga
        real = pe.alocar_rodadas([prev], _dias(date(2026, 10, 1), [0] * 7 + [1]))[0]
        r = pe.ritmo(prev, real)
        self.assertTrue(r['lenta'])
        self.assertEqual(r['lenta_seguidos'], 2)

    def _acompanhar(self, unidades, inicio=date(2026, 10, 1), margem_un=-10.0, teto=None,
                    situacao='ativo'):
        prev = self._prev()
        t = pe.teto_e_virada(prev)[0] if teto is None else teto
        plano = {'situacao': situacao, 'data_inicio': inicio, 'teto': t}
        return pe.acompanhar(plano, prev, _dias(inicio, unidades, margem_un, 0.0), self.HOJE)

    def test_pede_revisao_abaixo_do_teto(self):
        a = self._acompanhar([3] * 11, margem_un=-30.0)          # −990 < TETO
        self.assertEqual(a['situacao'], 'pede revisão')
        self.assertIn('TETO', a['recomendacao'])

    def test_ativo_dentro_do_teto(self):
        a = self._acompanhar([1] * 5, margem_un=-1.0)
        self.assertEqual(a['situacao'], 'ativo')
        self.assertEqual(a['rodada_atual'], 1)

    def test_aguardando_inicio(self):
        a = pe.acompanhar({'situacao': 'ativo', 'data_inicio': self.HOJE, 'teto': -10},
                          self._prev(), [], self.HOJE)
        self.assertEqual(a['situacao'], 'aguardando início')
        self.assertEqual(a['rodada_atual'], 1)

    def test_concluido_quando_a_rodada_no_preco_meta_fecha(self):
        prev = self._prev()
        por_dia = [r['vendas'] for r in prev]    # um dia fecha uma rodada (a sobra fica nela)
        a = pe.acompanhar({'situacao': 'ativo', 'data_inicio': date(2026, 9, 1), 'teto': -1e9},
                          prev, _dias(date(2026, 9, 1), por_dia, 5.0, 0.0), self.HOJE)
        self.assertEqual(a['situacao'], 'concluído')
        a = pe.acompanhar({'situacao': 'ativo', 'data_inicio': date(2026, 9, 1), 'teto': -1e9},
                          prev, _dias(date(2026, 9, 1), por_dia[:-1], 5.0, 0.0), self.HOJE)
        self.assertEqual(a['situacao'], 'ativo')
        self.assertEqual(a['rodada_atual'], len(prev))

    def test_encerrado_pela_gestora(self):
        a = self._acompanhar([1], situacao='encerrado')
        self.assertEqual(a['situacao'], 'encerrado')

    def test_rodada_negativa_lenta_7_dias_recomenda_margem_zero(self):
        a = self._acompanhar([0] * 14)                           # rodada 1 é negativa
        self.assertTrue(a['ritmo']['lenta'])
        self.assertGreaterEqual(a['ritmo']['lenta_seguidos'], 7)
        self.assertIn('margem zero', a['recomendacao'])


class TestCadastro(unittest.TestCase):
    HOJE = date(2026, 10, 12)

    def _bruta(self, **kw):
        b = {'loja': 'ML-LPT', 'anuncio': 'MLB3873776447', 'sku': 'L-0308', 'produto': 'x',
             'venda_concorrente': 300, 'meta_propria': None, 'preco_concorrente': '39,90',
             'preco_meta': 42.9, 'data_inicio': '15/10/2026', 'gestora': 'Larissa',
             'comissao': None, 'frete_acima': None, 'precos_mao': None, 'motivo': None}
        b.update(kw)
        return b

    @staticmethod
    def _calc(cad):
        rod = pe.prever(pe.montar_rodadas(pe.meta_mensal(cad['venda_concorrente'],
                                                         cad['meta_propria']),
                                          cad['preco_concorrente'], cad['preco_meta'],
                                          cad['precos_mao']), lambda p: p - 33.0)
        t, v = pe.teto_e_virada(rod)
        return rod, {'teto': t, 'virada': v, 'meta_mensal': 0, 'resultado_total': 0,
                     'vendas_total': 0, 'preco_margem_zero': 33.0, 'insumos': {}}

    def _previa(self, brutas, ativos=None, lojas=('ML-LPT', 'ML-Nala')):
        return pe.previa(brutas, ativos or {}, set(lojas), self.HOJE, self._calc)

    def test_normaliza_numeros_datas_e_precos_a_mao(self):
        c = pe.normalizar(self._bruta(precos_mao='1=33,90; 3=R$ 36,50', comissao='12,5'))
        self.assertEqual(c['preco_concorrente'], 39.9)
        self.assertEqual(c['data_inicio'], date(2026, 10, 15))
        self.assertEqual(c['precos_mao'], {1: 33.9, 3: 36.5})
        self.assertAlmostEqual(c['comissao'], 0.125)

    def test_recusa_com_todas_as_falhas(self):
        with self.assertRaises(ValueError) as e:
            pe.normalizar(self._bruta(anuncio='123', venda_concorrente=None, gestora=''))
        msg = str(e.exception)
        self.assertIn('MLB', msg)
        self.assertIn('concorrente', msg)
        self.assertIn('Gestora', msg)

    def test_novo(self):
        it = self._previa([self._bruta()])
        self.assertEqual(it[0]['acao'], 'novo')
        self.assertEqual(it[0]['n'], 2)

    def test_loja_fora_das_permitidas_e_recusada(self):
        it = self._previa([self._bruta()], lojas=('ML-Nala',))
        self.assertEqual(it[0]['acao'], 'recusado')

    def test_anuncio_repetido_no_arquivo(self):
        it = self._previa([self._bruta(), self._bruta()])
        self.assertEqual([i['acao'] for i in it], ['novo', 'recusado'])

    def _ativo(self, inicio, **kw):
        a = {'id': 7, 'loja': 'ML-LPT', 'anuncio': 'MLB3873776447', 'sku': 'L-0308',
             'venda_concorrente': 300, 'meta_propria': None, 'preco_concorrente': 39.9,
             'preco_meta': 42.9, 'data_inicio': inicio, 'gestora': 'Larissa',
             'comissao': None, 'frete_acima': None, 'precos_mao': {}, 'concluido': False}
        a.update(kw)
        return {('ML-LPT', 'MLB3873776447'): a}

    def test_subir_o_mesmo_arquivo_nao_muda_nada(self):
        it = self._previa([self._bruta()], self._ativo(date(2026, 10, 15)))
        self.assertEqual(it[0]['acao'], 'sem mudança')

    def test_plano_concluido_reenviado_igual_nao_vira_plano_novo(self):
        it = self._previa([self._bruta()], self._ativo(date(2026, 10, 15), concluido=True))
        self.assertEqual(it[0]['acao'], 'sem mudança')

    def test_revisao_de_plano_comecado_exige_motivo(self):
        ativos = self._ativo(date(2026, 10, 1))
        it = self._previa([self._bruta(preco_meta=44.9)], ativos)
        self.assertEqual(it[0]['acao'], 'recusado')
        self.assertIn('motivo', it[0]['erro'])
        it = self._previa([self._bruta(preco_meta=44.9, motivo='concorrente subiu')], ativos)
        self.assertEqual(it[0]['acao'], 'revisao')

    def test_plano_nao_comecado_substitui_sem_motivo(self):
        it = self._previa([self._bruta(preco_meta=44.9)], self._ativo(date(2026, 10, 20)))
        self.assertEqual(it[0]['acao'], 'cadastro')

    def test_plano_concluido_com_mudanca_vira_novo(self):
        it = self._previa([self._bruta(data_inicio='01/11/2026')],
                          self._ativo(date(2026, 9, 1), concluido=True))
        self.assertEqual(it[0]['acao'], 'novo')

    def test_preco_a_mao_em_rodada_inexistente_avisa(self):
        # ressalva 3 do auditor: não descartar calado
        obs = pe.agregar_observado([('ML-LPT', 'MLB3873776447', 'L-0308', 'UD', False, 10,
                                     360.0, 45.0, 68.0)])
        calc = pe.calculadora({'L-0308': {'nome': 'Suporte', 'categoria': 'UD', 'custo': 15.9}},
                              {'ML-LPT': 0.11}, obs)
        it = pe.previa([self._bruta(precos_mao='1=33,90; 30=50,00')], {}, {'ML-LPT'},
                       self.HOJE, calc)
        self.assertEqual(it[0]['acao'], 'novo')
        self.assertIn('30', it[0]['aviso'])
        self.assertIn('ignorado', pe.tabela_previa(it, {})['Aviso'][0])

    def test_linha_vazia_e_ignorada(self):
        vazia = {c: None for c, _ in pe.COLUNAS}
        vazia['produto'] = 'sobra do modelo'
        self.assertEqual(self._previa([vazia]), [])

    def test_excel_ida_e_volta_no_mesmo_layout(self):
        linha = pe.linha_do_plano({
            'loja': 'ML-LPT', 'anuncio': 'MLB3873776447', 'sku': 'L-0308',
            'venda_concorrente': 300, 'meta_propria': None, 'preco_concorrente': 39.9,
            'preco_meta': 42.9, 'data_inicio': date(2026, 10, 15), 'gestora': 'Larissa',
            'parametros': {'informado': {'comissao': 0.125, 'frete_acima': None},
                           'precos_mao': {'1': 33.9}}}, {'L-0308': 'Suporte inox'})
        brutas = pe.ler_excel(pe.excel_cadastro([linha]))
        self.assertEqual(len(brutas), 1)
        c = pe.normalizar(brutas[0])
        self.assertEqual(c['data_inicio'], date(2026, 10, 15))
        self.assertAlmostEqual(c['comissao'], 0.125)
        self.assertEqual(c['precos_mao'], {1: 33.9})
        # o arquivo baixado e subido sem mexer não muda o plano
        ativo = {**{k: c[k] for k in pe.CAMPOS_PLANO}, 'concluido': False}
        self.assertTrue(pe._igual(c, ativo))

    def test_excel_com_layout_errado_e_recusado(self):
        from openpyxl import Workbook
        wb = Workbook()
        wb.active.append(['Loja', 'Anúncio'])
        buf = io.BytesIO()
        wb.save(buf)
        with self.assertRaises(ValueError):
            pe.ler_excel(buf.getvalue())


class TestSqlEDdl(unittest.TestCase):
    """Sem banco: o DDL tem as CHECKs que o app supõe e as SQL não têm % solto."""

    def test_ddl(self):
        ddl = open(os.path.join(RAIZ, 'sql', 'plano_escala.sql'), encoding='utf-8').read()
        self.assertIn('ux_plano_escala_ativo', ddl)
        self.assertIn("WHERE situacao = 'ativo'", ddl)
        self.assertEqual(len(re.findall(r'CONSTRAINT \w+ CHECK', ddl)), 10)
        self.assertIn('IF n_check <> 10', ddl)
        for tipo in ('cadastro', 'revisao', 'encerramento', 'concluido'):
            self.assertIn(f"'{tipo}'", ddl)
        self.assertTrue(os.path.exists(os.path.join(RAIZ, 'sql', 'plano_escala_DESFAZER.sql')))

    def test_sql_sem_porcento_solto(self):
        for nome in dir(pe):
            if nome.startswith('SQL_'):
                sql = getattr(pe, nome)
                self.assertFalse(re.search(r'%(?!\()', sql), nome)

    def test_leituras_so_do_mercado_livre_e_ate_o_dia_fechado(self):
        for sql in (pe.SQL_REAL_VENDAS, pe.SQL_REAL_ADS, pe.SQL_OBSERVADO):
            self.assertIn('%(marketplace)s', sql)
            self.assertIn('<= %(fim)s', sql)
        self.assertIn('logistica = ANY', pe.SQL_OBSERVADO)       # sem FLEX
        # ressalva 2: parcela do ads em UNIDADES (diretas + indiretas)
        self.assertIn('vendas_diretas + vendas_indiretas', pe.SQL_REAL_ADS)


class TestPermissao(unittest.TestCase):

    def test_menu_e_niveis(self):
        import permissoes as pm
        self.assertIn('plano_escala', pm.MODULOS)
        self.assertIn('plano_escala', pm.MENU_MODULOS.values())
        self.assertEqual(pm.PERMISSOES['GESTOR']['plano_escala'], 'parcial')
        self.assertEqual(pm.PERMISSOES['DIRETOR']['plano_escala'], 'leitura')
        for perfil in pm.PERMISSOES:
            self.assertIn('plano_escala', pm.PERMISSOES[perfil])


if __name__ == '__main__':
    unittest.main()
