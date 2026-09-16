"""
Testa a cobertura do Full e o estado dos agendamentos (cobertura_full.py).

Os três primeiros casos são REAIS: números da v_cobertura_full em 15/09/2026,
conferidos à mão pelo Auditor. Se um deles mudar, a regra mudou.

Rodar:  python tests/teste_cobertura_full.py
"""

import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cobertura_full as cf  # noqa: E402

ML = cf.parametros('MERCADO LIVRE')


def linha(disp, transito, venda7, dias7, venda30, dias30, vendidas7=0, **extra):
    return dict(full_disponivel=disp, full_em_transferencia=transito,
                venda_dia_7d=venda7, dias_com_estoque_7d=dias7,
                venda_dia_30d=venda30, dias_com_estoque_30d=dias30,
                vendidas_7d=vendidas7, **extra)


class CasosReais15Set(unittest.TestCase):
    def test_k100_l_0390_seis_dias_pela_janela_de_7(self):
        r = cf.avaliar(linha(113, 0, '18.7142857142857143', 7, '16.5', 30, 132), ML)
        self.assertEqual(r['janela_que_disparou'], '7d')
        self.assertAlmostEqual(r['cobertura_disponivel'], 6.0, places=1)
        self.assertEqual(r['nivel'], cf.QUEBRA_GARANTIDA)
        self.assertFalse(r['acelerou'])

    def test_k_l_0414_nove_e_meio_e_onze_com_transito(self):
        r = cf.avaliar(linha(103, 16, '10.8571428571428571', 7, '8.4666666666666667', 30, 78), ML)
        self.assertEqual(r['janela_que_disparou'], '7d')
        self.assertAlmostEqual(r['cobertura_disponivel'], 9.5, places=1)
        self.assertAlmostEqual(r['cobertura_com_transito'], 11.0, places=1)
        self.assertEqual(r['nivel'], cf.VERMELHO)

    def test_l_0232_oito_dias_pela_janela_de_30(self):
        r = cf.avaliar(linha(20, 2, '2.4285714285714286', 7, '2.5', 30, 17), ML)
        self.assertEqual(r['janela_que_disparou'], '30d')
        self.assertAlmostEqual(r['cobertura_disponivel'], 8.0, places=1)
        self.assertEqual(r['nivel'], cf.VERMELHO)


class Janelas(unittest.TestCase):
    def test_semana_com_poucos_dias_de_estoque_usa_30_e_avisa(self):
        # 2 dias com estoque e 10 vendas: seriam 5/dia — ruído
        r = cf.avaliar(linha(30, 0, 5.0, 2, 1.0, 20, 10), ML)
        self.assertEqual(r['janela_que_disparou'], '30d')
        self.assertEqual(r['venda_dia'], 1.0)
        self.assertTrue(any('poucos dias' in a for a in r['avisos']))

    def test_acelerou_so_com_volume(self):
        r = cf.avaliar(linha(100, 0, 10.0, 7, 5.0, 30, 70), ML)
        self.assertTrue(r['acelerou'])
        # mesma proporção, mas só 4 unidades na semana: ruído
        r = cf.avaliar(linha(100, 0, 4 / 7, 7, 0.2, 30, 4), ML)
        self.assertFalse(r['acelerou'])

    def test_sem_venda_e_sem_giro_e_nao_manda_enviar(self):
        r = cf.avaliar(linha(50, 0, 0, 7, 0, 30), ML, agendado_aberto=0, dias_alvo=30)
        self.assertEqual(r['nivel'], cf.SEM_GIRO)
        self.assertEqual(r['enviar'], 0)

    def test_venda_negativa_nunca_chega_aqui_como_zero_errado(self):
        # a view já entrega GREATEST(0, ...); None em uma janela não quebra nada
        r = cf.avaliar(linha(10, 0, None, 0, 2.0, 12), ML)
        self.assertEqual(r['janela_que_disparou'], '30d')


class Niveis(unittest.TestCase):
    def test_fronteiras_com_lead_14_e_folga_7(self):
        esperado = [(6.9, cf.QUEBRA_GARANTIDA), (7, cf.VERMELHO), (20.9, cf.VERMELHO),
                    (21, cf.LARANJA), (27.9, cf.LARANJA), (28, cf.VERDE)]
        for cobertura, nivel in esperado:
            self.assertEqual(cf.nivel_por_cobertura(cobertura, ML), nivel, cobertura)

    def test_folga_e_ajustavel_na_tela(self):
        sem_folga = cf.parametros('MERCADO LIVRE', folga_semana_perdida_dias=0)
        self.assertEqual(cf.nivel_por_cobertura(14, sem_folga), cf.LARANJA)
        self.assertEqual(cf.nivel_por_cobertura(14, ML), cf.VERMELHO)


class Enviar(unittest.TestCase):
    def test_soma_lead_e_desconta_transito_e_agendado(self):
        # (30 + 21) x 2 = 102; menos 10 disponível, 5 em trânsito, 3 agendados
        self.assertEqual(cf.quanto_enviar(2.0, 10, 5, 3, 30, ML), 84)

    def test_nunca_negativo(self):
        self.assertEqual(cf.quanto_enviar(1.0, 500, 0, 0, 30, ML), 0)

    def test_sem_agendamento_informado_avisa(self):
        r = cf.avaliar(linha(10, 0, 2.0, 7, 2.0, 30, 14), ML, agendado_aberto=None, dias_alvo=30)
        self.assertTrue(any('agendamento' in a for a in r['avisos']))


class Avisos(unittest.TestCase):
    def test_bloqueio_fiscal_acima_de_60_dias(self):
        r = cf.avaliar(linha(5, 0, 1.0, 7, 1.0, 30, 7, full_bloqueado_fiscal=1,
                             dias_bloqueio_fiscal=61), ML)
        self.assertTrue(r['alerta_bloqueio_fiscal'])
        r = cf.avaliar(linha(5, 0, 1.0, 7, 1.0, 30, 7, full_bloqueado_fiscal=1,
                             dias_bloqueio_fiscal=46), ML)
        self.assertFalse(r['alerta_bloqueio_fiscal'])

    def test_dado_atrasado_e_serie_divergente_aparecem(self):
        r = cf.avaliar(linha(5, 0, 1.0, 7, 1.0, 30, 7, dado_atrasado=True,
                             data_do_dado=date(2026, 9, 10), serie_divergente=True), ML)
        self.assertEqual(len(r['avisos']), 2)


def ag(id_, data_coleta, quantidade, sinal=True, estoque='UP1', mk='MERCADO LIVRE'):
    return {'id': id_, 'marketplace': mk, 'loja': 'ML-LPT', 'estoque_id': estoque,
            'data_coleta': data_coleta, 'quantidade': quantidade,
            'fonte_tem_sinal_coleta': sinal}


def ondas(*dias, mk='MERCADO LIVRE'):
    return cf.dias_de_onda({(mk, d): 500 for d in dias}, ML)


def situacao(agendamentos, entradas, validos, hoje):
    return {a['id']: a for a in cf.situacao_agendamentos(
        agendamentos, {('MERCADO LIVRE', 'ML-LPT', 'UP1'): entradas}, validos, hoje,
        cf.parametros)}


class Agendamentos(unittest.TestCase):
    def test_nao_coletado_nao_herda_a_coleta_da_semana_seguinte(self):
        s = situacao([ag(1, date(2026, 9, 2), 50), ag(2, date(2026, 9, 9), 50)],
                     [(date(2026, 9, 9), 50)], ondas(date(2026, 9, 9)), date(2026, 9, 15))
        self.assertEqual((s[1]['coletado'], s[1]['situacao']), (0, cf.NAO_COLETADO))
        self.assertEqual((s[2]['coletado'], s[2]['situacao']), (50, cf.COLETADO))
        self.assertEqual(s[1]['a_descontar'] + s[2]['a_descontar'], 0)

    def test_ruido_fora_de_dia_de_onda_nao_fecha(self):
        s = situacao([ag(1, date(2026, 8, 5), 10)], [(date(2026, 8, 6), 10)],
                     ondas(date(2026, 8, 10)), date(2026, 8, 7))
        self.assertEqual((s[1]['coletado'], s[1]['situacao']), (0, cf.AGUARDANDO))
        self.assertEqual(s[1]['a_descontar'], 10)

    def test_entrada_no_dia_seguinte_a_onda_conta(self):
        s = situacao([ag(1, date(2026, 8, 10), 30)], [(date(2026, 8, 11), 30)],
                     ondas(date(2026, 8, 10)), date(2026, 8, 12))
        self.assertEqual(s[1]['situacao'], cf.COLETADO)

    def test_noventa_por_cento_ja_e_coletado(self):
        s = situacao([ag(1, date(2026, 9, 12), 50)], [(date(2026, 9, 12), 46)],
                     ondas(date(2026, 9, 12)), date(2026, 9, 13))
        self.assertEqual((s[1]['situacao'], s[1]['a_descontar']), (cf.COLETADO, 0))

    def test_fifo_entre_janelas_sobrepostas_sem_contar_duas_vezes(self):
        s = situacao([ag(2, date(2026, 9, 12), 30), ag(1, date(2026, 9, 10), 30)],
                     [(date(2026, 9, 12), 40)], ondas(date(2026, 9, 12)), date(2026, 9, 13))
        self.assertEqual((s[1]['coletado'], s[1]['situacao']), (30, cf.COLETADO))
        self.assertEqual((s[2]['coletado'], s[2]['situacao']), (10, cf.COLETADO_EM_PARTE))
        self.assertEqual(s[2]['a_descontar'], 20)
        self.assertEqual(s[1]['coletado'] + s[2]['coletado'], 40)

    def test_coletado_em_parte_para_de_descontar_quando_a_janela_fecha(self):
        s = situacao([ag(1, date(2026, 9, 1), 30)], [(date(2026, 9, 1), 10)],
                     ondas(date(2026, 9, 1)), date(2026, 9, 10))
        self.assertEqual((s[1]['situacao'], s[1]['a_descontar']), (cf.COLETADO_EM_PARTE, 0))

    def test_ainda_na_tolerancia_continua_aguardando(self):
        s = situacao([ag(1, date(2026, 9, 12), 30)], [], set(), date(2026, 9, 15))
        self.assertEqual(s[1]['situacao'], cf.AGUARDANDO)
        s = situacao([ag(1, date(2026, 9, 12), 30)], [], set(), date(2026, 9, 16))
        self.assertEqual(s[1]['situacao'], cf.NAO_COLETADO)

    def test_fonte_sem_sinal_desconta_so_enquanto_a_janela_esta_aberta(self):
        s = situacao([ag(1, date(2026, 9, 12), 30, sinal=False)], [], set(), date(2026, 9, 16))
        self.assertEqual((s[1]['situacao'], s[1]['a_descontar']), (cf.SEM_SINAL, 30))
        s = situacao([ag(1, date(2026, 9, 12), 30, sinal=False)], [], set(), date(2026, 9, 19))
        self.assertEqual(s[1]['a_descontar'], 0)

    def test_onda_e_do_marketplace_e_tem_corte(self):
        validos = cf.dias_de_onda({('MERCADO LIVRE', date(2026, 9, 1)): 99,
                                   ('MERCADO LIVRE', date(2026, 9, 2)): 100}, ML)
        self.assertNotIn(('MERCADO LIVRE', date(2026, 9, 1)), validos)
        self.assertIn(('MERCADO LIVRE', date(2026, 9, 2)), validos)
        self.assertIn(('MERCADO LIVRE', date(2026, 9, 3)), validos)

    def test_soma_do_agendado_por_estoque(self):
        s = cf.situacao_agendamentos(
            [ag(1, date(2026, 9, 14), 10), ag(2, date(2026, 9, 15), 5)], {}, set(),
            date(2026, 9, 15), cf.parametros)
        self.assertEqual(cf.agendado_por_estoque(s),
                         {('MERCADO LIVRE', 'ML-LPT', 'UP1'): 15})


if __name__ == '__main__':
    unittest.main(verbosity=1)
