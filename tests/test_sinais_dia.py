"""
Testa o painel Sinais do Dia (sinais_dia.py, frente [SINAIS DO DIA], v1).

Duas partes:
  - Sem banco: as regras de cada sinal com os casos reais da calibragem do
    [MESTRE ANÁLISES] (05/10/2026) — K-L-0421-A (Full por estoque_id, nunca
    somado por SKU), Garrafa LTT-CT3821 (sem estoque não é espiral, é
    ruptura), família de alicates (experiência por peça base) —, janelas com
    D-1/D-2 fora, corte de 10 por loja, permissão que NÃO abre tudo para
    usuário sem loja, e % solto nas SQL.
  - Com banco (NALA_TEST_DB_URL, usuário de permissão mínima): EXECUTA as SQL
    de verdade em tabelas TEMPORÁRIAS com os nomes das reais, com as CHECK
    COPIADAS da tabela real (lição de 05/10: a TEMP sem CHECK aceitava o que
    produção recusa), conferidas em pg_temp antes de qualquer INSERT, e SEMPRE
    ROLLBACK.

Rodar:  python -m pytest tests/test_sinais_dia.py
"""

import os
import re
import sys
import unittest
from datetime import date, datetime
from unittest import mock

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import sinais_dia as sd  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')
HOJE = date(2026, 10, 5)
LPT, NALA, RJ, SP = 'ML-LPT', 'ML-Nala', 'ML-YanniRJ', 'ML-YanniSP'


def _vend(loja, anuncio, sku, rec_sem=0, rec_base=0, qtd_ritmo=0, qtd_30=0,
          rec_30=0, margem_30=None):
    return (loja, anuncio, sku, rec_sem, rec_base, qtd_ritmo, qtd_30, rec_30, margem_30)


def _ads(gasto_sem=0, gasto_base=0, gasto_ontem=0, gasto_recente=0, dias_com_gasto=0,
         unid_recente=0, gasto_30=0, unid_ads_30=0, cliques_30=0):
    return dict(gasto_sem=gasto_sem, gasto_base=gasto_base, gasto_ontem=gasto_ontem,
                gasto_recente=gasto_recente, dias_com_gasto=dias_com_gasto,
                unid_recente=unid_recente, gasto_30=gasto_30, unid_ads_30=unid_ads_30,
                cliques_30=cliques_30)


class Janelas(unittest.TestCase):
    def test_d1_e_d2_fora_e_semanas_inteiras_sem_sobrepor(self):
        j = sd.janelas(HOJE)
        self.assertEqual(j['fim'], date(2026, 10, 2))          # D-3
        self.assertEqual(j['sem_ini'], date(2026, 9, 26))      # D-9
        self.assertEqual(j['base_fim'], date(2026, 9, 25))     # D-10
        self.assertEqual(j['base_ini'], date(2026, 8, 29))     # D-37
        self.assertEqual((j['base_fim'] - j['base_ini']).days + 1, 28)
        self.assertEqual(j['ritmo_ini'], date(2026, 9, 26))
        self.assertEqual(j['piso_ini'], date(2026, 9, 3))
        self.assertEqual(j['mesmo_dia'], [date(2026, 9, 27), date(2026, 9, 20),
                                          date(2026, 9, 13), date(2026, 9, 6)])
        self.assertTrue(all(d.weekday() == j['ontem'].weekday() for d in j['mesmo_dia']))

    def test_dia_1_usa_o_mes_de_ontem(self):
        j = sd.janelas(date(2026, 11, 1))
        self.assertEqual(j['mes_ini'], date(2026, 10, 1))
        self.assertEqual(sd.params(date(2026, 11, 1), [LPT])['ano_mes'], '2026-10')


class Full(unittest.TestCase):
    # K-L-0421-A na ML-LPT, números reais de 04/10/2026
    PONTE = [(LPT, 'MLB5183384677', 'MLBU5071722523', 'K-L-0421-A', True),
             (LPT, 'MLB4793561323', 'MLBU4131286394', 'K-L-0421-A', True)]
    FOTO = [(LPT, 'MLBU5071722523', date(2026, 10, 4), 6, 2, 0),
            (LPT, 'MLBU4131286394', date(2026, 10, 4), 89, 0, 0)]
    VENDAS = [_vend(LPT, 'MLB5183384677', 'K-L-0421-A', qtd_ritmo=21, qtd_30=65, rec_30=2000),
              _vend(LPT, 'MLB4793561323', 'K-L-0421-A', qtd_ritmo=2, qtd_30=12, rec_30=380)]

    def _full(self, foto=None, ponte=None, vendas=None, ads=None):
        _a, por_sku = sd.agregar_vendas(vendas if vendas is not None else self.VENDAS)
        return sd.sinais_full(foto or self.FOTO, ponte or self.PONTE, por_sku, ads or {},
                              sd.janelas(HOJE))

    def test_k_l_0421_a_por_estoque_id_nunca_somado(self):
        s = self._full()
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]['anuncio'], 'MLB5183384677')
        self.assertTrue(s[0]['urgente'])
        # (6 + 2) ÷ max(21/7, 65/30) = 2,7 dias; a soma do SKU (95) nunca aparece
        self.assertIn('3 dias de Full (6 disp. + 2 em transf.', s[0]['numero'])
        self.assertNotIn('95', s[0]['numero'])
        self.assertNotIn('97', s[0]['numero'])

    def test_piso_da_media_de_30_quando_a_semana_e_fraca(self):
        vendas = [_vend(LPT, 'MLB5183384677', 'K-L-0421-A', qtd_ritmo=7, qtd_30=90)]
        s = self._full(vendas=vendas)
        self.assertIn('÷ 3,0/dia', s[0]['numero'])          # 90/30 = 3 > 7/7 = 1

    def test_dois_anuncios_no_mesmo_estoque_somam_a_venda(self):
        ponte = [(LPT, 'MLB1', 'E1', 'L-0320', True), (LPT, 'MLB2', 'E1', 'L-0320', True)]
        foto = [(LPT, 'E1', date(2026, 10, 4), 20, 0, 0)]
        vendas = [_vend(LPT, 'MLB1', 'L-0320', qtd_ritmo=14, qtd_30=30),
                  _vend(LPT, 'MLB2', 'L-0320', qtd_ritmo=14, qtd_30=30)]
        s = self._full(foto, ponte, vendas)
        self.assertEqual(len(s), 1)
        self.assertIn('5 dias de Full', s[0]['numero'])     # 20 ÷ (28/7)

    def test_venda_de_outro_sku_no_mesmo_anuncio_nao_entra(self):
        ponte = [(LPT, 'MLB1', 'E1', 'L-0320', True)]
        foto = [(LPT, 'E1', date(2026, 10, 4), 20, 0, 0)]
        vendas = [_vend(LPT, 'MLB1', 'L-0320', qtd_ritmo=7, qtd_30=30),
                  _vend(LPT, 'MLB1', 'K2-L-0320', qtd_ritmo=700, qtd_30=3000)]
        s = self._full(foto, ponte, vendas)
        self.assertEqual(s, [])                               # 20 ÷ 1 = 20 dias

    def test_full_zerado_com_ads_ligado_e_envio_que_entrou(self):
        foto = [(LPT, 'MLBU5071722523', date(2026, 10, 4), 0, 30, 0),
                (LPT, 'MLBU4131286394', date(2026, 10, 4), 89, 0, 40)]
        ads = {(LPT, 'MLB5183384677'): _ads(gasto_ontem=12.5)}
        s = self._full(foto=foto, ads=ads)
        textos = [x['numero'] for x in s]
        self.assertTrue(any(t.startswith('Full zerado com ads ligado') and '30 em transferência' in t
                            and 'ads do anúncio, todas as variações' in t
                            for t in textos))
        self.assertTrue(any(t.startswith('Envio entrou no Full em 04/10: +40') for t in textos))

    def test_anuncio_fora_do_full_nao_tem_cobertura_de_full(self):
        ponte = [(LPT, 'MLB1', 'E1', 'L-0320', False)]
        foto = [(LPT, 'E1', date(2026, 10, 4), 0, 0, 0)]
        self.assertEqual(self._full(foto, ponte, [_vend(LPT, 'MLB1', 'L-0320', 7, 0, 7, 30)]), [])


class Ads(unittest.TestCase):
    def _ads(self, ads, vendas, dias_est, config=()):
        por_an, _ = sd.agregar_vendas(vendas)
        return sd.sinais_ads(ads, list(config), por_an, dias_est, sd.janelas(HOJE))

    def test_espiral_so_com_estoque_a_semana_toda(self):
        vendas = [_vend(LPT, 'MLB1', 'L-0320', rec_sem=300, rec_base=4000)]
        ads = {(LPT, 'MLB1'): _ads(gasto_sem=20, gasto_base=400)}
        s = self._ads(ads, vendas, {(LPT, 'MLB1'): 7})
        self.assertEqual([x['tipo'] for x in s], ['ADS'])
        self.assertTrue(s[0]['numero'].startswith('Espiral'))
        self.assertIn('TACOS', s[0]['numero'])

    def test_garrafa_ltt_ct3821_sem_full_e_ruptura_nao_espiral(self):
        vendas = [_vend(NALA, 'MLB7175749876', 'LTT-CT3821', rec_sem=150, rec_base=3200)]
        ads = {(NALA, 'MLB7175749876'): _ads(gasto_sem=10, gasto_base=300)}
        s = self._ads(ads, vendas, {(NALA, 'MLB7175749876'): 3})
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]['tipo'], 'FULL')
        self.assertTrue(s[0]['numero'].startswith('Ruptura: estoque em só 3 de 7'))
        self.assertFalse(any(x['numero'].startswith('Espiral') for x in s))

    def test_sem_leitura_de_estoque_a_espiral_nao_e_julgada(self):
        vendas = [_vend(LPT, 'MLB1', 'L-0320', rec_sem=300, rec_base=4000)]
        ads = {(LPT, 'MLB1'): _ads(gasto_sem=20, gasto_base=400)}
        por_an, _ = sd.agregar_vendas(vendas)
        s = sd.sinais_ads(ads, [], por_an, {}, sd.janelas(HOJE), com_estoque=False)
        self.assertEqual(s, [])

    def test_gasto_com_zero_venda_por_3_dias(self):
        ads = {(LPT, 'MLB1'): _ads(gasto_recente=31.2, dias_com_gasto=3, unid_recente=0),
               (LPT, 'MLB2'): _ads(gasto_recente=31.2, dias_com_gasto=2, unid_recente=0),
               (LPT, 'MLB3'): _ads(gasto_recente=31.2, dias_com_gasto=3, unid_recente=1)}
        s = self._ads(ads, [], {})
        self.assertEqual([x['anuncio'] for x in s], ['MLB1'])
        self.assertAlmostEqual(s[0]['em_jogo'], 31.2)

    def test_custo_por_venda_acima_da_margem_por_unidade(self):
        vendas = [_vend(LPT, 'MLB1', 'L-0320', qtd_30=10, margem_30=50)]   # R$ 5/un
        ads = {(LPT, 'MLB1'): _ads(gasto_30=80, unid_ads_30=10, cliques_30=200)}  # R$ 8/un
        s = self._ads(ads, vendas, {})
        self.assertEqual(len(s), 1)
        self.assertIn('R$ 8,00/un', s[0]['numero'])
        self.assertIn('margem R$ 5,00/un', s[0]['numero'])
        self.assertIn('CPC R$ 0,40', s[0]['numero'])
        self.assertAlmostEqual(s[0]['em_jogo'], 30.0)

    def test_custo_por_venda_nao_divide_unidades_de_skus_diferentes(self):
        vendas = [_vend(LPT, 'MLB1', 'L-0250-A', qtd_30=5, margem_30=10),
                  _vend(LPT, 'MLB1', 'L-0250-B', qtd_30=5, margem_30=10)]
        ads = {(LPT, 'MLB1'): _ads(gasto_30=80, unid_ads_30=10, cliques_30=200)}
        self.assertEqual(self._ads(ads, vendas, {}), [])

    def test_mudanca_de_roas_e_orcamento(self):
        config = [(LPT, 'MLB5379140666 Pote Divisoria', 'MLB5379140666 Pote Divisoria',
                   date(2026, 10, 4), HOJE, 14, 10, 12, 20)]
        vendas = [_vend(LPT, 'MLB5379140666', 'L-0400', rec_30=1500)]
        s = self._ads({}, vendas, {}, config)
        self.assertEqual(s[0]['anuncio'], 'MLB5379140666')
        self.assertIn('ROAS objetivo 14,0 → 10,0', s[0]['numero'])
        self.assertIn('orçamento R$ 12,00 → R$ 20,00/dia', s[0]['numero'])
        self.assertAlmostEqual(s[0]['em_jogo'], 1500)

    def test_anuncio_da_campanha(self):
        self.assertEqual(sd.anuncio_da_campanha('MLB4639959197 Torx + Allen'), 'MLB4639959197')
        self.assertIsNone(sd.anuncio_da_campanha('Campanha geral', None))


class Experiencia(unittest.TestCase):
    # Alicates da ML-LPT: 4 kits caíram de 75–100 para 65; o principal em 100.
    COMP = [('K2-L-0380', 'L-0380', 2), ('K3-L-0380', 'L-0380', 3),
            ('K4-L-0380', 'L-0380', 4), ('K5-L-0380', 'L-0380', 5)]
    PONTE = [(LPT, 'MLB10', 'E10', 'L-0380', True), (LPT, 'MLB2', 'E2', 'K2-L-0380', True),
             (LPT, 'MLB3', 'E3', 'K3-L-0380', True), (LPT, 'MLB4', 'E4', 'K4-L-0380', True),
             (LPT, 'MLB5', 'E5', 'K5-L-0380', True), (LPT, 'MLB9', 'E9', 'L-0999', True)]
    D = date(2026, 10, 5)

    def _exp(self, saude):
        import estoque_peca as ep
        por_an, _ = sd.agregar_vendas([_vend(LPT, 'MLB10', 'L-0380', rec_30=900),
                                       _vend(LPT, 'MLB2', 'K2-L-0380', rec_30=400)])
        return sd.sinais_experiencia(saude, self.PONTE, ep.agrupar_composicao(self.COMP), por_an)

    def test_familia_de_alicates_vira_um_sinal_com_o_principal(self):
        saude = [(LPT, 'MLB10', self.D, 100, 'Boa', 100, 'Boa'),
                 (LPT, 'MLB2', self.D, 65, 'Média', 100, 'Boa'),
                 (LPT, 'MLB3', self.D, 65, 'Média', 75, 'Boa'),
                 (LPT, 'MLB4', self.D, 65, 'Média', 100, 'Boa'),
                 (LPT, 'MLB5', self.D, 65, 'Média', 75, 'Boa')]
        s = self._exp(saude)
        self.assertEqual(len(s), 1)
        self.assertTrue(s[0]['numero'].startswith('4 anúncios da mesma peça pioraram'))
        self.assertEqual(s[0]['skus'], ('L-0380',))
        self.assertIn('principal MLB10 em 100', s[0]['familia'])
        self.assertIn('5 anúncios', s[0]['familia'])
        self.assertAlmostEqual(s[0]['em_jogo'], 400)

    def test_um_so_piorou_sai_sozinho_com_a_familia_ao_lado(self):
        saude = [(LPT, 'MLB10', self.D, 100, 'Boa', 100, 'Boa'),
                 (LPT, 'MLB2', self.D, 65, 'Média', 100, 'Boa'),
                 (LPT, 'MLB3', self.D, 75, 'Boa', 75, 'Boa')]
        s = self._exp(saude)
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]['anuncio'], 'MLB2')
        self.assertIn('100 (Boa) → 65 (Média)', s[0]['numero'])
        self.assertIn('principal MLB10 em 100', s[0]['familia'])

    def test_sem_nota_nao_e_piora(self):
        saude = [(LPT, 'MLB2', self.D, None, None, 100, 'Boa'),
                 (LPT, 'MLB3', self.D, 65, 'Média', None, None),
                 (LPT, 'MLB9', self.D, 100, 'Boa', 30, 'Ruim')]
        self.assertEqual(self._exp(saude), [])


class VendasEOrdem(unittest.TestCase):
    def test_queda_alta_e_relevancia(self):
        vendas = [_vend(LPT, 'MLB1', 'A', rec_sem=400, rec_base=4000),    # 1000/sem → -60%
                  _vend(LPT, 'MLB2', 'B', rec_sem=2500, rec_base=4000),   # +150%
                  _vend(LPT, 'MLB3', 'C', rec_sem=0, rec_base=900),       # irrelevante
                  _vend(LPT, 'MLB4', 'D', rec_sem=900, rec_base=4000)]    # -10%: nada
        por_an, _ = sd.agregar_vendas(vendas)
        s = sd.sinais_vendas(por_an, sd.janelas(HOJE))
        self.assertEqual(sorted(x['anuncio'] for x in s), ['MLB1', 'MLB2'])
        q = next(x for x in s if x['anuncio'] == 'MLB1')
        self.assertIn('semana 26/09–02/10', q['numero'])
        self.assertAlmostEqual(q['em_jogo'], 600)

    def test_ruptura_tira_a_queda_generica_do_mesmo_anuncio(self):
        dados = {'vendas': [_vend(NALA, 'MLB7', 'LTT-CT3821', rec_sem=150, rec_base=3200)],
                 'ads': [(NALA, 'MLB7', 10, 300, 0, 0, 0, 0, 0, 0, 0)],
                 'estoque_semana': [], 'ponte': [], 'foto': [], 'config': []}
        s, erros = sd.montar_sinais(dados, HOJE)
        self.assertEqual(erros, {})
        self.assertEqual([(x['tipo'], x['numero'].split(':')[0]) for x in s], [('FULL', 'Ruptura')])

    def test_corte_de_10_por_loja_ordenado_por_r(self):
        sinais = [sd._sinal(LPT, 'VENDAS', f'MLB{i}', ['A'], 'x', 's', i) for i in range(15)]
        sinais.append(sd._sinal(NALA, 'VENDAS', 'MLB99', ['A'], 'x', 's', 999))
        top, resto = sd.sinais_da_loja(sinais, LPT)
        self.assertEqual(len(top), 10)
        self.assertEqual(len(resto), 5)
        self.assertEqual([x['em_jogo'] for x in top], list(range(14, 4, -1)))
        self.assertNotIn('MLB99', [x['anuncio'] for x in top + resto])

    def test_fonte_com_erro_nao_derruba_as_outras(self):
        dados = {'vendas': [_vend(LPT, 'MLB1', 'A', rec_sem=0, rec_base=4000)]}
        s, erros = sd.montar_sinais(dados, HOJE)   # sem ponte/foto/ads/experiência
        self.assertEqual([x['tipo'] for x in s], ['VENDAS'])
        self.assertEqual(erros, {})

    def test_bloco_que_quebra_nao_derruba_os_outros(self):
        dados = {'vendas': [_vend(LPT, 'MLB1', 'A', rec_sem=0, rec_base=4000)],
                 'foto': [(LPT, 'E1', date(2026, 10, 4), 0, 0, 0)],
                 'ponte': [(LPT, 'MLB1', 'E1', 'A', True)],
                 'experiencia': [(LPT, 'MLB1', HOJE, 30, 'Ruim', 100, 'Boa')],
                 'ads': [], 'config': [], 'estoque_semana': []}
        with mock.patch.object(sd, 'sinais_full', side_effect=KeyError('dado inesperado')):
            s, erros = sd.montar_sinais(dados, HOJE)
        self.assertEqual(set(erros), {'full'})
        self.assertEqual(sorted({x['tipo'] for x in s}), ['EXPERIÊNCIA', 'VENDAS'])


class ResumoEFrescor(unittest.TestCase):
    def test_resumo_mesmo_dia_da_semana_e_meta(self):
        r = sd.resumo_lojas([(LPT, 3000, 16000, 12000)], [(LPT, 200000)], [LPT, NALA], HOJE)
        self.assertAlmostEqual(r[LPT]['media_mesmo_dia'], 4000)
        self.assertAlmostEqual(r[LPT]['var_ontem'], -0.25)
        self.assertAlmostEqual(r[LPT]['pct_meta'], 0.06)
        self.assertAlmostEqual(r[LPT]['esperado_linear'], 200000 * 4 / 31)
        self.assertIsNone(r[NALA]['meta'])                    # meta não cadastrada
        self.assertIsNone(r[NALA]['var_ontem'])

    def test_frescor_cedo_atrasado_e_loja_sem_ads(self):
        linhas = [('Vendas', LPT, datetime(2026, 10, 4, 6, 4)),
                  ('Estoque', LPT, datetime(2026, 10, 5, 4, 8))]
        cedo = sd.avaliar_frescor(linhas, [LPT, SP], datetime(2026, 10, 5, 7, 0))
        tarde = sd.avaliar_frescor(linhas, [LPT, SP], datetime(2026, 10, 5, 11, 0))
        self.assertEqual(cedo[LPT], [('cedo', 'Vendas: último dado 04/10 06:04')])
        self.assertEqual(tarde[LPT], [('atrasado', 'Vendas: último dado 04/10 06:04')])
        self.assertEqual(tarde[SP], [])                       # sem dado = sem alarme


class Permissao(unittest.TestCase):
    def test_restrito_sem_loja_nao_ve_nada_e_nao_consulta(self):
        import permissoes
        engine = mock.Mock()
        with mock.patch.object(permissoes, 've_todas_lojas', return_value=False), \
             mock.patch.object(permissoes, 'get_lojas_usuario', return_value=[]):
            st = mock.Mock()
            sd._render_mercado_livre(st, engine)
        engine.raw_connection.assert_not_called()
        st.caption.assert_called_with("Nenhuma loja atribuída ao seu perfil.")

    def test_tela_mostra_erro_do_bloco_e_as_outras_lojas_seguem(self):
        dados = {'vendas': [_vend(LPT, 'MLB1', 'A', rec_sem=0, rec_base=4000),
                            _vend(NALA, 'MLB2', 'B', rec_sem=0, rec_base=4000)],
                 'foto': [], 'ponte': [], 'resumo': [], 'metas': [], 'frescor': [],
                 'visitas': None}
        st = mock.MagicMock()
        st.columns.side_effect = lambda n: [mock.MagicMock() for _ in range(n)]
        engine = mock.MagicMock()
        tabela_real = sd.tabela_sinais

        def tabela_que_quebra_na_lpt(sinais, nomes):
            if sinais and sinais[0]['loja'] == LPT:
                raise ValueError('dado inesperado')
            return tabela_real(sinais, nomes)
        with mock.patch.object(sd, 'restricao_de_lojas', return_value=None), \
             mock.patch.object(sd, '_ler', return_value=[(LPT,), (NALA,)]), \
             mock.patch.object(sd, 'ler_tudo', return_value=(dados, {})), \
             mock.patch.object(sd.ep, 'ler_contagens', return_value=[]), \
             mock.patch.object(sd.ep, 'ler_nomes', return_value={}), \
             mock.patch.object(sd, 'sinais_vendas', side_effect=[KeyError('x')]):
            sd._render_mercado_livre(st, engine)
        erros = [c.args[0] for c in st.error.call_args_list]
        self.assertTrue(any('VENDAS' in e for e in erros), erros)
        self.assertEqual(st.success.call_count, 2)      # as duas lojas seguem: "Nenhum sinal"

        st = mock.MagicMock()
        st.columns.side_effect = lambda n: [mock.MagicMock() for _ in range(n)]
        with mock.patch.object(sd, 'restricao_de_lojas', return_value=None), \
             mock.patch.object(sd, '_ler', return_value=[(LPT,), (NALA,)]), \
             mock.patch.object(sd, 'ler_tudo', return_value=(dados, {})), \
             mock.patch.object(sd.ep, 'ler_contagens', return_value=[]), \
             mock.patch.object(sd.ep, 'ler_nomes', return_value={}), \
             mock.patch.object(sd, 'tabela_sinais', side_effect=tabela_que_quebra_na_lpt):
            sd._render_mercado_livre(st, engine)
        erros = [c.args[0] for c in st.error.call_args_list]
        self.assertEqual(erros, ["Tabela de sinais indisponível agora para esta loja."])
        self.assertEqual(st.dataframe.call_count, 1)    # a ML-Nala apareceu

    def test_restricao_e_lojas_visiveis(self):
        import permissoes
        with mock.patch.object(permissoes, 've_todas_lojas', return_value=True):
            self.assertIsNone(sd.restricao_de_lojas(None))
        with mock.patch.object(permissoes, 've_todas_lojas', return_value=False), \
             mock.patch.object(permissoes, 'get_lojas_usuario', return_value=[NALA, 'Shopee-LPT']):
            r = sd.restricao_de_lojas(None)
        self.assertEqual(sd.lojas_visiveis([LPT, NALA, RJ], r), [NALA])
        self.assertEqual(sd.lojas_visiveis([LPT, NALA], None), [LPT, NALA])

    def test_menu_e_perfis(self):
        import permissoes
        self.assertEqual(permissoes.MENU_MODULOS['📡 Sinais do Dia'], 'sinais')
        for perfil, perms in permissoes.PERMISSOES.items():
            self.assertIn('sinais', perms, perfil)
        self.assertEqual(permissoes.PERMISSOES['GESTOR']['sinais'], 'parcial')


class Sql(unittest.TestCase):
    def test_sql_nao_fixa_schema_nem_tem_porcento_solto(self):
        for sql in sd.TODAS_AS_SQL:
            self.assertNotIn('public.', sql)
            self.assertEqual(re.findall(r'%(?!\(\w+\)s)', sql), [], sql[:80])

    def test_todo_parametro_da_sql_existe(self):
        p = sd.params(HOJE, [LPT])
        for sql in sd.TODAS_AS_SQL:
            for nome in re.findall(r'%\((\w+)\)s', sql):
                self.assertIn(nome, p, nome)


# ============================================================
# COM BANCO
# ============================================================

_DDL = [
    """CREATE TEMP TABLE fact_vendas_snapshot (
        marketplace_origem varchar, loja_origem varchar, numero_pedido varchar,
        data_venda date, sku varchar, codigo_anuncio varchar, quantidade integer,
        valor_venda_efetivo numeric, margem_total numeric,
        data_processamento timestamp, arquivo_origem varchar)""",
    """CREATE TEMP TABLE dim_metas_loja (
        loja_origem varchar, marketplace varchar, ano_mes varchar, meta_receita numeric)""",
    """CREATE TEMP TABLE dim_estoque_anuncio (
        marketplace varchar, loja varchar, anuncio_id varchar, variacao_id varchar,
        estoque_id varchar, sku varchar, ativo boolean, em_full boolean)""",
    """CREATE TEMP TABLE fact_estoque_diario (
        marketplace varchar, loja varchar, estoque_id varchar, data date,
        full_disponivel integer, full_em_transferencia integer, galpao_disponivel integer,
        unidades_recebidas integer, origem varchar, fonte_venda varchar,
        data_captura timestamp)""",
    """CREATE TEMP TABLE fact_ads_performance (
        data date, marketplace varchar, loja varchar, codigo_anuncio varchar,
        cliques integer, vendas integer, gasto_ads numeric, id_campanha varchar,
        data_importacao timestamp)""",
    """CREATE TEMP TABLE fact_ads_campanha_config (
        marketplace varchar, loja varchar, campanha varchar, data_captura timestamp,
        roas_objetivo numeric, orcamento_diario numeric, id_campanha varchar)""",
    """CREATE TEMP TABLE fact_saude_anuncio (
        marketplace varchar, loja varchar, codigo_anuncio varchar, data date,
        tipo_sinal varchar, valor numeric, texto varchar, data_importacao timestamp)""",
    """CREATE TEMP TABLE dim_kit_composicao (
        kit_sku text, peca_sku text, quantidade integer)""",
    """CREATE TEMP TABLE dim_sku_mapeamento (
        sku_errado varchar PRIMARY KEY, sku_correto varchar NOT NULL)""",
    """CREATE TEMP TABLE dim_lojas (marketplace text, loja text)""",
]
TABELAS = ['fact_vendas_snapshot', 'dim_metas_loja', 'dim_estoque_anuncio',
           'fact_estoque_diario', 'fact_ads_performance', 'fact_ads_campanha_config',
           'fact_saude_anuncio', 'dim_kit_composicao', 'dim_sku_mapeamento', 'dim_lojas']


class _Conexao:
    """Repassa o cursor da conexão do teste e IGNORA rollback/close: o
    ROLLBACK de verdade é do teste (tearDown), senão as TEMP somem no meio."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def rollback(self):
        pass

    def close(self):
        pass


@unittest.skipUnless(DB_URL, 'defina NALA_TEST_DB_URL (usuário de permissão mínima)')
class ComBanco(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2
        cls.conn = psycopg2.connect(DB_URL, connect_timeout=15)
        cls.conn.autocommit = False

    @classmethod
    def tearDownClass(cls):
        cls.conn.rollback()
        cls.conn.close()

    def _copiar_checks(self, tabela):
        """CHECK da tabela REAL (pg_constraint) na TEMP de mesmo nome."""
        self.cur.execute(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = %s::regclass AND contype = 'c'", (f'public.{tabela}',))
        checks = self.cur.fetchall()
        for nome, definicao in checks:
            self.cur.execute(f'ALTER TABLE pg_temp.{tabela} ADD CONSTRAINT "{nome}" {definicao}')
        return len(checks)

    def setUp(self):
        c = self.cur = self.conn.cursor()
        for ddl in _DDL:
            c.execute(ddl)
        copiadas = {t: self._copiar_checks(t) for t in TABELAS}
        # as CHECK que existem hoje em produção (05/10/2026) chegaram à TEMP
        self.assertGreaterEqual(copiadas['fact_estoque_diario'], 2)
        self.assertGreaterEqual(copiadas['fact_vendas_snapshot'], 1)
        self.assertGreaterEqual(copiadas['dim_kit_composicao'], 3)
        self.assertGreaterEqual(copiadas['fact_ads_performance'], 1)
        c.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(%s) AS t(nome) JOIN pg_class c ON c.oid = t.nome::regclass
        """, (TABELAS,))
        self.assertTrue(c.fetchone()[0], 'tabela não resolveu para pg_temp')
        self._popular()
        self.c = _Conexao(self.conn)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def _popular(self):
        c, ML = self.cur, 'MERCADO LIVRE'
        c.execute("INSERT INTO dim_lojas VALUES (%s, %s), (%s, %s), ('SHOPEE', 'Shopee-LPT')",
                  (ML, LPT, ML, NALA))
        c.execute("INSERT INTO dim_metas_loja VALUES (%s, %s, '2026-10', 200000)", (LPT, ML))
        # K-L-0421-A: dois anúncios, dois estoques (números reais de 04/10)
        c.executemany("INSERT INTO dim_estoque_anuncio VALUES (%s,%s,%s,NULL,%s,%s,true,true)", [
            (ML, LPT, 'MLB5183384677', 'MLBU5071722523', 'K-L-0421-A'),
            (ML, LPT, 'MLB4793561323', 'MLBU4131286394', 'K-L-0421-A'),
            (ML, NALA, 'MLB7175749876', 'MLBU4334892068', 'LTT-CT3821')])
        foto = date(2026, 10, 4)
        c.executemany("INSERT INTO fact_estoque_diario VALUES "
                      "(%s,%s,%s,%s,%s,%s,NULL,0,'snapshot',NULL,%s)", [
            (ML, LPT, 'MLBU5071722523', foto, 6, 2, datetime(2026, 10, 5, 4, 8)),
            (ML, LPT, 'MLBU4131286394', foto, 89, 0, datetime(2026, 10, 5, 4, 8)),
            (ML, LPT, 'MLBU5071722523', date(2026, 10, 3), 99, 0, datetime(2026, 10, 4, 4, 8))]
            # Garrafa: Full zerado em 4 dos 7 dias da semana (só 3 com Full)
            + [(ML, NALA, 'MLBU4334892068', date(2026, 9, 26 + i) if i < 5 else date(2026, 10, i - 4),
                10 if i < 3 else 0, 0, datetime(2026, 10, 5, 4, 1)) for i in range(7)]
            + [(ML, NALA, 'MLBU4334892068', foto, 59, 0, datetime(2026, 10, 5, 4, 1))])
        vendas = []
        for i in range(21):                       # 21 un. na semana 26/09–02/10
            vendas.append((ML, LPT, f'P{i}', date(2026, 9, 26 + i % 5), 'K-L-0421-A',
                           'MLB5183384677', 1, 30, 3))
        for i in range(44):                       # +44 no resto dos 30 dias
            vendas.append((ML, LPT, f'Q{i}', date(2026, 9, 3 + i % 20), 'K-L-0421-A',
                           'MLB5183384677', 1, 30, 3))
        vendas.append((ML, LPT, 'H', HOJE, 'K-L-0421-A', 'MLB5183384677', 500, 9999, 0))  # hoje: fora
        vendas.append((ML, LPT, 'D1', date(2026, 10, 4), 'K-L-0421-A', 'MLB5183384677',
                       3, 90, 9))             # D-1: só no resumo
        vendas.append((ML, NALA, 'G1', date(2026, 9, 27), 'LTT-CT3821', 'MLB7175749876', 5, 150, 20))
        vendas.append((ML, NALA, 'G2', date(2026, 9, 10), 'LTT-CT3821', 'MLB7175749876', 100, 3200, 300))
        c.executemany("INSERT INTO fact_vendas_snapshot VALUES "
                      "(%s,%s,%s,%s,%s,%s,%s,%s,%s,'2026-10-05 06:04','API')", vendas)
        c.executemany("INSERT INTO fact_ads_performance VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [
            (date(2026, 9, 10), ML, NALA, 'MLB7175749876', 100, 10, 300, 'MLB7175749876 x',
             datetime(2026, 10, 5, 5, 31)),
            (date(2026, 9, 27), ML, NALA, 'MLB7175749876', 5, 0, 10, 'MLB7175749876 x',
             datetime(2026, 10, 5, 5, 31))])
        c.executemany("INSERT INTO fact_ads_campanha_config VALUES (%s,%s,%s,%s,%s,%s,%s)", [
            (ML, LPT, 'MLB5183384677 kit', datetime(2026, 10, 4, 5, 30), 14, 12, 'MLB5183384677 kit'),
            (ML, LPT, 'MLB5183384677 kit', datetime(2026, 10, 5, 5, 30), 10, 12, 'MLB5183384677 kit'),
            (ML, LPT, 'MLB4793561323 kit', datetime(2026, 10, 4, 5, 30), 9, 5, 'MLB4793561323 kit'),
            (ML, LPT, 'MLB4793561323 kit', datetime(2026, 10, 5, 5, 30), 9, 5, 'MLB4793561323 kit')])
        c.executemany("INSERT INTO fact_saude_anuncio VALUES (%s,%s,%s,%s,'experiencia_compra',%s,%s,%s)", [
            (ML, LPT, 'MLB5183384677', HOJE, 65, 'Média', datetime(2026, 10, 5, 5, 6)),
            (ML, LPT, 'MLB5183384677', date(2026, 9, 28), 100, 'Boa', datetime(2026, 9, 28, 5, 6)),
            (ML, LPT, 'MLB4793561323', HOJE, 100, 'Boa', datetime(2026, 10, 5, 5, 6))])

    def test_ponta_a_ponta_pelas_sql(self):
        dados, erros = sd.ler_tudo(self.c, HOJE, [LPT, NALA])
        self.assertEqual(erros, {})
        self.assertIsNone(dados['visitas'])                  # view ainda não existe
        s, erros_bloco = sd.montar_sinais(dados, HOJE)
        self.assertEqual(erros_bloco, {})
        por = {(x['loja'], x['tipo'], x['numero'].split(':')[0].split(' ')[0]): x for x in s}

        full = [x for x in s if x['tipo'] == 'FULL' and x['loja'] == LPT]
        self.assertEqual(len(full), 1)                       # só o de 6 un.; o de 89 não
        self.assertEqual(full[0]['anuncio'], 'MLB5183384677')
        self.assertTrue(full[0]['urgente'])
        self.assertIn('(6 disp. + 2 em transf.', full[0]['numero'])

        self.assertIn('ROAS objetivo 14,0 → 10,0', por[(LPT, 'ADS', 'Mudou')]['numero'])
        self.assertEqual(len([x for x in s if x['numero'].startswith('Mudou')]), 1)
        self.assertIn('100 (Boa) → 65 (Média)', por[(LPT, 'EXPERIÊNCIA', 'Experiência')]['numero'])

        rup = por[(NALA, 'FULL', 'Ruptura')]
        self.assertIn('só 3 de 7', rup['numero'])
        self.assertFalse(any(x['numero'].startswith('Espiral') for x in s))

        r = sd.resumo_lojas(dados['resumo'], dados['metas'], [LPT, NALA], HOJE)
        self.assertAlmostEqual(r[LPT]['ontem'], 90)          # D-1 entra no resumo
        self.assertEqual(r[LPT]['meta'], 200000)
        self.assertIsNone(r[NALA]['meta'])

        fres = {(f, l) for f, l, _u in dados['frescor']}
        self.assertIn(('Estoque', LPT), fres)
        self.assertIn(('Experiência', LPT), fres)

    def test_lojas_ml_e_restricao_pela_sql(self):
        lojas = [r[0] for r in sd._ler(self.c, sd.SQL_LOJAS_ML, {'marketplace': sd.MARKETPLACE})]
        self.assertEqual(lojas, [LPT, NALA])
        dados, _ = sd.ler_tudo(self.c, HOJE, [NALA])
        self.assertTrue(all(r[0] == NALA for r in dados['vendas']))
        self.assertTrue(all(r[0] == NALA for r in dados['foto']))

    def test_visitas_le_da_view_quando_ela_existe(self):
        self.cur.execute("""CREATE TEMP VIEW vw_visitas_dia AS
            SELECT 'MERCADO LIVRE'::varchar AS marketplace, 'ML-LPT'::varchar AS loja,
                   'MLB5183384677'::varchar AS codigo_anuncio, d::date AS data,
                   CASE WHEN d >= '2026-09-26' THEN 10 ELSE 100 END AS visitas
              FROM generate_series('2026-08-29'::date, '2026-10-04'::date, '1 day') d""")
        dados, erros = sd.ler_tudo(self.c, HOJE, [LPT])
        self.assertEqual(erros, {})
        self.assertEqual([tuple(r[:2]) for r in dados['visitas']], [(LPT, 'MLB5183384677')])
        self.assertEqual(int(dados['visitas'][0][2]), 70)    # 7 dias × 10
        self.assertEqual(int(dados['visitas'][0][3]), 2800)  # 28 dias × 100


if __name__ == '__main__':
    unittest.main()
