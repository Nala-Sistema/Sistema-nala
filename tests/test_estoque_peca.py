"""
Testa a cobertura de estoque em peça (estoque_peca.py, frente [KITS], 2ª entrega).

Duas partes:
  - Sem banco: a conta em peça (montar) com os números reais medidos em
    30/09/2026 — galpão de kit dividido pelo UpSeller, galpão divergente entre
    lojas, prep center, peça só em kit, piso pelo kit, kit sem composição — e
    % solto nas SQL.
  - Com banco: EXECUTA as SQL (ler) com os parâmetros ligados pelo psycopg2.

A parte com banco só roda com NALA_TEST_DB_URL (usuário de permissão mínima,
nunca o dono). Cria tabelas TEMPORÁRIAS com os mesmos nomes das reais (as de
kit com o DDL LIDO de sql/kits_composicao.sql), confere que resolvem para
pg_temp antes de inserir e termina SEMPRE em ROLLBACK.

Rodar:  python tests/test_estoque_peca.py   (ou: pytest tests/test_estoque_peca.py)
"""

import os
import re
import sys
import unittest
from datetime import date, datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import estoque_peca as ep  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')
D = date(2026, 9, 30)
LPT, NALA, RJ, SP = 'ML-LPT', 'ML-Nala', 'ML-YanniRJ', 'ML-YanniSP'

COMP = [('K10-LKE-3104-4030', 'LKE-3104-4030', 10),
        ('K3-LKE-3104-4030', 'LKE-3104-4030', 3),
        ('K2-L-0320', 'L-0320', 2),
        ('K-MIX', 'L-0320', 1), ('K-MIX', 'L-0321', 1),
        ('K3-L-0330', 'L-0330', 3),         # L-0330 só existe dentro de kit
        ('K2-L-0999', 'L-0999', 2)]


def _linha(pecas, peca):
    return pecas.set_index('peca').loc[peca]


class ContaEmPeca(unittest.TestCase):
    def _montar(self, estoque, vendas=(), pendentes=(), mapa=None, prazo=14):
        return ep.montar(list(estoque), list(vendas), COMP, pendentes, mapa, prazo)

    def test_galpao_do_kit_nao_e_multiplicado_e_full_do_kit_e(self):
        pecas, _, _ = self._montar([
            (LPT, 'e1', 'LKE-3104-4030', D, 17, 0, 1315),
            (LPT, 'e2', 'K10-LKE-3104-4030', D, 29, 0, 131),   # 1315 ÷ 10
            (NALA, 'e3', 'K3-LKE-3104-4030', D, 8, 0, 438)])   # 1315 ÷ 3
        l = _linha(pecas, 'LKE-3104-4030')
        self.assertEqual(l['galpao'], 1315)
        self.assertEqual(l['galpao_tipo'], 'conhecido')
        self.assertEqual(l['full'], 17 + 29 * 10 + 8 * 3)

    def test_galpao_igual_entre_lojas_nao_soma(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'LKE-3104-4030', D, 0, 0, 1315),
                                    (NALA, 'e2', 'LKE-3104-4030', D, 0, 0, 1315)])
        self.assertEqual(_linha(pecas, 'LKE-3104-4030')['galpao'], 1315)

    def test_galpao_divergente_usa_o_maior_e_avisa(self):
        pecas, _, avisos = self._montar([(LPT, 'e1', 'L-0377', D, 0, 0, 72),
                                         (RJ, 'e2', 'L-0377', D, 0, 0, 18)])
        l = _linha(pecas, 'L-0377')
        self.assertEqual((l['galpao'], l['galpao_diverge']), (72, True))
        self.assertEqual(avisos['galpao_divergente'], ['L-0377'])
        self.assertIn('⚠ publicado diferente entre lojas', ep.texto_galpao(l))

    def test_ruido_da_hora_da_coleta_nao_avisa(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'L-0303', D, 0, 0, 4389),
                                    (NALA, 'e2', 'L-0303', D, 0, 0, 4390)])
        self.assertFalse(_linha(pecas, 'L-0303')['galpao_diverge'])

    def test_prep_center_em_coluna_propria(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'L-0152', D, 0, 0, 900),
                                    (SP, 'e2', 'L-0152', D, 49, 0, 7)])
        l = _linha(pecas, 'L-0152')
        self.assertEqual((l['galpao'], l['prep_center'], l['full']), (900, 7, 49))

    def test_prep_center_nao_vira_galpao(self):
        pecas, _, _ = self._montar([(SP, 'e1', 'L-0155', D, 22, 0, 5)])
        l = _linha(pecas, 'L-0155')
        self.assertEqual((l['galpao_tipo'], l['prep_center']), ('desconhecido', 5))

    def test_peca_so_em_kit_tem_piso_pelo_kit_de_uma_peca(self):
        pecas, _, _ = self._montar(
            [(LPT, 'e1', 'K3-L-0330', D, 4, 0, 438)],
            vendas=[('K3-L-0330', 7, 30, 900.0)])
        l = _linha(pecas, 'L-0330')
        self.assertEqual((l['galpao_tipo'], l['galpao']), ('piso', 1314))
        self.assertTrue(l['cobertura_minima'])
        self.assertEqual(ep.texto_galpao(l), 'desconhecido (≥ 1.314 pelo kit, estimativa)')
        self.assertTrue(ep.texto_cobertura(l['cobertura_dias'], True).endswith('(mínimo)'))

    def test_kit_misto_nao_da_piso_e_galpao_fica_desconhecido(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'K-MIX', D, 0, 0, 40)],
                                   vendas=[('K-MIX', 1, 3, 90.0)])
        l = _linha(pecas, 'L-0321')
        self.assertEqual(l['galpao_tipo'], 'desconhecido')
        self.assertIsNone(l['galpao'])
        self.assertEqual(ep.texto_galpao(l), 'desconhecido')

    def test_venda_em_peca_e_rs_em_jogo_por_sku_distinto(self):
        pecas, kits, _ = self._montar(
            [(LPT, 'e1', 'L-0320', D, 0, 0, 30)],
            vendas=[('L-0320', 7, 30, 300.0), ('K2-L-0320', 7, 30, 1200.0),
                    ('K-MIX', 0, 15, 450.0)])
        l = _linha(pecas, 'L-0320')
        self.assertAlmostEqual(l['venda_dia_30d'], (30 + 30 * 2 + 15) / 30)
        self.assertAlmostEqual(l['em_jogo_30d'], 300 + 1200 + 450)
        # K-MIX depende de L-0320 e de L-0321: no total do topo conta UMA vez.
        mascara = pecas['peca'].isin(['L-0320', 'L-0321'])
        self.assertEqual(ep.skus_em_jogo(pecas, mascara), {'L-0320', 'K2-L-0320', 'K-MIX'})
        self.assertEqual({k['Kit'] for k in kits['L-0320']}, {'K2-L-0320', 'K-MIX'})

    def test_ruptura_pelo_prazo(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'L-0320', D, 0, 0, 30)],
                                   vendas=[('L-0320', 7, 90, 900.0)])   # 3/dia → 10 dias
        l = _linha(pecas, 'L-0320')
        self.assertAlmostEqual(l['cobertura_dias'], 10)
        self.assertTrue(l['ruptura'])
        pecas, _, _ = self._montar([(LPT, 'e1', 'L-0320', D, 0, 0, 30)],
                                   vendas=[('L-0320', 7, 90, 900.0)], prazo=7)
        self.assertFalse(_linha(pecas, 'L-0320')['ruptura'])

    def test_sem_venda_e_sem_giro(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'L-0320', D, 0, 0, 30)])
        l = _linha(pecas, 'L-0320')
        self.assertIsNone(l['cobertura_dias'])
        self.assertFalse(l['ruptura'])
        self.assertEqual(ep.texto_cobertura(None, False), 'sem giro')

    def test_kit_sem_composicao_entra_como_ele_mesmo_com_aviso(self):
        pecas, _, avisos = self._montar([(LPT, 'e1', 'K2-LVI-CANOA0506', D, 0, 0, 3)],
                                        vendas=[('K2-LVI-CANOA0506', 0, 3, 156.0)],
                                        pendentes={'K2-LVI-CANOA0506'})
        l = _linha(pecas, 'K2-LVI-CANOA0506')
        self.assertTrue(l['kit_sem_composicao'])
        self.assertEqual(avisos['kits_sem_composicao'], ['K2-LVI-CANOA0506'])

    def test_sku_do_anuncio_passa_pela_correcao(self):
        pecas, _, _ = self._montar([(LPT, 'e1', 'LKE-3104', D, 5, 0, 1315)],
                                   mapa={'LKE-3104': 'LKE-3104-4030'})
        self.assertEqual(list(pecas['peca']), ['LKE-3104-4030'])

    def test_nunca_existe_total_de_unidades(self):
        # A tela não soma unidades de peças diferentes: só contagens e R$.
        import inspect
        fonte = inspect.getsource(ep.render)
        self.assertNotIn("pecas['full'].sum()", fonte)
        self.assertNotIn("pecas['galpao'].sum()", fonte)


class Datas(unittest.TestCase):
    def test_trinta_dias_fechados_ate_ontem(self):
        p = ep.params_vendas(date(2026, 10, 1))
        self.assertEqual((p['ini_30'], p['ini_7'], p['fim']),
                         (date(2026, 9, 1), date(2026, 9, 24), date(2026, 9, 30)))

    def test_atraso_mesma_regra_da_cobertura_full(self):
        self.assertEqual(ep.tolerancia_atraso(datetime(2026, 10, 1, 9, 59)), 2)
        self.assertEqual(ep.tolerancia_atraso(datetime(2026, 10, 1, 10, 0)), 1)
        datas = {LPT: date(2026, 9, 30), NALA: date(2026, 9, 29)}
        self.assertEqual(ep.lojas_atrasadas(datas, date(2026, 10, 1),
                                            datetime(2026, 10, 1, 11, 0)), [NALA])


class Sql(unittest.TestCase):
    def test_nenhum_percent_solto_fora_dos_parametros(self):
        for sql in ep.TODAS_AS_SQL:
            self.assertNotIn('%', re.sub(r'%\(\w+\)s', '', sql))

    def test_sql_nao_fixa_schema(self):
        for sql in ep.TODAS_AS_SQL:
            self.assertNotIn('public.', sql)


# ============================================================
# COM BANCO
# ============================================================

def _ddl_kits_em_temp():
    with open(os.path.join(RAIZ, 'sql', 'kits_composicao.sql'), encoding='utf-8') as f:
        texto = f.read()
    tabelas = re.findall(r'^CREATE TABLE public\.\w+ \(.*?^\);', texto, re.S | re.M)
    assert len(tabelas) == 2, 'DDL de sql/kits_composicao.sql mudou'
    return [s.replace('public.', 'pg_temp.') for s in tabelas]


class _Conexao:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        pass

    def rollback(self):
        pass


@unittest.skipUnless(DB_URL, 'defina NALA_TEST_DB_URL (usuário de permissão mínima)')
class ComBanco(unittest.TestCase):
    TABELAS = ['fact_estoque_diario', 'dim_estoque_anuncio', 'fact_vendas_snapshot',
               'dim_kit_composicao', 'dim_kit_composicao_pendente',
               'dim_sku_mapeamento', 'dim_produtos']

    @classmethod
    def setUpClass(cls):
        import psycopg2
        cls.conn = psycopg2.connect(DB_URL, connect_timeout=15)
        cls.conn.autocommit = False

    @classmethod
    def tearDownClass(cls):
        cls.conn.rollback()
        cls.conn.close()

    def setUp(self):
        c = self.cur = self.conn.cursor()
        c.execute("""CREATE TEMP TABLE fact_estoque_diario (
            marketplace varchar, loja varchar, estoque_id varchar, data date,
            full_disponivel integer, full_em_transferencia integer,
            galpao_disponivel integer)""")
        c.execute("""CREATE TEMP TABLE dim_estoque_anuncio (
            marketplace varchar, loja varchar, anuncio_id varchar,
            estoque_id varchar, sku varchar)""")
        c.execute("""CREATE TEMP TABLE fact_vendas_snapshot (
            sku varchar, data_venda date, quantidade integer,
            valor_venda_efetivo numeric)""")
        for ddl in _ddl_kits_em_temp():
            c.execute(ddl)
        c.execute("""CREATE TEMP TABLE dim_sku_mapeamento (
            sku_errado varchar PRIMARY KEY, sku_correto varchar NOT NULL,
            data_criacao timestamp DEFAULT now())""")
        c.execute('CREATE TEMP TABLE dim_produtos (sku text, nome text)')
        c.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(%s) AS t(nome) JOIN pg_class c ON c.oid = t.nome::regclass
        """, (self.TABELAS,))
        self.assertTrue(c.fetchone()[0], 'tabela não resolveu para pg_temp')

        ML = 'MERCADO LIVRE'
        c.executemany('INSERT INTO fact_estoque_diario VALUES (%s,%s,%s,%s,%s,%s,%s)', [
            (ML, LPT, 'e1', D, 17, 2, 1315),                  # LKE-3104-4030
            (ML, LPT, 'e1', date(2026, 9, 29), 99, 0, 999),   # véspera: não entra
            (ML, LPT, 'e2', D, 29, 0, 131),                   # K10
            (ML, LPT, 'velho', date(2026, 9, 20), 50, 0, 50), # saiu do ML
            (ML, NALA, 'e3', D, 0, 0, 1315),
            ('SHOPEE', 'Shopee-LPT', 's1', D, 500, 0, 500)])  # outro marketplace
        c.executemany('INSERT INTO dim_estoque_anuncio VALUES (%s,%s,%s,%s,%s)', [
            (ML, LPT, 'MLB1', 'e1', 'LKE-3104-4030'),
            (ML, LPT, 'MLB2', 'e1', 'LKE-3104-4030'),         # 2 anúncios, 1 estoque
            (ML, LPT, 'MLB3', 'e2', 'K10-LKE-3104-4030'),
            (ML, LPT, 'MLB4', 'velho', 'LKE-3104-4030'),
            (ML, NALA, 'MLB5', 'e3', 'LKE-3104-4030'),
            ('SHOPEE', 'Shopee-LPT', 'S1', 's1', 'LKE-3104-4030')])
        c.execute("INSERT INTO dim_kit_composicao (kit_sku, peca_sku, quantidade, "
                  "arquivo_origem) VALUES ('K10-LKE-3104-4030', 'LKE-3104-4030', 10, 'x')")
        hoje = date(2026, 10, 1)
        c.executemany('INSERT INTO fact_vendas_snapshot VALUES (%s,%s,%s,%s)', [
            ('K10-LKE-3104-4030', date(2026, 9, 30), 3, 600),
            ('LKE-3104-4030', date(2026, 9, 1), 30, 900),     # 1º dia da janela
            ('LKE-3104-4030', date(2026, 8, 31), 500, 9999),  # fora (31 dias)
            ('LKE-3104-4030', hoje, 500, 9999)])              # hoje: fora
        self.hoje = hoje
        self.c = _Conexao(self.conn)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def test_le_e_monta_de_ponta_a_ponta(self):
        estoque, vendas, comp, pend, mapa = ep.ler(self.c, self.hoje)
        pecas, kits, _ = ep.montar(estoque, vendas, comp, pend, mapa)
        l = _linha(pecas, 'LKE-3104-4030')
        # Full: e1 uma vez (dois anúncios) + K10 × 10; véspera, 'velho' e Shopee fora.
        self.assertEqual(l['full'], 17 + 29 * 10)
        self.assertEqual(l['transferencia'], 2)
        self.assertEqual(l['galpao'], 1315)
        # Venda: 3 kits × 10 + 30 sozinha, 30 dias até ontem.
        self.assertAlmostEqual(l['venda_dia_30d'], (30 + 30) / 30)
        self.assertAlmostEqual(l['em_jogo_30d'], 1500)
        self.assertEqual([k['Kit'] for k in kits['LKE-3104-4030']], ['K10-LKE-3104-4030'])

    def test_nomes(self):
        self.cur.execute("INSERT INTO dim_produtos VALUES ('LKE-3104-4030', 'Bandeja 40x30')")
        self.assertEqual(ep.ler_nomes(self.c, {'LKE-3104-4030', 'X'}),
                         {'LKE-3104-4030': 'Bandeja 40x30'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
