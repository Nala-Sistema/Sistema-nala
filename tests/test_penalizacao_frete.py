"""
Testa a aba "🚚 Penalização de frete" de analise_produtos.py (29/09/2026).

Duas partes:
  - Sem banco: parâmetros, % solto na SQL e a divisão alarme × créditos.
  - Com banco: EXECUTA a SQL_PENALIZACAO_FRETE de verdade, com os parâmetros
    ligados pelo psycopg2 (o mesmo driver da tela). SQL da Shopee já quebrou
    produção por % solto e por base errada; olhar o texto não pega isso.

A parte com banco só roda com a variável NALA_TEST_DB_URL. Ela cria tabelas
TEMPORÁRIAS com os mesmos nomes das reais (o Postgres procura pg_temp
primeiro), confere que os nomes resolvem para pg_temp ANTES de inserir, e
termina SEMPRE em ROLLBACK. Nenhuma tabela real é lida nem escrita. Use o
usuário de permissão mínima, nunca o dono do banco.

Rodar:  python tests/test_penalizacao_frete.py   (ou: pytest tests/test_penalizacao_frete.py)
"""

import os
import re
import sys
import unittest
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analise_produtos as ap  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')


class SemBanco(unittest.TestCase):
    def test_nenhum_percent_solto_fora_dos_parametros_nomeados(self):
        for sql in (ap.SQL_PENALIZACAO_FRETE, ap.SQL_PENALIZACAO_DADO_ATE):
            sem_params = re.sub(r'%\(\w+\)s', '', sql)
            self.assertNotIn('%', sem_params)

    def test_parametros_usados_sao_os_entregues(self):
        usados = set(re.findall(r'%\((\w+)\)s', ap.SQL_PENALIZACAO_FRETE))
        p = ap.params_penalizacao_frete(date(2026, 9, 1), date(2026, 9, 30))
        self.assertEqual(usados, set(p))

    def test_lojas_none_e_todas_lista_vazia_e_nenhuma(self):
        self.assertIsNone(ap.params_penalizacao_frete(1, 2)['lojas'])
        self.assertEqual(ap.params_penalizacao_frete(1, 2, [])['lojas'], [])
        self.assertEqual(ap.params_penalizacao_frete(1, 2, ('A',))['lojas'], ['A'])

    def test_separa_alarme_de_credito_e_calcula_media_e_pct(self):
        df = pd.DataFrame([
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'K-L-0351', 'nome': 'Kit',
             'pedidos_total': 10, 'pedidos_penalizados': 8, 'frete_rs': 20.0,
             'pedidos_credito': 0, 'credito_rs': 0, 'peso_cobrado_kg': None, 'peso_anuncio_kg': None},
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'LVI-0005', 'nome': 'Blusa',
             'pedidos_total': 4, 'pedidos_penalizados': 0, 'frete_rs': 0,
             'pedidos_credito': 2, 'credito_rs': 7.45, 'peso_cobrado_kg': None, 'peso_anuncio_kg': None},
        ])
        alarme, creditos = ap.separar_alarme_creditos(df)
        self.assertEqual(list(alarme['sku']), ['K-L-0351'])
        self.assertEqual(alarme.loc[0, 'media_rs'], 2.5)
        self.assertEqual(alarme.loc[0, 'pct_penalizados'], 80.0)
        self.assertEqual(list(creditos['sku']), ['LVI-0005'])


# Só as colunas que a SQL lê.
_DDL = """
CREATE TEMP TABLE fact_vendas_snapshot (
    marketplace_origem varchar, loja_origem varchar, numero_pedido varchar,
    pedido_original varchar, data_venda date, sku varchar, frete numeric,
    arquivo_origem varchar
) ON COMMIT DROP;
CREATE TEMP TABLE dim_produtos (sku varchar, nome varchar) ON COMMIT DROP;
CREATE TEMP TABLE fact_pedidos_marketplace (
    marketplace varchar, loja varchar, numero_pedido varchar, data_venda date,
    peso_cobrado_g integer, peso_cadastrado_kg numeric
) ON COMMIT DROP;
"""

NALA, LPT, YANNI, TT = ('Shopee Lithouse(Nala)', 'Shopee-LPT',
                        'Shopee Litstore(Yanni)', 'TikTok-Nala')
D = date(2026, 9, 10)

# (marketplace, loja, numero_pedido, pedido_original, data, sku, frete, arquivo)
_VENDAS = [
    # Shopee API: carrinho com multa rateada entre dois SKUs
    ('SHOPEE', LPT, 'SP1', None, D, 'K3-LKE-3104-4030', 4.00, 'API'),
    ('SHOPEE', LPT, 'SP1', None, D, 'L-0320', 1.76, 'API'),
    ('SHOPEE', LPT, 'SP2', None, D, 'K3-LKE-3104-4030', 0, 'API'),
    # Shopee API em outra loja, mesmo SKU: linha separada por loja
    ('SHOPEE', NALA, 'SN1', None, D, 'K3-LKE-3104-4030', 2.00, 'API'),
    # Upload da Shopee não entra, nem com frete (prova o filtro de fonte)
    ('SHOPEE', YANNI, 'SY1', None, D, 'LNA-X', 9.99, 'pedidos_yanni.xlsx'),
    ('SHOPEE', LPT, 'SPU', None, D, 'K3-LKE-3104-4030', 9.99, 'pedidos_lpt.xlsx'),
    # TikTok: K-L-0351 com 2 pedidos penalizados de 3; o pedido T1 tem duas
    # linhas do mesmo SKU (duas variantes TikTok) e conta UMA vez
    ('TIKTOK', TT, 'TKTK_T1_a', 'T1', D, 'K-L-0351', 1.20, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_T1_b', 'T1', D, 'K-L-0351', 1.00, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_T2_a', 'T2', D, 'K-L-0351', 6.10, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_T3_a', 'T3', D, 'K-L-0351', 0, 'income.xlsx'),
    # TikTok: crédito negativo fica fora da soma
    ('TIKTOK', TT, 'TKTK_T4_a', 'T4', D, 'LVI-0005', -6.00, 'income.xlsx'),
    # SKU sem frete nenhum não aparece
    ('TIKTOK', TT, 'TKTK_T5_a', 'T5', D, 'L-0250-A', 0, 'income.xlsx'),
    # Fronteiras do período: 01/09 e 30/09 entram, 31/08 e 01/10 não
    ('TIKTOK', TT, 'TKTK_B1_a', 'B1', date(2026, 9, 1), 'L-0303', 5.70, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_B2_a', 'B2', date(2026, 9, 30), 'L-0303', 5.70, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_B3_a', 'B3', date(2026, 8, 31), 'L-0303', 5.70, 'income.xlsx'),
    ('TIKTOK', TT, 'TKTK_B4_a', 'B4', date(2026, 10, 1), 'L-0303', 5.70, 'income.xlsx'),
]
# Nome em duplicidade: não pode dobrar a soma.
_NOMES = [('K3-LKE-3104-4030', 'Bandeja Travessa Inox 40x30'),
          ('K3-LKE-3104-4030', 'Bandeja Travessa Inox 40x30'),
          ('K-L-0351', 'Kit Escorredor de talheres inox')]
_PESOS = [('SHOPEE', LPT, 'SP1', D, 1620, 0.400),
          ('SHOPEE', LPT, 'SP1', D, 1620, 0.400),   # duplicado de propósito
          ('SHOPEE', NALA, 'SN1', D, 1120, 0.400)]


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

    def setUp(self):
        self.cur = self.conn.cursor()
        self.cur.execute(_DDL)
        # Trava de segurança: os três nomes TÊM de resolver para pg_temp antes
        # de qualquer INSERT.
        self.cur.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(ARRAY['fact_vendas_snapshot', 'dim_produtos',
                              'fact_pedidos_marketplace']) AS t(nome)
            JOIN pg_class c ON c.oid = t.nome::regclass
        """)
        self.assertTrue(self.cur.fetchone()[0], 'tabela não resolveu para pg_temp')
        self.cur.executemany(
            "INSERT INTO fact_vendas_snapshot VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", _VENDAS)
        self.cur.executemany("INSERT INTO dim_produtos VALUES (%s, %s)", _NOMES)
        self.cur.executemany(
            "INSERT INTO fact_pedidos_marketplace VALUES (%s, %s, %s, %s, %s, %s)", _PESOS)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def _rodar(self, lojas=None, ini=date(2026, 9, 1), fim=date(2026, 9, 30)):
        self.cur.execute(ap.SQL_PENALIZACAO_FRETE,
                         ap.params_penalizacao_frete(ini, fim, lojas))
        cols = [d[0] for d in self.cur.description]
        df = pd.DataFrame(self.cur.fetchall(), columns=cols)
        return {(r['loja'], r['sku']): r for _, r in df.iterrows()}

    def test_shopee_carrinho_rateado_e_pedido_sem_multa_conta_no_total(self):
        r = self._rodar()[(LPT, 'K3-LKE-3104-4030')]
        self.assertEqual(r['pedidos_total'], 2)
        self.assertEqual(r['pedidos_penalizados'], 1)
        self.assertEqual(float(r['frete_rs']), 4.00)
        self.assertEqual(r['nome'], 'Bandeja Travessa Inox 40x30')
        self.assertAlmostEqual(float(r['peso_cobrado_kg']), 1.62)
        self.assertAlmostEqual(float(r['peso_anuncio_kg']), 0.40)
        self.assertEqual(float(self._rodar()[(LPT, 'L-0320')]['frete_rs']), 1.76)

    def test_mesmo_sku_em_lojas_diferentes_fica_separado(self):
        self.assertEqual(float(self._rodar()[(NALA, 'K3-LKE-3104-4030')]['frete_rs']), 2.00)

    def test_upload_da_shopee_nao_entra(self):
        r = self._rodar()
        self.assertNotIn((YANNI, 'LNA-X'), r)
        self.assertEqual(r[(LPT, 'K3-LKE-3104-4030')]['pedidos_total'], 2)

    def test_tiktok_pedido_com_duas_linhas_conta_uma_vez(self):
        r = self._rodar()[(TT, 'K-L-0351')]
        self.assertEqual(r['pedidos_total'], 3)
        self.assertEqual(r['pedidos_penalizados'], 2)
        self.assertEqual(float(r['frete_rs']), 8.30)
        self.assertIsNone(r['peso_cobrado_kg'])

    def test_credito_negativo_fora_da_soma(self):
        r = self._rodar()[(TT, 'LVI-0005')]
        self.assertEqual(r['pedidos_penalizados'], 0)
        self.assertEqual(float(r['frete_rs']), 0)
        self.assertEqual(float(r['credito_rs']), 6.00)
        self.assertEqual(r['nome'], None)

    def test_sku_sem_frete_nao_aparece(self):
        self.assertNotIn((TT, 'L-0250-A'), self._rodar())

    def test_fronteiras_do_periodo_inclusivas(self):
        r = self._rodar()[(TT, 'L-0303')]
        self.assertEqual(r['pedidos_penalizados'], 2)
        self.assertEqual(float(r['frete_rs']), 11.40)

    def test_filtro_de_lojas_lista_e_lista_vazia(self):
        self.assertEqual({k[0] for k in self._rodar(lojas=[TT])}, {TT})
        self.assertEqual(self._rodar(lojas=[]), {})

    def test_duplicata_no_nome_e_no_peso_nao_dobra_a_soma(self):
        total = sum(float(v['frete_rs']) for v in self._rodar().values())
        # 4,00 + 1,76 + 2,00 (Shopee API) + 8,30 (K-L-0351) + 11,40 (L-0303)
        self.assertAlmostEqual(total, 27.46)

    def test_dado_ate_executa(self):
        self.cur.execute(ap.SQL_PENALIZACAO_DADO_ATE)
        self.assertEqual(self.cur.fetchone()[0], date(2026, 10, 1))


if __name__ == '__main__':
    unittest.main(verbosity=2)
