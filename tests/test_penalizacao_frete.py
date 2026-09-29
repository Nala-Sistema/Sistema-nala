"""
Testa a aba "🚚 Penalização de frete" de analise_produtos.py (29/09/2026).

Duas partes:
  - Sem banco: parâmetros, % solto na SQL, a divisão alarme × créditos ×
    reembolsos e a extração do detalhe de frete do relatório do TikTok.
  - Com banco: EXECUTA a SQL_PENALIZACAO_FRETE de verdade, com os parâmetros
    ligados pelo psycopg2 (o mesmo driver da tela). SQL da Shopee já quebrou
    produção por % solto e por base errada; olhar o texto não pega isso.

A parte com banco só roda com a variável NALA_TEST_DB_URL. Ela cria tabelas
TEMPORÁRIAS com os mesmos nomes das reais (o Postgres procura pg_temp
primeiro), confere que os nomes resolvem para pg_temp ANTES de inserir, e
termina SEMPRE em ROLLBACK. Nenhuma tabela real é lida nem escrita. Use o
usuário de permissão mínima, nunca o dono do banco. A gravação do detalhe do
TikTok (processar_tiktok.gravar_frete_detalhe_tiktok) roda na mesma conexão,
com o commit dela anulado, e também termina em ROLLBACK.

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
import processar_tiktok as pt  # noqa: E402

_SQLS = (ap.SQL_PENALIZACAO_FRETE, ap.SQL_PENALIZACAO_FRETE_SEM_TK)

DB_URL = os.environ.get('NALA_TEST_DB_URL')


class SemBanco(unittest.TestCase):
    def test_nenhum_percent_solto_fora_dos_parametros_nomeados(self):
        for sql in _SQLS + (ap.SQL_PENALIZACAO_DADO_ATE, ap.SQL_EXISTE_TK):
            sem_params = re.sub(r'%\(\w+\)s', '', sql)
            self.assertNotIn('%', sem_params)
            self.assertNotIn('{', sql)   # o {tk} do modelo foi trocado

    def test_parametros_usados_sao_os_entregues(self):
        p = ap.params_penalizacao_frete(date(2026, 9, 1), date(2026, 9, 30))
        for sql in _SQLS:
            self.assertEqual(set(re.findall(r'%\((\w+)\)s', sql)), set(p))
        usados = set(re.findall(r'%\((\w+)\)s', ap.SQL_PENALIZACAO_DADO_ATE))
        self.assertEqual(usados, set(ap.params_penalizacao_dado_ate()))

    def test_card_conta_pedido_distinto_por_loja(self):
        # SP1 multou dois SKUs: é 1 pedido. O mesmo número em outra loja é outro.
        alarme = pd.DataFrame({
            'loja': ['Shopee-LPT', 'Shopee-LPT', 'Shopee-Nala'],
            'lista_pedidos_penalizados': [['SP1', 'SP2'], ['SP1'], ['SP1']],
        })
        self.assertEqual(ap.contar_pedidos_penalizados(alarme), 3)
        self.assertEqual(ap.contar_pedidos_penalizados(alarme.iloc[:0]), 0)
        self.assertEqual(ap.contar_pedidos_penalizados(
            pd.DataFrame({'loja': ['X'], 'lista_pedidos_penalizados': [None]})), 0)

    def test_aba_desenha_a_tabela_na_ordem_e_com_titulos_curtos(self):
        from unittest import mock
        resultado = pd.DataFrame([
            {'marketplace': 'SHOPEE', 'loja': 'Shopee-LPT', 'sku': 'K2-L-0320', 'nome': 'Kit escovas',
             'pedidos_total': 281, 'pedidos_penalizados': 30, 'lista_pedidos_penalizados': ['A'],
             'frete_rs': 43.37, 'pedidos_credito': 0, 'credito_rs': 0,
             'pedidos_reembolso': 0, 'frete_reembolso_rs': 0,
             'peso_cobrado_kg': 0.74, 'peso_anuncio_kg': 0.42},
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'K-L-0351', 'nome': None,
             'pedidos_total': 39, 'pedidos_penalizados': 26, 'lista_pedidos_penalizados': ['B'],
             'frete_rs': 53.93, 'pedidos_credito': 1, 'credito_rs': 6.0,
             'pedidos_reembolso': 1, 'frete_reembolso_rs': 7.2,
             'peso_cobrado_kg': 0.93, 'peso_anuncio_kg': 0.3},
        ])
        dado_ate = pd.DataFrame([[date(2026, 9, 29)]])
        with mock.patch.object(ap, 've_todas_lojas', return_value=True), \
             mock.patch.object(ap, 'filtro_periodo',
                               return_value=(date(2026, 9, 1), date(2026, 9, 29))), \
             mock.patch.object(ap, '_query_to_df', side_effect=[dado_ate, pd.DataFrame([[True]]), resultado]), \
             mock.patch.object(ap.st, 'dataframe', wraps=ap.st.dataframe) as tabela:
            ap._render_penalizacao_frete(engine=None)
        df, kwargs = tabela.call_args_list[0][0][0], tabela.call_args_list[0][1]
        self.assertEqual(list(df.columns), [
            'SKU', 'Produto', 'Loja', 'Frete R$', 'Média R$', 'Peso cobrado',
            'Peso anúncio', 'Pedidos pen.', 'Pedidos SKU', '% pen.'])
        self.assertEqual(list(df['SKU']), ['K-L-0351', 'K2-L-0320'])   # Frete R$ decrescente
        self.assertEqual(set(kwargs['column_config']), set(df.columns))
        self.assertEqual(kwargs['column_config']['Loja']['width'], 'medium')
        # três tabelas: alarme, créditos e reembolsos
        self.assertEqual(len(tabela.call_args_list), 3)

    def test_gestor_sem_loja_ve_aviso_e_nao_consulta(self):
        from unittest import mock
        with mock.patch.object(ap, 've_todas_lojas', return_value=False), \
             mock.patch.object(ap, 'get_lojas_usuario', return_value=[]), \
             mock.patch.object(ap, '_query_to_df') as consulta, \
             mock.patch.object(ap.st, 'caption') as caption, \
             mock.patch.object(ap.st, 'success') as success:
            ap._render_penalizacao_frete(engine=None)
        consulta.assert_not_called()
        success.assert_not_called()
        caption.assert_any_call("Nenhuma loja atribuída ao seu perfil.")

    def test_lojas_none_e_todas_lista_vazia_e_nenhuma(self):
        self.assertIsNone(ap.params_penalizacao_frete(1, 2)['lojas'])
        self.assertEqual(ap.params_penalizacao_frete(1, 2, [])['lojas'], [])
        self.assertEqual(ap.params_penalizacao_frete(1, 2, ('A',))['lojas'], ['A'])

    def test_separa_alarme_de_credito_e_calcula_media_e_pct(self):
        df = pd.DataFrame([
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'K-L-0351', 'nome': 'Kit',
             'pedidos_total': 10, 'pedidos_penalizados': 8, 'frete_rs': 20.0,
             'pedidos_credito': 0, 'credito_rs': 0, 'peso_cobrado_kg': None, 'peso_anuncio_kg': None,
             'pedidos_reembolso': 0, 'frete_reembolso_rs': 0},
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'LVI-0005', 'nome': 'Blusa',
             'pedidos_total': 4, 'pedidos_penalizados': 0, 'frete_rs': 0,
             'pedidos_credito': 2, 'credito_rs': 7.45, 'peso_cobrado_kg': None, 'peso_anuncio_kg': None,
             'pedidos_reembolso': 0, 'frete_reembolso_rs': 0},
            {'marketplace': 'TIKTOK', 'loja': 'TikTok-Nala', 'sku': 'LVI-0002', 'nome': 'Blusinha',
             'pedidos_total': 1, 'pedidos_penalizados': 0, 'frete_rs': 0,
             'pedidos_credito': 0, 'credito_rs': 0, 'peso_cobrado_kg': None, 'peso_anuncio_kg': None,
             'pedidos_reembolso': 1, 'frete_reembolso_rs': 7.2},
        ])
        alarme, creditos, reembolsos = ap.separar_alarme_creditos(df)
        self.assertEqual(list(alarme['sku']), ['K-L-0351'])
        self.assertEqual(alarme.loc[0, 'media_rs'], 2.5)
        self.assertEqual(alarme.loc[0, 'pct_penalizados'], 80.0)
        self.assertEqual(list(creditos['sku']), ['LVI-0005'])
        self.assertEqual(list(reembolsos['sku']), ['LVI-0002'])

    def test_escolhe_a_sql_pela_existencia_da_tabela(self):
        self.assertIs(ap.sql_penalizacao_frete(True), ap.SQL_PENALIZACAO_FRETE)
        self.assertIs(ap.sql_penalizacao_frete(False), ap.SQL_PENALIZACAO_FRETE_SEM_TK)
        self.assertIn('fact_tiktok_frete_detalhe', ap.SQL_PENALIZACAO_FRETE)
        self.assertNotIn('fact_tiktok_frete_detalhe', ap.SQL_PENALIZACAO_FRETE_SEM_TK)


# Relatório financeiro do TikTok, só as colunas que importam aqui.
_RELATORIO = pd.DataFrame([
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '584984692005700801',
     'ID do SKU': '1735553858218853956', 'Peso estimado do pacote cobrável': '50',
     'Peso da embalagem cobrável': '110', 'Custo líquido de frete': '-7.2',
     'Reembolsos de produtos': '-7.21'},
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585844917434419017',
     'ID do SKU': '1735166653608920644', 'Peso estimado do pacote cobrável': '300',
     'Peso da embalagem cobrável': '920', 'Custo líquido de frete': '-2.23',
     'Reembolsos de produtos': '0'},
    # mesma chave repetida: vale a última, como no snapshot
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585844917434419017',
     'ID do SKU': '1735166653608920644', 'Peso estimado do pacote cobrável': '300',
     'Peso da embalagem cobrável': '960', 'Custo líquido de frete': '-2.40',
     'Reembolsos de produtos': '0'},
    # a mesma chave de novo com célula vazia: vazio não apaga o 960
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585844917434419017',
     'ID do SKU': '1735166653608920644', 'Peso estimado do pacote cobrável': '',
     'Peso da embalagem cobrável': '', 'Custo líquido de frete': '',
     'Reembolsos de produtos': ''},
    # os 4 campos vazios: não vira linha
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585000000000000009',
     'ID do SKU': '1736000000000000009', 'Peso estimado do pacote cobrável': '',
     'Peso da embalagem cobrável': '/', 'Custo líquido de frete': '',
     'Reembolsos de produtos': 'nan'},
    # sem peso no arquivo: None, não zero
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585000000000000001',
     'ID do SKU': '1736547971990062660', 'Peso estimado do pacote cobrável': '',
     'Peso da embalagem cobrável': '/', 'Custo líquido de frete': '0',
     'Reembolsos de produtos': '0'},
    # reembolso de logística não é pedido; linha sem SKU fica fora
    {'Tipo de transação': 'Reembolso de logística', 'ID do pedido/ajuste': '7666545871418230535',
     'ID do SKU': '/', 'Peso estimado do pacote cobrável': '0',
     'Peso da embalagem cobrável': '0', 'Custo líquido de frete': '0',
     'Reembolsos de produtos': '0'},
    {'Tipo de transação': 'Pedido', 'ID do pedido/ajuste': '585000000000000002',
     'ID do SKU': '/', 'Peso estimado do pacote cobrável': '1',
     'Peso da embalagem cobrável': '1', 'Custo líquido de frete': '0',
     'Reembolsos de produtos': '0'},
])


class DetalheFreteTikTok(unittest.TestCase):
    def test_extrai_peso_e_reembolso_por_pedido_e_sku(self):
        d = {(x['pedido_original'], x['sku_tiktok']): x
             for x in pt.extrair_frete_detalhe(_RELATORIO)}
        self.assertEqual(len(d), 3)
        r = d[('584984692005700801', '1735553858218853956')]
        self.assertEqual((r['peso_estimado_g'], r['peso_embalagem_g']), (50.0, 110.0))
        self.assertEqual(r['custo_liquido_frete'], -7.2)
        self.assertEqual(r['reembolso_produtos'], 7.21)          # em módulo
        k = d[('585844917434419017', '1735166653608920644')]
        self.assertEqual(k['peso_embalagem_g'], 960.0)           # última preenchida vence
        self.assertEqual(k['custo_liquido_frete'], -2.40)        # vazio não apagou
        self.assertNotIn(('585000000000000009', '1736000000000000009'), d)
        v = d[('585000000000000001', '1736547971990062660')]
        self.assertIsNone(v['peso_estimado_g'])
        self.assertIsNone(v['peso_embalagem_g'])

    def test_layout_sem_as_colunas_novas_nao_quebra_nem_grava_vazio(self):
        velho = _RELATORIO[['Tipo de transação', 'ID do pedido/ajuste', 'ID do SKU']]
        self.assertEqual(pt.extrair_frete_detalhe(velho), [])

    def test_item_com_os_quatro_campos_vazios_nao_abre_conexao(self):
        class EngineProibido:
            def raw_connection(self):
                raise AssertionError('não deveria conectar')
        vazio = {'pedido_original': 'P', 'sku_tiktok': 'S', 'peso_estimado_g': None,
                 'peso_embalagem_g': None, 'custo_liquido_frete': None,
                 'reembolso_produtos': None}
        self.assertEqual(pt.gravar_frete_detalhe_tiktok(EngineProibido(), 'X', 'a', [vazio]),
                         (0, None))

    def test_falha_na_gravacao_nunca_levanta(self):
        class EngineQuebrado:
            def raw_connection(self):
                raise RuntimeError('tabela não existe')
        n, erro = pt.gravar_frete_detalhe_tiktok(
            EngineQuebrado(), 'TikTok-Nala', 'income.xlsx', pt.extrair_frete_detalhe(_RELATORIO))
        self.assertEqual(n, 0)
        self.assertIn('tabela não existe', erro)

    def test_lista_vazia_nao_abre_conexao(self):
        class EngineProibido:
            def raw_connection(self):
                raise AssertionError('não deveria conectar')
        self.assertEqual(pt.gravar_frete_detalhe_tiktok(EngineProibido(), 'X', 'a', []), (0, None))


# Só as colunas que a SQL lê.
_DDL = """
CREATE TEMP TABLE fact_vendas_snapshot (
    marketplace_origem varchar, loja_origem varchar, numero_pedido varchar,
    pedido_original varchar, data_venda date, sku varchar, frete numeric,
    arquivo_origem varchar, codigo_anuncio varchar
) ON COMMIT DROP;
CREATE TEMP TABLE dim_produtos (sku varchar, nome varchar) ON COMMIT DROP;
CREATE TEMP TABLE fact_pedidos_marketplace (
    marketplace varchar, loja varchar, numero_pedido varchar, data_venda date,
    peso_cobrado_g integer, peso_cadastrado_kg numeric
) ON COMMIT DROP;
-- Mesma definição de sql/tiktok_frete_detalhe.sql (PK inclusive: o ON
-- CONFLICT da gravação depende dela).
CREATE TEMP TABLE fact_tiktok_frete_detalhe (
    loja_origem varchar(100) NOT NULL, pedido_original varchar(50) NOT NULL,
    sku_tiktok varchar(50) NOT NULL, peso_estimado_g numeric(12,3),
    peso_embalagem_g numeric(12,3), custo_liquido_frete numeric(12,2),
    reembolso_produtos numeric(12,2), arquivo_origem varchar(255),
    gravado_em timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (loja_origem, pedido_original, sku_tiktok)
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
    # T6: frete cobrado em pedido com reembolso ao cliente (detalhe abaixo):
    # fora da soma quando o detalhe existe
    ('TIKTOK', TT, 'TKTK_T6_a', 'T6', D, 'K-L-0351', 3.00, 'income.xlsx'),
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
# Detalhe do TikTok, gravado pela função REAL do upload
# (processar_tiktok.gravar_frete_detalhe_tiktok). SKU TikTok = sufixo do
# numero_pedido do snapshot (TKTK_{pedido}_{sku}), igual ao codigo_anuncio.
_DETALHE_TT = [
    {'pedido_original': 'T1', 'sku_tiktok': 'a', 'peso_estimado_g': 300.0,
     'peso_embalagem_g': 900.0, 'custo_liquido_frete': -1.20, 'reembolso_produtos': 0.0},
    {'pedido_original': 'T1', 'sku_tiktok': 'b', 'peso_estimado_g': 300.0,
     'peso_embalagem_g': 960.0, 'custo_liquido_frete': -1.00, 'reembolso_produtos': 0.0},
    {'pedido_original': 'T6', 'sku_tiktok': 'a', 'peso_estimado_g': 300.0,
     'peso_embalagem_g': 950.0, 'custo_liquido_frete': -3.00, 'reembolso_produtos': 3.00},
]


class _EngineDoTeste:
    """Entrega a conexão do teste à função de gravação, com commit e close
    anulados: o que ela grava some no ROLLBACK do tearDown."""
    def __init__(self, conn):
        self._conn = conn

    def raw_connection(self):
        conn = self._conn

        class _Proxy:
            def cursor(self):
                return conn.cursor()

            def commit(self):
                pass

            def rollback(self):
                conn.rollback()

            def close(self):
                pass
        return _Proxy()


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
                              'fact_pedidos_marketplace',
                              'fact_tiktok_frete_detalhe']) AS t(nome)
            JOIN pg_class c ON c.oid = t.nome::regclass
        """)
        self.assertTrue(self.cur.fetchone()[0], 'tabela não resolveu para pg_temp')
        self.cur.executemany(
            "INSERT INTO fact_vendas_snapshot VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", _VENDAS)
        self.cur.execute("""
            UPDATE fact_vendas_snapshot SET codigo_anuncio = split_part(numero_pedido, '_', 3)
            WHERE marketplace_origem = 'TIKTOK'""")
        n, erro = pt.gravar_frete_detalhe_tiktok(
            _EngineDoTeste(self.conn), TT, 'income.xlsx', _DETALHE_TT)
        self.assertEqual((n, erro), (3, None))
        self.cur.executemany("INSERT INTO dim_produtos VALUES (%s, %s)", _NOMES)
        self.cur.executemany(
            "INSERT INTO fact_pedidos_marketplace VALUES (%s, %s, %s, %s, %s, %s)", _PESOS)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def _rodar(self, lojas=None, ini=date(2026, 9, 1), fim=date(2026, 9, 30),
               sql=ap.SQL_PENALIZACAO_FRETE):
        self.cur.execute(sql, ap.params_penalizacao_frete(ini, fim, lojas))
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
        self.assertEqual(r['pedidos_total'], 4)
        self.assertEqual(r['pedidos_penalizados'], 2)
        self.assertEqual(float(r['frete_rs']), 8.30)

    def test_tiktok_peso_vem_do_detalhe_so_nos_penalizados(self):
        r = self._rodar()[(TT, 'K-L-0351')]
        # T1a 0,90 kg e T1b 0,96 kg; T2 sem detalhe (upload antigo) não entra
        # na média; T6 reembolsado também não.
        self.assertAlmostEqual(float(r['peso_cobrado_kg']), 0.93)
        self.assertAlmostEqual(float(r['peso_anuncio_kg']), 0.30)

    def test_pedido_reembolsado_sai_da_soma_e_aparece_a_parte(self):
        r = self._rodar()[(TT, 'K-L-0351')]
        self.assertEqual(r['pedidos_reembolso'], 1)
        self.assertEqual(float(r['frete_reembolso_rs']), 3.00)
        self.assertNotIn('T6', r['lista_pedidos_penalizados'])

    def test_sem_a_tabela_do_detalhe_funciona_como_antes(self):
        r = self._rodar(sql=ap.SQL_PENALIZACAO_FRETE_SEM_TK)[(TT, 'K-L-0351')]
        self.assertEqual(r['pedidos_penalizados'], 3)
        self.assertEqual(float(r['frete_rs']), 11.30)
        self.assertEqual(r['pedidos_reembolso'], 0)
        self.assertIsNone(r['peso_cobrado_kg'])
        # Shopee não muda com ou sem o detalhe do TikTok
        self.assertEqual(float(self._rodar(sql=ap.SQL_PENALIZACAO_FRETE_SEM_TK)
                               [(LPT, 'K3-LKE-3104-4030')]['frete_rs']), 4.00)

    def test_regravar_o_mesmo_relatorio_atualiza_sem_duplicar(self):
        novo = dict(_DETALHE_TT[0], peso_embalagem_g=1000.0)
        n, erro = pt.gravar_frete_detalhe_tiktok(
            _EngineDoTeste(self.conn), TT, 'income2.xlsx', [novo])
        self.assertEqual((n, erro), (1, None))
        self.cur.execute("""SELECT count(*), max(peso_embalagem_g) FILTER (WHERE sku_tiktok = 'a'
                                AND pedido_original = 'T1'),
                                max(arquivo_origem) FILTER (WHERE pedido_original = 'T1'
                                AND sku_tiktok = 'a')
                            FROM fact_tiktok_frete_detalhe""")
        total, peso, arquivo = self.cur.fetchone()
        self.assertEqual((total, float(peso), arquivo), (3, 1000.0, 'income2.xlsx'))

    def test_regravar_com_valor_vazio_nao_apaga_o_bom(self):
        # R1 do auditor: relatório sobreposto com célula vazia.
        parcial = dict(_DETALHE_TT[2], peso_embalagem_g=None, reembolso_produtos=None,
                       custo_liquido_frete=-3.50)
        n, erro = pt.gravar_frete_detalhe_tiktok(
            _EngineDoTeste(self.conn), TT, 'income3.xlsx', [parcial])
        self.assertEqual((n, erro), (1, None))
        self.cur.execute("""SELECT peso_embalagem_g, reembolso_produtos, custo_liquido_frete,
                                   arquivo_origem
                            FROM fact_tiktok_frete_detalhe
                            WHERE pedido_original = 'T6' AND sku_tiktok = 'a'""")
        peso, reemb, custo, arquivo = self.cur.fetchone()
        self.assertEqual((float(peso), float(reemb), float(custo), arquivo),
                         (950.0, 3.00, -3.50, 'income3.xlsx'))
        # e o reembolso continua tirando o T6 da soma
        self.assertEqual(self._rodar()[(TT, 'K-L-0351')]['pedidos_reembolso'], 1)

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

    def test_filtro_com_varias_lojas(self):
        r = self._rodar(lojas=[LPT, TT])
        self.assertEqual({k[0] for k in r}, {LPT, TT})
        self.assertEqual(float(r[(LPT, 'K3-LKE-3104-4030')]['frete_rs']), 4.00)
        self.assertEqual(float(r[(TT, 'K-L-0351')]['frete_rs']), 8.30)
        # Loja pedida que não está no dado não quebra nem inventa linha.
        self.assertEqual({k[0] for k in self._rodar(lojas=[NALA, 'Loja-Inexistente'])}, {NALA})

    def test_carrinho_com_dois_skus_multados_e_um_pedido_na_lista(self):
        r = self._rodar(lojas=[LPT])
        alarme = pd.DataFrame([{'loja': k[0], 'lista_pedidos_penalizados': v['lista_pedidos_penalizados']}
                               for k, v in r.items()])
        # SP1 multou K3-LKE-3104-4030 e L-0320: soma por SKU daria 2.
        self.assertEqual(sum(v['pedidos_penalizados'] for v in r.values()), 2)
        self.assertEqual(ap.contar_pedidos_penalizados(alarme), 1)

    def test_duplicata_no_nome_e_no_peso_nao_dobra_a_soma(self):
        total = sum(float(v['frete_rs']) for v in self._rodar().values())
        # 4,00 + 1,76 + 2,00 (Shopee API) + 8,30 (K-L-0351) + 11,40 (L-0303)
        self.assertAlmostEqual(total, 27.46)

    def test_dado_ate_respeita_o_filtro_de_loja(self):
        def dado_ate(lojas):
            self.cur.execute(ap.SQL_PENALIZACAO_DADO_ATE, ap.params_penalizacao_dado_ate(lojas))
            return self.cur.fetchone()[0]
        self.assertEqual(dado_ate(None), date(2026, 10, 1))
        self.assertEqual(dado_ate([LPT, NALA]), D)
        self.assertIsNone(dado_ate([YANNI]))   # upload da Shopee não conta


if __name__ == '__main__':
    unittest.main(verbosity=2)
