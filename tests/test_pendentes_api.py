"""
Testa o caminho da venda PENDENTE QUE VEIO DA API (arquivo_origem = 'API'),
frente [VENDAS PENDENTES ML], 30/09/2026.

Regra: a aba Vendas Pendentes NÃO grava no snapshot a venda da API. Só grava o
mapeamento em dim_sku_mapeamento e marca 'Aguardando coleta'. Quem grava a
venda é o coletor (apaga as linhas 'API' da janela e regrava do espelho, com o
SKU que o mapeamento ensinou). A pendente só fecha quando a venda APARECE no
snapshot.

Duas partes:
  - Sem banco: o que a aba executa (e o que NÃO executa) para pendente de API
    e para pendente de upload (que segue como sempre).
  - Com banco (NALA_TEST_DB_URL, usuário de permissão mínima): o ciclo inteiro
    "corrigiu na aba -> some da aba -> aparece no snapshot na coleta seguinte,
    UMA vez só". Tabelas TEMPORÁRIAS com os nomes das reais, conferidas em
    pg_temp antes de qualquer INSERT, e SEMPRE ROLLBACK. O passo do coletor é
    reproduzido com a mesma semântica dele (DELETE das linhas 'API' da janela +
    INSERT do que o espelho mostra); o código do coletor em si é testado no
    repo nala-coletor-ml (tests/test_coletar_vendas_ml.py, test_vendas_ml.py).

Rodar:  python -m pytest tests/test_pendentes_api.py
"""

import os
import re
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import database_utils as du  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')

LOJA = 'ML-Nala'
MKT = 'MERCADO LIVRE'


class CursorGravador:
    """Cursor falso: guarda (sql, params) na ordem em que foram executados."""

    def __init__(self):
        self.executados = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executados.append((' '.join(sql.split()), params))

    def close(self):
        pass

    def sqls(self):
        return [s for s, _ in self.executados]


class ConexaoFalsa:
    def __init__(self):
        self.cur = CursorGravador()
        self.commits = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


class EngineFalso:
    def __init__(self):
        self.conn = ConexaoFalsa()

    def raw_connection(self):
        return self.conn


def item(id_, sku, sku_original, origem):
    return {
        'id': id_, 'sku': sku, 'sku_original': sku_original,
        'valor_venda_efetivo': 100.0, 'comissao': 10.0, 'imposto': 11.0,
        'frete': 5.0, 'quantidade': 1, 'marketplace_origem': MKT,
        'loja_origem': LOJA, 'numero_pedido': f'P{id_}',
        'data_venda': pd.Timestamp('2026-09-10'), 'codigo_anuncio': 'MLB1',
        'arquivo_origem': origem, 'desconto_parceiro': 0, 'outros_custos': 0,
        'logistica': None}


def _manual(itens):
    engine = EngineFalso()
    with mock.patch.object(du, 'buscar_skus_validos', lambda e: {'L-0320'}), \
         mock.patch.object(du, 'buscar_custos_skus', lambda e: {'L-0320': 20.0}):
        res = du.reprocessar_pendentes_manual(engine, itens)
    return res, engine.conn.cur


class SemBanco(unittest.TestCase):

    def test_pendente_de_api_NAO_insere_no_snapshot(self):
        res, cur = _manual([item(1, 'L-0320', 'SEM-SKU:MLB777', 'API')])
        self.assertFalse(any('INSERT INTO fact_vendas_snapshot' in s
                             for s in cur.sqls()))
        self.assertEqual(res['sucesso'], 1)
        self.assertEqual(res['aguardando'], 1)

    def test_pendente_de_api_grava_o_mapeamento_e_marca_aguardando(self):
        _, cur = _manual([item(1, 'L-0320', 'SEM-SKU:MLB777', 'API')])
        maps = [p for s, p in cur.executados
                if s.startswith('INSERT INTO dim_sku_mapeamento')]
        self.assertEqual(maps, [('SEM-SKU:MLB777', 'L-0320')])
        marcas = [p for s, p in cur.executados
                  if s.startswith('UPDATE fact_vendas_pendentes SET status = %s')]
        self.assertEqual(marcas, [['Aguardando coleta', 1]])

    def test_pendente_de_api_com_sku_ja_cadastrado_nao_precisa_de_mapeamento(self):
        res, cur = _manual([item(1, 'L-0320', 'L-0320', 'API')])
        self.assertFalse(any(s.startswith('INSERT INTO dim_sku_mapeamento')
                             for s in cur.sqls()))
        self.assertEqual(res['aguardando'], 1)

    def test_sku_nao_cadastrado_continua_sendo_erro_tambem_na_api(self):
        res, cur = _manual([item(1, 'NAO-EXISTE', 'SEM-SKU:MLB777', 'API')])
        self.assertEqual(res['sucesso'], 0)
        self.assertEqual(res['erros'], 1)
        self.assertFalse(any(s.startswith('INSERT INTO dim_sku_mapeamento')
                             for s in cur.sqls()))

    def test_pendente_de_upload_segue_como_sempre(self):
        res, cur = _manual([item(2, 'L-0320', 'L-320', 'vendas_setembro.xlsx')])
        self.assertTrue(any('INSERT INTO fact_vendas_snapshot' in s
                            for s in cur.sqls()))
        self.assertEqual(res['aguardando'], 0)
        marcas = [p for s, p in cur.executados
                  if s.startswith("UPDATE fact_vendas_pendentes SET status = 'Revisado manualmente'")]
        self.assertEqual(marcas, [[2]])

    def test_lote_misto_separa_api_de_upload(self):
        res, cur = _manual([item(1, 'L-0320', 'SEM-SKU:MLB777', 'API'),
                            item(2, 'L-0320', 'L-320', 'vendas_setembro.xlsx')])
        inserts = [s for s in cur.sqls() if 'INSERT INTO fact_vendas_snapshot' in s]
        self.assertEqual(len(inserts), 1)
        self.assertEqual((res['sucesso'], res['aguardando']), (2, 1))

    def test_por_sku_nao_insere_no_snapshot_para_api(self):
        engine = EngineFalso()
        df = pd.DataFrame([
            {'id': 1, 'arquivo_origem': 'API', 'valor_venda_efetivo': 10,
             'quantidade': 1, 'imposto': 1, 'marketplace_origem': MKT,
             'comissao': 1, 'tarifa_fixa': 0, 'frete': 0},
            {'id': 2, 'arquivo_origem': 'x.xlsx', 'valor_venda_efetivo': 10,
             'quantidade': 1, 'imposto': 1, 'marketplace_origem': MKT,
             'comissao': 1, 'tarifa_fixa': 0, 'frete': 0, 'data_venda': '2026-09-10',
             'loja_origem': LOJA, 'numero_pedido': 'P2', 'codigo_anuncio': 'M'}])
        with mock.patch.object(du, 'buscar_skus_validos', lambda e: {'L-0320'}), \
             mock.patch.object(du, 'buscar_custos_skus', lambda e: {'L-0320': 20.0}), \
             mock.patch.object(du, 'buscar_pendentes', lambda *a, **k: df):
            res = du.reprocessar_pendentes_por_sku(engine, 'L-0320')
        sqls = engine.conn.cur.sqls()
        self.assertEqual(sum('INSERT INTO fact_vendas_snapshot' in s for s in sqls), 1)
        self.assertEqual(res['aguardando'], 1)
        marcas = [p for s, p in engine.conn.cur.executados
                  if s.startswith('UPDATE fact_vendas_pendentes SET status = %s')]
        self.assertEqual(marcas, [['Aguardando coleta', 1]])

    def test_conciliacao_so_olha_linha_API_do_snapshot_e_aplica_o_mapeamento(self):
        sql = du.SQL_CONCILIAR_PENDENTES_API
        self.assertIn("s.arquivo_origem = 'API'", sql)
        self.assertIn('dim_sku_mapeamento', sql)
        self.assertIn("p.arquivo_origem = 'API'", sql)
        self.assertNotIn('%', sql)


# Só as colunas que as funções tocam.
_DDL = """
CREATE TEMP TABLE fact_vendas_pendentes (
    id integer NOT NULL, marketplace_origem varchar, loja_origem varchar,
    numero_pedido varchar, data_venda date, sku varchar, codigo_anuncio varchar,
    quantidade integer, valor_venda_efetivo numeric, imposto numeric,
    comissao numeric, frete numeric, tarifa_fixa numeric,
    arquivo_origem varchar, data_processamento timestamp DEFAULT now(),
    status varchar, motivo varchar, logistica varchar,
    UNIQUE (numero_pedido, sku, loja_origem)
) ON COMMIT DROP;
CREATE TEMP TABLE fact_vendas_snapshot (
    marketplace_origem varchar, loja_origem varchar, numero_pedido varchar,
    data_venda date, sku varchar, quantidade integer, arquivo_origem varchar
) ON COMMIT DROP;
CREATE TEMP TABLE dim_sku_mapeamento (
    sku_errado varchar PRIMARY KEY, sku_correto varchar,
    data_criacao timestamp DEFAULT now()
) ON COMMIT DROP;
"""


class _EngineDoTeste:
    """Entrega a conexão do teste às funções, com commit e close anulados: o
    que elas gravam some no ROLLBACK do tearDown."""
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
class CicloComBanco(unittest.TestCase):
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
        self.cur.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(ARRAY['fact_vendas_pendentes', 'fact_vendas_snapshot',
                              'dim_sku_mapeamento']) AS t(nome)
            JOIN pg_class c ON c.oid = t.nome::regclass
        """)
        self.assertTrue(self.cur.fetchone()[0], 'tabela não resolveu para pg_temp')
        self.engine = _EngineDoTeste(self.conn)

        def ler(query, engine, params=None):
            c = self.conn.cursor()
            c.execute(query, params)
            cols = [d[0] for d in c.description]
            return pd.DataFrame(c.fetchall(), columns=cols)
        p = mock.patch.object(du.pd, 'read_sql', ler)
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def _plantar_pendente(self, id_, pedido, sku, origem, status='Pendente'):
        self.cur.execute(
            "INSERT INTO fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (%s,%s,%s,%s,'2026-09-10',%s,100,%s,%s, now())",
            (id_, MKT, LOJA, pedido, sku, origem, status))

    def _coleta_seguinte(self, pedido, sku_certo):
        """O que sincronizar_snapshot faz: apaga as linhas 'API' da loja na
        janela e reinsere o que o espelho mostra agora (SKU ja' corrigido)."""
        self.cur.execute(
            "DELETE FROM fact_vendas_snapshot WHERE marketplace_origem = %s AND "
            "loja_origem = %s AND arquivo_origem = 'API' AND data_venda >= '2026-09-01'",
            (MKT, LOJA))
        self.cur.execute(
            "INSERT INTO fact_vendas_snapshot VALUES (%s,%s,%s,'2026-09-10',%s,1,'API')",
            (MKT, LOJA, pedido, sku_certo))

    def _n_snapshot(self, pedido):
        self.cur.execute(
            "SELECT count(*) FROM fact_vendas_snapshot WHERE numero_pedido = %s", (pedido,))
        return self.cur.fetchone()[0]

    def _status(self, id_):
        self.cur.execute("SELECT status FROM fact_vendas_pendentes WHERE id = %s", (id_,))
        return self.cur.fetchone()[0]

    def test_corrigiu_na_aba_some_da_aba_e_entra_no_snapshot_uma_vez_so(self):
        self._plantar_pendente(1, 'PACK1', 'SEM-SKU:MLB777', 'API')

        # 1) o time corrige na aba
        with mock.patch.object(du, 'buscar_skus_validos', lambda e: {'L-0320'}), \
             mock.patch.object(du, 'buscar_custos_skus', lambda e: {'L-0320': 20.0}):
            res = du.reprocessar_pendentes_manual(self.engine, [
                item(1, 'L-0320', 'SEM-SKU:MLB777', 'API') | {'numero_pedido': 'PACK1'}])
        self.assertEqual(res['aguardando'], 1)

        # 2) a aba NAO gravou a venda; o mapeamento ficou; a pendente espera
        self.assertEqual(self._n_snapshot('PACK1'), 0)
        self.cur.execute("SELECT sku_correto FROM dim_sku_mapeamento "
                         "WHERE sku_errado = 'SEM-SKU:MLB777'")
        self.assertEqual(self.cur.fetchone()[0], 'L-0320')
        self.assertEqual(self._status(1), 'Aguardando coleta')
        # ja' saiu da lista de pendentes por tipo (status = 'Pendente') e
        # aparece na seção "Aguardando a próxima coleta"
        self.assertEqual(len(du.buscar_aguardando_coleta(self.engine)), 1)
        self.assertEqual(du.buscar_aguardando_coleta(self.engine).iloc[0]['sku_destino'], 'L-0320')

        # 3) sem a coleta, nada fecha (nem com a tela aberta mil vezes)
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)
        self.assertEqual(self._status(1), 'Aguardando coleta')

        # 4) a coleta seguinte grava a venda — e mais uma noite depois, de novo:
        #    continua UMA linha (DELETE da janela + INSERT, como o coletor faz)
        self._coleta_seguinte('PACK1', 'L-0320')
        self._coleta_seguinte('PACK1', 'L-0320')
        self.assertEqual(self._n_snapshot('PACK1'), 1)

        # 5) a tela fecha a pendente; nao volta
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 1)
        self.assertEqual(self._status(1), 'Reprocessado')
        self.assertEqual(len(du.buscar_aguardando_coleta(self.engine)), 0)
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)

    def test_nao_fecha_por_outro_pedido_nem_por_linha_de_upload(self):
        self._plantar_pendente(1, 'PACK1', 'SEM-SKU:MLB777', 'API', 'Aguardando coleta')
        self.cur.execute("INSERT INTO dim_sku_mapeamento (sku_errado, sku_correto) "
                         "VALUES ('SEM-SKU:MLB777', 'L-0320')")
        # mesmo pedido+sku, mas veio do upload: nao prova nada sobre a API
        self.cur.execute("INSERT INTO fact_vendas_snapshot VALUES "
                         "(%s,%s,'PACK1','2026-09-10','L-0320',1,'vendas.xlsx')", (MKT, LOJA))
        # pedido diferente, da API
        self.cur.execute("INSERT INTO fact_vendas_snapshot VALUES "
                         "(%s,%s,'OUTRO','2026-09-10','L-0320',1,'API')", (MKT, LOJA))
        # mesmo pedido, SKU diferente do mapeado
        self.cur.execute("INSERT INTO fact_vendas_snapshot VALUES "
                         "(%s,%s,'PACK1','2026-09-10','OUTRO-SKU',1,'API')", (MKT, LOJA))
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)
        self.assertEqual(self._status(1), 'Aguardando coleta')

    def test_pendente_de_api_cadastrada_em_gestao_de_skus_tambem_fecha(self):
        """SKU sem cadastro que foi cadastrado (sem mapeamento): o coletor rele
        o pedido, a venda entra com o proprio sku, e a pendente ('Pendente')
        fecha sozinha."""
        self._plantar_pendente(1, 'PACK2', 'K2-LVI-CANOA0506', 'API')
        self._coleta_seguinte('PACK2', 'K2-LVI-CANOA0506')
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 1)
        self.assertEqual(self._status(1), 'Reprocessado')

    def test_pendente_de_upload_nunca_e_tocada_pela_conciliacao(self):
        self._plantar_pendente(1, 'P9', 'L-0320', 'vendas.xlsx')
        self.cur.execute("INSERT INTO fact_vendas_snapshot VALUES "
                         "(%s,%s,'P9','2026-09-10','L-0320',1,'API')", (MKT, LOJA))
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)
        self.assertEqual(self._status(1), 'Pendente')


if __name__ == '__main__':
    unittest.main()
