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

    def __init__(self, lojas_api=None, ja_no_snapshot=None, rowcount=0):
        self.executados = []
        self.rowcount = rowcount
        self._lojas_api = lojas_api or {}
        # (numero_pedido, sku, loja) que ja' estao no snapshot
        self._ja_no_snapshot = set(ja_no_snapshot or [])

    def fetchone(self):
        sql, params = self.executados[-1]
        if sql.startswith('SELECT 1 FROM fact_vendas_snapshot'):
            return (1,) if tuple(params[:3]) in self._ja_no_snapshot else None
        return None

    def execute(self, sql, params=None):
        self.executados.append((' '.join(sql.split()), params))

    def fetchall(self):
        # unica leitura do cursor: as lojas cuja venda vem da API
        return [(l, pd.Timestamp(d)) for l, d in self._lojas_api.items()]

    def close(self):
        pass

    def sqls(self):
        return [s for s, _ in self.executados]


class ConexaoFalsa:
    def __init__(self, lojas_api=None, ja_no_snapshot=None, rowcount=0):
        self.cur = CursorGravador(lojas_api, ja_no_snapshot, rowcount)
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
    def __init__(self, lojas_api=None, ja_no_snapshot=None, rowcount=0):
        self.conn = ConexaoFalsa(lojas_api, ja_no_snapshot, rowcount)

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


def _manual(itens, lojas_api=None, ja_no_snapshot=None):
    engine = EngineFalso(lojas_api, ja_no_snapshot)
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

    def test_aguardando_grava_a_hora_da_correcao_para_contar_a_espera(self):
        """R4 do auditor: os dias de espera contam da correcao, nao da criacao
        da pendente."""
        _, cur = _manual([item(1, 'L-0320', 'SEM-SKU:MLB777', 'API')])
        marca = [s for s in cur.sqls()
                 if s.startswith('UPDATE fact_vendas_pendentes SET status = %s')]
        self.assertEqual(len(marca), 1)
        self.assertIn('data_processamento = NOW()', marca[0])

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

    # ---- 05/10/2026: pendente de UPLOAD cuja venda a API ja' cobre ----
    # Caso real: K2-LVI-CANOA0506 no ML-YanniRJ (3 pendentes de upload de
    # 23-28/09). Reprocessar pela via de upload poria a venda no snapshot com
    # origem de upload E depois de novo pela API.
    API_YANNI = {'ML-YanniRJ': '2026-09-01'}

    def _upload_yanni(self, id_, data, loja='ML-YanniRJ'):
        return item(id_, 'K2-LVI-CANOA0506', 'K2-LVI-CANOA0506',
                    'vendas ML YANNI RJ 22 a 23-09.xlsx') | {
            'loja_origem': loja, 'data_venda': pd.Timestamp(data)}

    def test_upload_de_loja_api_dentro_do_periodo_api_NAO_insere_e_espera(self):
        engine_itens = [self._upload_yanni(1, '2026-09-23')]
        with mock.patch.object(du, 'buscar_skus_validos',
                               lambda e: {'K2-LVI-CANOA0506'}),              mock.patch.object(du, 'buscar_custos_skus', lambda e: {}):
            engine = EngineFalso(self.API_YANNI)
            res = du.reprocessar_pendentes_manual(engine, engine_itens)
        sqls = engine.conn.cur.sqls()
        self.assertFalse(any('INSERT INTO fact_vendas_snapshot' in s for s in sqls))
        self.assertEqual(res['aguardando'], 1)
        marcas = [p for s, p in engine.conn.cur.executados
                  if s.startswith('UPDATE fact_vendas_pendentes SET status = %s')]
        self.assertEqual(marcas, [['Aguardando coleta', 1]])
        # nem o 'Revisado manualmente' do caminho de upload
        self.assertFalse(any("'Revisado manualmente'" in s for s in sqls))

    def test_upload_anterior_ao_api_desde_segue_como_sempre(self):
        engine = EngineFalso(self.API_YANNI)
        with mock.patch.object(du, 'buscar_skus_validos',
                               lambda e: {'K2-LVI-CANOA0506'}),              mock.patch.object(du, 'buscar_custos_skus', lambda e: {}):
            res = du.reprocessar_pendentes_manual(
                engine, [self._upload_yanni(2, '2026-08-31')])
        self.assertTrue(any('INSERT INTO fact_vendas_snapshot' in s
                            for s in engine.conn.cur.sqls()))
        self.assertEqual(res['aguardando'], 0)

    def test_upload_de_loja_que_nao_e_api_segue_como_sempre(self):
        engine = EngineFalso(self.API_YANNI)       # so' o Yanni e' API
        with mock.patch.object(du, 'buscar_skus_validos',
                               lambda e: {'K2-LVI-CANOA0506'}),              mock.patch.object(du, 'buscar_custos_skus', lambda e: {}):
            res = du.reprocessar_pendentes_manual(
                engine, [self._upload_yanni(3, '2026-09-23', loja='Magalu-Nala')])
        self.assertTrue(any('INSERT INTO fact_vendas_snapshot' in s
                            for s in engine.conn.cur.sqls()))
        self.assertEqual(res['aguardando'], 0)

    def test_falha_ao_ler_lojas_api_fecha_a_conexao_e_nao_insere(self):
        """R2 do auditor: a excecao vazava a conexao e a tela mostrava erro cru."""
        class CursorQuebrado(CursorGravador):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if 'dim_fonte_dados' in sql:
                    raise RuntimeError('banco fora')

        class ConnQuebrada(ConexaoFalsa):
            def __init__(self):
                super().__init__()
                self.cur = CursorQuebrado()
                self.fechada = False
                self.desfeita = False

            def close(self):
                self.fechada = True

            def rollback(self):
                self.desfeita = True

        class EngineQuebrado:
            def __init__(self):
                self.conn = ConnQuebrada()

            def raw_connection(self):
                return self.conn

        engine = EngineQuebrado()
        with mock.patch.object(du, 'buscar_skus_validos', lambda e: {'L-0320'}),              mock.patch.object(du, 'buscar_custos_skus', lambda e: {'L-0320': 20.0}),              mock.patch.object(du.st, 'error') as erro_na_tela:
            res = du.reprocessar_pendentes_manual(
                engine, [item(1, 'L-0320', 'L-0320', 'x.xlsx')])
        self.assertTrue(engine.conn.fechada)
        self.assertTrue(engine.conn.desfeita)
        self.assertEqual((res['sucesso'], res['aguardando']), (0, 0))
        self.assertIn('Nada foi reprocessado', res['mensagem'])
        self.assertFalse(any('INSERT INTO' in s for s in engine.conn.cur.sqls()))
        erro_na_tela.assert_called_once()
        self.assertNotIn('banco fora', erro_na_tela.call_args[0][0])   # sem erro cru

    def test_todo_status_que_o_codigo_grava_esta_no_SQL_da_constraint(self):
        """05/10/2026: 'Aguardando coleta' nao estava na CHECK de producao e o
        botao Reprocessar quebrou. Todo status que a aba ou a conciliacao grava
        tem de constar do SQL que recria a constraint."""
        sql = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                                'sql', 'pendentes_status_aguardando.sql'),
                   encoding='utf-8').read()
        gravados = {du.STATUS_AGUARDANDO, 'Pendente', 'Reprocessado',
                    'Revisado manualmente'}
        for status in gravados:
            self.assertIn(f"'{status}'", sql, status)
        # e o codigo so' grava estes quatro: nenhum outro literal de status novo
        # em UPDATE de fact_vendas_pendentes sem passar por aqui
        fonte = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                                  'database_utils.py'), encoding='utf-8').read()
        for achado in re.findall(r"SET status = '([^']+)'", fonte):
            self.assertIn(achado, gravados)

    # ---- venda que JA' esta no snapshot (05/10/2026) ----

    def test_venda_ja_no_snapshot_fecha_a_pendente_sem_inserir_e_avisa(self):
        """Ex. real: L-0429 da ML-LPT, 12/08, entrou pelo upload seguinte; o
        reprocessar dava 'erro' calado pela chave unica."""
        it = item(7, 'L-0320', 'L-0320', 'vendas.xlsx')
        res, cur = _manual([it], ja_no_snapshot={('P7', 'L-0320', LOJA)})
        sqls = cur.sqls()
        self.assertFalse(any('INSERT INTO fact_vendas_snapshot' in s for s in sqls))
        fechou = [p for s, p in cur.executados
                  if s.startswith("UPDATE fact_vendas_pendentes SET status = 'Reprocessado'")]
        self.assertEqual(fechou, [[7]])
        self.assertFalse(any("'Revisado manualmente'" in s for s in sqls))
        self.assertEqual((res['sucesso'], res['erros'], res['ja_existia']), (1, 0, 1))
        self.assertIn('já estava(m) nas vendas', res['mensagem'])

    def test_venda_ja_no_snapshot_com_sku_corrigido_ainda_lembra_o_mapeamento(self):
        it = item(7, 'L-0320', 'L-320', 'vendas.xlsx')
        res, cur = _manual([it], ja_no_snapshot={('P7', 'L-0320', LOJA)})
        maps = [p for s, p in cur.executados
                if s.startswith('INSERT INTO dim_sku_mapeamento')]
        self.assertEqual(maps, [('L-320', 'L-0320')])
        self.assertEqual(res['mapeados'], 1)

    def test_venda_que_nao_esta_no_snapshot_insere_como_sempre(self):
        res, cur = _manual([item(8, 'L-0320', 'L-0320', 'vendas.xlsx')])
        self.assertTrue(any('INSERT INTO fact_vendas_snapshot' in s for s in cur.sqls()))
        self.assertEqual(res['ja_existia'], 0)

    def test_lote_com_uma_que_ja_existe_e_uma_nova(self):
        res, cur = _manual(
            [item(7, 'L-0320', 'L-0320', 'a.xlsx'), item(8, 'L-0320', 'L-0320', 'b.xlsx')],
            ja_no_snapshot={('P7', 'L-0320', LOJA)})
        inserts = [s for s in cur.sqls() if 'INSERT INTO fact_vendas_snapshot' in s]
        self.assertEqual(len(inserts), 1)
        self.assertEqual((res['sucesso'], res['ja_existia']), (2, 1))

    # ---- Excluir Selecionadas: a tela le res['mensagem'] (05/10/2026) ----

    def test_excluir_devolve_mensagem_no_sucesso(self):
        engine = EngineFalso(rowcount=2)
        res = du.excluir_pendentes_por_ids(engine, [1572, 1573])
        self.assertEqual(res['excluidos'], 2)
        self.assertEqual(res['erros'], 0)
        self.assertIn('2 pendente(s) excluída(s)', res['mensagem'])

    def test_excluir_devolve_mensagem_no_erro(self):
        class Quebrado:
            def raw_connection(self):
                raise RuntimeError('banco fora')
        with mock.patch.object(du.st, 'error'):
            res = du.excluir_pendentes_por_ids(Quebrado(), [1])
        self.assertEqual((res['excluidos'], res['erros']), (0, 1))
        self.assertIn('Erro ao excluir', res['mensagem'])

    def test_excluir_sem_ids_tambem_devolve_mensagem(self):
        self.assertIn('mensagem', du.excluir_pendentes_por_ids(EngineFalso(), []))

    def test_a_tela_so_le_chaves_que_excluir_devolve(self):
        fonte = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                                  'central_uploads.py'), encoding='utf-8').read()
        chaves = set(re.findall(r"res_del\['(\w+)'\]", fonte))
        self.assertIn('mensagem', chaves)
        for chave in chaves:
            self.assertIn(chave, du.excluir_pendentes_por_ids(EngineFalso(rowcount=1), [1]))
            self.assertIn(chave, du.excluir_pendentes_por_ids(EngineFalso(), []))

    def test_sob_a_api_regra_pura(self):
        lojas = {'ML-YanniRJ': pd.Timestamp('2026-09-01').date()}
        f = du._sob_a_api
        self.assertTrue(f('API', 'Qualquer', pd.Timestamp('2026-01-01'), {}))
        self.assertTrue(f('x.xlsx', 'ML-YanniRJ', pd.Timestamp('2026-09-01'), lojas))
        self.assertFalse(f('x.xlsx', 'ML-YanniRJ', pd.Timestamp('2026-08-31'), lojas))
        self.assertFalse(f('x.xlsx', 'Magalu-Nala', pd.Timestamp('2026-09-23'), lojas))
        # data ilegivel em loja API: na duvida, nao grava no snapshot
        self.assertTrue(f('x.xlsx', 'ML-YanniRJ', pd.NaT, lojas))
        self.assertFalse(f('x.xlsx', 'Magalu-Nala', pd.NaT, lojas))

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
        # e a pendente de UPLOAD de loja/periodo que ja' esta na API
        self.assertIn("f.fonte = 'api'", sql)
        self.assertIn('p.data_venda >= f.api_desde', sql)
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
CREATE TEMP TABLE dim_fonte_dados (
    marketplace varchar, loja varchar, assunto varchar, fonte varchar,
    api_desde date
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

    def _copiar_checks(self, tabela):
        """Copia da tabela REAL (pg_constraint) as CHECK constraints para a TEMP
        de mesmo nome. Sem isto a TEMP aceita qualquer status e o teste passa
        enquanto producao recusa (05/10/2026: 'Aguardando coleta')."""
        self.cur.execute(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = %s::regclass AND contype = 'c'", (f'public.{tabela}',))
        for nome, definicao in self.cur.fetchall():
            self.cur.execute(
                f'ALTER TABLE pg_temp.{tabela} ADD CONSTRAINT "{nome}" {definicao}')

    def setUp(self):
        self.cur = self.conn.cursor()
        self.cur.execute(_DDL)
        for tabela in ('fact_vendas_pendentes', 'fact_vendas_snapshot',
                       'dim_fonte_dados'):
            self._copiar_checks(tabela)
        self.cur.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(ARRAY['fact_vendas_pendentes', 'fact_vendas_snapshot',
                              'dim_sku_mapeamento', 'dim_fonte_dados']) AS t(nome)
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

    def test_a_constraint_REAL_de_status_aceita_o_status_que_o_codigo_grava(self):
        """Falha enquanto sql/pendentes_status_aguardando.sql nao rodar no banco
        que o teste usa: a TEMP leva a CHECK de producao."""
        self._plantar_pendente(1, 'PACK1', 'SEM-SKU:MLB777', 'API')
        self.cur.execute("SAVEPOINT s")
        try:
            self.cur.execute(
                "UPDATE pg_temp.fact_vendas_pendentes SET status = %s WHERE id = 1",
                (du.STATUS_AGUARDANDO,))
        except Exception as e:
            self.fail(f"a CHECK de status do banco recusa '{du.STATUS_AGUARDANDO}': {e}")

    def _plantar_pendente(self, id_, pedido, sku, origem, status='Pendente'):
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (%s,%s,%s,%s,'2026-09-10',%s,100,%s,%s, now())",
            (id_, MKT, LOJA, pedido, sku, origem, status))

    def _coleta_seguinte(self, pedido, sku_certo):
        """O que sincronizar_snapshot faz: apaga as linhas 'API' da loja na
        janela e reinsere o que o espelho mostra agora (SKU ja' corrigido)."""
        self.cur.execute(
            "DELETE FROM pg_temp.fact_vendas_snapshot WHERE marketplace_origem = %s AND "
            "loja_origem = %s AND arquivo_origem = 'API' AND data_venda >= '2026-09-01'",
            (MKT, LOJA))
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_snapshot VALUES (%s,%s,%s,'2026-09-10',%s,1,'API')",
            (MKT, LOJA, pedido, sku_certo))

    def _n_snapshot(self, pedido):
        self.cur.execute(
            "SELECT count(*) FROM pg_temp.fact_vendas_snapshot WHERE numero_pedido = %s", (pedido,))
        return self.cur.fetchone()[0]

    def _status(self, id_):
        self.cur.execute("SELECT status FROM pg_temp.fact_vendas_pendentes WHERE id = %s", (id_,))
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
        self.cur.execute("SELECT sku_correto FROM pg_temp.dim_sku_mapeamento "
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
        self.cur.execute("INSERT INTO pg_temp.dim_sku_mapeamento (sku_errado, sku_correto) "
                         "VALUES ('SEM-SKU:MLB777', 'L-0320')")
        # mesmo pedido+sku, mas veio do upload: nao prova nada sobre a API
        self.cur.execute("INSERT INTO pg_temp.fact_vendas_snapshot VALUES "
                         "(%s,%s,'PACK1','2026-09-10','L-0320',1,'vendas.xlsx')", (MKT, LOJA))
        # pedido diferente, da API
        self.cur.execute("INSERT INTO pg_temp.fact_vendas_snapshot VALUES "
                         "(%s,%s,'OUTRO','2026-09-10','L-0320',1,'API')", (MKT, LOJA))
        # mesmo pedido, SKU diferente do mapeado
        self.cur.execute("INSERT INTO pg_temp.fact_vendas_snapshot VALUES "
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

    def _loja_na_api(self, loja, desde='2026-09-01'):
        self.cur.execute(
            "INSERT INTO pg_temp.dim_fonte_dados VALUES ('MERCADO LIVRE', %s, 'vendas', 'api', %s)",
            (loja, desde))

    def test_K2_LVI_upload_de_loja_api_espera_e_fecha_pela_api_sem_duplicar(self):
        """O caso real de 05/10/2026: pendente de UPLOAD (ML-YanniRJ, 23/09) em
        loja e periodo que a API ja' cobre."""
        self._loja_na_api('ML-YanniRJ')
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (1690, %s, 'ML-YanniRJ', '2000015183298999', "
            "'2026-09-23', 'K2-LVI-CANOA0506', 52.42, 'vendas ML YANNI RJ 22 a 23-09.xlsx', "
            "'Pendente', now())", (MKT,))
        # sem a API ter gravado nada, a conciliacao nao fecha
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)

        # a aba NAO grava no snapshot (nem com origem de upload)
        with mock.patch.object(du, 'buscar_skus_validos',
                               lambda e: {'K2-LVI-CANOA0506'}),              mock.patch.object(du, 'buscar_custos_skus', lambda e: {}):
            res = du.reprocessar_pendentes_manual(self.engine, [
                item(1690, 'K2-LVI-CANOA0506', 'K2-LVI-CANOA0506',
                     'vendas ML YANNI RJ 22 a 23-09.xlsx') | {
                    'loja_origem': 'ML-YanniRJ', 'numero_pedido': '2000015183298999',
                    'data_venda': pd.Timestamp('2026-09-23')}])
        self.assertEqual(res['aguardando'], 1)
        self.assertEqual(self._n_snapshot('2000015183298999'), 0)
        self.assertEqual(self._status(1690), 'Aguardando coleta')

        # a coleta (origem API, mesma chave) grava a venda: UMA linha, mesmo
        # depois de duas noites
        # pg_temp. explicito (R1 do auditor): se a TEMP cair num rollback no meio,
        # um DELETE sem schema iria na tabela REAL.
        self.cur.execute("DELETE FROM pg_temp.fact_vendas_snapshot WHERE loja_origem = 'ML-YanniRJ'")
        for _ in range(2):
            self.cur.execute(
                "DELETE FROM pg_temp.fact_vendas_snapshot WHERE loja_origem = 'ML-YanniRJ' "
                "AND arquivo_origem = 'API'")
            self.cur.execute(
                "INSERT INTO pg_temp.fact_vendas_snapshot VALUES (%s, 'ML-YanniRJ', "
                "'2000015183298999', '2026-09-23', 'K2-LVI-CANOA0506', 1, 'API')", (MKT,))
        self.assertEqual(self._n_snapshot('2000015183298999'), 1)
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 1)
        self.assertEqual(self._status(1690), 'Reprocessado')
        self.assertEqual(len(du.buscar_aguardando_coleta(self.engine)), 0)

    def test_pendente_de_upload_ainda_Pendente_tambem_fecha_quando_a_api_grava(self):
        self._loja_na_api('ML-YanniRJ')
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (1701, %s, 'ML-YanniRJ', '2000018640143246', "
            "'2026-09-25', 'K2-LVI-CANOA0506', 51.72, 'vendas ML YANNI RJ 24 a 27-09.xlsx', "
            "'Pendente', now())", (MKT,))
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_snapshot VALUES (%s, 'ML-YanniRJ', "
            "'2000018640143246', '2026-09-25', 'K2-LVI-CANOA0506', 1, 'API')", (MKT,))
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 1)
        self.assertEqual(self._status(1701), 'Reprocessado')

    def test_upload_de_loja_fora_da_api_ou_antes_do_api_desde_nao_fecha_pela_api(self):
        self._loja_na_api('ML-YanniRJ')
        # antes do api_desde
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (1, %s, 'ML-YanniRJ', 'A1', '2026-08-31', 'S', 1, "
            "'x.xlsx', 'Pendente', now())", (MKT,))
        # loja que nao e' API
        self.cur.execute(
            "INSERT INTO pg_temp.fact_vendas_pendentes (id, marketplace_origem, loja_origem, "
            "numero_pedido, data_venda, sku, valor_venda_efetivo, arquivo_origem, status, "
            "data_processamento) VALUES (2, 'MAGALU', 'Magalu-Nala', 'A2', '2026-09-20', 'S', 1, "
            "'y.csv', 'Pendente', now())")
        for ped, loja, dv in (('A1', 'ML-YanniRJ', '2026-08-31'),
                              ('A2', 'Magalu-Nala', '2026-09-20')):
            self.cur.execute(
                "INSERT INTO pg_temp.fact_vendas_snapshot VALUES (%s, %s, %s, %s, 'S', 1, 'API')",
                (MKT, loja, ped, dv))
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)

    def test_pendente_de_upload_nunca_e_tocada_pela_conciliacao(self):
        self._plantar_pendente(1, 'P9', 'L-0320', 'vendas.xlsx')
        self.cur.execute("INSERT INTO pg_temp.fact_vendas_snapshot VALUES "
                         "(%s,%s,'P9','2026-09-10','L-0320',1,'API')", (MKT, LOJA))
        self.assertEqual(du.conciliar_pendentes_api(self.engine), 0)
        self.assertEqual(self._status(1), 'Pendente')


if __name__ == '__main__':
    unittest.main()
