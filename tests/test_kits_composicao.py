"""
Testa a carga da composição de kits (kits_composicao.py, frente [KITS], 30/09/2026).

Duas partes:
  - Sem banco: limpeza do SKU que vem do Excel, leitura do export do UpSeller,
    recusas, o plano (novo / alterado / igual / pendente / ausente) e % solto
    nas SQL.
  - Com banco: EXECUTA as SQL de verdade (prévia, gravação, reprocessar), com
    os parâmetros ligados pelo psycopg2, o mesmo driver da tela.

A parte com banco só roda com a variável NALA_TEST_DB_URL (usuário de
permissão mínima, nunca o dono). Ela cria tabelas TEMPORÁRIAS com os mesmos
nomes das reais — as de kit com o DDL LIDO de sql/kits_composicao.sql, então
PK e CHECKs são idênticos aos de produção —, confere que os nomes resolvem para
pg_temp ANTES de inserir, e termina SEMPRE em ROLLBACK. Nenhuma tabela real é
lida nem escrita.

Rodar:  python tests/test_kits_composicao.py   (ou: pytest tests/test_kits_composicao.py)
"""

import os
import re
import sys
import unittest

import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import kits_composicao as kc  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')


def _export(linhas):
    """DataFrame no formato do export do UpSeller (cabeçalho com acento)."""
    return pd.DataFrame(linhas, columns=['KIT SKU', 'Título', 'SKU de Produto',
                                         'Qtd. SKU de Produto'], dtype=object)


class LimpezaDoSku(unittest.TestCase):
    def test_tab_e_espaco_inquebravel_do_excel_saem(self):
        self.assertEqual(kc.normalizar_sku('\tK2-L-0320 '), 'K2-L-0320')
        self.assertEqual(kc.normalizar_sku(' L-0320 \t'), 'L-0320')

    def test_espaco_inquebravel_no_meio_vira_espaco(self):
        # 'LBR-050808 CX' existe no cadastro com espaço comum no meio.
        self.assertEqual(kc.normalizar_sku('LBR-050808 CX'), 'LBR-050808 CX')

    def test_nao_muda_maiuscula_minuscula(self):
        self.assertEqual(kc.normalizar_sku('k3-L-0426'), 'k3-L-0426')

    def test_vazio_nan_e_numero(self):
        self.assertIsNone(kc.normalizar_sku(None))
        self.assertIsNone(kc.normalizar_sku(float('nan')))
        self.assertIsNone(kc.normalizar_sku('   '))
        self.assertEqual(kc.normalizar_sku(12345.0), '12345')


class LeituraDoExport(unittest.TestCase):
    def test_cabecalho_do_upseller_com_e_sem_acento(self):
        linhas, erro = kc.ler_export_upseller(_export([['K2-L-0320', 't', 'L-0320', 2]]))
        self.assertIsNone(erro)
        self.assertEqual((linhas[0]['kit'], linhas[0]['peca'], linhas[0]['qtd']),
                         ('K2-L-0320', 'L-0320', 2))

    def test_arquivo_errado_avisa_as_colunas(self):
        _, erro = kc.ler_export_upseller(pd.DataFrame({'sku': ['x']}))
        self.assertIn('kit sku', erro)

    def test_quantidade(self):
        self.assertEqual(kc._quantidade('3'), 3)
        self.assertEqual(kc._quantidade(2.0), 2)
        self.assertIsNone(kc._quantidade(2.5))
        self.assertIsNone(kc._quantidade(0))
        self.assertIsNone(kc._quantidade('dois'))

    def test_recusa_derruba_o_kit_inteiro(self):
        linhas, _ = kc.ler_export_upseller(_export([
            ['K-MIX', 't', 'L-1', 1],
            ['K-MIX', 't', 'L-2', 0],            # quantidade inválida
            ['K-DUP', 't', 'L-1', 1],
            ['K-DUP', 't', 'L-1', 2],            # mesma peça, qtd diferente
            ['K-EU', 't', 'K-EU', 1],            # kit é peça dele mesmo
            ['K-OK', 't', 'L-1', 2],
            ['K-OK', 't', 'L-1', 2],             # repetida igual: ignora
        ]))
        kits, recusados = kc.montar_kits(linhas)
        self.assertEqual(kits, {'K-OK': {'L-1': 2}})
        self.assertEqual(set(recusados), {'K-MIX', 'K-DUP', 'K-EU'})


class Plano(unittest.TestCase):
    CAD = {'K2-L-0320', 'L-0320', 'K-L-0351', 'L-0351', 'L-0352', 'k3-L-0426',
           'L-0426', 'K-VELHO', 'L-9'}

    def test_novo_alterado_igual_e_ausente_nao_apagado(self):
        atual = {'K2-L-0320': {'L-0320': 2},
                 'K-L-0351': {'L-0351': 1},
                 'K-VELHO': {'L-9': 3}}
        kits = {'K2-L-0320': {'L-0320': 2},                       # igual
                'K-L-0351': {'L-0351': 1, 'L-0352': 1},           # alterado
                'k3-L-0426': {'L-0426': 3}}                       # novo
        p = kc.planejar(kits, self.CAD, atual, {})
        self.assertEqual(p['iguais'], ['K2-L-0320'])
        self.assertEqual([k for k, *_ in p['alterados']], ['K-L-0351'])
        self.assertEqual([k for k, _ in p['novos']], ['k3-L-0426'])
        self.assertEqual(p['ausentes'], ['K-VELHO'])              # só listado

    def test_cadastro_comparado_exato_sem_upper(self):
        p = kc.planejar({'K3-L-0426': {'L-0426': 3}}, self.CAD, {}, {})
        self.assertEqual(p['novos'], [])
        kit, _, kit_ok, sem, _ = p['pendentes'][0]
        self.assertEqual((kit, kit_ok, sem), ('K3-L-0426', False, ()))

    def test_peca_sem_cadastro_segura_o_kit_inteiro(self):
        atual = {'K-L-0351': {'L-0351': 1}}
        p = kc.planejar({'K-L-0351': {'L-0351': 1, 'L-NOVA': 1}}, self.CAD, atual, {})
        self.assertEqual(p['alterados'], [])
        kit, comp, kit_ok, sem, em_vigor = p['pendentes'][0]
        self.assertEqual((kit, kit_ok, sem, em_vigor), ('K-L-0351', True, ('L-NOVA',), True))
        self.assertEqual(dict(comp), {'L-0351': 1, 'L-NOVA': 1})

    def test_kit_dentro_de_kit_recusado(self):
        kits = {'K2-L-0320': {'L-0320': 2}, 'K-GRANDE': {'K2-L-0320': 1, 'L-9': 1}}
        p = kc.planejar(kits, self.CAD | {'K-GRANDE'}, {}, {})
        self.assertEqual([k for k, _ in p['recusados']], ['K-GRANDE'])
        self.assertEqual([k for k, _ in p['novos']], ['K2-L-0320'])

    def test_kit_novo_que_ja_e_peca_de_kit_em_vigor_recusado(self):
        atual = {'K-VELHO': {'L-9': 3}}
        p = kc.planejar({'L-9': {'L-0320': 2}}, self.CAD, atual, {})
        self.assertEqual([k for k, _ in p['recusados']], ['L-9'])

    def test_kit_que_esta_so_na_pendente_nao_entra_como_peca(self):
        # K-ESPERA não tem cadastro (está na pendente), mas é kit.
        p = kc.planejar({'K-GRANDE': {'K-ESPERA': 1, 'L-9': 1}},
                        self.CAD | {'K-GRANDE', 'K-ESPERA'}, {},
                        {'K-ESPERA': {'L-0320': 2}})
        self.assertEqual([k for k, _ in p['recusados']], ['K-GRANDE'])
        self.assertEqual(p['novos'], [])

    def test_pendencia_que_resolveu_sai(self):
        p = kc.planejar({'K2-L-0320': {'L-0320': 2}}, self.CAD, {},
                        {'K2-L-0320': {'L-0320': 2}})
        self.assertEqual(p['pendencias_resolvidas'], ['K2-L-0320'])

    def test_plano_repetido_e_igual(self):
        args = ({'K-L-0351': {'L-0352': 1, 'L-0351': 1}}, self.CAD,
                {'K-L-0351': {'L-0351': 1}}, {})
        self.assertEqual(kc.planejar(*args), kc.planejar(*args))


class CorrecaoDeSku(unittest.TestCase):
    """dim_sku_mapeamento aplicada na carga (aprovado pelo Mestre em 01/10/2026)."""
    REAL = {'K-10-LKE-3104-4030': 'K10-LKE-3104-4030'}     # registrada em julho
    CAD = {'K10-LKE-3104-4030', 'LKE-3104-4030', 'L-0320', 'L-0321', 'K2-L-0320'}

    def test_caso_real_entra_com_o_sku_do_sistema(self):
        p = kc.planejar({'K-10-LKE-3104-4030': {'LKE-3104-4030': 10}}, self.CAD, {}, {},
                        mapa=self.REAL)
        self.assertEqual(p['novos'], [('K10-LKE-3104-4030', (('LKE-3104-4030', 10),))])
        self.assertEqual(p['traducoes'],
                         [('K-10-LKE-3104-4030', 'K10-LKE-3104-4030', '')])
        self.assertEqual(p['pendentes'], [])

    def test_sem_correcao_registrada_continua_pendente(self):
        p = kc.planejar({'K-10-LKE-3104-4030': {'LKE-3104-4030': 10}}, self.CAD, {}, {})
        self.assertEqual([k for k, *_ in p['pendentes']], ['K-10-LKE-3104-4030'])
        self.assertEqual(p['traducoes'], [])

    def test_pendencia_guardada_com_o_sku_antigo_sai_quando_entra(self):
        p = kc.planejar({'K-10-LKE-3104-4030': {'LKE-3104-4030': 10}}, self.CAD, {},
                        {'K-10-LKE-3104-4030': {'LKE-3104-4030': 10}}, mapa=self.REAL)
        self.assertEqual(p['pendencias_resolvidas'], ['K-10-LKE-3104-4030'])

    def test_correcao_na_peca(self):
        p = kc.planejar({'K2-L-0320': {'L-320': 2}}, self.CAD, {}, {},
                        mapa={'L-320': 'L-0320'})
        self.assertEqual(p['novos'], [('K2-L-0320', (('L-0320', 2),))])

    def test_cadeia_aplica_um_salto_e_avisa(self):
        p = kc.planejar({'K-A': {'L-0320': 1}}, self.CAD | {'K-X'}, {}, {},
                        mapa={'K-A': 'K-X', 'K-X': 'K-B'})
        self.assertEqual([k for k, _ in p['novos']], ['K-X'])
        self.assertIn('um salto só', p['traducoes'][0][2])

    def test_destino_fora_do_cadastro_avisa_e_fica_pendente(self):
        p = kc.planejar({'K-A': {'L-0320': 1}}, self.CAD, {}, {}, mapa={'K-A': 'K-NOVO'})
        self.assertEqual([k for k, *_ in p['pendentes']], ['K-NOVO'])
        self.assertIn('não está no cadastro', p['traducoes'][0][2])

    def test_dois_kits_que_viram_o_mesmo_sao_recusados(self):
        p = kc.planejar({'K-10-LKE-3104-4030': {'LKE-3104-4030': 10},
                         'K10-LKE-3104-4030': {'LKE-3104-4030': 10}},
                        self.CAD, {}, {}, mapa=self.REAL)
        self.assertEqual(p['novos'], [])
        self.assertEqual(len(p['recusados']), 2)

    def test_duas_pecas_do_mesmo_kit_que_viram_o_mesmo_sku_recusam_o_kit(self):
        # L-320 corrige para L-0320, que já é a outra peça do kit: somar ou
        # escolher uma das quantidades seria adivinhar. O kit inteiro sai.
        p = kc.planejar({'K2-L-0320': {'L-320': 1, 'L-0320': 2}}, self.CAD, {}, {},
                        mapa={'L-320': 'L-0320'})
        self.assertEqual(p['novos'], [])
        self.assertEqual(p['pendentes'], [])
        self.assertEqual([k for k, _ in p['recusados']], ['K2-L-0320'])
        self.assertIn('duas peças viram o mesmo SKU L-0320', p['recusados'][0][1][0])

    def test_pendente_renomeado_sai_do_nome_antigo(self):
        # K-A corrigido para K-B, que segue sem cadastro: sai como K-A, volta como K-B.
        p = kc.planejar({'K-A': {'L-0320': 1}}, self.CAD, {}, {'K-A': {'L-0320': 1}},
                        mapa={'K-A': 'K-B'})
        self.assertEqual([k for k, *_ in p['pendentes']], ['K-B'])
        self.assertEqual(p['pendencias_renomeadas'], ['K-A'])


class Sql(unittest.TestCase):
    def test_nenhum_percent_solto_fora_dos_parametros(self):
        for sql in kc.TODAS_AS_SQL:
            sem_params = re.sub(r'%\(\w+\)s', '', sql)
            self.assertNotIn('%', sem_params)

    def test_sql_nao_fixa_schema(self):
        # Sem "public.": o teste com banco depende das TEMP com o mesmo nome.
        for sql in kc.TODAS_AS_SQL:
            self.assertNotIn('public.', sql)


class Tela(unittest.TestCase):
    def test_erro_de_banco_vira_aviso_e_nao_esconde_as_outras_secoes(self):
        from unittest import mock
        import streamlit as st

        class _EngineQuebrado:
            def raw_connection(self):
                raise RuntimeError('banco fora')

            def connect(self):
                raise RuntimeError('banco fora')

        erros = []
        with mock.patch.object(st, 'error', side_effect=lambda m, *a, **k: erros.append(m)), \
                mock.patch.object(kc.pd, 'read_sql', side_effect=RuntimeError('banco fora')):
            kc.render_aba_kits(_EngineQuebrado(), is_admin=True)   # não pode levantar
        # Pendências e composição falham, cada uma com o seu aviso.
        self.assertEqual(len(erros), 2)
        self.assertIn('pendências', erros[0])
        self.assertIn('composição em vigor', erros[1])


# ============================================================
# COM BANCO
# ============================================================

def _ddl_real_em_temp():
    """CREATE TABLE/INDEX de sql/kits_composicao.sql, apontados para pg_temp."""
    with open(os.path.join(RAIZ, 'sql', 'kits_composicao.sql'), encoding='utf-8') as f:
        texto = f.read()
    tabelas = re.findall(r'^CREATE TABLE public\.\w+ \(.*?^\);', texto, re.S | re.M)
    indices = re.findall(r'^CREATE INDEX \w+ ON public\.\w+ \(\w+\);', texto, re.M)
    assert len(tabelas) == 2 and len(indices) == 1, 'DDL de sql/kits_composicao.sql mudou'
    return [s.replace('public.', 'pg_temp.') for s in tabelas + indices]


class _Conexao:
    """Entrega a conexão do teste às funções, com commit e rollback anulados:
    o que elas gravam some no ROLLBACK do tearDown."""
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
        self.cur.execute('CREATE TEMP TABLE dim_produtos (sku text)')
        for ddl in _ddl_real_em_temp():
            self.cur.execute(ddl)
        # Mesmas colunas e PK de produção (conferido em 01/10/2026).
        self.cur.execute("""
            CREATE TEMP TABLE dim_sku_mapeamento (
                sku_errado varchar PRIMARY KEY, sku_correto varchar NOT NULL,
                data_criacao timestamp DEFAULT now())""")
        self.cur.execute("""
            CREATE TEMP TABLE fact_vendas_snapshot (
                sku varchar, data_venda date, quantidade integer,
                valor_venda_efetivo numeric)""")
        self.cur.execute("""
            CREATE TEMP TABLE fact_vendas_pendentes (
                sku varchar, data_venda date, quantidade integer,
                valor_venda_efetivo numeric, status varchar)""")
        # Trava de segurança: os nomes TÊM de resolver para pg_temp antes de
        # qualquer INSERT.
        self.cur.execute("""
            SELECT bool_and(c.relnamespace = pg_my_temp_schema())
            FROM unnest(ARRAY['dim_produtos', 'dim_kit_composicao',
                              'dim_kit_composicao_pendente', 'dim_sku_mapeamento',
                              'fact_vendas_snapshot', 'fact_vendas_pendentes']) AS t(nome)
            JOIN pg_class c ON c.oid = t.nome::regclass
        """)
        self.assertTrue(self.cur.fetchone()[0], 'tabela não resolveu para pg_temp')
        self.cur.executemany('INSERT INTO dim_produtos VALUES (%s)', [
            ('K2-L-0320',), ('L-0320',), ('K-L-0351',), ('L-0351',), ('L-0352',),
            ('k3-L-0426',), ('L-0426',), ('K10-LKE-3104-4030',), ('LKE-3104-4030',)])
        self.c = _Conexao(self.conn)

    def tearDown(self):
        self.cur.close()
        self.conn.rollback()

    def _carregar(self, linhas, arquivo='Export_Kit_teste.xlsx'):
        ls, erro = kc.ler_export_upseller(_export(linhas))
        self.assertIsNone(erro)
        kits, recusados = kc.montar_kits(ls)
        plano = kc.previa(self.c, kits, recusados)
        return kc.gravar(self.c, kits, recusados, arquivo, 'teste', plano)

    def _composicao(self):
        self.cur.execute('SELECT kit_sku, peca_sku, quantidade FROM dim_kit_composicao')
        return kc._agrupar(self.cur.fetchall())

    def _pendentes(self):
        self.cur.execute('SELECT kit_sku, peca_sku, kit_cadastrado, peca_cadastrada '
                         'FROM dim_kit_composicao_pendente ORDER BY 1, 2')
        return self.cur.fetchall()

    def test_carga_incremental_de_ponta_a_ponta(self):
        r = self._carregar([['K2-L-0320', 't', 'L-0320', 2],
                            ['K-L-0351', 't', 'L-0351', 1],
                            ['K-L-0351', 't', 'L-0352', 1],
                            ['K3-L-0426', 't', 'L-0426', 3]])   # K maiúsculo: sem cadastro
        self.assertEqual((r['novos'], r['pendentes']), (2, 1))
        self.assertEqual(self._composicao(), {'K2-L-0320': {'L-0320': 2},
                                              'K-L-0351': {'L-0351': 1, 'L-0352': 1}})
        self.assertEqual(self._pendentes(), [('K3-L-0426', 'L-0426', False, True)])

        # 2ª carga: K-L-0351 perde L-0352, K2-L-0320 some do arquivo (fica).
        r = self._carregar([['K-L-0351', 't', 'L-0351', 1]])
        self.assertEqual(r['alterados'], 1)
        self.assertEqual(self._composicao(), {'K2-L-0320': {'L-0320': 2},
                                              'K-L-0351': {'L-0351': 1}})
        self.assertEqual(self._pendentes(), [('K3-L-0426', 'L-0426', False, True)])

    def test_linha_que_nao_mudou_guarda_arquivo_e_data(self):
        self._carregar([['K-L-0351', 't', 'L-0351', 1], ['K-L-0351', 't', 'L-0352', 1]])
        self.cur.execute("UPDATE dim_kit_composicao SET arquivo_origem = 'primeiro'")
        self._carregar([['K-L-0351', 't', 'L-0351', 1], ['K-L-0351', 't', 'L-0352', 2]])
        self.cur.execute('SELECT peca_sku, arquivo_origem FROM dim_kit_composicao ORDER BY 1')
        self.assertEqual(self.cur.fetchall(), [('L-0351', 'primeiro'),
                                               ('L-0352', 'Export_Kit_teste.xlsx')])

    def test_reprocessar_depois_do_cadastro(self):
        self._carregar([['K-NOVO', 't', 'L-0320', 4]])
        self.assertEqual(self._pendentes(), [('K-NOVO', 'L-0320', False, True)])
        self.cur.execute("INSERT INTO dim_produtos VALUES ('K-NOVO')")
        r = kc.reprocessar_pendentes(self.c, 'teste')
        self.assertEqual(r['novos'], 1)
        self.assertEqual(self._pendentes(), [])
        self.assertEqual(self._composicao()['K-NOVO'], {'L-0320': 4})

    def test_previa_desatualizada_nao_grava(self):
        ls, _ = kc.ler_export_upseller(_export([['K-NOVO', 't', 'L-0320', 4]]))
        kits, rec = kc.montar_kits(ls)
        plano = kc.previa(self.c, kits, rec)          # K-NOVO sem cadastro: pendente
        self.cur.execute("INSERT INTO dim_produtos VALUES ('K-NOVO')")
        with self.assertRaises(kc.PreviaDesatualizada):
            kc.gravar(self.c, kits, rec, 'x.xlsx', 'teste', plano)

    def test_check_do_ddl_real_barra_espaco_nas_pontas(self):
        import psycopg2
        self.cur.execute('SAVEPOINT s')
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.cur.execute("INSERT INTO dim_kit_composicao (kit_sku, peca_sku, quantidade, "
                             "arquivo_origem) VALUES ('K-1 ', 'L-1', 1, 'x')")
        self.cur.execute('ROLLBACK TO SAVEPOINT s')

    # --- correção de SKU (01/10/2026) -------------------------------------

    def _mapear(self, errado, correto):
        self.cur.execute('INSERT INTO dim_sku_mapeamento (sku_errado, sku_correto) '
                         'VALUES (%s, %s)', (errado, correto))

    def test_caso_real_entra_na_carga_pela_correcao(self):
        self._mapear('K-10-LKE-3104-4030', 'K10-LKE-3104-4030')
        r = self._carregar([['K-10-LKE-3104-4030', 't', 'LKE-3104-4030', 10]])
        self.assertEqual(r['novos'], 1)
        self.assertEqual(self._composicao(), {'K10-LKE-3104-4030': {'LKE-3104-4030': 10}})

    def test_caso_real_entra_no_reprocessar(self):
        # Como está em produção hoje: gravado pendente antes da correção valer.
        self._carregar([['K-10-LKE-3104-4030', 't', 'LKE-3104-4030', 10]])
        self.assertEqual(self._pendentes(),
                         [('K-10-LKE-3104-4030', 'LKE-3104-4030', False, True)])
        self._mapear('K-10-LKE-3104-4030', 'K10-LKE-3104-4030')
        r = kc.reprocessar_pendentes(self.c, 'teste')
        self.assertEqual(r['novos'], 1)
        self.assertEqual(r['traducoes'][0][:2], ('K-10-LKE-3104-4030', 'K10-LKE-3104-4030'))
        self.assertEqual(self._pendentes(), [])
        self.assertEqual(self._composicao(), {'K10-LKE-3104-4030': {'LKE-3104-4030': 10}})

    def test_corrigir_sku_grava_e_reprocessar_usa(self):
        self._carregar([['K-NOVO', 't', 'L-0320', 2]])
        ok, msg = kc.corrigir_sku(self.c, 'K-NOVO', 'K2-L-0320')
        self.assertTrue(ok, msg)
        self.assertEqual(kc.reprocessar_pendentes(self.c, 'teste')['novos'], 1)
        self.assertEqual(self._composicao(), {'K2-L-0320': {'L-0320': 2}})

    def test_corrigir_sku_nao_troca_correcao_existente(self):
        self._mapear('K-X', 'K2-L-0320')
        ok, msg = kc.corrigir_sku(self.c, 'K-X', 'K-L-0351')
        self.assertFalse(ok)
        self.assertIn('K2-L-0320', msg)
        self.cur.execute("SELECT sku_correto FROM dim_sku_mapeamento WHERE sku_errado = 'K-X'")
        self.assertEqual(self.cur.fetchone()[0], 'K2-L-0320')

    def test_corrigir_sku_exige_destino_cadastrado(self):
        ok, _ = kc.corrigir_sku(self.c, 'K-X', 'K-NAO-EXISTE')
        self.assertFalse(ok)
        self.cur.execute('SELECT count(*) FROM dim_sku_mapeamento')
        self.assertEqual(self.cur.fetchone()[0], 0)

    def test_corrigir_sku_recusa_sku_cadastrado_como_errado(self):
        # Mapear um SKU cadastrado desviaria as vendas dele.
        ok, msg = kc.corrigir_sku(self.c, 'L-0320', 'L-0351')
        self.assertFalse(ok)
        self.assertIn('desviaria', msg)

    def test_vendas_90d_soma_snapshot_e_so_pendente_em_aberto(self):
        self.cur.executemany(
            'INSERT INTO fact_vendas_snapshot VALUES (%s, CURRENT_DATE - %s, %s, %s)',
            [('K10-LKE-3104-4030', 5, 2, 400), ('K10-LKE-3104-4030', 120, 1, 200)])
        self.cur.executemany(
            'INSERT INTO fact_vendas_pendentes VALUES (%s, CURRENT_DATE - 3, 1, %s, %s)',
            [('K-NOVO', 50, 'Pendente'), ('K-NOVO', 70, 'Reprocessado')])
        v = kc.vendas_90d(self.c, ['K10-LKE-3104-4030', 'K-NOVO'])
        self.assertEqual(v, {'K10-LKE-3104-4030': (400.0, 2), 'K-NOVO': (50.0, 1)})


if __name__ == '__main__':
    unittest.main(verbosity=2)
