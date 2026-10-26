"""
Testa as ações da análise de ads no Sinais do Dia ([HISTÓRICO E AÇÕES] etapa 2,
sql/ads_acoes.sql). Sem banco:
  - sinal 'ads_acao' só para ação MARCADA, chave 'acao:<id>', prazo vencido = urgente;
  - quem vê e quem decide: Larissa (GESTOR com as 14 lojas em dim_usuario_lojas)
    NÃO vê as ações da Shopee; ADMIN decide tudo; quem vê todas só lê;
  - Ciente: "Feito"/"proposital"/"alarme falso" encerram a ação (não volta pela
    data); "Já em andamento" volta; ciente de ação fora das lojas_acao é recusado;
  - caixinha: prazo hoje..hoje+60, só ação permitida, grava numa transação;
  - leitura do dia: sem as ações o job do histórico grava o dia mesmo assim.

Rodar:  python -m pytest tests/test_acoes_ads.py
"""

import os
import sys
import unittest
from datetime import date, datetime, timedelta
from unittest import mock

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import sinais_dia as sd  # noqa: E402

HOJE = date(2026, 10, 22)
LPT, NALA, SH_LPT, SH_NALA = 'ML-LPT', 'ML-Nala', 'Shopee-LPT', 'Shopee Lithouse(Nala)'


def _acao(id_, loja=LPT, marcada=False, prazo=None, checar=date(2026, 10, 25), sku='K-L-0351'):
    return {'id': id_, 'loja': loja, 'ciclo': 'C28', 'periodo_ini': date(2026, 10, 12),
            'periodo_fim': date(2026, 10, 18), 'ordem': id_, 'objeto': 'kit alicates',
            'codigo_anuncio': 'MLB5183384677', 'sku': sku, 'nome_produto': 'Kit alicates',
            'acao': 'Pausar', 'dono': 'Larissa', 'numero_esperado': 'gasto 0',
            'checar_em': checar, 'vai_executar': True if marcada else None,
            'prazo': prazo if marcada else None,
            'decidido_por': 'Larissa' if marcada else None,
            'decidido_em': datetime(2026, 10, 22, 9) if marcada else None}


class SinalDaAcao(unittest.TestCase):
    def test_so_marcada_vira_sinal_com_chave_e_prazo(self):
        s = sd.sinais_acoes([_acao(1), _acao(2, marcada=True, prazo=date(2026, 10, 29))], HOJE)
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]['regra'], 'ads_acao')
        self.assertEqual(s[0]['objeto'], 'acao:2')
        self.assertEqual(s[0]['tipo'], 'AÇÃO ADS')
        self.assertFalse(s[0]['urgente'])
        self.assertIn('29/10', s[0]['sugestao'])
        self.assertEqual(s[0]['skus'], ('K-L-0351',))

    def test_prazo_vencido_fica_urgente_e_vermelho(self):
        s = sd.sinais_acoes([_acao(3, marcada=True, prazo=date(2026, 10, 21))], HOJE)
        self.assertTrue(s[0]['urgente'])
        self.assertEqual(s[0]['cor'], 'vermelho')
        self.assertIn('vencido', s[0]['sugestao'])

    def test_aceita_tupla_da_sql(self):
        a = _acao(4, marcada=True, prazo=date(2026, 10, 30))
        s = sd.sinais_acoes([tuple(a[c] for c in sd.ACAO_COLS)], HOJE)
        self.assertEqual(s[0]['objeto'], 'acao:4')

    def test_regra_nova_nas_listas(self):
        self.assertIn('ads_acao', sd.REGRAS)
        self.assertIn('feito', sd.MOTIVOS)
        self.assertNotIn('ads_acao', sd.REGRAS_DE_VENDA)   # carga pendente não suspende


class QuemVeQuemDecide(unittest.TestCase):
    RESP = {LPT: 'Larissa', NALA: 'Larissa', SH_LPT: 'Patricia', SH_NALA: 'Patricia'}

    def test_larissa_nao_ve_as_acoes_da_shopee(self):
        ve, decide = sd.lojas_das_acoes([SH_LPT, SH_NALA], self.RESP, 'Larissa', 'GESTOR', False)
        self.assertEqual((ve, decide), (set(), set()))
        ve, decide = sd.lojas_das_acoes([LPT, NALA], self.RESP, 'Larissa', 'GESTOR', False)
        self.assertEqual(decide, {LPT, NALA})

    def test_patricia_decide_shopee_e_so_le_o_ml(self):
        ve, decide = sd.lojas_das_acoes([LPT, SH_LPT], self.RESP, 'Patricia', 'CONTROLADORIA', True)
        self.assertEqual(decide, {SH_LPT})
        self.assertEqual(ve, {LPT, SH_LPT})

    def test_admin_decide_tudo_e_sem_responsavel_ninguem_decide(self):
        _ve, decide = sd.lojas_das_acoes([LPT, SH_LPT], self.RESP, 'admin', 'ADMIN', True)
        self.assertEqual(decide, {LPT, SH_LPT})
        _ve, decide = sd.lojas_das_acoes(['ML-YanniRJ'], self.RESP, 'Larissa', 'GESTOR', False)
        self.assertEqual(decide, set())
        _ve, decide = sd.lojas_das_acoes([LPT], self.RESP, '', 'GESTOR', False)
        self.assertEqual(decide, set())


def _ciente(motivo, ate):
    return (10, LPT, 'ads_acao', 'acao:2', motivo, None, ate, None, 'x', 'Larissa',
            datetime(2026, 10, 15, 9))


class CienteDaAcao(unittest.TestCase):
    def setUp(self):
        self.s = sd.sinais_acoes([_acao(2, marcada=True, prazo=date(2026, 10, 29))], HOJE)

    def test_feito_nao_volta_quando_o_silencio_vence(self):
        for motivo in sd.MOTIVOS_ENCERRAM_ACAO:
            ativos, sil = sd.aplicar_cientes(self.s, [_ciente(motivo, date(2026, 10, 16))], HOJE)
            self.assertEqual((len(ativos), len(sil)), (0, 1), motivo)

    def test_em_andamento_volta_na_data(self):
        ativos, sil = sd.aplicar_cientes(self.s, [_ciente('em_andamento', date(2026, 10, 16))], HOJE)
        self.assertEqual((len(ativos), len(sil)), (1, 0))
        ativos, sil = sd.aplicar_cientes(self.s, [_ciente('em_andamento', date(2026, 10, 30))], HOJE)
        self.assertEqual((len(ativos), len(sil)), (0, 1))

    def test_ciente_de_acao_so_nas_lojas_que_decide(self):
        conn = mock.MagicMock()
        item = [(self.s[0], 'feito', '', HOJE + timedelta(days=7))]
        with self.assertRaises(PermissionError):
            sd.gravar_cientes(conn, item, 'Patricia', HOJE, [LPT], sd.MARKETPLACE, lojas_acao=set())
        conn.cursor.assert_not_called()
        self.assertEqual(sd.gravar_cientes(conn, item, 'Larissa', HOJE, [LPT], sd.MARKETPLACE,
                                           lojas_acao={LPT}), 1)
        conn.commit.assert_called_once()

    def test_ciente_de_outros_sinais_nao_muda(self):
        outro = sd._sinal(LPT, 'ADS', 'MLB1', ('A',), 'x', 'y', 10, regra='ads_custo')
        conn = mock.MagicMock()
        self.assertEqual(sd.gravar_cientes(conn, [(outro, 'proposital', '', HOJE)], 'Larissa',
                                           HOJE, [LPT]), 1)


class FeitoSoNaAcao(unittest.TestCase):
    """Auditor R2: o motivo "Feito" vale só para o sinal 'ads_acao'."""

    def test_validar(self):
        ate = HOJE + timedelta(days=7)
        self.assertEqual(sd.validar_ciente('feito', '', ate, HOJE, 'ads_acao'), [])
        self.assertTrue(sd.validar_ciente('feito', '', ate, HOJE, 'ads_custo'))
        self.assertTrue(sd.validar_ciente('feito', '', ate, HOJE))
        self.assertEqual(sd.validar_ciente('proposital', '', ate, HOJE, 'ads_custo'), [])

    def test_lista_do_formulario(self):
        self.assertIn('feito', sd.motivos_para(['ads_acao', 'ads_acao']))
        self.assertNotIn('feito', sd.motivos_para(['ads_acao', 'vendas_queda']))
        self.assertNotIn('feito', sd.motivos_para([]))
        self.assertEqual(len(sd.motivos_para(['vendas_queda'])), len(sd.MOTIVOS) - 1)

    def test_gravar_recusa_feito_em_outro_sinal(self):
        outro = sd._sinal(LPT, 'ADS', 'MLB1', ('A',), 'x', 'y', 10, regra='ads_custo')
        conn = mock.MagicMock()
        with self.assertRaises(ValueError):
            sd.gravar_cientes(conn, [(outro, 'feito', '', HOJE)], 'Larissa', HOJE, [LPT],
                              lojas_acao={LPT})
        conn.cursor.assert_not_called()

    def test_excel_lista_por_linha_e_previa(self):
        import openpyxl
        acao = sd.sinais_acoes([_acao(7, marcada=True, prazo=date(2026, 10, 30))], HOJE)[0]
        outro = sd._sinal(LPT, 'ADS', 'MLB1', ('A',), 'x', 'y', 10, regra='ads_custo')
        wb = openpyxl.load_workbook(__import__('io').BytesIO(
            sd.excel_sinais([acao, outro], {}, HOJE)))
        ws = wb['Sinais']
        col = sd.COLUNAS_EXCEL.index('Motivo')
        letra = openpyxl.utils.get_column_letter(col + 1)
        listas = {}
        for dv in ws.data_validations.dataValidation:
            for rng in str(dv.sqref).split():
                listas[rng] = dv.formula1
        self.assertIn('Feito', listas[f'{letra}2'])          # linha da ação
        self.assertNotIn('Feito', listas[f'{letra}3'])       # linha do outro sinal
        linhas = [{'_linha': 3, 'Loja': LPT, 'Regra': 'ads_custo', 'Objeto': outro['objeto'],
                   'Status': 'Ciente', 'Motivo': 'Feito', 'Nota': '', 'Silenciar até': None}]
        itens, previa = sd.previa_excel(linhas, [acao, outro], [LPT], HOJE)
        self.assertEqual(itens, [])
        self.assertIn('❌', previa['Resultado'].iloc[0])


class Caixinha(unittest.TestCase):
    def test_prazo_padrao(self):
        self.assertEqual(sd.prazo_padrao(_acao(1, checar=date(2026, 10, 25)), HOJE), date(2026, 10, 25))
        self.assertEqual(sd.prazo_padrao(_acao(1, checar=date(2026, 10, 19)), HOJE),
                         HOJE + timedelta(days=sd.PRAZO_ACAO_PADRAO))

    def test_valida_antes_de_tocar_no_banco(self):
        conn = mock.MagicMock()
        for itens, usuario in (([(9, HOJE)], 'Larissa'),                      # ação não permitida
                               ([(1, HOJE - timedelta(days=1))], 'Larissa'),  # prazo no passado
                               ([(1, HOJE + timedelta(days=61))], 'Larissa'), # prazo longe demais
                               ([(1, HOJE)], '')):                            # sem usuário
            with self.assertRaises(ValueError):
                sd.gravar_decisoes(conn, itens, usuario, HOJE, {1, 2})
        conn.cursor.assert_not_called()

    def test_grava_numa_transacao_e_conta_as_novas(self):
        conn = mock.MagicMock()
        cur = conn.cursor.return_value
        type(cur).rowcount = mock.PropertyMock(side_effect=[1, 0])   # a 2ª já estava decidida
        n = sd.gravar_decisoes(conn, [(1, HOJE), (2, HOJE + timedelta(days=5))], 'Larissa',
                               HOJE, {1, 2})
        self.assertEqual(n, 1)
        self.assertEqual(cur.execute.call_count, 2)
        conn.commit.assert_called_once()

    def test_erro_do_banco_desfaz(self):
        conn = mock.MagicMock()
        conn.cursor.return_value.execute.side_effect = RuntimeError('gatilho recusou')
        with self.assertRaises(RuntimeError):
            sd.gravar_decisoes(conn, [(1, HOJE)], 'Patricia', HOJE, {1})
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


class LeituraDoDia(unittest.TestCase):
    def test_montar_sinais_inclui_as_acoes(self):
        dados = {'acoes': [_acao(5, marcada=True, prazo=date(2026, 10, 30))]}
        sinais, erros = sd.montar_sinais(dados, HOJE)
        self.assertEqual([s['objeto'] for s in sinais if s['regra'] == 'ads_acao'], ['acao:5'])
        self.assertEqual(erros, {})

    def test_job_grava_o_dia_sem_as_acoes(self):
        pacote = {'erros': {'acoes': 'sem permissão'}, 'erros_bloco': {}, 'sinais': [],
                  'dados': {'frescor': []}, 'upload_ate': {}}
        with mock.patch.object(sd, '_ler_lojas_ml', return_value=[LPT]), \
             mock.patch.object(sd, '_ler_pacote', return_value=pacote):
            sinais, n, sem_projecao = sd.sinais_hoje_para_historico(mock.MagicMock(), HOJE,
                                                                   sd.MARKETPLACE)
        self.assertEqual((sinais, n, sem_projecao), ([], 1, False))

    def test_job_grava_o_dia_quando_a_montagem_das_acoes_quebra(self):
        # auditor R4: sinais_acoes quebrar na MONTAGEM (erros_bloco) não para o job
        pacote = {'erros': {}, 'erros_bloco': {'acoes': "KeyError('prazo')"}, 'sinais': [],
                  'dados': {'frescor': []}, 'upload_ate': {}}
        with mock.patch.object(sd, '_ler_lojas_ml', return_value=[LPT]), \
             mock.patch.object(sd, '_ler_pacote', return_value=pacote):
            self.assertEqual(sd.sinais_hoje_para_historico(mock.MagicMock(), HOJE,
                                                           sd.MARKETPLACE), ([], 1, False))
        # o bloco de qualquer OUTRA regra continua barrando o dia
        pacote['erros_bloco'] = {'vendas': 'x'}
        with mock.patch.object(sd, '_ler_lojas_ml', return_value=[LPT]), \
             mock.patch.object(sd, '_ler_pacote', return_value=pacote):
            with self.assertRaises(sd._LeituraIncompleta):
                sd.sinais_hoje_para_historico(mock.MagicMock(), HOJE, sd.MARKETPLACE)

    def test_montagem_real_com_acao_quebrada_cai_so_no_bloco_acoes(self):
        quebrada = _acao(6, marcada=True, prazo=date(2026, 10, 30))
        del quebrada['prazo']
        sinais, erros = sd.montar_sinais({'acoes': [quebrada]}, HOJE)
        self.assertEqual(list(erros), ['acoes'])
        self.assertEqual(sinais, [])

    def test_job_sem_a_projecao_continua_avisando(self):
        pacote = {'erros': {'performance': 'x'}, 'erros_bloco': {}, 'sinais': [],
                  'dados': {'frescor': []}, 'upload_ate': {}}
        with mock.patch.object(sd, '_ler_lojas_ml', return_value=[LPT]), \
             mock.patch.object(sd, '_ler_pacote', return_value=pacote):
            _s, _n, sem_projecao = sd.sinais_hoje_para_historico(mock.MagicMock(), HOJE,
                                                                sd.MARKETPLACE)
        self.assertTrue(sem_projecao)

    def test_plano_traz_as_anteriores_abertas(self):
        # auditor R6: marcadas de planos anteriores sem resposta seguem na tela
        a = _acao(1, marcada=True, prazo=date(2026, 10, 27))
        linha = tuple(a[c] for c in sd.ACAO_COLS)
        with mock.patch.object(sd, '_ler', side_effect=[[(True,)], [], [linha], [(LPT, 'Larissa')]]) as m:
            acoes, resp, abertas = sd.ler_plano(mock.MagicMock(), HOJE, [LPT])
        self.assertEqual((acoes, resp), ([], {LPT: 'Larissa'}))
        self.assertEqual([x['id'] for x in abertas], [1])
        sql_abertas, p = m.call_args_list[2].args[1:]
        self.assertIs(sql_abertas, sd.SQL_ACOES_ABERTAS_ANTERIORES)
        self.assertEqual(set(p['encerram']), set(sd.MOTIVOS_ENCERRAM_ACAO))
        # vencida (27/10 < hoje no C30) vira sinal urgente
        s = sd.sinais_acoes(abertas, date(2026, 11, 2))
        self.assertTrue(s[0]['urgente'])
        self.assertEqual(s[0]['objeto'], 'acao:1')

    def test_anteriores_abertas_so_saem_com_motivo_que_encerra(self):
        self.assertIn("s.motivo = ANY(%(encerram)s)", sd.SQL_ACOES_ABERTAS_ANTERIORES)
        self.assertIn("s.encerrado_em IS NULL", sd.SQL_ACOES_ABERTAS_ANTERIORES)
        self.assertIn("d.vai_executar", sd.SQL_ACOES_ABERTAS_ANTERIORES)

    def test_plano_sem_a_tabela_e_none(self):
        with mock.patch.object(sd, '_ler', return_value=[(False,)]):
            self.assertIsNone(sd.ler_plano(mock.MagicMock(), HOJE, [LPT]))

    def test_tabela_plano_mostra_quem_marcou(self):
        df = sd.tabela_plano([_acao(1), _acao(2, marcada=True, prazo=date(2026, 10, 29))], {})
        self.assertEqual(df['Situação'].iloc[0], '')
        self.assertIn('Larissa', df['Situação'].iloc[1])
        self.assertIn('29/10', df['Situação'].iloc[1])


class DdlConfereComOCodigo(unittest.TestCase):
    """As listas de regra/motivo do sql/ads_acoes.sql são as mesmas do código."""

    def test_listas_do_ddl(self):
        import re
        texto = open(os.path.join(RAIZ, 'sql', 'ads_acoes.sql'), encoding='utf-8').read()
        for nome in ('sinal_historico_regra_valida', 'sinal_ciente_regra_valida'):
            m = re.search(nome + r" CHECK \(regra IN \((.*?)\)\)", texto, re.S)
            self.assertEqual(set(re.findall(r"'(\w+)'", m.group(1))), set(sd.REGRAS), nome)
        m = re.search(r"sinal_ciente_motivo_valido CHECK \(motivo IN \((.*?)\)\)", texto, re.S)
        self.assertEqual(set(re.findall(r"'(\w+)'", m.group(1))), set(sd.MOTIVOS))
        self.assertIn(f"hoje + {sd.PRAZO_ACAO_MAX}", texto)


if __name__ == '__main__':
    unittest.main()
