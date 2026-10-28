"""
Testa sql/usuario_app.sql contra o código do app (28/10/2026).

  - A lista que concede (tabela temporária app_nala_previsto) e a lista da
    consulta de prova no fim do arquivo são IGUAIS.
  - Toda escrita que o código do app faz (INSERT INTO, UPDATE ... SET,
    DELETE FROM) está coberta pelo privilégio previsto. Se alguém escrever
    numa tabela nova sem pôr o GRANT no SQL, este teste quebra — no banco,
    a tela falharia calada.
  - As três que o app NUNCA escreve (sinal_historico, ads_acao, dim_estoque)
    estão só com SELECT.

Sem banco. Rodar:  python tests/test_usuario_app.py
"""

import os
import re
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(RAIZ, 'sql', 'usuario_app.sql'), encoding='utf-8') as f:
    _SQL = f.read()

_PAR = re.compile(r"\('(\w+)',\s*'([^']+)'\)")

# Escrita que está no código mas não roda no app: o job diário grava o
# histórico com usuário próprio (jobs/historico_sinais.py -> sinais_dia).
SO_FORA_DO_APP = {('INSERT', 'sinal_historico')}


def _lista(trecho):
    return dict(_PAR.findall(trecho))


def _lista_concede():
    ini = _SQL.index('INSERT INTO app_nala_previsto')
    return _lista(_SQL[ini:_SQL.index(';', ini)])


def _lista_prova():
    ini = _SQL.index('WITH previsto (tabela, privs) AS (VALUES')
    return _lista(_SQL[ini:_SQL.index('objs AS MATERIALIZED', ini)])


def _privs(texto):
    """'SELECT, INSERT, UPDATE(a, b)' -> {'SELECT', 'INSERT', 'UPDATE'}"""
    return {p.split('(')[0].strip() for p in re.sub(r'\([^)]*\)', '', texto).split(',')}


def _escritas_do_codigo():
    arquivos = subprocess.run(['git', 'ls-files', '*.py'], cwd=RAIZ, capture_output=True,
                              text=True, check=True).stdout.split()
    achadas = set()
    for arq in arquivos:
        if arq.startswith(('tests/', 'jobs/')):
            continue
        with open(os.path.join(RAIZ, arq), encoding='utf-8') as f:
            txt = f.read()
        for priv, padrao in (('INSERT', r'INSERT\s+INTO\s+(\w+)'),
                             ('UPDATE', r'\bUPDATE\s+(\w+)\s+SET\b'),
                             ('DELETE', r'DELETE\s+FROM\s+(\w+)')):
            for tabela in re.findall(padrao, txt):
                achadas.add((priv, tabela, arq))
    return achadas


class UsuarioApp(unittest.TestCase):
    def test_lista_que_concede_igual_a_da_prova(self):
        concede, prova = _lista_concede(), _lista_prova()
        self.assertGreater(len(concede), 60)
        self.assertEqual(concede, prova)

    def test_toda_escrita_do_codigo_tem_grant(self):
        previsto = _lista_concede()
        faltando = []
        for priv, tabela, arq in sorted(_escritas_do_codigo()):
            if (priv, tabela) in SO_FORA_DO_APP:
                continue
            if priv not in _privs(previsto.get(tabela, '')):
                faltando.append(f'{priv} {tabela} ({arq})')
        self.assertEqual(faltando, [])

    def test_tres_proibidas_so_leitura(self):
        previsto = _lista_concede()
        for tabela in ('sinal_historico', 'ads_acao', 'dim_estoque'):
            self.assertEqual(previsto[tabela], 'SELECT', tabela)

    def test_ordem_canonica_dos_privilegios(self):
        # a conferência compara texto: tem que seguir SELECT, INSERT, UPDATE, DELETE
        ordem = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
        for tabela, texto in _lista_concede().items():
            lista = [p.split('(')[0].strip() for p in re.sub(r'\([^)]*\)', '', texto).split(',')]
            self.assertEqual(lista, [p for p in ordem if p in lista], tabela)


if __name__ == '__main__':
    unittest.main()
