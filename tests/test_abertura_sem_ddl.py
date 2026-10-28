"""
Testa que o app não depende de DDL para funcionar com o usuário app_nala
(sem CREATE/ALTER no schema), 28/10/2026.

  - A checagem da abertura (app.SQL_SCHEMA_EM_DIA) cobre TODA tabela, índice
    e coluna que as _garantir_* criam. Se alguém acrescentar uma peça lá e
    esquecer aqui, a abertura nunca cria a peça nova.
  - O upload do TikTok grava os nomes sem CREATE TABLE na mesma transação
    (o erro de permissão derrubava a gravação sem aviso).
  - Com banco: a checagem roda e dá verdadeiro na produção de hoje. Só roda
    com NALA_TEST_DB_URL (só leitura basta: lê o catálogo).

O app.py não é importado (faz set_page_config ao importar); a SQL e as
funções são lidas do texto.

Rodar:  python tests/test_abertura_sem_ddl.py   (ou: pytest tests/test_abertura_sem_ddl.py)
"""

import ast
import os
import re
import sys
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import processar_tiktok as pt  # noqa: E402

DB_URL = os.environ.get('NALA_TEST_DB_URL')

with open(os.path.join(RAIZ, 'app.py'), encoding='utf-8') as f:
    _FONTE = f.read()
_ARVORE = ast.parse(_FONTE)


def _constante(nome):
    for no in _ARVORE.body:
        if isinstance(no, ast.Assign) and any(
                getattr(t, 'id', None) == nome for t in no.targets):
            return ast.literal_eval(no.value)
    raise KeyError(nome)


def _texto_das_garantir():
    partes = []
    for no in _ARVORE.body:
        if isinstance(no, ast.FunctionDef) and no.name.startswith('_garantir_') \
                and no.name != '_garantir_schema':
            partes.append(ast.get_source_segment(_FONTE, no))
    return '\n'.join(partes)


class SemBanco(unittest.TestCase):
    def test_checagem_cobre_tudo_que_as_garantir_criam(self):
        sql = _constante('SQL_SCHEMA_EM_DIA')
        ddl = _texto_das_garantir()
        objetos = re.findall(r'(?:TABLE|INDEX) IF NOT EXISTS (\w+)', ddl)
        colunas = re.findall(r'ADD COLUMN IF NOT EXISTS (\w+)', ddl)
        self.assertGreaterEqual(len(objetos), 9)
        for nome in objetos:
            self.assertIn(f"'public.{nome}'", sql, nome)
        for col in colunas:
            self.assertIn(f"'{col}')", sql, col)
        # coluna criada dentro do DO $$ (dim_usuarios.nome)
        self.assertIn("('dim_usuarios', 'nome')", sql)

    def test_abertura_chama_so_a_garantir_schema(self):
        main = next(n for n in _ARVORE.body
                    if isinstance(n, ast.FunctionDef) and n.name == 'main')
        chamadas = {c.func.id for c in ast.walk(main)
                    if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)}
        self.assertIn('_garantir_schema', chamadas)
        self.assertEqual({c for c in chamadas if c.startswith('_garantir_')},
                         {'_garantir_schema'})

    def test_nomes_do_tiktok_sem_create(self):
        executados = []

        class Cursor:
            def execute(self, sql, params=None):
                executados.append(sql)

            def close(self):
                pass

        class Conn:
            def cursor(self):
                return Cursor()

            def commit(self):
                executados.append('COMMIT')

            def close(self):
                pass

        class Engine:
            def raw_connection(self):
                return Conn()

        pt._upsert_tiktok_nomes(Engine(), {'123': ('Produto', 'Variante')})
        self.assertFalse(any('CREATE' in s.upper() for s in executados))
        self.assertTrue(any('INSERT INTO dim_tiktok_skus' in s for s in executados))
        self.assertEqual(executados[-1], 'COMMIT')


@unittest.skipUnless(DB_URL, 'sem NALA_TEST_DB_URL')
class ComBanco(unittest.TestCase):
    def test_producao_de_hoje_esta_em_dia(self):
        import psycopg2
        conn = psycopg2.connect(DB_URL)
        try:
            cur = conn.cursor()
            cur.execute(_constante('SQL_SCHEMA_EM_DIA'))
            self.assertIs(cur.fetchone()[0], True)
        finally:
            conn.rollback()
            conn.close()


if __name__ == '__main__':
    unittest.main()
