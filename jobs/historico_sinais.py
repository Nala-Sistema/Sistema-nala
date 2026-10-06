"""
Grava em sinal_historico os sinais do dia da Sinais do Dia (v1.3 "Aparece desde").

Usa a MESMA montagem da tela (sinais_dia.sinais_hoje_para_historico ->
sinais_da_loja_hoje): a regra dos sinais é escrita uma vez só. Grava os sinais
ANTES do Ciente, com upsert pela chave (data + marketplace + loja + regra +
objeto): rodar de novo no mesmo dia só atualiza.

Roda:
  - todo dia às 12h de Brasília no GitHub Actions do repo Sistema-nala
    (.github/workflows/sinais_historico.yml); não grava antes das 10h de
    Brasília (as coletas do dia podem não ter rodado);
  - backfill (o Mestre, na máquina do Thiago):
        python jobs/historico_sinais.py --desde 2026-09-06 --ate 2026-10-06
    Ressalva do backfill: usa a venda como está hoje (já sem os cancelamentos
    posteriores) e a ligação anúncio × estoque atual; foto de estoque, ads,
    config e saúde são as do dia.

Conexão: SINAIS_HIST_DB_URL (usuário sinais_historico, permissão mínima).

REPO PÚBLICO: os logs do Actions são públicos. Este script só imprime datas e
CONTAGENS ("gravou N sinais em M lojas"). Nada de SKU, produto, R$, nome de
loja, mensagem de erro do banco (pode trazer dado) nem stack: em falha imprime
só o TIPO do erro e sai com código 1.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import sinais_dia as sd  # noqa: E402

HORA_MINIMA = 10                 # antes das 10h de Brasília não grava (coletas pendentes)
MARKETPLACES = (sd.MARKETPLACE, sd.SHOPEE)


class _Engine:
    """O mínimo que sinais_dia usa de um engine: raw_connection()."""

    def __init__(self, url):
        self._url = url

    def raw_connection(self):
        import psycopg2
        return psycopg2.connect(self._url, connect_timeout=20)


def _data(texto):
    return datetime.strptime(texto, '%Y-%m-%d').date()


def dias_para_gravar(agora, desde=None, ate=None):
    """[datas]. Sem --desde: o dia de hoje (Brasília), ou [] antes das
    HORA_MINIMA. Com --desde: de desde até `ate` (padrão: ontem)."""
    hoje = agora.date()
    if desde is None:
        return [] if agora.hour < HORA_MINIMA else [hoje]
    fim = ate or (hoje - timedelta(days=1))
    if fim >= hoje:
        fim = hoje - timedelta(days=1)
    n = (fim - desde).days
    return [desde + timedelta(days=i) for i in range(n + 1)] if n >= 0 else []


def gravar_dia(engine, dia, mkt):
    """(sinais gravados, lojas lidas) de um dia num marketplace."""
    sinais, n_lojas = sd.sinais_hoje_para_historico(engine, dia, mkt)
    conn = engine.raw_connection()
    try:
        n = sd.gravar_historico(conn, sinais, dia, mkt)
    finally:
        conn.close()
    return n, n_lojas


def main(argv=None, agora=None, engine=None, saida=print):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--desde', type=_data, help='backfill: primeiro dia (AAAA-MM-DD)')
    ap.add_argument('--ate', type=_data, help='backfill: último dia (padrão: ontem)')
    args = ap.parse_args(argv)

    agora = agora or datetime.now(sd.BRT)
    dias = dias_para_gravar(agora, args.desde, args.ate)
    if not dias:
        saida(f"Nada a gravar ({agora:%d/%m %H:%M} de Brasília; o job grava a partir das "
              f"{HORA_MINIMA}h).")
        return 0
    if engine is None:
        url = os.environ.get('SINAIS_HIST_DB_URL')
        if not url:
            saida("Falta a variável SINAIS_HIST_DB_URL.")
            return 2
        engine = _Engine(url)

    total = 0
    for dia in dias:
        for mkt in MARKETPLACES:
            try:
                n, n_lojas = gravar_dia(engine, dia, mkt)
            except Exception as e:  # noqa: BLE001 — só o TIPO: repo público
                saida(f"{dia:%d/%m/%Y} {sd.MARKETPLACES[mkt]['aba']}: falhou "
                      f"({type(e).__name__}). Nada gravado deste dia/marketplace.")
                return 1
            total += n
            saida(f"{dia:%d/%m/%Y} {sd.MARKETPLACES[mkt]['aba']}: gravou {n} sinais em "
                  f"{n_lojas} lojas.")
    saida(f"Pronto: {total} sinais em {len(dias)} dia(s).")
    return 0


if __name__ == '__main__':
    sys.exit(main())
