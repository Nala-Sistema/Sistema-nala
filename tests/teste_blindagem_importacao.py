"""Testa a blindagem contra o cenario real de 13/08 e casos normais."""
import os, sys, types
import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Stubs para importar gestao_skus sem Streamlit/DB
st_stub = types.ModuleType("streamlit")

class _Col:
    """Stub de coluna do Streamlit — aceita qualquer chamada."""
    def __getattr__(self, _):
        return lambda *a, **k: None

for nome in ("header","subheader","markdown","info","success","error","warning",
             "dataframe","metric","progress","empty","text","balloons","button",
             "text_input","file_uploader","download_button","write","rerun"):
    setattr(st_stub, nome, lambda *a, **k: None)
st_stub.columns = lambda spec, **k: [_Col() for _ in
                                     (range(spec) if isinstance(spec, int) else spec)]
st_stub.tabs = lambda labels, **k: [_Col() for _ in labels]
st_stub.session_state = {}
sys.modules["streamlit"] = st_stub

db_stub = types.ModuleType("database_utils")
db_stub.get_engine = lambda *a, **k: None
sys.modules["database_utils"] = db_stub

sys.path.insert(0, RAIZ)
import gestao_skus as g

falhas = []
def check(nome, cond):
    print(("  OK   " if cond else "  FALHA") + f"  {nome}")
    if not cond:
        falhas.append(nome)

# ---------------------------------------------------------------
print("\n1. CENARIO REAL 13/08 — planilha derruba custo de todos os SKUs")
skus = [f"SKU-{i:03d}" for i in range(100)]
df = pd.DataFrame({
    "sku": skus,
    "nome": [f"Produto {i}" for i in range(100)],
    "preco_a_ser_considerado": [12.0] * 100,   # o valor errado que entrou
})
registros, erros = g._preparar_registros(df)
estado = {s: {"nome": f"Produto {i}", "preco_a_ser_considerado": 30.0,
              "preco_compra": None, "embalagem": None, "mdo": None,
              "custo_ads": None, "outros_custos": None}
          for i, s in enumerate(skus)}
diff = g._montar_diff(registros, estado)
risco = g._avaliar_risco(diff)
check("planilha lida sem erro de parse", not erros)
check("circuit breaker BLOQUEIA", risco is not None)
check("conta as 100 quedas", risco and risco["n_quedas"] == 100)

# ---------------------------------------------------------------
print("\n2. Ajuste legitimo — 8 de 100 SKUs com custo renegociado pra baixo")
df2 = df.copy()
df2["preco_a_ser_considerado"] = [12.0]*8 + [30.0]*92
registros2, _ = g._preparar_registros(df2)
diff2 = g._montar_diff(registros2, estado)
check("circuit breaker LIBERA (8% < 20%)", g._avaliar_risco(diff2) is None)

print("\n2b. Limite: 25 de 100 SKUs com queda forte")
df2b = df.copy()
df2b["preco_a_ser_considerado"] = [12.0]*25 + [30.0]*75
registros2b, _ = g._preparar_registros(df2b)
check("circuit breaker BLOQUEIA (25% > 20%)",
      g._avaliar_risco(g._montar_diff(registros2b, estado)) is not None)

# ---------------------------------------------------------------
print("\n3. Coluna de custo AUSENTE — nao pode tocar em custo")
df3 = pd.DataFrame({"sku": skus[:5], "nome": ["A","B","C","D","E"]})
registros3, _ = g._preparar_registros(df3)
check("preco vira None (preserva banco)",
      all(r["preco_a_ser_considerado"] is None for r in registros3))
diff3 = g._montar_diff(registros3, estado)
check("diff marca PRESERVA CUSTO",
      all(d["situacao"] == "PRESERVA CUSTO" for d in diff3))
check("sem bloqueio", g._avaliar_risco(diff3) is None)

# ---------------------------------------------------------------
print("\n4. Celulas VAZIAS na coluna de custo — o caso que zerava a base")
df4 = pd.DataFrame({
    "sku": skus[:5], "nome": ["A","B","C","D","E"],
    "preco_a_ser_considerado": [None, "", float("nan"), "  ", None],
})
registros4, _ = g._preparar_registros(df4)
check("todas viram None, nenhuma vira 0.0",
      all(r["preco_a_ser_considerado"] is None for r in registros4))

# ---------------------------------------------------------------
print("\n5. Zero EXPLICITO continua sendo zero")
df5 = pd.DataFrame({"sku": ["X-1"], "nome": ["Z"], "custo_ads": ["0,00"]})
registros5, _ = g._preparar_registros(df5)
check("'0,00' -> 0.0 (nao None)", registros5[0]["custo_ads"] == 0.0)

# ---------------------------------------------------------------
print("\n6. Formato decimal US nao inflaciona mais 100x")
df6 = pd.DataFrame({"sku": ["X-1"], "nome": ["Z"],
                    "preco_a_ser_considerado": ["134.71"]})
registros6, _ = g._preparar_registros(df6)
check("'134.71' -> 134.71 (antes: 13471.0)",
      abs(registros6[0]["preco_a_ser_considerado"] - 134.71) < 0.001)

# ---------------------------------------------------------------
print("\n7. Linhas invalidas sao rejeitadas com o numero da linha")
df7 = pd.DataFrame({
    "sku": ["OK-1", "", "OK-2"],
    "nome": ["Bom", "Sem sku", ""],
    "preco_a_ser_considerado": [10.0, 10.0, 10.0],
})
registros7, erros7 = g._preparar_registros(df7)
check("2 erros detectados", len(erros7) == 2)
check("1 registro valido", len(registros7) == 1)
check("aponta linha 3 do Excel", erros7[0]["linha"] == 3)

# ---------------------------------------------------------------
print("\n8. Valor negativo e sinalizado")
df8 = pd.DataFrame({"sku": ["X-1"], "nome": ["Z"],
                    "preco_a_ser_considerado": [-5.0]})
_, erros8 = g._preparar_registros(df8)
check("custo negativo vira erro", len(erros8) == 1)

# ---------------------------------------------------------------
print("\n9. SKU novo nao dispara o breaker")
df9 = pd.DataFrame({"sku": ["NOVO-1"], "nome": ["Novo"],
                    "preco_a_ser_considerado": [5.0]})
registros9, _ = g._preparar_registros(df9)
diff9 = g._montar_diff(registros9, {})
check("marcado como NOVO", diff9[0]["situacao"] == "NOVO")
check("sem bloqueio", g._avaliar_risco(diff9) is None)

# ---------------------------------------------------------------
print("\n10. NaN no banco nao desarma o circuit breaker")
# Metade da base com NaN em preco_a_ser_considerado (existe de verdade no banco)
estado_nan = {}
for i, s in enumerate(skus):
    estado_nan[s] = {"nome": f"P{i}",
                     "preco_a_ser_considerado": float("nan") if i % 2 else 30.0,
                     "preco_compra": None, "embalagem": None, "mdo": None,
                     "custo_ads": None, "outros_custos": None}
diff10 = g._montar_diff(registros, estado_nan)
risco10 = g._avaliar_risco(diff10)
check("NaN nao vira variacao falsa",
      all(d["variacao_pct"] is None or d["variacao_pct"] == d["variacao_pct"]
          for d in diff10))
check("breaker ainda bloqueia com os 50 comparaveis", risco10 is not None)

# ---------------------------------------------------------------
print("\n11. _render_diff nao usa matplotlib (nao esta no requirements)")
fonte = open(os.path.join(RAIZ, "gestao_skus.py"), encoding="utf-8").read()
import re
# Ignora comentarios: so interessa uso real, nao mencao em texto
codigo = "\n".join(l.split("#")[0] for l in fonte.splitlines())
# .style.format() e pandas puro e ja roda em producao na Tab 1 — pode ficar.
# So os stylers com colormap e que puxam matplotlib.
stylers_matplotlib = ["background_gradient", "text_gradient", ".bar(", "hide_columns"]
for styler in stylers_matplotlib:
    check(f"sem {styler}", styler not in codigo)
check("sem import matplotlib", not re.search(r"import\s+matplotlib", codigo))

print("\n12. _render_diff roda sem estourar")
try:
    g._render_diff(diff, ["preco_a_ser_considerado"])
    g._render_diff([], [])
    check("render ok com diff cheio e vazio", True)
except Exception as e:
    check(f"render ok ({e})", False)

print("\n" + "="*55)
print("FALHAS:", falhas if falhas else "nenhuma")
sys.exit(1 if falhas else 0)
