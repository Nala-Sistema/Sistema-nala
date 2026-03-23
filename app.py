import streamlit as st
import os
import importlib
import pandas as pd
from datetime import datetime, date, timedelta

# CONFIGURAÇÃO DE PÁGINA
st.set_page_config(page_title="Sistema Nala - Gestão", layout="wide", page_icon="🏪")

# ESTILO VISUAL NALA (Premium)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Montserrat', sans-serif; }
    [data-testid="stSidebar"] { background-color: #002b5e; border-right: 2px solid #d4af37; }
    [data-testid="stSidebar"] * { color: white !important; }
    .stMetric { background-color: white; padding: 15px; border-radius: 10px; border-top: 4px solid #d4af37; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    .stButton>button { border-radius: 8px; background-color: #002b5e; color: #d4af37 !important; font-weight: bold; border: 1px solid #d4af37; width: 100%; }
    </style>
    """, unsafe_allow_html=True)

def carregar_modulo(nome_modulo):
    """Função para carregar e recarregar os módulos .py dinamicamente"""
    try:
        modulo = importlib.import_module(nome_modulo)
        importlib.reload(modulo)
        modulo.main()
    except Exception as e:
        st.error(f"⚠️ Erro crítico no módulo '{nome_modulo}':")
        st.exception(e)


def _buscar_metricas_inicio(engine):
    """Busca métricas reais do mês atual e anterior para o painel."""
    hoje = date.today()
    primeiro_mes = hoje.replace(day=1)
    if hoje.month == 1:
        primeiro_ant = date(hoje.year - 1, 12, 1)
    else:
        primeiro_ant = date(hoje.year, hoje.month - 1, 1)
    ultimo_ant = primeiro_mes - timedelta(days=1)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Mês atual
        cursor.execute("""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(COUNT(*), 0),
                   COALESCE(AVG(margem_percentual), 0)
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s
        """, (primeiro_mes,))
        fat_atual, ped_atual, margem_atual = cursor.fetchone()

        # Mês anterior
        cursor.execute("""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(COUNT(*), 0),
                   COALESCE(AVG(margem_percentual), 0)
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s AND data_venda <= %s
        """, (primeiro_ant, ultimo_ant))
        fat_ant, ped_ant, margem_ant = cursor.fetchone()

        # SKUs ativos
        cursor.execute("SELECT COUNT(*) FROM dim_produtos WHERE status = 'Ativo'")
        total_skus = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        # Calcular variações
        var_fat = ((float(fat_atual) / float(fat_ant) - 1) * 100) if fat_ant > 0 else 0
        var_ped = ((int(ped_atual) / int(ped_ant) - 1) * 100) if ped_ant > 0 else 0
        var_margem = float(margem_atual) - float(margem_ant)

        def fmt_brl(v):
            v = float(v)
            if v >= 1000:
                return f"R$ {v/1000:,.1f}k".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        return {
            'faturamento': fmt_brl(fat_atual),
            'var_fat': f"{var_fat:+.1f}%",
            'pedidos': f"{int(ped_atual):,}".replace(",", "."),
            'var_ped': f"{var_ped:+.1f}%",
            'margem': f"{float(margem_atual):.1f}%",
            'var_margem': f"{var_margem:+.1f}%",
            'skus': str(total_skus),
        }
    except Exception:
        return {
            'faturamento': "—", 'var_fat': None,
            'pedidos': "—", 'var_ped': None,
            'margem': "—", 'var_margem': None,
            'skus': "—",
        }


def main():
    if 'logado' not in st.session_state:
        st.session_state.logado = False
    if 'perfil' not in st.session_state:
        st.session_state.perfil = None
    # v3.5: Inicializar ambiente
    if 'ambiente_nala' not in st.session_state:
        st.session_state.ambiente_nala = "Produção"

    # --- TELA DE LOGIN ---
    if not st.session_state.logado:
        col1, col2, col3 = st.columns([1, 1.2, 1])
        with col2:
            st.title("Hub Nala - Login")
            with st.form("login"):
                u = st.text_input("Usuário")
                p = st.text_input("Senha", type="password")
                if st.form_submit_button("Acessar"):
                    if u == "admin" and p == "admin123":
                        st.session_state.logado = True
                        st.session_state.perfil = "Admin"
                        st.rerun()
                    elif u == "controladoria" and p == "nala2025":
                        st.session_state.logado = True
                        st.session_state.perfil = "Controladoria"
                        st.rerun()
                    else:
                        st.error("Acesso negado. Usuário ou senha incorretos.")

    # --- ÁREA LOGADA ---
    else:
        with st.sidebar:
            if os.path.exists("logo.png"):
                st.image("logo.png", use_container_width=True)
            else:
                st.markdown("<h1 style='color: #d4af37; text-align: center;'>NALA</h1>", unsafe_allow_html=True)

            st.markdown("---")

            opcoes_menu = [
                "🏠 Início",
                "📊 Performance",
                "📦 SKUs",
                "💰 Vendas",
                "🏷️ Tags",
            ]

            if st.session_state.perfil in ["Admin", "Controladoria"]:
                opcoes_menu.append("🛒 Compras")

            opcoes_menu.append("⚙️ Config")

            aba = st.radio("Menu Principal:", opcoes_menu)

            st.markdown("---")

            # ─── v3.5: TOGGLE DE AMBIENTE (só Admin) ───
            if st.session_state.perfil == "Admin":
                ambiente = st.selectbox(
                    "🔧 Ambiente:",
                    ["Produção", "Dev"],
                    index=0 if st.session_state.ambiente_nala == "Produção" else 1,
                    key="sel_ambiente_nala",
                )
                if ambiente != st.session_state.ambiente_nala:
                    st.session_state.ambiente_nala = ambiente
                    st.rerun()
                if ambiente == "Dev":
                    st.warning("⚠️ AMBIENTE DE TESTE")
                st.markdown("---")

            if st.button("🚪 Sair"):
                st.session_state.logado = False
                st.session_state.perfil = None
                st.rerun()

        # --- ROTEAMENTO DE PÁGINAS ---
        if aba == "🏠 Início":
            st.title("📊 Painel de Controle")

            # Métricas reais do banco
            from database_utils import get_engine
            engine = get_engine()
            m = _buscar_metricas_inicio(engine)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Faturamento Mês", m['faturamento'], m['var_fat'])
            c2.metric("Pedidos Totais", m['pedidos'], m['var_ped'])
            c3.metric("Margem Média", m['margem'], m['var_margem'])
            c4.metric("SKUs na Base", m['skus'])

            st.divider()
            # v3.5: Mostrar ambiente atual
            env_label = f"🟢 Produção" if st.session_state.ambiente_nala == "Produção" else "🟡 Dev (Teste)"
            st.info(f"💡 Bem-vindo, {st.session_state.perfil}. Ambiente: {env_label}. Use o menu lateral para navegar.")

        elif aba == "📊 Performance":
            carregar_modulo("performance")

        elif aba == "📦 SKUs":
            carregar_modulo("gestao_skus")

        elif aba == "💰 Vendas":
            carregar_modulo("central_uploads")

        elif aba == "🏷️ Tags":
            carregar_modulo("gestao_tags")

        elif aba == "🛒 Compras":
            carregar_modulo("app_compras")

        elif aba == "⚙️ Config":
            carregar_modulo("configuracoes")

if __name__ == "__main__":
    main()
