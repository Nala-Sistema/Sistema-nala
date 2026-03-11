import streamlit as st
import os
import importlib

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

def main():
    # Inicialização do estado de sessão
    if 'logado' not in st.session_state:
        st.session_state.logado = False
    if 'perfil' not in st.session_state:
        st.session_state.perfil = None
    
    # --- TELA DE LOGIN ---
    if not st.session_state.logado:
        col1, col2, col3 = st.columns([1, 1.2, 1])
        with col2:
            st.title("Hub Nala - Login")
            with st.form("login"):
                u = st.text_input("Usuário")
                p = st.text_input("Senha", type="password")
                if st.form_submit_button("Acessar"):
                    # Definição de Perfis e Acessos
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
            # Logo ou Título
            if os.path.exists("logo.png"): 
                st.image("logo.png", use_container_width=True)
            else: 
                st.markdown("<h1 style='color: #d4af37; text-align: center;'>NALA</h1>", unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Construção Dinâmica do Menu baseada no Perfil
            opcoes_menu = ["🏠 Início", "📦 SKUs", "💰 Vendas", "🏷️ Tags"]
            
            # Somente Admin ou Controladoria enxergam o módulo de Compras
            if st.session_state.perfil in ["Admin", "Controladoria"]:
                opcoes_menu.append("🛒 Compras")
            
            opcoes_menu.append("⚙️ Config")
            
            # Seletor de Abas
            aba = st.radio("Menu Principal:", opcoes_menu)
            
            st.markdown("---")
            # Botão de Logout
            if st.button("🚪 Sair"):
                st.session_state.logado = False
                st.session_state.perfil = None
                st.rerun()

        # --- ROTEAMENTO DE PÁGINAS ---
        if aba == "🏠 Início":
            st.title("📊 Painel de Controle")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Faturamento Mês", "R$ 52.400", "+12%")
            c2.metric("Pedidos Totais", "1.245", "+5%")
            c3.metric("Margem Média", "18.5%", "-2%")
            c4.metric("SKUs na Base", "778")
            st.divider()
            st.info(f"💡 Bem-vindo, {st.session_state.perfil}. Use o menu lateral para navegar.")
            
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
