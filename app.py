import streamlit as st
import os
import importlib
import pandas as pd
from datetime import datetime, date, timedelta

# ============================================================
# CONFIGURAÇÃO DE PÁGINA
# ============================================================
st.set_page_config(page_title="Sistema Nala - Gestão", layout="wide", page_icon="🏪")

# ESTILO VISUAL NALA (Premium)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Montserrat', sans-serif; }
    [data-testid="stSidebar"] { background-color: #002b5e; border-right: 2px solid #d4af37; }
    [data-testid="stSidebar"] * { color: white !important; }
    .stMetric { background-color: white; padding: 15px; border-radius: 10px;
                border-top: 4px solid #d4af37; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    .stButton>button { border-radius: 8px; background-color: #002b5e;
                       color: #d4af37 !important; font-weight: bold;
                       border: 1px solid #d4af37; width: 100%; }
    </style>
""", unsafe_allow_html=True)


# ============================================================
# CONEXÃO COM BANCO — agnóstico ao ambiente
# Lê exclusivamente do st.secrets["DB_URL"] configurado no Streamlit Cloud.
# Cada app (Produção / Dev) tem seu próprio Secret apontando pro banco correto.
# ============================================================
from database_utils import get_engine

def _is_dev_environment():
    """Detecta se estamos no ambiente Dev baseado na URL do banco."""
    try:
        db_url = st.secrets.get("DB_URL", "")
        return "ep-icy-shadow" in db_url
    except Exception:
        return False


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def carregar_modulo(nome_modulo):
    """Carrega e recarrega módulos .py dinamicamente."""
    try:
        modulo = importlib.import_module(nome_modulo)
        importlib.reload(modulo)
        modulo.main()
    except Exception as e:
        st.error(f"⚠️ Erro crítico no módulo '{nome_modulo}':")
        st.exception(e)


# ============================================================
# AUTENTICAÇÃO
# ============================================================

def _autenticar_usuario(username: str, senha: str, engine) -> dict | None:
    """
    Autentica usuário contra dim_usuarios usando bcrypt.
    Retorna dict com dados do usuário ou None se falhar.
    """
    import bcrypt

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id_usuario, username, password_hash, role,
                   COALESCE(nome, username) as nome
            FROM dim_usuarios
            WHERE username = %s AND ativo = TRUE
        """, (username.strip(),))

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return None

        id_usuario, db_username, password_hash, role, nome = row

        # Verificar senha com bcrypt
        if not bcrypt.checkpw(senha.encode('utf-8'), password_hash.encode('utf-8')):
            cursor.close()
            conn.close()
            return None

        # Buscar lojas permitidas (para GESTOR)
        lojas_permitidas = []
        cursor.execute("""
            SELECT dl.loja
            FROM dim_usuario_lojas dul
            JOIN dim_lojas dl ON dul.id_loja = dl.id
            WHERE dul.id_usuario = %s
        """, (id_usuario,))
        lojas_permitidas = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return {
            'id_usuario': id_usuario,
            'username': db_username,
            'nome': nome,
            'role': role,
            'lojas_permitidas': lojas_permitidas,
        }

    except Exception as e:
        st.error(f"Erro na autenticação: {e}")
        return None


def _contar_usuarios(engine) -> int:
    """Conta quantos usuários ativos existem no banco."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_usuarios WHERE ativo = TRUE")
        total = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return total
    except Exception:
        return -1  # erro = não sabemos


def _criar_primeiro_admin(username: str, senha: str, engine) -> bool:
    """Cria o primeiro usuário ADMIN quando o sistema está vazio."""
    import bcrypt

    try:
        password_hash = bcrypt.hashpw(
            senha.encode('utf-8'), bcrypt.gensalt()
        ).decode('utf-8')

        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dim_usuarios (username, password_hash, role, nome)
            VALUES (%s, %s, 'ADMIN', %s)
        """, (username.strip(), password_hash, username.strip()))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao criar usuário: {e}")
        return False


def _tela_setup_inicial(engine):
    """
    Tela exibida quando não há nenhum usuário no sistema.
    Permite criar o primeiro ADMIN.
    """
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        st.markdown("<h1 style='text-align:center; color:#d4af37;'>NALA</h1>",
                    unsafe_allow_html=True)
        st.title("Configuração Inicial")
        st.info("Nenhum usuário encontrado. Crie o primeiro administrador para começar.")

        with st.form("form_setup"):
            username = st.text_input("Username do Admin")
            senha = st.text_input("Senha (mín. 6 caracteres)", type="password")
            senha_conf = st.text_input("Confirmar Senha", type="password")

            if st.form_submit_button("🚀 Criar Admin e Começar", type="primary"):
                if not username or not username.strip():
                    st.error("Username não pode ser vazio.")
                elif len(senha) < 6:
                    st.error("Senha deve ter pelo menos 6 caracteres.")
                elif senha != senha_conf:
                    st.error("As senhas não coincidem.")
                else:
                    if _criar_primeiro_admin(username, senha, engine):
                        st.success(f"Admin '{username}' criado! Faça login abaixo.")
                        st.rerun()


def _tela_login(engine):
    """Tela de login do sistema."""
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        if os.path.exists("logo.png"):
            st.image("logo.png", width=200)
        else:
            st.markdown("<h1 style='text-align:center; color:#d4af37;'>NALA</h1>",
                        unsafe_allow_html=True)

        st.title("Hub Nala - Login")

        with st.form("login"):
            username = st.text_input("Usuário")
            senha = st.text_input("Senha", type="password")

            if st.form_submit_button("Acessar", type="primary"):
                if not username or not senha:
                    st.error("Preencha usuário e senha.")
                else:
                    usuario = _autenticar_usuario(username, senha, engine)

                    if usuario:
                        st.session_state.logado = True
                        st.session_state.usuario = usuario
                        st.rerun()
                    else:
                        st.error("Acesso negado. Usuário ou senha incorretos.")



# ============================================================
# PAINEL DE INÍCIO
# ============================================================

def _buscar_metricas_inicio(engine):
    """Busca métricas reais do mês atual e anterior para o painel."""
    from permissoes import ve_todas_lojas, get_lojas_usuario, filtrar_query_por_loja

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

        # Montar filtro de loja
        where_loja = ""
        params_loja = []
        if not ve_todas_lojas():
            lojas = get_lojas_usuario(engine)
            if lojas:
                placeholders = ', '.join(['%s'] * len(lojas))
                where_loja = f" AND loja_origem IN ({placeholders})"
                params_loja = list(lojas)

        # Mês atual
        cursor.execute(f"""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(COUNT(*), 0),
                   COALESCE(AVG(margem_percentual), 0)
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s {where_loja}
        """, [primeiro_mes] + params_loja)
        fat_atual, ped_atual, margem_atual = cursor.fetchone()

        # Mês anterior
        cursor.execute(f"""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(COUNT(*), 0),
                   COALESCE(AVG(margem_percentual), 0)
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s AND data_venda <= %s {where_loja}
        """, [primeiro_ant, ultimo_ant] + params_loja)
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


# ============================================================
# ÁREA LOGADA — ROTEAMENTO
# ============================================================

def _area_logada(engine):
    """Área principal após login — sidebar + roteamento de módulos."""
    from permissoes import (
        get_opcoes_menu, get_modulo_do_menu,
        mostrar_badge_leitura, mostrar_badge_filtro_loja,
        eh_somente_leitura, pode_acessar,
    )

    usuario = st.session_state.get('usuario', {})
    role = usuario.get('role', '')
    nome = usuario.get('nome', usuario.get('username', ''))

    # ---- SIDEBAR ----
    with st.sidebar:
        if os.path.exists("logo.png"):
            st.image("logo.png", width=200)
        else:
            st.markdown("<h1 style='color: #d4af37; text-align: center;'>NALA</h1>",
                        unsafe_allow_html=True)

        # Badge de ambiente Dev (detectado automaticamente pelo Secret)
        if _is_dev_environment():
            st.warning("⚠️ AMBIENTE DE DESENVOLVIMENTO (TESTES)")

        st.markdown("---")

        # Menu dinâmico baseado no perfil
        opcoes_menu = get_opcoes_menu()
        aba = st.radio("Menu Principal:", opcoes_menu)

        st.markdown("---")
        st.caption(f"👤 {nome} ({role})")

        if st.button("🚪 Sair"):
            st.session_state.logado = False
            st.session_state.usuario = {}
            st.rerun()

    # ---- ROTEAMENTO ----
    modulo = get_modulo_do_menu(aba)

    if modulo == 'inicio':
        st.title("📊 Painel de Controle")

        mostrar_badge_filtro_loja()

        m = _buscar_metricas_inicio(engine)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Faturamento Mês", m['faturamento'], m['var_fat'])
        c2.metric("Pedidos Totais", m['pedidos'], m['var_ped'])
        c3.metric("Margem Média", m['margem'], m['var_margem'])
        c4.metric("SKUs na Base", m['skus'])

        st.divider()
        st.info(f"Bem-vindo, {nome}. Use o menu lateral para navegar.")

    elif modulo == 'performance':
        mostrar_badge_leitura('performance')
        mostrar_badge_filtro_loja()
        carregar_modulo("performance")

    elif modulo == 'skus':
        mostrar_badge_leitura('skus')
        carregar_modulo("gestao_skus")

    elif modulo == 'vendas':
        mostrar_badge_leitura('vendas')
        mostrar_badge_filtro_loja()
        carregar_modulo("central_uploads")

    elif modulo == 'tags':
        mostrar_badge_leitura('tags')
        mostrar_badge_filtro_loja()
        carregar_modulo("gestao_tags")

    elif modulo == 'compras':
        mostrar_badge_leitura('compras')
        carregar_modulo("app_compras")

    elif modulo == 'calculadora':
        carregar_modulo("calculadora")

    elif modulo == 'ia':
        mostrar_badge_filtro_loja()
        carregar_modulo("nala_ia")

    elif modulo == 'kanban':
        carregar_modulo("kanban_board")

    elif modulo == 'tabela_preco':
        from tabela_preco import tabela_preco_page
        tabela_preco_page()

    elif modulo == 'ads':
        mostrar_badge_filtro_loja()
        from analise_ads import modulo_ads
        modulo_ads(engine)

    elif modulo == 'config':
        mostrar_badge_leitura('config')
        carregar_modulo("configuracoes")


# ============================================================
# MAIN
# ============================================================

def main():
    # Inicializar session_state
    if 'logado' not in st.session_state:
        st.session_state.logado = False
    if 'usuario' not in st.session_state:
        st.session_state.usuario = {}

    # Engine do banco — lê do st.secrets["DB_URL"] (agnóstico ao ambiente)
    engine = get_engine()

    # Garantir que tabela dim_usuario_lojas existe
    _garantir_tabela_usuario_lojas(engine)

    # Fluxo principal
    if not st.session_state.logado:
        # Verificar se existem usuários no sistema
        total = _contar_usuarios(engine)

        if total == 0:
            _tela_setup_inicial(engine)
        else:
            _tela_login(engine)
    else:
        _area_logada(engine)


def _garantir_tabela_usuario_lojas(engine):
    """Cria tabela dim_usuario_lojas se não existir (auto-migration)."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_usuario_lojas (
                id SERIAL PRIMARY KEY,
                id_usuario INTEGER NOT NULL REFERENCES dim_usuarios(id_usuario) ON DELETE CASCADE,
                id_loja INTEGER NOT NULL REFERENCES dim_lojas(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(id_usuario, id_loja)
            )
        """)

        # Garantir coluna 'nome' em dim_usuarios (pode não existir)
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'dim_usuarios' AND column_name = 'nome'
                ) THEN
                    ALTER TABLE dim_usuarios ADD COLUMN nome VARCHAR(100);
                END IF;
            END $$;
        """)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass  # Silencioso — se falhar, o login ainda funciona


if __name__ == "__main__":
    main()
