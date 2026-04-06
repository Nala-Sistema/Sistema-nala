"""
MÓDULO: Permissões e RBAC — Sistema Nala
=========================================
Centraliza TODA a lógica de controle de acesso.
Importar nos demais módulos:
    from permissoes import pode_acessar, pode_ver_custos, get_lojas_usuario, filtrar_df_por_loja

Perfis:
    ADMIN         — Acesso total, gerencia usuários
    CONTROLADORIA — Igual ao ADMIN, exceto gestão de usuários
    DIRETOR       — Vê tudo (todas lojas, custos), mas somente leitura
    COMPRAS       — Custos, SKUs, fornecedores; vendas apenas leitura
    GESTOR        — Apenas lojas atribuídas; sem custos detalhados
"""

import streamlit as st
import pandas as pd

# ============================================================
# DEFINIÇÃO DOS PERFIS E PERMISSÕES
# ============================================================

# Módulos disponíveis no sistema
MODULOS = [
    'inicio', 'performance', 'skus', 'vendas', 'tags',
    'compras', 'config', 'calculadora', 'ia', 'kanban',
    'tabela_preco', 'ads',
]

# Mapa de permissões por perfil
# Valores: 'completo', 'leitura', 'parcial', False (sem acesso)
PERMISSOES = {
    'ADMIN': {
        'inicio':       'completo',
        'performance':  'completo',
        'skus':         'completo',
        'vendas':       'completo',
        'tags':         'completo',
        'compras':      'completo',
        'config':       'completo',
        'calculadora':  'completo',
        'ia':           'completo',
        'kanban':       'completo',
        'tabela_preco': 'completo',
        'ads':          'completo',
    },
    'CONTROLADORIA': {
        'inicio':       'completo',
        'performance':  'completo',
        'skus':         'completo',
        'vendas':       'completo',
        'tags':         'completo',
        'compras':      'completo',
        'config':       'completo',
        'calculadora':  'completo',
        'ia':           'completo',
        'kanban':       'completo',
        'tabela_preco': 'completo',
        'ads':          'completo',
    },
    'DIRETOR': {
        'inicio':       'leitura',
        'performance':  'leitura',
        'skus':         'leitura',
        'vendas':       'leitura',
        'tags':         'leitura',
        'compras':      'leitura',
        'config':       'leitura',
        'calculadora':  'completo',
        'ia':           'completo',
        'kanban':       'completo',
        'tabela_preco': 'leitura',
        'ads':          'leitura',
    },
    'COMPRAS': {
        'inicio':       'completo',
        'performance':  'completo',
        'skus':         'completo',
        'vendas':       'leitura',
        'tags':         'completo',
        'compras':      'completo',
        'config':       False,
        'calculadora':  'completo',
        'ia':           'completo',
        'kanban':       'completo',
        'tabela_preco': 'completo',
        'ads':          'completo',
    },
    'GESTOR': {
        'inicio':       'parcial',      # filtrado por loja
        'performance':  'parcial',      # filtrado por loja
        'skus':         False,
        'vendas':       'parcial',      # filtrado por loja
        'tags':         'parcial',      # filtrado por loja
        'compras':      False,
        'config':       'parcial',      # só aba de anúncios
        'calculadora':  'completo',
        'ia':           'parcial',      # filtrado por loja + sem custos
        'kanban':       'completo',
        'tabela_preco': 'parcial',      # filtrado por marketplace
        'ads':          'parcial',      # filtrado por marketplace
    },
}

# Quais perfis veem custos detalhados (FOB, frete internacional, componentes)
PERFIS_VER_CUSTOS = {'ADMIN', 'CONTROLADORIA', 'DIRETOR', 'COMPRAS'}

# Quais perfis podem gerenciar usuários
PERFIS_GERENCIAR_USUARIOS = {'ADMIN'}

# Quais perfis veem todas as lojas (sem filtro)
PERFIS_TODAS_LOJAS = {'ADMIN', 'CONTROLADORIA', 'DIRETOR', 'COMPRAS'}

# Colunas de custo que devem ser ocultadas para perfis sem acesso
COLUNAS_CUSTO_OCULTAS = [
    'custo_fob', 'frete_internacional', 'custo_unitario_componente',
    'custo_embalagem', 'custo_etiqueta', 'custo_extras',
    'preco_compra', 'custo_total_componentes',
]

# Mapeamento menu → módulo interno
MENU_MODULOS = {
    '🏠 Início':          'inicio',
    '📊 Performance':     'performance',
    '📦 SKUs':            'skus',
    '💰 Vendas':          'vendas',
    '🏷️ Tags':           'tags',
    '🛒 Compras':         'compras',
    '🧮 Calculadora':     'calculadora',
    '🤖 Nala IA':         'ia',
    '📋 Kanban':          'kanban',
    '💲 Tabela de Preço': 'tabela_preco',
    '📊 Análise de Ads':  'ads',
    '⚙️ Config':          'config',
}


# ============================================================
# FUNÇÕES DE VERIFICAÇÃO
# ============================================================

def _get_role() -> str:
    """Retorna o role do usuário logado."""
    usuario = st.session_state.get('usuario', {})
    return usuario.get('role', '')


def _get_usuario():
    """Retorna o dict completo do usuário logado."""
    return st.session_state.get('usuario', {})


def pode_acessar(modulo: str) -> bool:
    """
    Verifica se o usuário logado pode acessar um módulo.
    Retorna True se tem qualquer nível de acesso (completo, leitura, parcial).
    """
    role = _get_role()
    if not role:
        return False
    perms = PERMISSOES.get(role, {})
    return bool(perms.get(modulo, False))


def get_nivel_acesso(modulo: str) -> str:
    """
    Retorna o nível de acesso ao módulo: 'completo', 'leitura', 'parcial' ou ''.
    """
    role = _get_role()
    if not role:
        return ''
    perms = PERMISSOES.get(role, {})
    nivel = perms.get(modulo, False)
    return nivel if nivel else ''


def eh_somente_leitura(modulo: str) -> bool:
    """Retorna True se o acesso ao módulo é somente leitura."""
    return get_nivel_acesso(modulo) == 'leitura'


def pode_ver_custos() -> bool:
    """Verifica se o usuário pode ver custos detalhados (FOB, frete, etc.)."""
    return _get_role() in PERFIS_VER_CUSTOS


def pode_gerenciar_usuarios() -> bool:
    """Verifica se o usuário pode criar/editar/desativar usuários."""
    return _get_role() in PERFIS_GERENCIAR_USUARIOS


def ve_todas_lojas() -> bool:
    """Verifica se o usuário vê todas as lojas (sem filtro)."""
    return _get_role() in PERFIS_TODAS_LOJAS


# ============================================================
# FILTRO POR LOJA
# ============================================================

def get_lojas_usuario(engine=None) -> list:
    """
    Retorna lista de lojas que o usuário pode acessar.
    Lista vazia = sem restrição (vê tudo).

    Primeiro tenta session_state (já carregado no login).
    Se não existir e engine for fornecido, busca do banco.
    """
    usuario = _get_usuario()

    # Se já tem no session_state, usa direto
    lojas = usuario.get('lojas_permitidas', [])
    if lojas or ve_todas_lojas():
        return lojas

    # Se é GESTOR sem lojas carregadas, busca do banco
    if engine and usuario.get('id_usuario'):
        try:
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT dl.loja
                FROM dim_usuario_lojas dul
                JOIN dim_lojas dl ON dul.id_loja = dl.id
                WHERE dul.id_usuario = %s
            """, (usuario['id_usuario'],))
            lojas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            # Atualiza session_state pra não buscar de novo
            if 'usuario' in st.session_state:
                st.session_state.usuario['lojas_permitidas'] = lojas
        except Exception:
            lojas = []

    return lojas


def filtrar_df_por_loja(df: pd.DataFrame, col_loja: str = 'loja_origem',
                        engine=None) -> pd.DataFrame:
    """
    Filtra um DataFrame para mostrar apenas lojas permitidas.
    Se o perfil vê todas as lojas, retorna o df sem alteração.

    Args:
        df: DataFrame a filtrar
        col_loja: nome da coluna de loja no df
        engine: engine SQLAlchemy (para buscar lojas se necessário)
    """
    if df.empty or ve_todas_lojas():
        return df

    lojas = get_lojas_usuario(engine)
    if not lojas:
        return df  # Sem restrição configurada

    if col_loja in df.columns:
        return df[df[col_loja].isin(lojas)].copy()

    return df


def filtrar_query_por_loja(where_parts: list, params: list,
                           col_loja: str = 'loja_origem',
                           engine=None):
    """
    Adiciona cláusula WHERE para filtrar por lojas permitidas em queries SQL.
    Modifica where_parts e params in-place.

    Uso:
        where_parts = ["data_venda >= %s"]
        params = [data_inicio]
        filtrar_query_por_loja(where_parts, params, 'loja_origem', engine)
        query = f"SELECT * FROM fact_vendas_snapshot WHERE {' AND '.join(where_parts)}"
    """
    if ve_todas_lojas():
        return

    lojas = get_lojas_usuario(engine)
    if lojas:
        placeholders = ', '.join(['%s'] * len(lojas))
        where_parts.append(f"{col_loja} IN ({placeholders})")
        params.extend(lojas)


# ============================================================
# PROTEÇÃO DE COLUNAS
# ============================================================

def ocultar_colunas_custo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas de custo detalhado se o usuário não tem permissão.
    Mantém 'preco_a_ser_considerado' que é visível a todos.
    """
    if pode_ver_custos() or df.empty:
        return df

    colunas_remover = [c for c in COLUNAS_CUSTO_OCULTAS if c in df.columns]
    if colunas_remover:
        return df.drop(columns=colunas_remover)
    return df


# ============================================================
# MENU DINÂMICO
# ============================================================

def get_opcoes_menu() -> list:
    """
    Retorna lista de opções de menu baseada no perfil do usuário.
    Formato: ['🏠 Início', '📊 Performance', ...]
    """
    opcoes = []
    for label, modulo in MENU_MODULOS.items():
        if pode_acessar(modulo):
            opcoes.append(label)
    return opcoes


def get_modulo_do_menu(label_menu: str) -> str:
    """Converte label do menu para nome interno do módulo."""
    return MENU_MODULOS.get(label_menu, '')


# ============================================================
# INDICADORES VISUAIS
# ============================================================

def mostrar_badge_leitura(modulo: str):
    """Mostra badge de 'somente leitura' se aplicável."""
    if eh_somente_leitura(modulo):
        st.info("👁️ Modo leitura — você pode visualizar mas não editar.")


def mostrar_badge_filtro_loja():
    """Mostra quais lojas o usuário está vendo."""
    if not ve_todas_lojas():
        lojas = get_lojas_usuario()
        if lojas:
            lojas_str = ', '.join(lojas)
            st.caption(f"🔒 Visualizando: {lojas_str}")


# ============================================================
# CONTEXTO PARA IA
# ============================================================

def get_contexto_ia() -> dict:
    """
    Retorna contexto de permissões para o módulo de IA.
    A IA usa isso para saber o que pode ou não responder.
    """
    role = _get_role()
    return {
        'role': role,
        'ver_custos': pode_ver_custos(),
        'lojas_permitidas': get_lojas_usuario(),
        've_todas_lojas': ve_todas_lojas(),
        'modulos_acessiveis': [m for m in MODULOS if pode_acessar(m)],
        'colunas_ocultas': [] if pode_ver_custos() else COLUNAS_CUSTO_OCULTAS,
    }
