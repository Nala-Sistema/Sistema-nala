"""
MÓDULO: Sistema Kanban (Trello)
Sistema Nala - Gestão de Projetos e Tarefas

Funcionalidades:
- Quadros personalizados
- Colunas drag-and-drop
- Cards com detalhes completos
- Checklist, comentários, anexos
- Filtros e pesquisa
- Histórico de ações

Perfil necessário: Todos (RBAC por quadro)
"""

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime, date, timedelta

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

# v3.1: Conexão agnóstica — lê DB_URL do Secrets do Streamlit
# Cada app (Produção / Dev) tem seu próprio Secret.

# Cores por prioridade
CORES_PRIORIDADE = {
    'URGENTE': '#EB5A46',
    'ALTA': '#FF9F1A',
    'MEDIA': '#F2D600',
    'BAIXA': '#61BD4F'
}

# ============================================================================
# FUNÇÕES DE BANCO
# ============================================================================

def get_db_connection():
    """Cria conexão com banco via st.secrets (agnóstico ao ambiente)."""
    try:
        db_url = st.secrets["DB_URL"]
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        st.error(f"❌ Erro ao conectar: {e}")
        return None

def buscar_quadros():
    """Busca todos os quadros ativos"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM kanban_quadros 
            WHERE ativo = TRUE 
            ORDER BY data_criacao DESC
        """)
        quadros = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(q) for q in quadros]
    except Exception as e:
        st.error(f"Erro: {e}")
        return []

def buscar_colunas(id_quadro):
    """Busca colunas de um quadro"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM kanban_colunas 
            WHERE id_quadro = %s AND ativo = TRUE
            ORDER BY ordem
        """, (id_quadro,))
        colunas = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(c) for c in colunas]
    except Exception as e:
        st.error(f"Erro: {e}")
        return []

def buscar_cards(id_coluna, filtros=None):
    """Busca cards de uma coluna"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT c.*, u.nome as nome_responsavel
            FROM kanban_cards c
            LEFT JOIN dim_usuarios u ON c.responsavel = u.id_usuario
            WHERE c.id_coluna = %s 
            AND c.arquivado = FALSE
        """
        params = [id_coluna]
        
        # Aplicar filtros
        if filtros:
            if filtros.get('prioridade'):
                query += " AND c.prioridade = %s"
                params.append(filtros['prioridade'])
            
            if filtros.get('responsavel'):
                query += " AND c.responsavel = %s"
                params.append(filtros['responsavel'])
            
            if filtros.get('busca'):
                query += " AND (c.titulo ILIKE %s OR c.descricao ILIKE %s)"
                busca = f"%{filtros['busca']}%"
                params.extend([busca, busca])
        
        query += " ORDER BY c.ordem, c.data_criacao"
        
        cursor.execute(query, params)
        cards = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(c) for c in cards]
    except Exception as e:
        st.error(f"Erro: {e}")
        return []

def criar_quadro(nome, descricao, cor, icone, id_criador):
    """Cria novo quadro"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO kanban_quadros (nome, descricao, cor, icone, id_criador)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id_quadro
        """, (nome, descricao, cor, icone, id_criador))
        
        id_quadro = cursor.fetchone()[0]
        
        # Criar colunas padrão
        colunas_padrao = ['A Fazer', 'Em Progresso', 'Concluído']
        for i, col_nome in enumerate(colunas_padrao):
            cursor.execute("""
                INSERT INTO kanban_colunas (id_quadro, nome, ordem)
                VALUES (%s, %s, %s)
            """, (id_quadro, col_nome, i))
        
        conn.commit()
        cursor.close()
        conn.close()
        return id_quadro
    except Exception as e:
        st.error(f"Erro: {e}")
        return None

def criar_coluna(id_quadro, nome, ordem):
    """Cria nova coluna"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO kanban_colunas (id_quadro, nome, ordem)
            VALUES (%s, %s, %s)
        """, (id_quadro, nome, ordem))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

def criar_card(id_coluna, titulo, descricao, prioridade, responsavel, data_prazo, etiquetas, criado_por):
    """Cria novo card"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Buscar próxima ordem
        cursor.execute("""
            SELECT COALESCE(MAX(ordem), 0) + 1 
            FROM kanban_cards 
            WHERE id_coluna = %s
        """, (id_coluna,))
        ordem = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO kanban_cards 
            (id_coluna, titulo, descricao, prioridade, responsavel, data_prazo, etiquetas, criado_por, ordem)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_card
        """, (id_coluna, titulo, descricao, prioridade, responsavel, data_prazo, etiquetas, criado_por, ordem))
        
        id_card = cursor.fetchone()[0]
        
        # Registrar histórico
        cursor.execute("""
            INSERT INTO kanban_historico (id_card, id_usuario, acao, descricao)
            VALUES (%s, %s, 'CRIOU', 'Card criado')
        """, (id_card, criado_por))
        
        conn.commit()
        cursor.close()
        conn.close()
        return id_card
    except Exception as e:
        st.error(f"Erro: {e}")
        return None

def mover_card(id_card, nova_coluna, id_usuario):
    """Move card para outra coluna"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Buscar próxima ordem na coluna destino
        cursor.execute("""
            SELECT COALESCE(MAX(ordem), 0) + 1 
            FROM kanban_cards 
            WHERE id_coluna = %s
        """, (nova_coluna,))
        nova_ordem = cursor.fetchone()[0]
        
        # Atualizar card
        cursor.execute("""
            UPDATE kanban_cards 
            SET id_coluna = %s, ordem = %s
            WHERE id_card = %s
        """, (nova_coluna, nova_ordem, id_card))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

def buscar_checklist(id_card):
    """Busca checklist de um card"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM kanban_checklist
            WHERE id_card = %s
            ORDER BY ordem
        """, (id_card,))
        items = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(i) for i in items]
    except Exception as e:
        return []

def adicionar_item_checklist(id_card, texto):
    """Adiciona item ao checklist"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COALESCE(MAX(ordem), 0) + 1 
            FROM kanban_checklist 
            WHERE id_card = %s
        """, (id_card,))
        ordem = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO kanban_checklist (id_card, texto, ordem)
            VALUES (%s, %s, %s)
        """, (id_card, texto, ordem))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def toggle_checklist_item(id_item):
    """Marca/desmarca item do checklist"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE kanban_checklist
            SET concluido = NOT concluido,
                data_conclusao = CASE WHEN concluido THEN NULL ELSE CURRENT_TIMESTAMP END
            WHERE id_item = %s
        """, (id_item,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def buscar_comentarios(id_card):
    """Busca comentários de um card"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT c.*, u.nome as nome_usuario
            FROM kanban_comentarios c
            JOIN dim_usuarios u ON c.id_usuario = u.id_usuario
            WHERE c.id_card = %s
            ORDER BY c.data_criacao DESC
        """, (id_card,))
        comentarios = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(c) for c in comentarios]
    except Exception as e:
        return []

def adicionar_comentario(id_card, id_usuario, comentario):
    """Adiciona comentário a um card"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO kanban_comentarios (id_card, id_usuario, comentario)
            VALUES (%s, %s, %s)
        """, (id_card, id_usuario, comentario))
        
        # Registrar no histórico
        cursor.execute("""
            INSERT INTO kanban_historico (id_card, id_usuario, acao, descricao)
            VALUES (%s, %s, 'COMENTOU', %s)
        """, (id_card, id_usuario, f'Comentário: {comentario[:50]}...'))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        return False

def buscar_usuarios():
    """Busca todos os usuários"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id_usuario, COALESCE(nome, username) as nome FROM dim_usuarios WHERE ativo = TRUE")
        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(u) for u in usuarios]
    except Exception as e:
        return []

# ============================================================================
# INTERFACE STREAMLIT
# ============================================================================

def renderizar_card(card):
    """Renderiza um card"""
    
    # Cor da borda por prioridade
    cor = CORES_PRIORIDADE.get(card['prioridade'], '#EBECF0')
    
    with st.container():
        # Card container
        st.markdown(f"""
        <div style='
            background: white;
            border-left: 4px solid {cor};
            border-radius: 4px;
            padding: 12px;
            margin-bottom: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
            cursor: pointer;
        '>
            <div style='font-weight: 600; margin-bottom: 8px;'>{card['titulo']}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Botões de ação (invisíveis até hover - simulado com expander)
        with st.expander("✏️ Detalhes", expanded=False):
            
            # Descrição
            if card.get('descricao'):
                st.markdown(f"**Descrição:** {card['descricao']}")
            
            # Metadados
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Prioridade:** {card['prioridade']}")
                if card.get('nome_responsavel'):
                    st.markdown(f"**Responsável:** {card['nome_responsavel']}")
            
            with col2:
                if card.get('data_prazo'):
                    prazo = card['data_prazo']
                    hoje = date.today()
                    
                    if prazo < hoje:
                        st.error(f"⏰ Atrasado: {prazo}")
                    elif prazo == hoje:
                        st.warning(f"⏰ Hoje: {prazo}")
                    else:
                        st.info(f"📅 Prazo: {prazo}")
            
            # Checklist preview
            checklist = buscar_checklist(card['id_card'])
            if checklist:
                concluidos = sum(1 for item in checklist if item['concluido'])
                total = len(checklist)
                progresso = (concluidos / total * 100) if total > 0 else 0
                st.progress(progresso / 100, text=f"✓ {concluidos}/{total} itens")
            
            # Ações
            st.markdown("---")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📝 Editar", key=f"edit_{card['id_card']}", use_container_width=True):
                    st.session_state.card_editando = card['id_card']
                    st.rerun()
            
            with col2:
                if st.button("💬 Comentar", key=f"comment_{card['id_card']}", use_container_width=True):
                    st.session_state.card_comentando = card['id_card']
                    st.rerun()
            
            with col3:
                if st.button("📋 Checklist", key=f"check_{card['id_card']}", use_container_width=True):
                    st.session_state.card_checklist = card['id_card']
                    st.rerun()

def modal_editar_card(id_card):
    """Modal para editar card"""
    st.markdown("### ✏️ Editar Card")
    
    # Buscar card
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM kanban_cards WHERE id_card = %s", (id_card,))
    card = dict(cursor.fetchone())
    cursor.close()
    conn.close()
    
    with st.form("form_editar_card"):
        titulo = st.text_input("Título", value=card['titulo'])
        descricao = st.text_area("Descrição", value=card.get('descricao', ''))
        
        col1, col2 = st.columns(2)
        
        with col1:
            prioridade = st.selectbox(
                "Prioridade",
                options=['BAIXA', 'MEDIA', 'ALTA', 'URGENTE'],
                index=['BAIXA', 'MEDIA', 'ALTA', 'URGENTE'].index(card['prioridade'])
            )
        
        with col2:
            usuarios = buscar_usuarios()
            opcoes_usuarios = {u['id_usuario']: u['nome'] for u in usuarios}
            responsavel = st.selectbox(
                "Responsável",
                options=list(opcoes_usuarios.keys()),
                format_func=lambda x: opcoes_usuarios[x],
                index=list(opcoes_usuarios.keys()).index(card['responsavel']) if card.get('responsavel') in opcoes_usuarios else 0
            )
        
        data_prazo = st.date_input("Data Prazo", value=card.get('data_prazo'))
        
        submitted = st.form_submit_button("💾 Salvar", use_container_width=True)
        
        if submitted:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE kanban_cards
                SET titulo = %s, descricao = %s, prioridade = %s, 
                    responsavel = %s, data_prazo = %s
                WHERE id_card = %s
            """, (titulo, descricao, prioridade, responsavel, data_prazo, id_card))
            conn.commit()
            cursor.close()
            conn.close()
            
            st.success("✅ Card atualizado!")
            del st.session_state.card_editando
            st.rerun()
    
    if st.button("❌ Cancelar"):
        del st.session_state.card_editando
        st.rerun()

def modal_checklist(id_card):
    """Modal para gerenciar checklist"""
    st.markdown("### 📋 Checklist")
    
    checklist = buscar_checklist(id_card)
    
    # Listar items
    if checklist:
        for item in checklist:
            col1, col2 = st.columns([0.1, 0.9])
            
            with col1:
                if st.checkbox("", value=item['concluido'], key=f"check_item_{item['id_item']}"):
                    if not item['concluido']:
                        toggle_checklist_item(item['id_item'])
                        st.rerun()
                else:
                    if item['concluido']:
                        toggle_checklist_item(item['id_item'])
                        st.rerun()
            
            with col2:
                estilo = "text-decoration: line-through; color: #999;" if item['concluido'] else ""
                st.markdown(f"<span style='{estilo}'>{item['texto']}</span>", unsafe_allow_html=True)
        
        # Progresso
        concluidos = sum(1 for i in checklist if i['concluido'])
        st.progress(concluidos / len(checklist), text=f"{concluidos}/{len(checklist)} concluídos")
    else:
        st.info("Nenhum item no checklist")
    
    # Adicionar item
    st.markdown("---")
    with st.form("form_add_checklist"):
        novo_item = st.text_input("Novo item")
        if st.form_submit_button("➕ Adicionar"):
            if novo_item:
                adicionar_item_checklist(id_card, novo_item)
                st.success("✅ Item adicionado!")
                st.rerun()
    
    if st.button("❌ Fechar"):
        del st.session_state.card_checklist
        st.rerun()

def modal_comentarios(id_card):
    """Modal para comentários"""
    st.markdown("### 💬 Comentários")
    
    comentarios = buscar_comentarios(id_card)
    
    # Listar comentários
    if comentarios:
        for com in comentarios:
            with st.container():
                st.markdown(f"**{com['nome_usuario']}** · {com['data_criacao'].strftime('%d/%m/%Y %H:%M')}")
                st.markdown(com['comentario'])
                st.markdown("---")
    else:
        st.info("Nenhum comentário ainda")
    
    # Adicionar comentário
    with st.form("form_comentario"):
        novo_comentario = st.text_area("Adicionar comentário")
        if st.form_submit_button("💬 Enviar"):
            if novo_comentario:
                id_usuario = st.session_state.get('user_id', 1)
                adicionar_comentario(id_card, id_usuario, novo_comentario)
                st.success("✅ Comentário adicionado!")
                st.rerun()
    
    if st.button("❌ Fechar"):
        del st.session_state.card_comentando
        st.rerun()

def main():
    st.title("📋 Kanban - Gestão de Projetos")
    
    # Verificar login
    if 'user_id' not in st.session_state:
        st.session_state.user_id = 1  # Demo
    
    # Sidebar: Seleção de quadro
    with st.sidebar:
        st.header("📊 Quadros")
        
        quadros = buscar_quadros()
        
        if quadros:
            opcoes_quadros = {q['id_quadro']: f"{q['icone']} {q['nome']}" for q in quadros}
            quadro_selecionado = st.selectbox(
                "Selecione o quadro:",
                options=list(opcoes_quadros.keys()),
                format_func=lambda x: opcoes_quadros[x]
            )
        else:
            st.warning("Nenhum quadro disponível")
            quadro_selecionado = None
        
        st.markdown("---")
        
        # Botão criar quadro
        if st.button("➕ Novo Quadro", use_container_width=True):
            st.session_state.criando_quadro = True
        
        # Filtros
        st.markdown("---")
        st.subheader("🔍 Filtros")
        
        prioridade_filtro = st.selectbox(
            "Prioridade",
            options=['Todas', 'URGENTE', 'ALTA', 'MEDIA', 'BAIXA']
        )
        
        busca_filtro = st.text_input("🔎 Buscar", placeholder="Título ou descrição...")
    
    # Modal criar quadro
    if st.session_state.get('criando_quadro'):
        with st.form("form_criar_quadro"):
            st.subheader("➕ Novo Quadro")
            
            nome = st.text_input("Nome do Quadro")
            descricao = st.text_area("Descrição")
            
            col1, col2 = st.columns(2)
            with col1:
                cor = st.color_picker("Cor", value="#0079BF")
            with col2:
                icone = st.text_input("Ícone (emoji)", value="📋")
            
            if st.form_submit_button("💾 Criar Quadro", use_container_width=True):
                if nome:
                    criar_quadro(nome, descricao, cor, icone, st.session_state.user_id)
                    st.success("✅ Quadro criado!")
                    del st.session_state.criando_quadro
                    st.rerun()
        
        if st.button("❌ Cancelar", key="btn_cancelar_quadro"):
            del st.session_state.criando_quadro
            st.rerun()
    
    # Exibir quadro selecionado
    if quadro_selecionado:
        quadro = next(q for q in quadros if q['id_quadro'] == quadro_selecionado)
        
        st.markdown(f"## {quadro['icone']} {quadro['nome']}")
        if quadro.get('descricao'):
            st.markdown(quadro['descricao'])
        
        st.markdown("---")
        
        # Preparar filtros
        filtros = {}
        if prioridade_filtro != 'Todas':
            filtros['prioridade'] = prioridade_filtro
        if busca_filtro:
            filtros['busca'] = busca_filtro
        
        # Buscar colunas
        colunas = buscar_colunas(quadro_selecionado)
        
        if colunas:
            # Criar colunas do Streamlit
            cols = st.columns(len(colunas))
            
            for i, coluna in enumerate(colunas):
                with cols[i]:
                    # Header da coluna
                    st.markdown(f"### {coluna['nome']}")
                    
                    # Botão adicionar card
                    if st.button("➕ Card", key=f"add_card_{coluna['id_coluna']}", use_container_width=True):
                        st.session_state.criando_card = coluna['id_coluna']
                    
                    st.markdown("---")
                    
                    # Cards
                    cards = buscar_cards(coluna['id_coluna'], filtros)
                    
                    if cards:
                        for card in cards:
                            renderizar_card(card)
                    else:
                        st.info("Nenhum card")
                    
                    # Botão mover (simples select)
                    if cards:
                        st.markdown("---")
                        card_mover = st.selectbox(
                            "Mover card:",
                            options=[None] + [c['id_card'] for c in cards],
                            format_func=lambda x: "Selecione..." if x is None else next(c['titulo'] for c in cards if c['id_card'] == x),
                            key=f"select_move_{coluna['id_coluna']}"
                        )
                        
                        if card_mover:
                            colunas_destino = [c for c in colunas if c['id_coluna'] != coluna['id_coluna']]
                            destino = st.selectbox(
                                "Para:",
                                options=[c['id_coluna'] for c in colunas_destino],
                                format_func=lambda x: next(c['nome'] for c in colunas if c['id_coluna'] == x),
                                key=f"select_dest_{coluna['id_coluna']}"
                            )
                            
                            if st.button("→ Mover", key=f"btn_move_{coluna['id_coluna']}"):
                                mover_card(card_mover, destino, st.session_state.user_id)
                                st.success("✅ Card movido!")
                                st.rerun()
        
        # Modal criar card
        if st.session_state.get('criando_card'):
            st.markdown("---")
            with st.form("form_criar_card"):
                st.subheader("➕ Novo Card")
                
                titulo = st.text_input("Título *")
                descricao = st.text_area("Descrição")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    prioridade = st.selectbox("Prioridade", options=['BAIXA', 'MEDIA', 'ALTA', 'URGENTE'])
                
                with col2:
                    usuarios = buscar_usuarios()
                    opcoes_usuarios = {u['id_usuario']: u['nome'] for u in usuarios}
                    responsavel = st.selectbox(
                        "Responsável",
                        options=list(opcoes_usuarios.keys()),
                        format_func=lambda x: opcoes_usuarios[x]
                    )
                
                with col3:
                    data_prazo = st.date_input("Prazo")
                
                etiquetas_input = st.text_input("Etiquetas (separadas por vírgula)")
                etiquetas = [e.strip() for e in etiquetas_input.split(',')] if etiquetas_input else []
                
                if st.form_submit_button("💾 Criar Card", use_container_width=True):
                    if titulo:
                        criar_card(
                            st.session_state.criando_card,
                            titulo,
                            descricao,
                            prioridade,
                            responsavel,
                            data_prazo,
                            etiquetas,
                            st.session_state.user_id
                        )
                        st.success("✅ Card criado!")
                        del st.session_state.criando_card
                        st.rerun()
            
            if st.button("❌ Cancelar", key="btn_cancelar_card"):
                del st.session_state.criando_card
                st.rerun()
    
    # Modais
    if st.session_state.get('card_editando'):
        modal_editar_card(st.session_state.card_editando)
    
    if st.session_state.get('card_checklist'):
        modal_checklist(st.session_state.card_checklist)
    
    if st.session_state.get('card_comentando'):
        modal_comentarios(st.session_state.card_comentando)

if __name__ == "__main__":
    main()
