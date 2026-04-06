"""
MÓDULO: Nala IA — Nala Lupa (Auditora de Inteligência de Mercado)
Sistema Nala v3.1

Identidade: Auditora Sênior de Inteligência de Mercado
Motor: Google Gemini (gemini-2.0-flash)
Segurança: RBAC por design — filtra dados pelo perfil do usuário logado

Funcionalidades:
  [1] Consulta de vendas, margens, performance (via SQL no banco)
  [2] Análise de links/HTML de anúncios (ML, Shopee)
  [3] Integração com Kanban (leitura de cards, diagnósticos)
  [4] Simulação de viabilidade (regras de comissão por marketplace)
  [5] Auditoria de SEO e cadastro

Regras Matemáticas (Nala Standard):
  - Imposto: 10% default (editável por loja em dim_lojas)
  - Margem alvo: 15% (alerta abaixo)
  - Comissões: ML 12%/17%, Shopee escalonada, Amazon 12%, Magalu 14.8%, Shein 16%
"""

import streamlit as st
import pandas as pd
import json
from datetime import datetime, date, timedelta
from database_utils import get_engine

# ============================================================
# CONFIGURAÇÃO GEMINI
# ============================================================

def _get_gemini_model():
    """Inicializa e retorna o modelo Gemini."""
    try:
        import google.generativeai as genai
        api_key = st.secrets["GEMINI_API_KEY"]
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        return model
    except KeyError:
        st.error("❌ GEMINI_API_KEY não configurada nos Secrets do Streamlit.")
        return None
    except Exception as e:
        st.error(f"❌ Erro ao inicializar Gemini: {e}")
        return None


# ============================================================
# SYSTEM PROMPT — IDENTIDADE DA NALA LUPA
# ============================================================

def _build_system_prompt(contexto_usuario, schema_resumo, dados_contexto=""):
    """
    Monta o system prompt da Nala Lupa com contexto do usuário e dados.
    """
    role = contexto_usuario.get('role', 'GESTOR')
    nome = contexto_usuario.get('nome', 'Usuário')
    ver_custos = contexto_usuario.get('ver_custos', False)
    lojas = contexto_usuario.get('lojas_permitidas', [])
    lojas_txt = ', '.join(lojas) if lojas else 'Todas as lojas'

    restricoes_custo = ""
    if not ver_custos:
        restricoes_custo = """
RESTRIÇÃO DE DADOS SENSÍVEIS:
Este usuário NÃO tem permissão para ver custos detalhados.
NUNCA revele: custo FOB, frete internacional, custo de embalagem, custo de componentes, preço de compra.
Você pode falar sobre: preço de venda, margem percentual, faturamento, quantidade vendida.
Se o usuário perguntar sobre custos, responda: "Essa informação está restrita ao seu perfil de acesso."
"""

    restricoes_loja = ""
    if lojas:
        restricoes_loja = f"""
RESTRIÇÃO DE LOJAS:
Este usuário só tem acesso aos dados das lojas: {lojas_txt}
NUNCA mostre dados de outras lojas. Se perguntar sobre lojas que não tem acesso, responda:
"Você não tem permissão para acessar dados dessa loja."
"""

    return f"""Você é a NALA LUPA — Auditora Sênior de Inteligência de Mercado do Grupo Nala.

PERSONALIDADE:
- Direta, objetiva e profissional
- Sempre responde em português brasileiro
- Usa formatação BR: R$ 1.234,56 e dd/mm/aaaa
- Foca em dados concretos e ações práticas
- Alerta quando margem está abaixo de 15%

USUÁRIO LOGADO:
- Nome: {nome}
- Perfil: {role}
- Lojas com acesso: {lojas_txt}

{restricoes_custo}
{restricoes_loja}

REGRAS MATEMÁTICAS (NALA STANDARD):
- Imposto padrão: 10% sobre preço de venda (varia por loja)
- Margem líquida alvo: 15% (sinalizar ALERTA se abaixo)
- Comissões ML: 12% (Clássico) ou 17% (Premium) + R$6,50 fixo (abaixo R$79)
- Comissões Shopee: escalonada (20%+R$4 até R$79,99 / 14%+R$16 até R$99,99 / etc.)
- Comissões Amazon: 12% + R$6,50 DBA ou R$5,50 FBA (abaixo R$79)
- Comissões Magalu: 14,8% + R$5 fixo (abaixo R$79)
- Comissões Shein: 16% + R$5 fixo
- Regra R$78,99: ML, Amazon, Magalu — acima desse valor, taxa fixa vira custo de frete

CAPACIDADES:
1. Consultar vendas, faturamento, margens por período/loja/marketplace
2. Analisar anúncios via link ou HTML (SEO, preço, fotos, selos)
3. Comparar nosso anúncio vs concorrente
4. Sugerir otimizações de título, preço e estratégia
5. Consultar e criar cards no Kanban
6. Calcular viabilidade de produtos novos

QUANDO ANALISAR UM ANÚNCIO (HTML ou link):
- Mapeie: preço, título, fotos, selos (Full, Loja Oficial, Vendedor Indicado), vídeo
- SEO do título: ML ideal 60 chars focado em solução / Shopee ideal 100 chars com keywords
- Sugira palavras-chave para campos ocultos (Modelo, Marca, Variações)
- Se preço próximo ao líder, sugira estratégia de Kit
- Calcule margem estimada usando as regras acima

DADOS DISPONÍVEIS NO BANCO:
{schema_resumo}

{dados_contexto}

FORMATO DAS RESPOSTAS:
- Use tabelas quando comparar dados
- Destaque alertas com ⚠️
- Use ✅ para itens positivos e ❌ para negativos
- Sempre conclua com "Ação sugerida:" quando relevante
"""


# ============================================================
# CONSULTAS AO BANCO (COM FILTRO RBAC)
# ============================================================

def _get_schema_resumo():
    """Retorna resumo das tabelas disponíveis para o prompt."""
    return """
TABELAS PRINCIPAIS:
- fact_vendas_snapshot: vendas consolidadas (data_venda, sku, loja_origem, marketplace_origem, 
  quantidade, preco_venda, valor_venda_efetivo, custo_unitario, custo_total, imposto, comissao, 
  frete, tarifa_fixa, total_tarifas, valor_liquido, margem_total, margem_percentual, codigo_anuncio, logistica)
- dim_produtos: cadastro de produtos (sku, nome, status, preco_a_ser_considerado)
- dim_lojas: lojas cadastradas (marketplace, loja, imposto, custo_flex)
- dim_config_marketplace: config Amazon por ASIN (asin, sku, logistica, comissao_percentual, taxa_fixa, frete_estimado)
- kanban_quadros: quadros do Kanban (nome, descricao)
- kanban_cards: cards/tarefas (titulo, descricao, prioridade, data_prazo)
- kanban_colunas: colunas dos quadros (nome, ordem)
"""


def _buscar_dados_contexto(engine, contexto_usuario):
    """
    Busca dados resumidos do banco para dar contexto à IA.
    Respeita RBAC: filtra por lojas do usuário.
    """
    lojas = contexto_usuario.get('lojas_permitidas', [])
    ver_custos = contexto_usuario.get('ver_custos', False)

    where_loja = ""
    params = []
    if lojas:
        placeholders = ', '.join(['%s'] * len(lojas))
        where_loja = f"AND loja_origem IN ({placeholders})"
        params = list(lojas)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Resumo do mês atual
        primeiro_dia = date.today().replace(day=1)
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_vendas,
                COALESCE(SUM(valor_venda_efetivo), 0) as faturamento,
                COALESCE(AVG(margem_percentual), 0) as margem_media,
                COUNT(DISTINCT sku) as skus_vendidos,
                COUNT(DISTINCT loja_origem) as lojas_ativas
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s {where_loja}
        """, [primeiro_dia] + params)
        row = cursor.fetchone()
        
        resumo_mes = {
            'total_vendas': row[0],
            'faturamento': float(row[1]),
            'margem_media': float(row[2]),
            'skus_vendidos': row[3],
            'lojas_ativas': row[4],
        }

        # Top 5 SKUs do mês
        colunas_select = "sku, SUM(quantidade) as qtd, SUM(valor_venda_efetivo) as receita"
        if ver_custos:
            colunas_select += ", AVG(margem_percentual) as margem_pct"

        cursor.execute(f"""
            SELECT {colunas_select}
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s {where_loja}
            GROUP BY sku
            ORDER BY receita DESC
            LIMIT 5
        """, [primeiro_dia] + params)
        top_skus = cursor.fetchall()

        # Marketplaces ativos
        cursor.execute(f"""
            SELECT marketplace_origem, COUNT(*) as vendas, SUM(valor_venda_efetivo) as receita
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s {where_loja}
            GROUP BY marketplace_origem
            ORDER BY receita DESC
        """, [primeiro_dia] + params)
        mkts = cursor.fetchall()

        cursor.close()
        conn.close()

        # Montar texto de contexto
        ctx = f"""
DADOS DO MÊS ATUAL ({primeiro_dia.strftime('%d/%m/%Y')} até hoje):
- Total de vendas: {resumo_mes['total_vendas']}
- Faturamento: R$ {resumo_mes['faturamento']:,.2f}
- Margem média: {resumo_mes['margem_media']:.1f}%
- SKUs vendidos: {resumo_mes['skus_vendidos']}
- Lojas ativas: {resumo_mes['lojas_ativas']}

TOP 5 PRODUTOS (por receita):
"""
        for i, sku_row in enumerate(top_skus, 1):
            linha = f"  {i}. {sku_row[0]} — {sku_row[1]} un — R$ {float(sku_row[2]):,.2f}"
            if ver_custos and len(sku_row) > 3:
                linha += f" — margem {float(sku_row[3]):.1f}%"
            ctx += linha + "\n"

        ctx += "\nVENDAS POR MARKETPLACE:\n"
        for mkt_row in mkts:
            ctx += f"  - {mkt_row[0]}: {mkt_row[1]} vendas — R$ {float(mkt_row[2]):,.2f}\n"

        return ctx

    except Exception as e:
        return f"(Erro ao buscar dados do banco: {e})"


def _executar_consulta_segura(engine, pergunta, contexto_usuario):
    """
    Executa consulta SQL gerada pela IA com filtros RBAC.
    Retorna DataFrame com resultados.
    """
    lojas = contexto_usuario.get('lojas_permitidas', [])
    ver_custos = contexto_usuario.get('ver_custos', False)

    # Colunas que devem ser removidas para perfis sem acesso a custos
    colunas_ocultas = []
    if not ver_custos:
        colunas_ocultas = [
            'custo_unitario', 'custo_total', 'custo_fob',
            'frete_internacional', 'preco_compra',
            'custo_embalagem', 'custo_etiqueta', 'custo_extras',
            'preco_a_ser_considerado'
        ]

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Busca vendas filtradas
        where_parts = ["1=1"]
        params = []

        if lojas:
            placeholders = ', '.join(['%s'] * len(lojas))
            where_parts.append(f"loja_origem IN ({placeholders})")
            params.extend(lojas)

        # Período padrão: últimos 30 dias
        data_inicio = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
        where_parts.append("data_venda >= %s")
        params.append(data_inicio)

        query = f"""
            SELECT marketplace_origem, loja_origem, sku, codigo_anuncio,
                   data_venda, quantidade, preco_venda, valor_venda_efetivo,
                   comissao, frete, tarifa_fixa, imposto, total_tarifas,
                   valor_liquido, margem_total, margem_percentual
            FROM fact_vendas_snapshot
            WHERE {' AND '.join(where_parts)}
            ORDER BY data_venda DESC
            LIMIT 500
        """

        cursor.execute(query, params)
        colunas = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        df = pd.DataFrame(rows, columns=colunas)

        # Remover colunas sensíveis
        for col in colunas_ocultas:
            if col in df.columns:
                df = df.drop(columns=[col])

        return df

    except Exception as e:
        return pd.DataFrame()


def _buscar_dados_kanban(engine):
    """Busca resumo do Kanban para contexto da IA."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT q.nome as quadro, col.nome as coluna, c.titulo, c.prioridade, 
                   c.descricao, c.data_prazo
            FROM kanban_cards c
            JOIN kanban_colunas col ON c.id_coluna = col.id_coluna
            JOIN kanban_quadros q ON col.id_quadro = q.id_quadro
            WHERE c.arquivado = FALSE
            ORDER BY q.nome, col.ordem, c.ordem
            LIMIT 50
        """)
        colunas = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            return "KANBAN: Nenhum card ativo."

        ctx = "CARDS ATIVOS NO KANBAN:\n"
        for row in rows:
            prazo = row[5].strftime('%d/%m/%Y') if row[5] else 'sem prazo'
            ctx += f"  [{row[0]}] {row[1]} → {row[2]} ({row[3]}) — prazo: {prazo}\n"
        return ctx

    except Exception:
        return "KANBAN: Não foi possível carregar."


# ============================================================
# INTERFACE DO CHAT
# ============================================================

def _render_chat_message(role, content):
    """Renderiza uma mensagem do chat."""
    if role == "user":
        with st.chat_message("user"):
            st.markdown(content)
    else:
        with st.chat_message("assistant", avatar="🕵️‍♀️"):
            st.markdown(content)


def _processar_arquivo_html(uploaded_file):
    """Processa arquivo HTML/TXT uploadado e retorna o conteúdo."""
    try:
        content = uploaded_file.read().decode('utf-8', errors='ignore')
        # Limitar tamanho para não estourar o contexto
        if len(content) > 15000:
            content = content[:15000] + "\n... [TRUNCADO - arquivo muito grande]"
        return content
    except Exception as e:
        return f"Erro ao ler arquivo: {e}"


def main():
    st.header("🕵️‍♀️ Nala Lupa — Inteligência de Mercado")

    # ---- Verificar permissões ----
    try:
        from permissoes import get_contexto_ia, pode_acessar
        contexto_usuario = get_contexto_ia()
    except Exception:
        # Fallback se permissoes.py não existir
        usuario = st.session_state.get('usuario', {})
        contexto_usuario = {
            'role': usuario.get('role', 'ADMIN'),
            'ver_custos': True,
            'lojas_permitidas': [],
            've_todas_lojas': True,
            'modulos_acessiveis': [],
            'colunas_ocultas': [],
        }

    role = contexto_usuario.get('role', '')
    nome = st.session_state.get('usuario', {}).get('nome', 'Usuário')

    # ---- Inicializar modelo Gemini ----
    model = _get_gemini_model()
    if not model:
        return

    # ---- Engine do banco ----
    engine = get_engine()

    # ---- Inicializar histórico de chat ----
    if 'nala_ia_messages' not in st.session_state:
        st.session_state.nala_ia_messages = []

    if 'nala_ia_context_loaded' not in st.session_state:
        st.session_state.nala_ia_context_loaded = False

    # ---- Sidebar: modo de operação ----
    with st.sidebar:
        st.markdown("---")
        st.subheader("🕵️‍♀️ Nala Lupa")

        modo = st.radio(
            "Modo de análise:",
            [
                "💬 Chat Livre",
                "🛒 Análise de Compra",
                "⚔️ Otimização e Escala",
                "🔍 Auditoria de Anúncio",
            ],
            key="nala_ia_modo"
        )

        st.markdown("---")

        # Upload de HTML para análise
        st.caption("📎 Anexar HTML de anúncio:")
        uploaded_html = st.file_uploader(
            "Arraste o HTML aqui",
            type=['html', 'htm', 'txt'],
            key="nala_ia_upload",
            label_visibility="collapsed"
        )

        if st.button("🗑️ Limpar conversa", use_container_width=True):
            st.session_state.nala_ia_messages = []
            st.session_state.nala_ia_context_loaded = False
            st.rerun()

    # ---- Carregar contexto do banco (uma vez por sessão) ----
    if not st.session_state.nala_ia_context_loaded:
        with st.spinner("Carregando dados do sistema..."):
            dados_contexto = _buscar_dados_contexto(engine, contexto_usuario)
            kanban_contexto = _buscar_dados_kanban(engine)
            st.session_state.nala_ia_dados_contexto = dados_contexto
            st.session_state.nala_ia_kanban_contexto = kanban_contexto
            st.session_state.nala_ia_context_loaded = True

    # ---- Montar system prompt ----
    schema_resumo = _get_schema_resumo()
    dados_ctx = st.session_state.get('nala_ia_dados_contexto', '')
    kanban_ctx = st.session_state.get('nala_ia_kanban_contexto', '')
    contexto_completo = f"{dados_ctx}\n\n{kanban_ctx}"

    system_prompt = _build_system_prompt(
        contexto_usuario, schema_resumo, contexto_completo
    )

    # ---- Mensagem de boas-vindas ----
    if not st.session_state.nala_ia_messages:
        # Adicionar orientação baseada no modo
        orientacao = ""
        if modo == "🛒 Análise de Compra":
            orientacao = (
                "Estou no modo **Análise de Compra (Incubadora)**. "
                "Me envie o link ou HTML de um produto que está avaliando comprar. "
                "Vou calcular se vale o investimento para um lote de teste de 30 unidades."
            )
        elif modo == "⚔️ Otimização e Escala":
            orientacao = (
                "Estou no modo **Otimização e Escala (Combate)**. "
                "Me envie o link/HTML do **nosso anúncio** e do **concorrente líder**. "
                "Vou comparar ponto a ponto e montar o plano de ataque."
            )
        elif modo == "🔍 Auditoria de Anúncio":
            orientacao = (
                "Estou no modo **Auditoria Interna**. "
                "Me envie o HTML de edição do nosso anúncio. "
                "Vou varrer em busca de erros técnicos, SEO fraco e oportunidades perdidas."
            )
        else:
            orientacao = (
                "Pode me perguntar sobre vendas, margens, performance, "
                "análise de anúncios, ou qualquer dúvida sobre o negócio. "
                "Também posso consultar o Kanban e ajudar com tarefas."
            )

        boas_vindas = f"Olá, {nome}! Sou a **Nala Lupa** 🕵️‍♀️\n\n{orientacao}"
        st.session_state.nala_ia_messages.append({
            "role": "assistant",
            "content": boas_vindas
        })

    # ---- Renderizar histórico ----
    for msg in st.session_state.nala_ia_messages:
        _render_chat_message(msg["role"], msg["content"])

    # ---- Input do usuário ----
    prompt_usuario = st.chat_input("Pergunte à Nala Lupa...")

    if prompt_usuario:
        # Verificar se tem HTML anexado
        html_anexado = ""
        if uploaded_html:
            html_anexado = _processar_arquivo_html(uploaded_html)
            prompt_completo = f"{prompt_usuario}\n\n---\nHTML DO ANÚNCIO ANEXADO:\n{html_anexado}"
        else:
            prompt_completo = prompt_usuario

        # Adicionar contexto do modo
        if modo == "🛒 Análise de Compra" and "compra" not in prompt_usuario.lower():
            prompt_completo = f"[MODO: Análise de Compra/Incubadora - Lote teste 30un]\n{prompt_completo}"
        elif modo == "⚔️ Otimização e Escala":
            prompt_completo = f"[MODO: Otimização e Escala - Comparação com concorrente]\n{prompt_completo}"
        elif modo == "🔍 Auditoria de Anúncio":
            prompt_completo = f"[MODO: Auditoria Interna - Buscar erros e oportunidades]\n{prompt_completo}"

        # Mostrar mensagem do usuário
        st.session_state.nala_ia_messages.append({
            "role": "user",
            "content": prompt_usuario + ("\n\n📎 _HTML anexado_" if html_anexado else "")
        })
        _render_chat_message("user", prompt_usuario)

        # ---- Chamar Gemini ----
        with st.chat_message("assistant", avatar="🕵️‍♀️"):
            with st.spinner("Analisando..."):
                try:
                    # Montar histórico para o Gemini
                    historico_gemini = []
                    for msg in st.session_state.nala_ia_messages[:-1]:  # excluir última (acabou de adicionar)
                        gemini_role = "user" if msg["role"] == "user" else "model"
                        historico_gemini.append({
                            "role": gemini_role,
                            "parts": [msg["content"]]
                        })

                    # Iniciar chat com system prompt
                    chat = model.start_chat(history=historico_gemini)

                    # Enviar mensagem com system instruction no primeiro turno
                    if len(historico_gemini) <= 1:
                        mensagem_final = f"{system_prompt}\n\n---\nPERGUNTA DO USUÁRIO:\n{prompt_completo}"
                    else:
                        mensagem_final = prompt_completo

                    response = chat.send_message(mensagem_final)
                    resposta_ia = response.text

                    # Renderizar resposta
                    st.markdown(resposta_ia)

                    # Salvar no histórico
                    st.session_state.nala_ia_messages.append({
                        "role": "assistant",
                        "content": resposta_ia
                    })

                except Exception as e:
                    erro_msg = f"Erro ao consultar Gemini: {str(e)}"
                    st.error(erro_msg)
                    st.session_state.nala_ia_messages.append({
                        "role": "assistant",
                        "content": f"❌ {erro_msg}"
                    })


if __name__ == "__main__":
    main()
