import streamlit as st
import os
import importlib
import pandas as pd
from datetime import datetime, date, timedelta

# ============================================================
# CONFIGURAÇÃO DE PÁGINA
# ============================================================
st.set_page_config(page_title="Sistema Nala - Gestão", layout="wide", page_icon="🏪")

# ESTILO VISUAL NALA (Premium Corporate v2)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', -apple-system, sans-serif; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #001a38 0%, #002b5e 100%);
        border-right: 1px solid rgba(212,175,55,0.25);
    }
    [data-testid="stSidebar"] * { color: #e0e4e8 !important; }
    [data-testid="stSidebar"] .stRadio label:hover { color: #d4af37 !important; }
    [data-testid="stSidebar"] hr { border-color: rgba(212,175,55,0.2); margin: 8px 0; }

    /* Metrics */
    [data-testid="stMetric"] {
        background: #ffffff; padding: 14px 18px; border-radius: 6px;
        border-left: 3px solid #d4af37;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    [data-testid="stMetricLabel"] { font-size: 0.78rem; color: #5a6a7a; font-weight: 500; }
    [data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 700; color: #0a1f3c; }

    /* Buttons */
    .stButton>button {
        border-radius: 6px; background: #002b5e; color: #ffffff !important;
        font-weight: 600; border: none; padding: 8px 20px;
        transition: all 0.2s ease; letter-spacing: 0.01em;
    }
    .stButton>button:hover { background: #003d82; box-shadow: 0 2px 8px rgba(0,43,94,0.3); }

    /* Panorama table */
    .panorama-container {
        background: #fff; border-radius: 8px; padding: 0;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06); overflow: hidden;
        margin-top: 6px;
    }
    .panorama-title {
        font-size: 0.85rem; font-weight: 600; color: #0a1f3c;
        padding: 14px 18px 10px; margin: 0; border-bottom: 1px solid #eef1f5;
    }
    .panorama-table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    .panorama-table th {
        background: #f4f6f9; color: #3a4a5c; padding: 9px 14px;
        text-align: left; font-weight: 600; font-size: 0.76rem;
        text-transform: uppercase; letter-spacing: 0.04em;
        border-bottom: 2px solid #e2e6ec;
    }
    .panorama-table td {
        padding: 9px 14px; border-bottom: 1px solid #f0f2f5; color: #1a2a3a;
    }
    .panorama-table tr:hover td { background: #fafbfd; }
    .panorama-table .row-total td {
        font-weight: 700; background: #f8f9fb; border-top: 2px solid #d4af37;
        color: #0a1f3c; font-size: 0.83rem;
    }
    .pct-up { color: #0e8a3e; font-weight: 600; }
    .pct-down { color: #d63031; font-weight: 600; }
    .pct-zero { color: #7a8a9a; font-weight: 500; }

    /* General refinements */
    h1 { font-weight: 700; letter-spacing: -0.02em; color: #0a1f3c; }
    .stDivider { margin: 12px 0 !important; }
    </style>
""", unsafe_allow_html=True)


# ============================================================
# CONEXÃO COM BANCO — agnóstico ao ambiente
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
    """Autentica usuário contra dim_usuarios usando bcrypt."""
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

        if not bcrypt.checkpw(senha.encode('utf-8'), password_hash.encode('utf-8')):
            cursor.close()
            conn.close()
            return None

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
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_usuarios WHERE ativo = TRUE")
        total = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return total
    except Exception:
        return -1


def _criar_primeiro_admin(username: str, senha: str, engine) -> bool:
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
# HELPERS DE MESES
# ============================================================

_MESES_PT = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']


def _gerar_opcoes_meses(n=12):
    """Gera dict {label: 'YYYY-MM'} dos últimos N meses para selectbox."""
    hoje = date.today()
    opcoes = {}
    for i in range(n):
        ano = hoje.year
        mes = hoje.month - i
        while mes < 1:
            mes += 12
            ano -= 1
        label = f"{_MESES_PT[mes - 1]} {ano}"
        valor = f"{ano:04d}-{mes:02d}"
        opcoes[label] = valor
    return opcoes


def _ano_mes_para_datas(ano_mes):
    """Retorna (primeiro_dia, ultimo_dia) como date objects."""
    from calendar import monthrange
    ano, mes = int(ano_mes[:4]), int(ano_mes[5:7])
    primeiro = date(ano, mes, 1)
    _, dias = monthrange(ano, mes)
    ultimo = date(ano, mes, dias)
    return primeiro, ultimo


def _mes_anterior(ano_mes):
    """Retorna ano_mes string do mês anterior."""
    ano, mes = int(ano_mes[:4]), int(ano_mes[5:7])
    mes -= 1
    if mes < 1:
        mes = 12
        ano -= 1
    return f"{ano:04d}-{mes:02d}"


# ============================================================
# PAINEL DE INÍCIO — MÉTRICAS
# ============================================================

def _buscar_metricas_inicio(engine, ano_mes=None):
    """Busca métricas do mês selecionado e anterior para o painel."""
    from permissoes import ve_todas_lojas, get_lojas_usuario

    if ano_mes is None:
        ano_mes = date.today().strftime('%Y-%m')

    primeiro_sel, ultimo_sel = _ano_mes_para_datas(ano_mes)
    mes_ant = _mes_anterior(ano_mes)
    primeiro_ant, ultimo_ant = _ano_mes_para_datas(mes_ant)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        where_loja = ""
        params_loja = []
        if not ve_todas_lojas():
            lojas = get_lojas_usuario(engine)
            if lojas:
                placeholders = ', '.join(['%s'] * len(lojas))
                where_loja = f" AND loja_origem IN ({placeholders})"
                params_loja = list(lojas)

        # Mês selecionado
        cursor.execute(f"""
            SELECT COALESCE(SUM(valor_venda_efetivo), 0),
                   COALESCE(COUNT(*), 0),
                   COALESCE(AVG(margem_percentual), 0)
            FROM fact_vendas_snapshot
            WHERE data_venda >= %s AND data_venda <= %s {where_loja}
        """, [primeiro_sel, ultimo_sel] + params_loja)
        fat_sel, ped_sel, margem_sel = cursor.fetchone()

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

        var_fat = ((float(fat_sel) / float(fat_ant) - 1) * 100) if fat_ant > 0 else 0
        var_ped = ((int(ped_sel) / int(ped_ant) - 1) * 100) if ped_ant > 0 else 0
        var_margem = float(margem_sel) - float(margem_ant)

        def fmt_brl(v):
            v = float(v)
            if v >= 1000:
                return f"R$ {v/1000:,.1f}k".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        return {
            'faturamento': fmt_brl(fat_sel),
            'var_fat': f"{var_fat:+.1f}%",
            'pedidos': f"{int(ped_sel):,}".replace(",", "."),
            'var_ped': f"{var_ped:+.1f}%",
            'margem': f"{float(margem_sel):.1f}%",
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
# PANORAMA DE VENDAS POR LOJA
# ============================================================

def _buscar_metas_panorama(engine, ano_mes):
    """Busca metas de todas as lojas para o mês. Retorna dict {loja: {meta_receita, modelo}}."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT loja_origem, meta_receita, modelo_projecao FROM dim_metas_loja WHERE ano_mes = %s",
            (ano_mes,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            row[0]: {'meta_receita': float(row[1] or 0), 'modelo': row[2] or 'Linear'}
            for row in rows
        }
    except Exception:
        return {}


def _buscar_panorama_lojas(engine, ano_mes=None):
    """
    Busca panorama de vendas por loja para o mês selecionado.
    Retorna rows: (loja, ult_att, max_mes_sel, vendas_sel, fat_sel, fat_ant_total, fat_ant_prop)
    """
    from permissoes import ve_todas_lojas, get_lojas_usuario

    if ano_mes is None:
        ano_mes = date.today().strftime('%Y-%m')

    primeiro_sel, ultimo_sel = _ano_mes_para_datas(ano_mes)
    mes_ant = _mes_anterior(ano_mes)
    primeiro_ant, ultimo_ant = _ano_mes_para_datas(mes_ant)
    fallback_ant = primeiro_ant - timedelta(days=1)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Filtro RBAC — duas versões (CTE sem alias, SELECT principal com alias f.)
        where_loja_cte = ""
        where_loja_main = ""
        params_loja = []
        if not ve_todas_lojas():
            lojas = get_lojas_usuario(engine)
            if lojas:
                placeholders = ', '.join(['%s'] * len(lojas))
                where_loja_cte = f" AND loja_origem IN ({placeholders})"
                where_loja_main = f" AND f.loja_origem IN ({placeholders})"
                params_loja = list(lojas)

        query = f"""
            WITH max_dates AS (
                SELECT loja_origem,
                       MAX(data_venda) FILTER (WHERE data_venda >= %s AND data_venda <= %s) AS max_mes_sel
                FROM fact_vendas_snapshot
                WHERE 1=1 {where_loja_cte}
                GROUP BY loja_origem
            )
            SELECT
                f.loja_origem,
                MAX(f.data_venda)                                                                                AS ultima_att,
                md.max_mes_sel,
                COUNT(*)           FILTER (WHERE f.data_venda >= %s AND f.data_venda <= %s)                      AS vendas_sel,
                COALESCE(SUM(f.valor_venda_efetivo) FILTER (WHERE f.data_venda >= %s AND f.data_venda <= %s), 0) AS fat_sel,
                COALESCE(SUM(f.valor_venda_efetivo) FILTER (WHERE f.data_venda >= %s AND f.data_venda <= %s), 0) AS fat_ant_total,
                COALESCE(SUM(f.valor_venda_efetivo) FILTER (
                    WHERE f.data_venda >= %s
                      AND f.data_venda <= COALESCE(md.max_mes_sel - INTERVAL '1 month', %s::date)
                ), 0)                                                                                            AS fat_ant_prop
            FROM fact_vendas_snapshot f
            LEFT JOIN max_dates md ON f.loja_origem = md.loja_origem
            WHERE 1=1 {where_loja_main}
            GROUP BY f.loja_origem, md.max_mes_sel
            ORDER BY f.loja_origem
        """

        params = [
            primeiro_sel, ultimo_sel,
        ] + params_loja + [
            primeiro_sel, ultimo_sel,
            primeiro_sel, ultimo_sel,
            primeiro_ant, ultimo_ant,
            primeiro_ant, fallback_ant,
        ] + params_loja

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return rows

    except Exception as e:
        st.error(f"Erro ao buscar panorama: {e}")
        return []


def _renderizar_panorama(rows, metas, ano_mes, engine):
    """Renderiza tabela HTML do panorama com Meta, Projetado e Performance."""
    from performance_utils import calcular_projecao, get_dias_vendas

    # Nomes dos meses
    ano_sel, mes_sel = int(ano_mes[:4]), int(ano_mes[5:7])
    nome_mes_sel = _MESES_PT[mes_sel - 1]
    mes_ant_str = _mes_anterior(ano_mes)
    mes_ant_num = int(mes_ant_str[5:7])
    nome_mes_ant = _MESES_PT[mes_ant_num - 1]

    _, ultimo_sel = _ano_mes_para_datas(ano_mes)
    dias_mes = ultimo_sel.day  # total de dias no mês

    def fmt_brl(v):
        v = float(v)
        formatted = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {formatted}"

    def fmt_brl_short(v):
        v = float(v)
        if v >= 1000:
            return f"R$ {v/1000:,.1f}k".replace(",", "X").replace(".", ",").replace("X", ".")
        return fmt_brl(v)

    def fmt_int(v):
        return f"{int(v):,}".replace(",", ".")

    def fmt_pct_vs(fat_sel, fat_ant_prop, max_date_sel):
        fat_sel = float(fat_sel)
        fat_ant_prop = float(fat_ant_prop)
        if max_date_sel is None:
            return '<span class="pct-zero">—</span>'

        ref_str = f'<span style="color:#9aa5b4;font-size:0.72rem;display:block;margin-top:1px">até dia {max_date_sel.day:02d}</span>'

        if fat_ant_prop == 0:
            if fat_sel > 0:
                return f'<span class="pct-up">▲ novo</span>{ref_str}'
            return '<span class="pct-zero">—</span>'
        pct = (fat_sel / fat_ant_prop - 1) * 100
        if pct > 0:
            return f'<span class="pct-up">▲ +{pct:.1f}%</span>{ref_str}'
        elif pct < 0:
            return f'<span class="pct-down">▼ {pct:.1f}%</span>{ref_str}'
        return f'<span class="pct-zero">0,0%</span>{ref_str}'

    def fmt_perf(perf):
        if perf is None:
            return '<span class="pct-zero">—</span>'
        if perf >= 100:
            return f'<span class="pct-up">🟢 {perf:.1f}%</span>'
        elif perf >= 80:
            return f'<span style="color:#e67e22;font-weight:600">🟡 {perf:.1f}%</span>'
        else:
            return f'<span class="pct-down">🔴 {perf:.1f}%</span>'

    # Header
    html = f'''<div class="panorama-container">
    <div class="panorama-title">Panorama de Vendas por Loja — {nome_mes_sel} {ano_sel}</div>
    <table class="panorama-table">
    <thead><tr>
        <th>Loja</th>
        <th>Últ. Att</th>
        <th style="text-align:right">Fat. {nome_mes_ant}</th>
        <th style="text-align:right">% vs {nome_mes_ant}</th>
        <th style="text-align:right">Meta {nome_mes_sel}</th>
        <th style="text-align:right">Vendas {nome_mes_sel}</th>
        <th style="text-align:right">Fat. {nome_mes_sel}</th>
        <th style="text-align:right">Projetado</th>
        <th style="text-align:right">Performance</th>
    </tr></thead><tbody>'''

    # Acumuladores para linha total
    tot_vendas = 0
    tot_fat_sel = 0.0
    tot_fat_ant = 0.0
    tot_fat_ant_prop = 0.0
    tot_meta = 0.0
    tot_projetado = 0.0
    tot_projetado_com_meta = 0.0  # apenas lojas COM meta — usado no cálculo de performance

    for loja, ult_att, max_mes, vendas, fat_sel, fat_ant_t, fat_ant_p in rows:
        ult_str = ult_att.strftime('%d/%m/%Y') if ult_att else '—'
        fat_sel_f = float(fat_sel)
        vendas_int = int(vendas)

        tot_vendas += vendas_int
        tot_fat_sel += fat_sel_f
        tot_fat_ant += float(fat_ant_t)
        tot_fat_ant_prop += float(fat_ant_p)

        # Meta e modelo da loja
        meta_info = metas.get(loja)
        meta_receita = meta_info['meta_receita'] if meta_info else 0
        modelo = meta_info['modelo'] if meta_info else 'Linear'

        # Projetado baseado na última data de vendas lançadas para ESTA loja
        projetado = 0.0
        if max_mes is not None and fat_sel_f > 0:
            max_date = max_mes
            if isinstance(max_date, datetime):
                max_date = max_date.date()
            dias_vendas, _ = get_dias_vendas(ano_mes, data_ref=max_date)
            projetado = calcular_projecao(fat_sel_f, dias_vendas, dias_mes, modelo)

        # Performance = projetado / meta
        perf = (projetado / meta_receita * 100) if meta_receita > 0 and projetado > 0 else None

        tot_meta += meta_receita
        tot_projetado += projetado
        if meta_receita > 0:
            tot_projetado_com_meta += projetado

        # Formatação
        meta_html = fmt_brl_short(meta_receita) if meta_receita > 0 else '<span class="pct-zero">—</span>'
        proj_html = fmt_brl_short(projetado) if projetado > 0 else '<span class="pct-zero">—</span>'

        html += f'''<tr>
            <td><strong>{loja}</strong></td>
            <td>{ult_str}</td>
            <td style="text-align:right">{fmt_brl(fat_ant_t)}</td>
            <td style="text-align:right">{fmt_pct_vs(fat_sel, fat_ant_p, max_mes)}</td>
            <td style="text-align:right">{meta_html}</td>
            <td style="text-align:right">{fmt_int(vendas_int)}</td>
            <td style="text-align:right">{fmt_brl(fat_sel_f)}</td>
            <td style="text-align:right">{proj_html}</td>
            <td style="text-align:right">{fmt_perf(perf)}</td>
        </tr>'''

    # ─── Linha TOTAL ───
    tot_pct_html = '<span class="pct-zero">—</span>'
    if tot_fat_ant_prop > 0:
        tot_pct = (tot_fat_sel / tot_fat_ant_prop - 1) * 100
        if tot_pct > 0:
            tot_pct_html = f'<span class="pct-up">▲ +{tot_pct:.1f}%</span>'
        elif tot_pct < 0:
            tot_pct_html = f'<span class="pct-down">▼ {tot_pct:.1f}%</span>'
        else:
            tot_pct_html = '<span class="pct-zero">0,0%</span>'
    elif tot_fat_sel > 0:
        tot_pct_html = '<span class="pct-up">▲ novo</span>'

    tot_meta_html = fmt_brl_short(tot_meta) if tot_meta > 0 else '<span class="pct-zero">—</span>'
    tot_proj_html = fmt_brl_short(tot_projetado) if tot_projetado > 0 else '<span class="pct-zero">—</span>'
    # Performance total: considera APENAS projetado de lojas que têm meta cadastrada
    tot_perf = (tot_projetado_com_meta / tot_meta * 100) if tot_meta > 0 and tot_projetado_com_meta > 0 else None

    html += f'''<tr class="row-total">
        <td>TOTAL</td>
        <td></td>
        <td style="text-align:right">{fmt_brl(tot_fat_ant)}</td>
        <td style="text-align:right">{tot_pct_html}</td>
        <td style="text-align:right">{tot_meta_html}</td>
        <td style="text-align:right">{fmt_int(tot_vendas)}</td>
        <td style="text-align:right">{fmt_brl(tot_fat_sel)}</td>
        <td style="text-align:right">{tot_proj_html}</td>
        <td style="text-align:right">{fmt_perf(tot_perf)}</td>
    </tr>'''

    html += '</tbody></table></div>'

    st.markdown(html, unsafe_allow_html=True)

    # Nota explicativa
    lojas_com_meta = sum(1 for loja, *_ in rows if metas.get(loja, {}).get('meta_receita', 0) > 0)
    total_lojas = len(rows)
    if lojas_com_meta > 0 and lojas_com_meta < total_lojas:
        st.caption(f"ℹ️ Performance total calculada apenas sobre as {lojas_com_meta} loja(s) com meta cadastrada. "
                   f"Lojas sem meta contribuem para o faturamento e projetado geral, mas não entram no cálculo de performance.")


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
            st.image("logo.png", width=130)
        else:
            st.markdown("<h2 style='color: #d4af37; text-align: center; margin:0;'>NALA</h2>",
                        unsafe_allow_html=True)

        if _is_dev_environment():
            st.warning("⚠️ AMBIENTE DE DESENVOLVIMENTO (TESTES)")

        st.markdown("---")

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

        # ─── SELETOR DE MÊS ───
        opcoes_meses = _gerar_opcoes_meses(12)
        col_mes, col_space = st.columns([1, 3])
        with col_mes:
            mes_sel_label = st.selectbox("📅 Mês de referência:",
                                         list(opcoes_meses.keys()),
                                         key="painel_mes_ref")
        ano_mes = opcoes_meses[mes_sel_label]
        nome_mes = mes_sel_label.split()[0]  # ex: "Abril"
        mes_ant_num = int(_mes_anterior(ano_mes)[5:7])
        nome_mes_ant = _MESES_PT[mes_ant_num - 1]

        # ─── MÉTRICAS ───
        m = _buscar_metricas_inicio(engine, ano_mes)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Faturamento {nome_mes}", m['faturamento'], m['var_fat'])
        c2.metric(f"Pedidos {nome_mes}", m['pedidos'], m['var_ped'])
        c3.metric("Margem Média", m['margem'], m['var_margem'])
        c4.metric("SKUs na Base", m['skus'])

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

        # ─── PANORAMA ───
        rows = _buscar_panorama_lojas(engine, ano_mes)
        metas_pan = _buscar_metas_panorama(engine, ano_mes)

        if rows:
            _renderizar_panorama(rows, metas_pan, ano_mes, engine)
        else:
            st.info("Nenhuma venda encontrada para exibir o panorama.")

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
    if 'logado' not in st.session_state:
        st.session_state.logado = False
    if 'usuario' not in st.session_state:
        st.session_state.usuario = {}

    engine = get_engine()
    _garantir_tabela_usuario_lojas(engine)

    if not st.session_state.logado:
        total = _contar_usuarios(engine)
        if total == 0:
            _tela_setup_inicial(engine)
        else:
            _tela_login(engine)
    else:
        _area_logada(engine)


def _garantir_tabela_usuario_lojas(engine):
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
        pass


if __name__ == "__main__":
    main()
