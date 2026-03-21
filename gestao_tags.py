"""
GESTÃO DE TAGS - Sistema Nala
Versão: 2.0 (21/03/2026)

CHANGELOG v2.0:
  - REWRITE COMPLETO
  - 5 Tabs: Anúncios, Classificar, Produtos/SKU, Visão Geral, Config Tags
  - FIX: "só mostra ML" — agora popula dim_tags_anuncio de todos os marketplaces
  - NOVO: Tags flexíveis do cardápio (dim_tags_opcoes) em vez de 4 opções fixas
  - NOVO: Campo observações exposto na UI
  - NOVO: Tab Produtos/SKU — Curva ABC por SKU com filtros de loja/período
  - NOVO: Tab Config Tags — cadastrar/editar opções do cardápio
  - NOVO: Tab Visão Geral melhorada — filtros, cards, top 10

Versão 1.0:
  - 3 tabs: Lista, Atribuir, Visão Geral
  - Tags fixas (Novo/Escalando/Estável/Descontinuado)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database_utils import get_engine, recalcular_curva_abc

# ============================================================
# QUERIES AUXILIARES
# ============================================================

def _raw_query(engine, sql, params=None):
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        if cursor.description:
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return pd.DataFrame(rows, columns=cols)
        cursor.close()
        conn.close()
        return pd.DataFrame()
    except Exception:
        try:
            conn.close()
        except:
            pass
        return pd.DataFrame()


def _raw_execute(engine, sql, params=None):
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return affected
    except Exception:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return -1


def _buscar_opcoes_tags(engine, tipo='anuncio'):
    """Busca opções de tags ativas do cardápio."""
    df = _raw_query(engine,
        "SELECT nome_tag, cor FROM dim_tags_opcoes WHERE tipo = %s AND ativo = TRUE ORDER BY nome_tag",
        (tipo,))
    return df['nome_tag'].tolist() if not df.empty else []


def _buscar_marketplaces(engine):
    """Lista marketplaces com vendas."""
    df = _raw_query(engine,
        "SELECT DISTINCT marketplace FROM dim_lojas ORDER BY marketplace")
    return df['marketplace'].tolist() if not df.empty else []


def _buscar_lojas(engine, marketplace=None):
    """Lista lojas, opcionalmente filtradas por marketplace."""
    if marketplace and marketplace != 'Todos':
        df = _raw_query(engine,
            "SELECT loja FROM dim_lojas WHERE marketplace = %s ORDER BY loja",
            (marketplace,))
    else:
        df = _raw_query(engine, "SELECT loja FROM dim_lojas ORDER BY loja")
    return df['loja'].tolist() if not df.empty else []


# ============================================================
# POPULAR TAGS DE TODOS OS MARKETPLACES
# ============================================================

def _popular_tags_anuncio(engine):
    """
    Popula dim_tags_anuncio com todos os anúncios que existem em vendas
    mas ainda não estão na tabela de tags. Resolve o bug 'só mostra ML'.
    """
    sql = """
        INSERT INTO dim_tags_anuncio (marketplace, codigo_anuncio, sku, data_criacao)
        SELECT DISTINCT v.marketplace_origem, v.codigo_anuncio, MAX(v.sku)
        FROM fact_vendas_snapshot v
        WHERE v.codigo_anuncio IS NOT NULL AND TRIM(v.codigo_anuncio) != ''
          AND NOT EXISTS (
              SELECT 1 FROM dim_tags_anuncio t
              WHERE t.marketplace = v.marketplace_origem
                AND t.codigo_anuncio = v.codigo_anuncio
          )
        GROUP BY v.marketplace_origem, v.codigo_anuncio
    """
    return _raw_execute(engine, sql)


# ============================================================
# TAB 1: LISTA DE ANÚNCIOS TAGUEADOS
# ============================================================

def _buscar_tags_completo(engine, dias=30, marketplace=None, sem_status=False, busca=None):
    """Busca tags cruzando com vendas e nomes de produtos."""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    filtros = []
    params = [data_corte]

    if sem_status:
        filtros.append("(t.tag_status IS NULL OR TRIM(t.tag_status) = '' OR t.tag_status = 'None')")
    if marketplace and marketplace != 'Todos':
        filtros.append("t.marketplace = %s")
        params.append(marketplace)
    if busca:
        filtros.append("(t.sku ILIKE %s OR p.nome ILIKE %s)")
        params.extend([f"%{busca}%", f"%{busca}%"])

    where = (" AND " + " AND ".join(filtros)) if filtros else ""

    query = f"""
        SELECT
            t.marketplace as "Marketplace",
            t.codigo_anuncio as "Cód. Anúncio",
            t.sku as "SKU",
            COALESCE(p.nome, '') as "Produto",
            t.tag_curva as "Curva",
            t.tag_status as "Status",
            COALESCE(t.observacoes, '') as "Observações",
            COUNT(v.id) as "Vendas",
            COALESCE(SUM(v.valor_venda_efetivo), 0) as "Receita"
        FROM dim_tags_anuncio t
        LEFT JOIN dim_produtos p ON t.sku = p.sku
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= %s
        WHERE 1=1 {where}
        GROUP BY t.marketplace, t.codigo_anuncio, t.sku, p.nome,
                 t.tag_curva, t.tag_status, t.observacoes
        ORDER BY "Receita" DESC NULLS LAST
    """
    return _raw_query(engine, query, tuple(params))


def _salvar_edicoes_lote(engine, df_editado):
    """Salva tag_status e observacoes editadas."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for _, row in df_editado.iterrows():
            status = str(row.get("Status") or '') if row.get("Status") else None
            obs = str(row.get("Observações") or '') if row.get("Observações") else None
            cursor.execute("""
                UPDATE dim_tags_anuncio
                SET tag_status = %s, observacoes = %s, data_atualizacao = NOW()
                WHERE marketplace = %s AND codigo_anuncio = %s
            """, (status, obs, row["Marketplace"], row["Cód. Anúncio"]))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False


def tab_lista_tags(engine):
    st.subheader("📋 Catálogo Geral de Anúncios")

    # Filtros
    col1, col2, col3 = st.columns([1, 1, 2])
    dias = col1.selectbox("Período:", [30, 60, 90, 180],
                          format_func=lambda x: f"Últimos {x} dias", key="t1_dias")
    mktps = ['Todos'] + _buscar_marketplaces(engine)
    mktp = col2.selectbox("Marketplace:", mktps, key="t1_mktp")
    busca = col3.text_input("🔍 Busca (SKU ou Produto):", key="t1_busca")

    # Botão recalcular curva
    c_btn1, c_btn2 = st.columns([1, 3])
    with c_btn1:
        if st.button("🔄 Recalcular Curva ABC", key="btn_recalc"):
            with st.spinner("Recalculando..."):
                novos = _popular_tags_anuncio(engine)
                result = recalcular_curva_abc(engine, dias=dias)
                total = result.get('total_anuncios', 0)
                st.success(f"✅ {total} anúncios com curva recalculada. {novos} novos adicionados.")
                st.rerun()

    opcoes_tags = [''] + _buscar_opcoes_tags(engine, 'anuncio')

    df = _buscar_tags_completo(engine, dias=dias, marketplace=mktp if mktp != 'Todos' else None, busca=busca or None)

    if df.empty:
        st.info("Nenhum anúncio encontrado. Clique em 'Recalcular Curva ABC' para popular.")
        return

    st.caption(f"{len(df)} anúncios encontrados")

    df_editado = st.data_editor(
        df,
        column_config={
            "Status": st.column_config.SelectboxColumn("Status", options=opcoes_tags),
            "Observações": st.column_config.TextColumn("Observações", width="medium"),
            "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
            "Vendas": st.column_config.NumberColumn(format="%d"),
            "Curva": st.column_config.TextColumn(width="small"),
        },
        disabled=["Marketplace", "Cód. Anúncio", "SKU", "Produto", "Curva", "Vendas", "Receita"],
        hide_index=True,
        use_container_width=True,
        key="editor_lista_v2",
    )

    if st.button("💾 Salvar Alterações", type="primary", use_container_width=True, key="btn_salvar_t1"):
        if _salvar_edicoes_lote(engine, df_editado):
            st.success("✅ Tags e observações salvas!")
            st.rerun()


# ============================================================
# TAB 2: CLASSIFICAR (PENDENTES)
# ============================================================

def tab_atribuir_status(engine):
    st.subheader("🏷️ Anúncios Pendentes de Classificação")
    st.caption("Anúncios que ainda não possuem tag manual.")

    col1, col2 = st.columns([1, 2])
    mktp = col1.selectbox("Marketplace:", ['Todos'] + _buscar_marketplaces(engine), key="t2_mktp")
    busca = col2.text_input("🔍 Filtrar (SKU ou Produto):", key="t2_busca")

    df_sem = _buscar_tags_completo(engine, dias=90,
                                   marketplace=mktp if mktp != 'Todos' else None,
                                   sem_status=True, busca=busca or None)

    if df_sem.empty:
        st.success("✅ Todos os anúncios já possuem status manual.")
        return

    st.caption(f"{len(df_sem)} anúncios pendentes")
    opcoes_tags = [''] + _buscar_opcoes_tags(engine, 'anuncio')

    df_editado = st.data_editor(
        df_sem,
        column_config={
            "Status": st.column_config.SelectboxColumn(options=opcoes_tags),
            "Observações": st.column_config.TextColumn("Observações", width="medium"),
            "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
        },
        disabled=["Marketplace", "Cód. Anúncio", "SKU", "Produto", "Curva", "Vendas", "Receita"],
        hide_index=True,
        use_container_width=True,
        key="editor_atribuir_v2",
    )

    if st.button("💾 Atribuir Status", type="primary", use_container_width=True, key="btn_salvar_t2"):
        if _salvar_edicoes_lote(engine, df_editado):
            st.success("✅ Classificação salva!")
            st.rerun()


# ============================================================
# TAB 3: PRODUTOS / SKU
# ============================================================

def _calcular_curva_abc_sku(engine, dias=30, marketplace=None, loja=None):
    """Calcula curva ABC por SKU em tempo real conforme filtros."""
    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    filtros = ["v.data_venda >= %s", "v.sku IS NOT NULL", "TRIM(v.sku) != ''"]
    params = [data_corte]

    if marketplace and marketplace != 'Todos':
        filtros.append("v.marketplace_origem = %s")
        params.append(marketplace)
    if loja and loja != 'Todas':
        filtros.append("v.loja_origem = %s")
        params.append(loja)

    where = " AND ".join(filtros)

    sql = f"""
        SELECT v.sku,
               COALESCE(p.nome, '') as produto,
               SUM(v.quantidade) as qtd_vendas,
               SUM(v.valor_venda_efetivo) as receita,
               AVG(v.margem_percentual) as margem_media,
               COUNT(DISTINCT v.loja_origem) as lojas,
               COUNT(DISTINCT v.marketplace_origem) as marketplaces,
               COALESCE(ts.tag_status, '') as tag_status,
               COALESCE(ts.observacoes, '') as observacoes
        FROM fact_vendas_snapshot v
        LEFT JOIN dim_produtos p ON v.sku = p.sku
        LEFT JOIN dim_tags_sku ts ON v.sku = ts.sku
        WHERE {where}
        GROUP BY v.sku, p.nome, ts.tag_status, ts.observacoes
        ORDER BY receita DESC
    """
    df = _raw_query(engine, sql, tuple(params))
    if df.empty:
        return df

    # Calcular curva ABC
    receita_total = df['receita'].sum()
    if receita_total > 0:
        df['pct_acumulado'] = df['receita'].cumsum() / receita_total * 100
        df['curva'] = 'C'
        df.loc[df['pct_acumulado'] <= 80, 'curva'] = 'A'
        df.loc[(df['pct_acumulado'] > 80) & (df['pct_acumulado'] <= 95), 'curva'] = 'B'
    else:
        df['curva'] = 'C'
        df['pct_acumulado'] = 0

    return df


def _salvar_tags_sku_lote(engine, df_editado):
    """Salva tags de SKU editadas."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for _, row in df_editado.iterrows():
            status = row.get('Tag') or None
            obs = row.get('Observações') or None
            curva = row.get('Curva') or None
            cursor.execute("""
                INSERT INTO dim_tags_sku (sku, tag_curva, tag_status, observacoes, data_atualizacao)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (sku)
                DO UPDATE SET tag_curva = EXCLUDED.tag_curva,
                              tag_status = EXCLUDED.tag_status,
                              observacoes = EXCLUDED.observacoes,
                              data_atualizacao = NOW()
            """, (row['SKU'], curva, status, obs))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False


def tab_produtos_sku(engine):
    st.subheader("📦 Tags e Curva ABC por Produto/SKU")
    st.caption("Classificação calculada em tempo real conforme os filtros selecionados.")

    # Filtros
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
    dias = col1.selectbox("Período:", [30, 60, 90, 180],
                          format_func=lambda x: f"{x} dias", key="t3_dias")
    mktps = ['Todos'] + _buscar_marketplaces(engine)
    mktp = col2.selectbox("Marketplace:", mktps, key="t3_mktp")
    lojas = ['Todas'] + _buscar_lojas(engine, mktp if mktp != 'Todos' else None)
    loja = col3.selectbox("Loja:", lojas, key="t3_loja")
    busca = col4.text_input("🔍 Busca (SKU ou Produto):", key="t3_busca")

    df = _calcular_curva_abc_sku(engine, dias=dias,
                                  marketplace=mktp if mktp != 'Todos' else None,
                                  loja=loja if loja != 'Todas' else None)

    if df.empty:
        st.info("Nenhuma venda encontrada para os filtros selecionados.")
        return

    if busca:
        df = df[df['sku'].str.contains(busca, case=False, na=False) |
                df['produto'].str.contains(busca, case=False, na=False)]

    # Resumo no topo
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total SKUs", len(df))
    c2.metric("Curva A", len(df[df['curva'] == 'A']))
    c3.metric("Curva B", len(df[df['curva'] == 'B']))
    c4.metric("Curva C", len(df[df['curva'] == 'C']))

    # Preparar para exibição
    df_display = df[['sku', 'produto', 'curva', 'tag_status', 'qtd_vendas',
                      'receita', 'margem_media', 'lojas', 'observacoes']].copy()
    df_display.columns = ['SKU', 'Produto', 'Curva', 'Tag', 'Vendas',
                          'Receita', 'Margem %', 'Lojas', 'Observações']
    df_display['Margem %'] = pd.to_numeric(df_display['Margem %'], errors='coerce').fillna(0).round(1)

    opcoes_tags_sku = [''] + _buscar_opcoes_tags(engine, 'sku')

    df_editado = st.data_editor(
        df_display,
        column_config={
            "Tag": st.column_config.SelectboxColumn(options=opcoes_tags_sku),
            "Observações": st.column_config.TextColumn(width="medium"),
            "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
            "Vendas": st.column_config.NumberColumn(format="%d"),
            "Margem %": st.column_config.NumberColumn(format="%.1f%%"),
            "Curva": st.column_config.TextColumn(width="small", disabled=True),
        },
        disabled=["SKU", "Produto", "Curva", "Vendas", "Receita", "Margem %", "Lojas"],
        hide_index=True,
        use_container_width=True,
        key="editor_sku_v2",
    )

    if st.button("💾 Salvar Tags de Produto", type="primary", use_container_width=True, key="btn_salvar_t3"):
        if _salvar_tags_sku_lote(engine, df_editado):
            st.success("✅ Tags de produtos salvas!")
            st.rerun()


# ============================================================
# TAB 4: VISÃO GERAL (MELHORADA)
# ============================================================

def tab_visao_geral(engine):
    st.subheader("📊 Indicadores de Tags")

    # Filtros
    col1, col2 = st.columns([1, 1])
    dias = col1.selectbox("Período:", [30, 60, 90, 180],
                          format_func=lambda x: f"Últimos {x} dias", key="t4_dias")
    mktps = ['Todos'] + _buscar_marketplaces(engine)
    mktp = col2.selectbox("Marketplace:", mktps, key="t4_mktp")

    data_corte = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

    mktp_filter = ""
    params = [data_corte]
    if mktp != 'Todos':
        mktp_filter = "AND v.marketplace_origem = %s"
        params.append(mktp)

    # Cards resumo
    sql_resumo = f"""
        SELECT
            COUNT(DISTINCT t.codigo_anuncio) as total_anuncios,
            COUNT(DISTINCT CASE WHEN t.tag_status IS NULL OR TRIM(t.tag_status) = ''
                  THEN t.codigo_anuncio END) as sem_tag,
            COALESCE(SUM(v.valor_venda_efetivo), 0) as receita_total,
            COALESCE(COUNT(v.id), 0) as total_vendas
        FROM dim_tags_anuncio t
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= %s {mktp_filter}
    """
    df_resumo = _raw_query(engine, sql_resumo, tuple(params))

    if not df_resumo.empty:
        r = df_resumo.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Anúncios", int(r.get('total_anuncios', 0)))
        c2.metric("Sem Tag", int(r.get('sem_tag', 0)))
        receita = float(r.get('receita_total', 0))
        c3.metric("Receita Total", f"R$ {receita:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        c4.metric("Total Vendas", int(r.get('total_vendas', 0)))

    st.divider()

    # Receita por Curva e por Status
    sql_curva = f"""
        SELECT COALESCE(t.tag_curva, 'Sem Curva') as curva,
               COUNT(DISTINCT t.codigo_anuncio) as anuncios,
               COALESCE(SUM(v.valor_venda_efetivo), 0) as receita,
               COALESCE(COUNT(v.id), 0) as vendas
        FROM dim_tags_anuncio t
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= %s {mktp_filter}
        GROUP BY t.tag_curva
        ORDER BY receita DESC
    """
    sql_status = f"""
        SELECT COALESCE(NULLIF(TRIM(t.tag_status), ''), 'Sem Status') as status,
               COUNT(DISTINCT t.codigo_anuncio) as anuncios,
               COALESCE(SUM(v.valor_venda_efetivo), 0) as receita,
               COALESCE(COUNT(v.id), 0) as vendas
        FROM dim_tags_anuncio t
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= %s {mktp_filter}
        GROUP BY status
        ORDER BY receita DESC
    """

    c_left, c_right = st.columns(2)

    with c_left:
        st.markdown("**💰 Receita por Curva**")
        df_curva = _raw_query(engine, sql_curva, tuple(params))
        if not df_curva.empty:
            df_curva.columns = ['Curva', 'Anúncios', 'Receita', 'Vendas']
            receita_total = df_curva['Receita'].sum()
            df_curva['% Total'] = (df_curva['Receita'] / receita_total * 100).round(1) if receita_total > 0 else 0
            st.dataframe(df_curva, column_config={
                "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
                "% Total": st.column_config.NumberColumn(format="%.1f%%"),
            }, hide_index=True, use_container_width=True)

    with c_right:
        st.markdown("**🏷️ Receita por Status**")
        df_status = _raw_query(engine, sql_status, tuple(params))
        if not df_status.empty:
            df_status.columns = ['Status', 'Anúncios', 'Receita', 'Vendas']
            receita_total = df_status['Receita'].sum()
            df_status['% Total'] = (df_status['Receita'] / receita_total * 100).round(1) if receita_total > 0 else 0
            st.dataframe(df_status, column_config={
                "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
                "% Total": st.column_config.NumberColumn(format="%.1f%%"),
            }, hide_index=True, use_container_width=True)

    st.divider()

    # Top 10 Curva A
    st.markdown("**🏆 Top 10 Anúncios Curva A**")
    sql_top = f"""
        SELECT t.marketplace as "Marketplace", t.codigo_anuncio as "Anúncio",
               t.sku as "SKU", COALESCE(p.nome, '') as "Produto",
               COALESCE(SUM(v.valor_venda_efetivo), 0) as "Receita",
               COALESCE(SUM(v.quantidade), 0) as "Vendas"
        FROM dim_tags_anuncio t
        LEFT JOIN dim_produtos p ON t.sku = p.sku
        LEFT JOIN fact_vendas_snapshot v ON t.marketplace = v.marketplace_origem
            AND t.codigo_anuncio = v.codigo_anuncio
            AND v.data_venda >= %s {mktp_filter}
        WHERE t.tag_curva = 'A'
        GROUP BY t.marketplace, t.codigo_anuncio, t.sku, p.nome
        ORDER BY "Receita" DESC
        LIMIT 10
    """
    df_top = _raw_query(engine, sql_top, tuple(params))
    if not df_top.empty:
        st.dataframe(df_top, column_config={
            "Receita": st.column_config.NumberColumn(format="R$ %.2f"),
            "Vendas": st.column_config.NumberColumn(format="%d"),
        }, hide_index=True, use_container_width=True)
    else:
        st.info("Nenhum anúncio classificado como Curva A. Recalcule a curva ABC na tab Anúncios.")


# ============================================================
# TAB 5: CONFIG TAGS
# ============================================================

def tab_config_tags(engine):
    st.subheader("⚙️ Configurar Opções de Tags")
    st.caption("Gerencie o cardápio de tags disponíveis para anúncios e produtos/SKU.")

    # Listar tags existentes
    df_tags = _raw_query(engine,
        "SELECT id, nome_tag, tipo, cor, ativo FROM dim_tags_opcoes ORDER BY tipo, nome_tag")

    if not df_tags.empty:
        st.markdown("**Tags cadastradas:**")

        # Separar por tipo
        for tipo in ['anuncio', 'sku']:
            label = "📢 Anúncios" if tipo == 'anuncio' else "📦 Produtos/SKU"
            df_tipo = df_tags[df_tags['tipo'] == tipo].copy()
            if df_tipo.empty:
                continue

            st.markdown(f"**{label}:**")
            for _, row in df_tipo.iterrows():
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                cor = row['cor'] or '#6B7280'
                col1.markdown(f"<span style='color:{cor}; font-weight:bold;'>●</span> {row['nome_tag']}",
                              unsafe_allow_html=True)
                col2.text(row['tipo'])
                status_text = "✅ Ativa" if row['ativo'] else "❌ Inativa"
                col3.text(status_text)
                if row['ativo']:
                    if col4.button("Desativar", key=f"desativar_{row['id']}"):
                        _raw_execute(engine,
                            "UPDATE dim_tags_opcoes SET ativo = FALSE WHERE id = %s", (int(row['id']),))
                        st.rerun()
                else:
                    if col4.button("Ativar", key=f"ativar_{row['id']}"):
                        _raw_execute(engine,
                            "UPDATE dim_tags_opcoes SET ativo = TRUE WHERE id = %s", (int(row['id']),))
                        st.rerun()

    st.divider()

    # Cadastrar nova tag
    st.markdown("**➕ Nova Tag:**")
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    nome_nova = col1.text_input("Nome da tag:", key="nova_tag_nome")
    tipo_nova = col2.selectbox("Tipo:", ['anuncio', 'sku'], key="nova_tag_tipo")
    cor_nova = col3.color_picker("Cor:", '#3B82F6', key="nova_tag_cor")
    col4.markdown("<br>", unsafe_allow_html=True)
    if col4.button("Cadastrar", key="btn_cadastrar_tag"):
        if nome_nova.strip():
            result = _raw_execute(engine, """
                INSERT INTO dim_tags_opcoes (nome_tag, tipo, cor)
                VALUES (%s, %s, %s)
                ON CONFLICT (nome_tag, tipo) DO NOTHING
            """, (nome_nova.strip(), tipo_nova, cor_nova))
            if result >= 0:
                st.success(f"✅ Tag '{nome_nova}' cadastrada!")
                st.rerun()
            else:
                st.error("Erro ao cadastrar tag.")
        else:
            st.warning("Informe o nome da tag.")


# ============================================================
# MAIN
# ============================================================

def main():
    st.title("🏷️ Gestão de Tags Nala")
    engine = get_engine()

    # Auto-popular tags na primeira vez
    count_tags = _raw_query(engine, "SELECT COUNT(*) as c FROM dim_tags_anuncio")
    if not count_tags.empty and int(count_tags.iloc[0]['c']) == 0:
        with st.spinner("Populando anúncios pela primeira vez..."):
            novos = _popular_tags_anuncio(engine)
            recalcular_curva_abc(engine, dias=90)
            st.toast(f"✅ {novos} anúncios adicionados e curva ABC calculada!")

    t1, t2, t3, t4, t5 = st.tabs([
        "📋 Anúncios",
        "🏷️ Classificar",
        "📦 Produtos/SKU",
        "📊 Visão Geral",
        "⚙️ Config Tags",
    ])

    with t1:
        tab_lista_tags(engine)
    with t2:
        tab_atribuir_status(engine)
    with t3:
        tab_produtos_sku(engine)
    with t4:
        tab_visao_geral(engine)
    with t5:
        tab_config_tags(engine)


if __name__ == "__main__":
    main()
