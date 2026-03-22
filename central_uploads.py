"""
CENTRAL DE UPLOADS - Sistema Nala
VERSAO 3.5 (21/03/2026):
  - NOVO: Colunas Curva ABC e Tag Manual nas Vendas Consolidadas + Export Excel
  - NOVO: Fix Magalu codigo_anuncio vazio (preenche com SKU em runtime)

VERSAO 3.4 (17/03/2026):
  - NOVO: Detecção automática de devoluções ao reimportar (todos marketplaces)
         Se pedido mudou de "entregue" para "devolvido/cancelado", move para fact_devolucoes
  - NOVO: Excluir lançamento individual do histórico (com vendas associadas)
  - NOVO: Reprocessar lançamento para outra loja (corrigir loja errada)
  - NOVO: Correções pontuais em vendas individuais (tab Consolidadas)
  - FIX: Periodo obrigatorio APENAS para Amazon (Shein/Magalu auto-detectam)
  - FIX: Preview com valores formatados (2 casas decimais)
  - FIX: Historico reprocessados usa buscar_pendentes_revisados
  - Pedido original exibido na tabela de vendas consolidadas
  - Integracao Shein e Magalu
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import (
    get_engine, gravar_log_upload, buscar_pendentes, buscar_pendentes_resumo,
    reprocessar_pendentes_por_sku, recalcular_curva_abc, buscar_pendentes_por_tipo,
    reprocessar_pendentes_manual, gravar_mapeamento_sku, buscar_custos_skus,
    buscar_skus_validos, buscar_pendentes_revisados,
)
from processar_ml import processar_arquivo_ml, gravar_vendas_ml
from processar_shopee import processar_arquivo_shopee, gravar_vendas_shopee
from processar_amazon import processar_arquivo_amazon, gravar_vendas_amazon
from processar_shein import processar_arquivo_shein, gravar_vendas_shein
from processar_magalu import processar_arquivo_magalu, gravar_vendas_magalu


# ============================================================
# HELPERS GERAIS
# ============================================================

def _detectar_marketplace(mktp):
    mktp_upper = mktp.upper()
    if 'MERCADO' in mktp_upper and 'LIVRE' in mktp_upper: return 'ML'
    if 'SHOPEE' in mktp_upper: return 'SHOPEE'
    if 'AMAZON' in mktp_upper: return 'AMAZON'
    if 'SHEIN' in mktp_upper: return 'SHEIN'
    if 'MAGALU' in mktp_upper or 'MAGAZINE' in mktp_upper: return 'MAGALU'
    return 'DESCONHECIDO'


def _converter_data_br_para_banco(data_str):
    if not data_str or str(data_str).strip() == '': return None
    try: return datetime.strptime(str(data_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except: return str(data_str).strip()


def _exibir_alertas_comissao(alertas):
    if not alertas: return
    st.warning(f"⚠️ **{len(alertas)} pedido(s) com comissão diferente da tabela vigente**")
    with st.expander(f"🔍 Ver detalhes ({len(alertas)} pedidos)", expanded=False):
        df_a = pd.DataFrame(alertas).rename(columns={
            'pedido':'Pedido','sku':'SKU','comissao_arquivo':'Cobrado (R$)',
            'comissao_esperada':'Esperado (R$)','divergencia':'Diferença (R$)'})
        for c in ['Cobrado (R$)','Esperado (R$)','Diferença (R$)']:
            df_a[c] = df_a[c].apply(formatar_valor)
        st.dataframe(df_a, use_container_width=True, hide_index=True)


def _buscar_skus_para_filtro(engine, texto_busca):
    if not texto_busca or not texto_busca.strip():
        return pd.DataFrame(columns=['sku','nome'])
    query = "SELECT sku, nome FROM dim_produtos WHERE status = 'Ativo' AND (sku ILIKE %s OR nome ILIKE %s) ORDER BY sku LIMIT 50"
    termo = f"%{texto_busca.strip()}%"
    try:
        conn = engine.raw_connection(); cursor = conn.cursor()
        cursor.execute(query, (termo, termo))
        cols = [d[0] for d in cursor.description]; rows = cursor.fetchall()
        cursor.close(); conn.close()
        return pd.DataFrame(rows, columns=cols)
    except: return pd.DataFrame(columns=['sku','nome'])


def _buscar_vendas_parametrizada(engine, data_ini, data_fim, marketplace=None, loja=None, skus=None):
    query = "SELECT * FROM fact_vendas_snapshot WHERE data_venda BETWEEN %s AND %s"
    params = [str(data_ini), str(data_fim)]
    if marketplace: query += " AND marketplace_origem = %s"; params.append(marketplace)
    if loja: query += " AND loja_origem = %s"; params.append(loja)
    if skus and len(skus) > 0:
        ph = ','.join(['%s']*len(skus)); query += f" AND sku IN ({ph})"; params.extend(skus)
    query += " ORDER BY data_venda DESC"
    try:
        conn = engine.raw_connection(); cursor = conn.cursor()
        cursor.execute(query, params)
        cols = [d[0] for d in cursor.description]; rows = cursor.fetchall()
        cursor.close(); conn.close()
        return pd.DataFrame(rows, columns=cols)
    except: return pd.DataFrame()


# ============================================================
# v3.5: ENRIQUECER VENDAS COM TAGS (Curva ABC + Tag Manual)
# ============================================================

def _enriquecer_com_tags(engine, df):
    """
    Adiciona colunas 'curva' e 'tag' ao DataFrame de vendas,
    cruzando com dim_tags_anuncio por (marketplace_origem, codigo_anuncio).
    Também corrige Magalu sem codigo_anuncio (preenche com SKU em runtime).
    """
    if df.empty:
        df['curva'] = ''
        df['tag'] = ''
        return df

    # Fix Magalu: preencher codigo_anuncio vazio com SKU
    mask_magalu = (
        (df['marketplace_origem'] == 'MAGALU') &
        (df['codigo_anuncio'].isna() | (df['codigo_anuncio'].astype(str).str.strip() == ''))
    )
    if mask_magalu.any():
        df.loc[mask_magalu, 'codigo_anuncio'] = df.loc[mask_magalu, 'sku']

    # Buscar todas as tags
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT marketplace, codigo_anuncio, tag_curva, tag_status FROM dim_tags_anuncio")
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        df_tags = pd.DataFrame(rows, columns=cols)
    except Exception:
        df['curva'] = ''
        df['tag'] = ''
        return df

    if df_tags.empty:
        df['curva'] = ''
        df['tag'] = ''
        return df

    # Renomear para merge
    df_tags = df_tags.rename(columns={
        'marketplace': 'marketplace_origem',
        'tag_curva': 'curva',
        'tag_status': 'tag',
    })

    # Merge
    df = df.merge(
        df_tags[['marketplace_origem', 'codigo_anuncio', 'curva', 'tag']],
        on=['marketplace_origem', 'codigo_anuncio'],
        how='left'
    )
    df['curva'] = df['curva'].fillna('')
    df['tag'] = df['tag'].fillna('')

    return df


# ============================================================
# FEATURE 1: SISTEMA DE DEVOLUÇÕES
# Ao reimportar, compara status com o banco.
# Se pedido mudou de entregue → devolvido/cancelado, move p/ fact_devolucoes.
# Regra vale para TODOS os marketplaces.
# ============================================================

def _garantir_tabela_devolucoes(engine):
    """Cria fact_devolucoes se não existir — espelha fact_vendas_snapshot + metadados."""
    ddl = """
    CREATE TABLE IF NOT EXISTS fact_devolucoes (
        id SERIAL PRIMARY KEY,
        -- campos espelhados de fact_vendas_snapshot
        venda_id_original INTEGER,
        numero_pedido VARCHAR(100),
        pedido_original VARCHAR(100),
        data_venda DATE,
        sku VARCHAR(50),
        codigo_anuncio VARCHAR(200),
        quantidade INTEGER DEFAULT 1,
        preco_venda NUMERIC(12,2) DEFAULT 0,
        valor_venda_efetivo NUMERIC(12,2) DEFAULT 0,
        custo_unitario NUMERIC(12,2) DEFAULT 0,
        custo_total NUMERIC(12,2) DEFAULT 0,
        imposto NUMERIC(12,2) DEFAULT 0,
        comissao NUMERIC(12,2) DEFAULT 0,
        frete NUMERIC(12,2) DEFAULT 0,
        total_tarifas NUMERIC(12,2) DEFAULT 0,
        valor_liquido NUMERIC(12,2) DEFAULT 0,
        margem_total NUMERIC(12,2) DEFAULT 0,
        margem_percentual NUMERIC(8,2) DEFAULT 0,
        marketplace_origem VARCHAR(50),
        loja_origem VARCHAR(100),
        modo_envio VARCHAR(50),
        tipo_logistica VARCHAR(50),
        arquivo_origem VARCHAR(200),
        -- campos de devolução
        motivo_devolucao VARCHAR(200),
        status_novo VARCHAR(50),
        data_devolucao TIMESTAMP DEFAULT NOW(),
        movido_de_vendas BOOLEAN DEFAULT TRUE,
        arquivo_deteccao VARCHAR(200),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_devolucoes_pedido ON fact_devolucoes(numero_pedido);
    CREATE INDEX IF NOT EXISTS idx_devolucoes_data ON fact_devolucoes(data_venda);
    CREATE INDEX IF NOT EXISTS idx_devolucoes_marketplace ON fact_devolucoes(marketplace_origem);
    """
    try:
        conn = engine.raw_connection(); cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit(); cursor.close(); conn.close()
    except Exception as e:
        print(f"[NALA] Erro ao criar fact_devolucoes: {e}")


def _extrair_pedidos_descartes(descartes):
    """
    Extrai números de pedido da lista de descartes (cancelados/devolvidos).
    Aceita múltiplos formatos — cada processador pode usar campos diferentes.
    Retorna dict { numero_pedido: motivo }.
    """
    pedidos = {}
    if not descartes:
        return pedidos

    for item in descartes:
        if isinstance(item, dict):
            # Tenta vários campos de pedido possíveis
            pedido = (
                item.get('numero_pedido') or item.get('pedido') or
                item.get('order_id') or item.get('pedido_original') or
                item.get('numero_orden') or ''
            )
            pedido = str(pedido).strip()
            if not pedido:
                continue
            # Tenta extrair motivo
            motivo = (
                item.get('motivo') or item.get('status') or
                item.get('motivo_descarte') or item.get('razao') or 'Devolvido/Cancelado'
            )
            pedidos[pedido] = str(motivo)
        elif isinstance(item, (list, tuple)) and len(item) >= 1:
            pedidos[str(item[0]).strip()] = item[1] if len(item) > 1 else 'Devolvido/Cancelado'
        elif isinstance(item, str):
            pedidos[item.strip()] = 'Devolvido/Cancelado'

    return pedidos


def _processar_devolucoes(engine, descartes, marketplace, loja, arquivo_nome):
    """
    Compara pedidos descartados (cancelados/devolvidos) com fact_vendas_snapshot.
    Se o pedido existia como venda, move para fact_devolucoes.

    Regra universal — vale para TODOS os marketplaces.

    Returns: (movidos, erros) — contagem de registros movidos e erros
    """
    pedidos_map = _extrair_pedidos_descartes(descartes)
    if not pedidos_map:
        return 0, 0

    _garantir_tabela_devolucoes(engine)

    pedidos_list = list(pedidos_map.keys())
    movidos = 0
    erros = 0

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Busca vendas existentes cujo pedido apareceu como devolvido/cancelado
        placeholders = ','.join(['%s'] * len(pedidos_list))
        cursor.execute(f"""
            SELECT * FROM fact_vendas_snapshot
            WHERE numero_pedido IN ({placeholders})
              AND marketplace_origem = %s
              AND loja_origem = %s
        """, pedidos_list + [marketplace, loja])

        cols = [d[0] for d in cursor.description]
        vendas_existentes = cursor.fetchall()

        if not vendas_existentes:
            cursor.close(); conn.close()
            return 0, 0

        # Para cada venda encontrada, move para fact_devolucoes
        ids_mover = []
        for row in vendas_existentes:
            venda = dict(zip(cols, row))
            venda_id = venda.get('id')
            numero_pedido = str(venda.get('numero_pedido', ''))
            motivo = pedidos_map.get(numero_pedido, 'Devolvido/Cancelado')

            try:
                cursor.execute("""
                    INSERT INTO fact_devolucoes (
                        venda_id_original, numero_pedido, pedido_original, data_venda,
                        sku, codigo_anuncio, quantidade, preco_venda, valor_venda_efetivo,
                        custo_unitario, custo_total, imposto, comissao, frete,
                        total_tarifas, valor_liquido, margem_total, margem_percentual,
                        marketplace_origem, loja_origem, modo_envio, tipo_logistica,
                        arquivo_origem, motivo_devolucao, status_novo,
                        data_devolucao, movido_de_vendas, arquivo_deteccao
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), TRUE, %s
                    )
                """, (
                    venda_id,
                    venda.get('numero_pedido'),
                    venda.get('pedido_original'),
                    venda.get('data_venda'),
                    venda.get('sku'),
                    venda.get('codigo_anuncio'),
                    venda.get('quantidade', 1),
                    venda.get('preco_venda', 0),
                    venda.get('valor_venda_efetivo', 0),
                    venda.get('custo_unitario', 0),
                    venda.get('custo_total', 0),
                    venda.get('imposto', 0),
                    venda.get('comissao', 0),
                    venda.get('frete', 0),
                    venda.get('total_tarifas', 0),
                    venda.get('valor_liquido', 0),
                    venda.get('margem_total', 0),
                    venda.get('margem_percentual', 0),
                    venda.get('marketplace_origem'),
                    venda.get('loja_origem'),
                    venda.get('modo_envio'),
                    venda.get('tipo_logistica'),
                    venda.get('arquivo_origem'),
                    motivo,
                    motivo,
                    arquivo_nome,
                ))
                ids_mover.append(venda_id)
                movidos += 1
            except Exception as e:
                erros += 1
                print(f"[NALA] Erro ao mover pedido {numero_pedido} para devoluções: {e}")

        # Remove de fact_vendas_snapshot os registros movidos
        if ids_mover:
            ph = ','.join(['%s'] * len(ids_mover))
            cursor.execute(f"DELETE FROM fact_vendas_snapshot WHERE id IN ({ph})", ids_mover)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        erros += 1
        print(f"[NALA] Erro geral ao processar devoluções: {e}")

    return movidos, erros


# ============================================================
# FEATURE 2: EXCLUIR LANÇAMENTO DO HISTÓRICO
# Remove registro do log_uploads + vendas associadas de fact_vendas_snapshot
# ============================================================

def _excluir_lancamento(engine, log_id, marketplace, loja, arquivo_nome, periodo_inicio, periodo_fim):
    """
    Exclui lançamento do log_uploads e todas as vendas associadas em fact_vendas_snapshot.
    Usa marketplace + loja + arquivo_nome + período para match seguro.
    Returns: (vendas_excluidas, sucesso, mensagem)
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # 1. Excluir vendas associadas
        where_vendas = "marketplace_origem = %s AND loja_origem = %s AND arquivo_origem = %s"
        params_vendas = [marketplace, loja, arquivo_nome]

        if periodo_inicio and periodo_fim:
            where_vendas += " AND data_venda BETWEEN %s AND %s"
            params_vendas.extend([str(periodo_inicio), str(periodo_fim)])

        cursor.execute(f"DELETE FROM fact_vendas_snapshot WHERE {where_vendas}", params_vendas)
        vendas_excluidas = cursor.rowcount

        # 2. Excluir também devoluções associadas (se existirem)
        try:
            cursor.execute(f"DELETE FROM fact_devolucoes WHERE {where_vendas}", params_vendas)
        except:
            pass  # Tabela pode não existir ainda

        # 3. Excluir do log
        cursor.execute("DELETE FROM log_uploads WHERE id = %s", (log_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return vendas_excluidas, True, f"Lançamento excluído: {vendas_excluidas} venda(s) removida(s)"

    except Exception as e:
        return 0, False, f"Erro ao excluir: {e}"


# ============================================================
# FEATURE 3: REPROCESSAR PARA OUTRA LOJA
# Atualiza loja_origem + recalcula imposto de vendas já gravadas
# ============================================================

def _reprocessar_outra_loja(engine, log_id, marketplace, loja_antiga, arquivo_nome,
                             periodo_inicio, periodo_fim, loja_nova, imposto_novo):
    """
    Troca a loja de um lançamento inteiro:
    1. Atualiza loja_origem em fact_vendas_snapshot
    2. Recalcula imposto com a alíquota da nova loja
    3. Recalcula margem (valor_liquido e margem_total/margem_percentual)
    4. Atualiza log_uploads
    Returns: (atualizados, sucesso, mensagem)
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Monta WHERE para identificar vendas do lançamento
        where = "marketplace_origem = %s AND loja_origem = %s AND arquivo_origem = %s"
        params = [marketplace, loja_antiga, arquivo_nome]
        if periodo_inicio and periodo_fim:
            where += " AND data_venda BETWEEN %s AND %s"
            params.extend([str(periodo_inicio), str(periodo_fim)])

        # Recalcula: imposto = valor_venda_efetivo * (imposto_novo / 100)
        # valor_liquido = valor_venda_efetivo - comissao - imposto_novo_calc - frete - total_tarifas
        # margem_total = valor_liquido - custo_total
        # margem_percentual = (margem_total / valor_venda_efetivo * 100) ou 0
        imposto_decimal = float(imposto_novo) / 100.0

        cursor.execute(f"""
            UPDATE fact_vendas_snapshot SET
                loja_origem = %s,
                imposto = valor_venda_efetivo * %s,
                valor_liquido = valor_venda_efetivo - comissao - (valor_venda_efetivo * %s) - frete - COALESCE(total_tarifas, 0),
                margem_total = (valor_venda_efetivo - comissao - (valor_venda_efetivo * %s) - frete - COALESCE(total_tarifas, 0)) - custo_total,
                margem_percentual = CASE
                    WHEN valor_venda_efetivo > 0 THEN
                        (((valor_venda_efetivo - comissao - (valor_venda_efetivo * %s) - frete - COALESCE(total_tarifas, 0)) - custo_total)
                         / valor_venda_efetivo * 100)
                    ELSE 0
                END
            WHERE {where}
        """, [loja_nova, imposto_decimal, imposto_decimal, imposto_decimal, imposto_decimal] + params)

        atualizados = cursor.rowcount

        # Atualiza log_uploads
        cursor.execute("UPDATE log_uploads SET loja = %s WHERE id = %s", (loja_nova, log_id))

        conn.commit()
        cursor.close()
        conn.close()

        return atualizados, True, f"Lançamento migrado para '{loja_nova}': {atualizados} venda(s) recalculada(s)"

    except Exception as e:
        return 0, False, f"Erro ao reprocessar: {e}"


# ============================================================
# FEATURE 4: CORREÇÕES PONTUAIS EM VENDAS INDIVIDUAIS
# Permite editar campos financeiros de vendas já gravadas
# ============================================================

def _salvar_correcao_venda(engine, venda_id, campos_alterados):
    """
    Salva correções pontuais em uma venda.
    Recalcula valor_liquido, margem_total e margem_percentual.
    Returns: (sucesso, mensagem)
    """
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        sets = []
        params = []
        for campo, valor in campos_alterados.items():
            sets.append(f"{campo} = %s")
            params.append(valor)

        if sets:
            cursor.execute(f"UPDATE fact_vendas_snapshot SET {', '.join(sets)} WHERE id = %s",
                           params + [venda_id])

        # Recalcula campos derivados
        cursor.execute("""
            UPDATE fact_vendas_snapshot SET
                valor_liquido = valor_venda_efetivo - comissao - imposto - frete - COALESCE(total_tarifas, 0),
                margem_total = (valor_venda_efetivo - comissao - imposto - frete - COALESCE(total_tarifas, 0)) - custo_total,
                margem_percentual = CASE
                    WHEN valor_venda_efetivo > 0 THEN
                        (((valor_venda_efetivo - comissao - imposto - frete - COALESCE(total_tarifas, 0)) - custo_total)
                         / valor_venda_efetivo * 100)
                    ELSE 0
                END
            WHERE id = %s
        """, (venda_id,))

        conn.commit()
        cursor.close()
        conn.close()
        return True, "Venda corrigida com sucesso"

    except Exception as e:
        return False, f"Erro ao corrigir: {e}"


# ============================================================
# TAB 1: PROCESSAR UPLOAD
# ============================================================

def tab_processar_upload(engine):
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except Exception:
        st.error("⚠️ Erro ao carregar lojas."); return
    if df_lojas.empty:
        st.warning("⚠️ Cadastre lojas no módulo Config primeiro."); return

    col1, col2, col3 = st.columns(3)
    mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
    lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
    loja = col2.selectbox("Loja:", lojas)
    imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
    st.info(f"📍 {loja} | Imposto: {formatar_percentual(imposto)}")

    mp = _detectar_marketplace(mktp)
    data_ini = None
    data_fim = None

    # FIX v3.3: Periodo APENAS para Amazon (Shein e Magalu auto-detectam das datas dos pedidos)
    if mp == 'AMAZON':
        st.markdown("**📅 Período do Relatório (obrigatório para Amazon)**")
        col_d1, col_d2 = st.columns(2)
        data_ini = col_d1.date_input("Data Início:", value=None, key="periodo_data_ini")
        data_fim = col_d2.date_input("Data Fim:", value=None, key="periodo_data_fim")
        if not data_ini or not data_fim:
            st.warning("⚠️ Selecione as datas para continuar."); return
        if data_ini > data_fim:
            st.error("❌ Data início > data fim."); return
        st.caption(f"Período: {data_ini.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

    # Upload de arquivo(s)
    if mp == 'AMAZON': tipos = ['csv']
    elif mp == 'MAGALU': tipos = ['csv']
    elif mp == 'SHEIN': tipos = ['xlsx', 'csv']
    else: tipos = ['xlsx']

    if mp == 'MAGALU':
        st.markdown("**📂 A Magalu requer dois relatórios:**")
        col_up1, col_up2 = st.columns(2)
        arquivo_pedidos = col_up1.file_uploader("📋 Relatório de PEDIDOS (CSV)", type=['csv'], key="mglu_pedidos")
        arquivo_pacotes = col_up2.file_uploader("📦 Relatório de PACOTES (CSV)", type=['csv'], key="mglu_pacotes")
        arquivo = arquivo_pedidos
        arquivos_ok = arquivo_pedidos is not None and arquivo_pacotes is not None
    else:
        arquivo = st.file_uploader(f"📂 Upload do arquivo de vendas", type=tipos)
        arquivo_pedidos = None; arquivo_pacotes = None
        arquivos_ok = arquivo is not None

    if arquivos_ok and st.button("🔍 ANALISAR ARQUIVO", type="primary"):
        with st.spinner("Processando arquivo..."):
            if mp == 'ML':
                df_proc, info = processar_arquivo_ml(arquivo, loja, imposto, engine)
            elif mp == 'SHOPEE':
                df_proc, info = processar_arquivo_shopee(arquivo, loja, imposto, engine)
            elif mp == 'AMAZON':
                df_proc, info = processar_arquivo_amazon(arquivo, loja, imposto, engine, data_ini, data_fim)
            elif mp == 'SHEIN':
                df_proc, info = processar_arquivo_shein(arquivo, loja, imposto, engine)
            elif mp == 'MAGALU':
                df_proc, info = processar_arquivo_magalu(arquivo_pedidos, arquivo_pacotes, loja, imposto, engine)
            else:
                st.error(f"⚠️ Processador para '{mktp}' não implementado."); return

            if df_proc is not None:
                st.session_state['df_proc'] = df_proc
                st.session_state['info'] = info
                st.session_state['mktp'] = mktp
                st.session_state['mp_key'] = mp
                st.session_state['loja'] = loja
                st.session_state['arquivo_nome'] = arquivo.name if arquivo else arquivo_pedidos.name
                st.session_state['data_ini'] = data_ini
                st.session_state['data_fim'] = data_fim
                st.rerun()
            else:
                st.error(f"❌ {info}")

    # PREVIEW
    if 'df_proc' in st.session_state:
        df_proc = st.session_state['df_proc']
        info = st.session_state['info']
        mktp = st.session_state['mktp']
        mp_key = st.session_state.get('mp_key', 'ML')
        loja = st.session_state['loja']
        arquivo_nome = st.session_state['arquivo_nome']

        st.success(f"✅ {info['total_linhas']} vendas processadas!")

        col_a, col_b, col_c = st.columns(3)
        col_a.info(f"📅 Período: {info.get('periodo_inicio','-')} a {info.get('periodo_fim','-')}")
        col_b.info(f"🏪 Loja: {loja}")
        col_c.info(f"📦 Arquivo: {arquivo_nome}")

        if info.get('linhas_descartadas', 0) > 0:
            st.warning(f"⚠️ {info['linhas_descartadas']} linhas descartadas")
        if info.get('skus_sem_custo', 0) > 0:
            st.warning(f"⚠️ {info['skus_sem_custo']} SKUs sem custo cadastrado")
        if info.get('carrinhos_encontrados', 0) > 0:
            st.info(f"🛒 {info['carrinhos_encontrados']} carrinho(s) detectado(s)")
        if info.get('skus_corrigidos', 0) > 0:
            st.info(f"🔧 {info['skus_corrigidos']} SKU(s) corrigido(s) automaticamente")
        if info.get('descartes'):
            st.info(f"🗑️ {len(info['descartes'])} linha(s) descartada(s) serão rastreadas")
        if mp_key == 'ML' and info.get('pendentes_carrinho'):
            st.warning(f"⚠️ {len(info['pendentes_carrinho'])} venda(s) com divergência financeira")
        if mp_key == 'SHOPEE':
            _exibir_alertas_comissao(info.get('alertas_comissao', []))
        if mp_key == 'AMAZON' and info.get('asins_sem_config'):
            st.warning(f"⚠️ {len(info['asins_sem_config'])} ASIN(s) sem configuração")

        # FIX v3.3: Preview com formatacao correta
        if not df_proc.empty:
            st.subheader("📋 Preview das Vendas (primeiras 20 linhas)")
            df_preview = df_proc.head(20).copy()

            colunas_valor = ['receita', 'tarifa', 'imposto', 'frete', 'custo', 'margem',
                             'comissao', 'taxa_fixa', 'taxa_estocagem', 'valor_liquido',
                             'preco_venda', 'desconto_parceiro', 'desconto_marketplace', 'total_tarifas']
            for col in colunas_valor:
                if col in df_preview.columns:
                    df_preview[col] = df_preview[col].apply(formatar_valor)
            if 'margem_pct' in df_preview.columns:
                df_preview['margem_pct'] = df_preview['margem_pct'].apply(formatar_percentual)

            colunas_exibir = []
            for c in ['pedido_original', 'pedido', 'data', 'sku', 'sku_original', 'qtd',
                       'preco_venda', 'receita', 'comissao', 'tarifa_fixa', 'frete',
                       'imposto', 'custo', 'valor_liquido', 'margem', 'margem_pct', 'modo_envio']:
                if c in df_preview.columns:
                    colunas_exibir.append(c)

            st.dataframe(df_preview[colunas_exibir], use_container_width=True, height=400)
        else:
            st.info("ℹ️ Nenhuma venda normal para preview.")

        st.divider()
        col_btn1, col_btn2 = st.columns([1, 3])

        if col_btn1.button("💾 GRAVAR NO BANCO", type="primary", use_container_width=True):
            with st.spinner("Gravando vendas no banco..."):
                descartadas = 0; atualizados = 0

                if mp_key == 'ML':
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_ml(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                elif mp_key == 'SHOPEE':
                    registros, erros, skus_invalidos, duplicatas, pendentes = gravar_vendas_shopee(
                        df_proc, mktp, loja, arquivo_nome, engine)
                    descartadas = 0; atualizados = 0
                elif mp_key == 'AMAZON':
                    d_ini = st.session_state.get('data_ini'); d_fim = st.session_state.get('data_fim')
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_amazon(
                        df_proc, mktp, loja, arquivo_nome, engine, d_ini, d_fim,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                elif mp_key == 'SHEIN':
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_shein(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                elif mp_key == 'MAGALU':
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_magalu(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                else:
                    st.error("⚠️ Processador não identificado."); return

                # ─── v3.4: DETECÇÃO DE DEVOLUÇÕES ───
                # Compara descartes com vendas existentes no banco.
                # Se pedido existia e agora veio como devolvido/cancelado → move p/ fact_devolucoes
                devolvidos = 0
                descartes_list = info.get('descartes', [])
                if descartes_list:
                    devolvidos, erros_dev = _processar_devolucoes(
                        engine, descartes_list, mktp, loja, arquivo_nome
                    )

                # Log
                try:
                    conn = engine.raw_connection(); cursor = conn.cursor()
                    p_ini = _converter_data_br_para_banco(info.get('periodo_inicio'))
                    p_fim = _converter_data_br_para_banco(info.get('periodo_fim'))
                    if not p_ini and st.session_state.get('data_ini'): p_ini = str(st.session_state['data_ini'])
                    if not p_fim and st.session_state.get('data_fim'): p_fim = str(st.session_state['data_fim'])
                    cursor.execute("""INSERT INTO log_uploads (data_upload, marketplace, loja, arquivo_nome,
                        periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status)
                        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (mktp, loja, arquivo_nome, p_ini, p_fim, info['total_linhas'], registros, erros,
                         'SUCESSO' if registros > 0 else 'ERRO'))
                    conn.commit(); cursor.close(); conn.close()
                except Exception as e:
                    st.error(f"⚠️ Erro ao gravar log: {e}")

                if registros > 0: st.success(f"✅ {registros} vendas gravadas!"); st.balloons()
                if duplicatas > 0: st.info(f"🔄 {duplicatas} duplicata(s) ignorada(s)")
                if pendentes > 0: st.warning(f"⏳ {pendentes} pendente(s) — veja tab Vendas Pendentes")
                if descartadas > 0: st.info(f"🗑️ {descartadas} cancelada(s)/devolvida(s) rastreadas")
                if atualizados > 0: st.info(f"🔄 {atualizados} registro(s) do período substituídos")
                # ─── v3.4: Feedback devoluções ───
                if devolvidos > 0:
                    st.warning(f"↩️ {devolvidos} pedido(s) movido(s) para devoluções (status mudou de entregue → devolvido/cancelado)")
                if erros > 0: st.warning(f"⚠️ {erros} linha(s) com erro")
                if skus_invalidos:
                    lista = ', '.join(sorted(list(skus_invalidos))[:10])
                    if len(skus_invalidos) > 10: lista += f" ... (+{len(skus_invalidos)-10})"
                    st.error(f"❌ SKUs não cadastrados: {lista}")
                if registros > 0:
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass

                for key in ['df_proc','info','mktp','mp_key','loja','arquivo_nome','data_ini','data_fim']:
                    st.session_state.pop(key, None)


# ============================================================
# TAB 2: VENDAS CONSOLIDADAS (com correções pontuais v3.4)
# ============================================================

def tab_vendas_consolidadas(engine):
    st.subheader("📊 Vendas Consolidadas")

    col1, col2, col3 = st.columns(3)
    periodo = col1.selectbox("Período:", ["Hoje","Ontem","Últimos 7 dias","Últimos 15 dias","Últimos 30 dias","Personalizado"])
    hoje = datetime.now().date()

    if periodo == "Hoje": data_ini = data_fim = hoje
    elif periodo == "Ontem": data_ini = data_fim = hoje - timedelta(days=1)
    elif "7" in periodo: data_ini = hoje - timedelta(days=7); data_fim = hoje
    elif "15" in periodo: data_ini = hoje - timedelta(days=15); data_fim = hoje
    elif "30" in periodo: data_ini = hoje - timedelta(days=30); data_fim = hoje
    else:
        data_ini = col2.date_input("De:", hoje - timedelta(days=30))
        data_fim = col3.date_input("Até:", hoje)
        col2.caption(f"Selecionado: {data_ini.strftime('%d/%m/%Y')}")
        col3.caption(f"Selecionado: {data_fim.strftime('%d/%m/%Y')}")

    col_f1, col_f2 = st.columns(2)
    df_lojas = pd.read_sql("SELECT DISTINCT marketplace, loja FROM dim_lojas", engine)
    mktp_filtro = col_f1.selectbox("Marketplace:", ["Todos"] + sorted(df_lojas['marketplace'].unique().tolist()))
    lojas_disp = df_lojas[df_lojas['marketplace'] == mktp_filtro]['loja'].tolist() if mktp_filtro != "Todos" else df_lojas['loja'].tolist()
    loja_filtro = col_f2.selectbox("Loja:", ["Todas"] + sorted(lojas_disp))

    st.markdown("**🔍 Filtrar por SKU ou Nome do Produto**")
    texto_busca = st.text_input("Buscar:", placeholder="Ex: 321, escova, kit jogo", key="busca_sku_consolidadas")
    skus_sel = []
    if texto_busca.strip():
        df_skus = _buscar_skus_para_filtro(engine, texto_busca)
        if not df_skus.empty:
            opcoes = []; mapa = {}
            for _, r in df_skus.iterrows():
                op = f"{r['sku']} — {str(r['nome'])[:60]}"; opcoes.append(op); mapa[op] = r['sku']
            st.caption(f"Encontrados {len(opcoes)} SKU(s)")
            sels = st.multiselect("Selecionar:", options=opcoes, default=opcoes if len(opcoes)<=5 else [], key="ms_sku")
            skus_sel = [mapa[s] for s in sels]
        else:
            st.info(f"Nenhum SKU encontrado com '{texto_busca}'.")

    mktp_p = mktp_filtro if mktp_filtro != "Todos" else None
    loja_p = loja_filtro if loja_filtro != "Todas" else None
    skus_p = skus_sel if skus_sel else None

    if texto_busca.strip() and not skus_sel:
        st.warning("⚠️ Selecione pelo menos um SKU."); return

    df_vendas = _buscar_vendas_parametrizada(engine, data_ini, data_fim, marketplace=mktp_p, loja=loja_p, skus=skus_p)
    if df_vendas.empty:
        st.warning("⚠️ Nenhuma venda encontrada."); return

    # ─── v3.5: ENRIQUECER COM TAGS ───
    df_vendas = _enriquecer_com_tags(engine, df_vendas)

    df_cc = df_vendas[df_vendas['custo_total'] > 0]
    df_sc = df_vendas[df_vendas['custo_total'] == 0]
    dias_d = (data_fim - data_ini).days
    df_ant = _buscar_vendas_parametrizada(engine, data_ini - timedelta(days=dias_d+1), data_fim - timedelta(days=dias_d+1),
        marketplace=mktp_p, loja=loja_p, skus=skus_p)
    df_ac = df_ant[df_ant['custo_total'] > 0] if not df_ant.empty else pd.DataFrame()

    st.markdown("### 📈 Indicadores do Período")
    c1, c2, c3, c4 = st.columns(4)
    rec_a = df_cc['valor_venda_efetivo'].sum(); rec_ant = df_ac['valor_venda_efetivo'].sum() if not df_ac.empty else 0
    var_r = ((rec_a - rec_ant)/rec_ant*100) if rec_ant > 0 else 0
    c1.metric("Receita Total", formatar_valor(rec_a), f"{formatar_percentual(var_r)} vs anterior")
    ped_a = len(df_cc); ped_ant = len(df_ac) if not df_ac.empty else 0
    var_p = ((ped_a-ped_ant)/ped_ant*100) if ped_ant > 0 else 0
    c2.metric("Pedidos", formatar_quantidade(ped_a), formatar_percentual(var_p))
    mg_a = df_cc['margem_percentual'].mean() if not df_cc.empty else 0
    mg_ant = df_ac['margem_percentual'].mean() if not df_ac.empty else 0
    c3.metric("Margem Média", formatar_percentual(mg_a), formatar_percentual(mg_a - mg_ant))
    c4.metric("Pendentes", formatar_quantidade(len(df_sc)),
        formatar_valor(df_sc['valor_venda_efetivo'].sum()) + " não contabilizados", delta_color="off")

    st.divider()
    st.subheader("📋 Detalhamento de Vendas")
    df_d = df_vendas.copy()
    df_d['data_venda'] = pd.to_datetime(df_d['data_venda']).dt.strftime('%d/%m/%Y')
    df_d['valor_venda_efetivo'] = df_d['valor_venda_efetivo'].apply(formatar_valor)
    df_d['custo_total'] = df_d['custo_total'].apply(formatar_valor)
    df_d['margem_percentual'] = df_d['margem_percentual'].apply(formatar_percentual)

    # v3.5: Coluna pedido_original com fallback para numero_pedido
    # Amazon (relatório agregado) mostra "-"
    if 'pedido_original' in df_d.columns:
        df_d['pedido_original'] = df_d.apply(
            lambda r: str(r['pedido_original']) if pd.notna(r['pedido_original']) and str(r['pedido_original']).strip() not in ('', 'None', 'nan')
            else ('-' if 'AMAZON' in str(r.get('marketplace_origem', '')).upper()
                  else str(r.get('numero_pedido', '-'))),
            axis=1
        )
    else:
        df_d['pedido_original'] = df_d.apply(
            lambda r: '-' if 'AMAZON' in str(r.get('marketplace_origem', '')).upper()
            else str(r.get('numero_pedido', '-')),
            axis=1
        )

    cols_exibir = ['data_venda', 'loja_origem', 'pedido_original', 'sku', 'codigo_anuncio',
                   'curva', 'tag', 'quantidade',
                   'valor_venda_efetivo', 'custo_total', 'margem_percentual']

    st.dataframe(df_d[cols_exibir], use_container_width=True, height=600)

    if st.button("📥 Download Excel"):
        buffer = io.BytesIO()
        df_e = df_vendas.copy()
        df_e['data_venda'] = pd.to_datetime(df_e['data_venda']).dt.strftime('%d/%m/%Y')
        # v3.5: Coluna pedido_original com fallback para numero_pedido
        if 'pedido_original' in df_e.columns:
            df_e['pedido_original'] = df_e.apply(
                lambda r: str(r['pedido_original']) if pd.notna(r['pedido_original']) and str(r['pedido_original']).strip() not in ('', 'None', 'nan')
                else str(r.get('numero_pedido', '')),
                axis=1
            )
        # v3.5: Trocar numero_pedido pelo pedido_original no Excel
        if 'pedido_original' in df_e.columns and 'numero_pedido' in df_e.columns:
            df_e = df_e.drop(columns=['numero_pedido'])
            # Mover pedido_original para depois de loja_origem
            cols = list(df_e.columns)
            cols.remove('pedido_original')
            pos = cols.index('loja_origem') + 1
            cols.insert(pos, 'pedido_original')
            df_e = df_e[cols]  
        for col in ['preco_venda','valor_venda_efetivo','custo_unitario','custo_total','imposto','comissao','frete','total_tarifas','valor_liquido','margem_total']:
            if col in df_e.columns: df_e[col] = df_e[col].apply(lambda x: f"{float(x):.2f}".replace('.',','))
        if 'margem_percentual' in df_e.columns: df_e['margem_percentual'] = df_e['margem_percentual'].apply(lambda x: f"{float(x):.2f}".replace('.',','))
        with pd.ExcelWriter(buffer, engine='openpyxl') as w: df_e.to_excel(w, index=False, sheet_name='Vendas')
        st.download_button("⬇️ Baixar", buffer.getvalue(),
            f"vendas_{data_ini.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ─── v3.4: CORREÇÕES PONTUAIS ───
    st.divider()
    with st.expander("✏️ Correções Pontuais (editar venda individual)", expanded=False):
        st.caption("Edite campos financeiros de vendas individuais. Os campos derivados (líquido, margem) serão recalculados automaticamente.")

        # Busca ID do pedido para corrigir
        id_corrigir = st.number_input("ID da venda (coluna ID):", min_value=1, step=1, key="id_correcao")

        if st.button("🔍 Carregar venda", key="btn_carregar_correcao"):
            try:
                conn = engine.raw_connection(); cursor = conn.cursor()
                cursor.execute("SELECT * FROM fact_vendas_snapshot WHERE id = %s", (int(id_corrigir),))
                cols_v = [d[0] for d in cursor.description]; row = cursor.fetchone()
                cursor.close(); conn.close()
                if row:
                    st.session_state['venda_correcao'] = dict(zip(cols_v, row))
                else:
                    st.warning("Venda não encontrada."); st.session_state.pop('venda_correcao', None)
            except Exception as e:
                st.error(f"Erro: {e}")

        if 'venda_correcao' in st.session_state:
            v = st.session_state['venda_correcao']
            st.markdown(f"**Pedido:** {v.get('numero_pedido','-')} | **SKU:** {v.get('sku','-')} | "
                        f"**Loja:** {v.get('loja_origem','-')} | **Data:** {v.get('data_venda','-')}")

            c1, c2, c3 = st.columns(3)
            novo_receita = c1.number_input("Receita (R$):", value=float(v.get('valor_venda_efetivo', 0)),
                                           format="%.2f", key="corr_receita")
            novo_comissao = c2.number_input("Comissão (R$):", value=float(v.get('comissao', 0)),
                                            format="%.2f", key="corr_comissao")
            novo_imposto = c3.number_input("Imposto (R$):", value=float(v.get('imposto', 0)),
                                           format="%.2f", key="corr_imposto")

            c4, c5, c6 = st.columns(3)
            novo_frete = c4.number_input("Frete (R$):", value=float(v.get('frete', 0)),
                                         format="%.2f", key="corr_frete")
            novo_custo = c5.number_input("Custo Total (R$):", value=float(v.get('custo_total', 0)),
                                         format="%.2f", key="corr_custo")
            novo_tarifas = c6.number_input("Total Tarifas (R$):", value=float(v.get('total_tarifas', 0) or 0),
                                           format="%.2f", key="corr_tarifas")

            # Preview do recalculo
            liq_prev = novo_receita - novo_comissao - novo_imposto - novo_frete - novo_tarifas
            mg_prev = liq_prev - novo_custo
            mg_pct_prev = (mg_prev / novo_receita * 100) if novo_receita > 0 else 0
            st.info(f"📊 Preview: Líquido = {formatar_valor(liq_prev)} | "
                    f"Margem = {formatar_valor(mg_prev)} ({formatar_percentual(mg_pct_prev)})")

            if st.button("💾 Salvar Correção", key="btn_salvar_correcao", type="primary"):
                campos = {
                    'valor_venda_efetivo': novo_receita,
                    'comissao': novo_comissao,
                    'imposto': novo_imposto,
                    'frete': novo_frete,
                    'custo_total': novo_custo,
                    'total_tarifas': novo_tarifas,
                }
                ok, msg = _salvar_correcao_venda(engine, int(v['id']), campos)
                if ok:
                    st.success(f"✅ {msg}")
                    st.session_state.pop('venda_correcao', None)
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

    # ─── DELETAR (existente) ───
    st.divider()
    with st.expander("🗑️ Deletar Vendas (ADMIN)", expanded=False):
        st.warning("⚠️ **ATENÇÃO:** Ação irreversível!")
        modo = st.radio("Modo:", ["Selecionar individuais","Deletar marketplace inteiro"], horizontal=True)
        if modo == "Selecionar individuais":
            df_del = df_vendas[['id','data_venda','numero_pedido','sku','codigo_anuncio','quantidade','valor_venda_efetivo','margem_percentual']].copy()
            df_del['data_venda'] = pd.to_datetime(df_del['data_venda']).dt.strftime('%d/%m/%Y')
            df_del.insert(0, 'Excluir', False)
            df_ed = st.data_editor(df_del, column_config={
                'Excluir': st.column_config.CheckboxColumn("Excluir?", default=False),
                'id': st.column_config.NumberColumn("ID", disabled=True),
                'valor_venda_efetivo': st.column_config.NumberColumn("Receita", format="%.2f", disabled=True),
            }, use_container_width=True, height=400, hide_index=True, key="del_ed")
            ids = df_ed[df_ed['Excluir']==True]['id'].tolist()
            if ids:
                st.info(f"📌 {len(ids)} selecionada(s)")
                conf = st.checkbox(f"✅ Confirmo excluir {len(ids)} venda(s)")
                if st.button("🗑️ EXCLUIR", type="secondary"):
                    if conf:
                        try:
                            conn = engine.raw_connection(); cursor = conn.cursor()
                            cursor.execute(f"DELETE FROM fact_vendas_snapshot WHERE id IN ({','.join(['%s']*len(ids))})", ids)
                            conn.commit(); cursor.close(); conn.close()
                            st.success(f"✅ Excluído!"); st.rerun()
                        except Exception as e: st.error(f"❌ {e}")
                    else: st.warning("Confirme antes.")
        else:
            st.error("⛔ Apaga TODAS as vendas do marketplace!")
            df_ld = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)
            c1, c2 = st.columns(2)
            cd = c1.text_input("Digite 'DELETAR':"); md = c2.selectbox("Marketplace:", [""]+df_ld['marketplace'].tolist())
            if st.button("🗑️ DELETAR TUDO", type="secondary"):
                if cd == "DELETAR" and md:
                    try:
                        conn = engine.raw_connection(); cursor = conn.cursor()
                        cursor.execute("DELETE FROM fact_vendas_snapshot WHERE marketplace_origem = %s", (md,))
                        d = cursor.rowcount
                        cursor.execute("DELETE FROM log_uploads WHERE marketplace = %s", (md,))
                        conn.commit(); cursor.close(); conn.close()
                        st.success(f"✅ {d} vendas deletadas!"); st.rerun()
                    except Exception as e: st.error(f"❌ {e}")


# ============================================================
# TAB 3: HISTORICO (com excluir + reprocessar outra loja v3.4)
# ============================================================

def tab_historico_uploads(engine):
    st.subheader("📚 Histórico de Importações")

    try:
        df_log = pd.read_sql("""SELECT id, data_upload, marketplace, loja, arquivo_nome,
            periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status
            FROM log_uploads ORDER BY data_upload DESC LIMIT 200""", engine)
    except Exception as e:
        st.error(f"⚠️ Erro: {e}"); return

    if df_log.empty:
        st.info("Nenhuma importação registrada."); return

    # Formata para exibição
    df_exib = df_log.copy()
    df_exib['data_upload'] = pd.to_datetime(df_exib['data_upload']).dt.strftime('%d/%m/%Y %H:%M')
    for c in ['periodo_inicio','periodo_fim']:
        if c in df_exib.columns:
            df_exib[c] = pd.to_datetime(df_exib[c], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')

    st.dataframe(df_exib.drop(columns=['id']), use_container_width=True, height=400)

    # ─── v3.4: EXCLUIR LANÇAMENTO ───
    st.divider()
    with st.expander("🗑️ Excluir Lançamento (remove upload + vendas associadas)", expanded=False):
        st.warning("⚠️ Remove o registro do histórico E todas as vendas gravadas por esse upload.")

        # Monta opções legíveis
        opcoes_log = {}
        for _, row in df_log.iterrows():
            dt = pd.to_datetime(row['data_upload']).strftime('%d/%m/%Y %H:%M')
            p_ini = pd.to_datetime(row['periodo_inicio'], errors='coerce')
            p_fim = pd.to_datetime(row['periodo_fim'], errors='coerce')
            p_ini_str = p_ini.strftime('%d/%m/%Y') if pd.notna(p_ini) else '-'
            p_fim_str = p_fim.strftime('%d/%m/%Y') if pd.notna(p_fim) else '-'
            label = f"[{dt}] {row['marketplace']} — {row['loja']} — {row['arquivo_nome']} ({p_ini_str} a {p_fim_str}) — {row['linhas_importadas']} vendas"
            opcoes_log[label] = row

        sel_label = st.selectbox("Selecione o lançamento:", list(opcoes_log.keys()), key="sel_excluir_log")

        if sel_label:
            sel = opcoes_log[sel_label]
            st.caption(f"ID: {sel['id']} | Marketplace: {sel['marketplace']} | Loja: {sel['loja']} | "
                       f"Arquivo: {sel['arquivo_nome']} | Linhas importadas: {sel['linhas_importadas']}")

            conf_excluir = st.checkbox("✅ Confirmo que desejo excluir este lançamento e TODAS as vendas associadas",
                                       key="conf_excluir_lancamento")

            if st.button("🗑️ EXCLUIR LANÇAMENTO", type="secondary", key="btn_excluir_lancamento"):
                if not conf_excluir:
                    st.warning("Confirme antes de excluir."); return

                vendas_del, ok, msg = _excluir_lancamento(
                    engine,
                    log_id=int(sel['id']),
                    marketplace=sel['marketplace'],
                    loja=sel['loja'],
                    arquivo_nome=sel['arquivo_nome'],
                    periodo_inicio=sel['periodo_inicio'],
                    periodo_fim=sel['periodo_fim'],
                )
                if ok:
                    st.success(f"✅ {msg}")
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

    # ─── v3.4: REPROCESSAR PARA OUTRA LOJA ───
    with st.expander("🔄 Reprocessar para Outra Loja (corrigir loja errada)", expanded=False):
        st.info("Use quando subiu um relatório na loja errada. As vendas serão migradas e o imposto recalculado.")

        opcoes_repr = {}
        for _, row in df_log.iterrows():
            dt = pd.to_datetime(row['data_upload']).strftime('%d/%m/%Y %H:%M')
            label = f"[{dt}] {row['marketplace']} — {row['loja']} — {row['arquivo_nome']} — {row['linhas_importadas']} vendas"
            opcoes_repr[label] = row

        sel_repr_label = st.selectbox("Selecione o lançamento:", list(opcoes_repr.keys()), key="sel_repr_log")

        if sel_repr_label:
            sel_r = opcoes_repr[sel_repr_label]

            # Carrega lojas do mesmo marketplace para destino
            try:
                df_lojas_mktp = pd.read_sql(
                    "SELECT loja, imposto FROM dim_lojas WHERE marketplace = %s",
                    engine, params=(sel_r['marketplace'],)
                )
            except:
                df_lojas_mktp = pd.DataFrame(columns=['loja','imposto'])

            if df_lojas_mktp.empty:
                st.warning("Nenhuma outra loja cadastrada neste marketplace."); return

            # Remove a loja atual das opções
            lojas_destino = df_lojas_mktp[df_lojas_mktp['loja'] != sel_r['loja']]
            if lojas_destino.empty:
                st.warning("Só existe uma loja neste marketplace. Cadastre outra em Config."); return

            opcoes_loja = {}
            for _, lr in lojas_destino.iterrows():
                label_loja = f"{lr['loja']} (Imposto: {formatar_percentual(lr['imposto'])})"
                opcoes_loja[label_loja] = lr

            sel_nova_loja = st.selectbox("Nova loja destino:", list(opcoes_loja.keys()), key="sel_nova_loja")

            if sel_nova_loja:
                nova = opcoes_loja[sel_nova_loja]
                st.caption(f"De: **{sel_r['loja']}** → Para: **{nova['loja']}** | "
                           f"Novo imposto: {formatar_percentual(nova['imposto'])}")

                conf_repr = st.checkbox("✅ Confirmo a migração deste lançamento", key="conf_repr_loja")

                if st.button("🔄 MIGRAR LANÇAMENTO", type="primary", key="btn_repr_loja"):
                    if not conf_repr:
                        st.warning("Confirme antes de migrar."); return

                    atualiz, ok, msg = _reprocessar_outra_loja(
                        engine,
                        log_id=int(sel_r['id']),
                        marketplace=sel_r['marketplace'],
                        loja_antiga=sel_r['loja'],
                        arquivo_nome=sel_r['arquivo_nome'],
                        periodo_inicio=sel_r['periodo_inicio'],
                        periodo_fim=sel_r['periodo_fim'],
                        loja_nova=nova['loja'],
                        imposto_novo=float(nova['imposto']),
                    )
                    if ok:
                        st.success(f"✅ {msg}")
                        try: recalcular_curva_abc(engine, dias=30)
                        except: pass
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")

    # ─── v3.4: DEVOLUÇÕES ───
    with st.expander("↩️ Devoluções Detectadas", expanded=False):
        _garantir_tabela_devolucoes(engine)
        try:
            df_dev = pd.read_sql("""
                SELECT numero_pedido, sku, data_venda, marketplace_origem, loja_origem,
                       valor_venda_efetivo, motivo_devolucao, data_devolucao, arquivo_deteccao
                FROM fact_devolucoes
                ORDER BY data_devolucao DESC
                LIMIT 200
            """, engine)
            if not df_dev.empty:
                df_dev['data_venda'] = pd.to_datetime(df_dev['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
                df_dev['data_devolucao'] = pd.to_datetime(df_dev['data_devolucao'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M').fillna('-')
                df_dev['valor_venda_efetivo'] = df_dev['valor_venda_efetivo'].apply(formatar_valor)

                c1, c2, c3 = st.columns(3)
                c1.metric("Total Devoluções", formatar_quantidade(len(df_dev)))
                # Precisamos dos valores brutos para somar — refaz query simples
                try:
                    df_soma = pd.read_sql("SELECT COALESCE(SUM(valor_venda_efetivo),0) as total FROM fact_devolucoes", engine)
                    c2.metric("Valor Total", formatar_valor(df_soma['total'].iloc[0]))
                except:
                    pass

                st.dataframe(df_dev, use_container_width=True, height=300, hide_index=True)
            else:
                st.info("Nenhuma devolução detectada ainda.")
        except Exception as e:
            st.info("Tabela de devoluções será criada no próximo upload.")


# ============================================================
# TAB 4: VENDAS PENDENTES
# ============================================================

def tab_vendas_pendentes(engine):
    st.subheader("⏳ Vendas Pendentes")
    st.markdown("Vendas que precisam de revisão: **SKU não cadastrado**, **ASIN não configurado** ou **divergência financeira**.")

    df_resumo = buscar_pendentes_resumo(engine)
    if df_resumo.empty:
        st.success("✅ Nenhuma venda pendente!")
        _exibir_historico(engine); return

    c1, c2, c3 = st.columns(3)
    c1.metric("SKUs Pendentes", formatar_quantidade(len(df_resumo)))
    c2.metric("Vendas Pendentes", formatar_quantidade(int(df_resumo['total_vendas'].sum())))
    c3.metric("Receita Não Contabilizada", formatar_valor(df_resumo['receita_total'].sum()))

    st.divider(); _secao_pend_sku(engine)
    st.divider(); _secao_pend_div(engine)
    st.divider(); _exibir_historico(engine)


def _secao_pend_sku(engine):
    st.markdown("### 🔧 Pendentes por SKU não cadastrado")
    st.caption("Corrija o SKU ou cadastre em Gestão de SKUs. Correções serão lembradas.")
    df = buscar_pendentes_por_tipo(engine, tipo='sku')
    if df.empty: st.success("✅ Nenhuma pendente por SKU."); return

    skus_v = buscar_skus_validos(engine)
    df_e = df[['id','sku','numero_pedido','data_venda','loja_origem','marketplace_origem',
        'valor_venda_efetivo','codigo_anuncio','quantidade','comissao','imposto','frete','motivo']].copy()
    df_e['sku_original'] = df_e['sku'].copy()
    df_e['data_venda'] = pd.to_datetime(df_e['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
    df_e.insert(0, 'Sel', False)

    df_ed = st.data_editor(df_e, column_config={
        'Sel': st.column_config.CheckboxColumn("Sel", default=False),
        'id': st.column_config.NumberColumn("ID", disabled=True),
        'sku': st.column_config.TextColumn("SKU (editável)"),
        'sku_original': None,
        'valor_venda_efetivo': st.column_config.NumberColumn("Receita", format="%.2f", disabled=True),
        'comissao': st.column_config.NumberColumn("Tarifa", format="%.2f", disabled=True),
        'imposto': st.column_config.NumberColumn("Imposto", format="%.2f", disabled=True),
        'frete': st.column_config.NumberColumn("Frete", format="%.2f", disabled=True),
    }, use_container_width=True, height=400, hide_index=True, key="ed_pend_sku")

    sels = df_ed[df_ed['Sel']==True]
    if len(sels) > 0:
        mods = sels[sels['sku'] != sels['sku_original']]
        if len(mods) > 0: st.info(f"🔧 {len(mods)} SKU(s) corrigido(s).")
        nf = [str(r['sku']).strip() for _,r in sels.iterrows() if str(r['sku']).strip() not in skus_v]
        if nf: st.warning(f"⚠️ Não cadastrado(s): {', '.join(nf)}")
        st.info(f"📌 {len(sels)} selecionada(s)")
        if st.button("🔄 Reprocessar SKUs", key="btn_sku", type="primary"):
            with st.spinner("Reprocessando..."):
                itens = [{'id':r['id'],'sku':str(r['sku']).strip(),'sku_original':str(r['sku_original']).strip(),
                    'valor_venda_efetivo':r['valor_venda_efetivo'],'comissao':r['comissao'],'imposto':r['imposto'],
                    'frete':r['frete'],'quantidade':r['quantidade'],'marketplace_origem':r['marketplace_origem'],
                    'loja_origem':r['loja_origem'],'numero_pedido':r['numero_pedido'],
                    'data_venda':pd.to_datetime(r['data_venda'],format='%d/%m/%Y',errors='coerce'),
                    'codigo_anuncio':r.get('codigo_anuncio',''),'arquivo_origem':''} for _,r in sels.iterrows()]
                res = reprocessar_pendentes_manual(engine, itens)
                if res['sucesso'] > 0:
                    st.success(f"✅ {res['mensagem']}")
                    if res['mapeados'] > 0: st.info(f"🔧 {res['mapeados']} mapeamento(s) salvo(s)")
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else: st.error(f"❌ {res['mensagem']}")


def _secao_pend_div(engine):
    st.markdown("### 💰 Pendentes por Divergência Financeira")
    st.caption("Ajuste valores e reprocesse.")
    df = buscar_pendentes_por_tipo(engine, tipo='divergencia')
    if df.empty: st.success("✅ Nenhuma por divergência."); return

    df_e = df[['id','sku','numero_pedido','data_venda','loja_origem','marketplace_origem',
        'valor_venda_efetivo','codigo_anuncio','quantidade','comissao','imposto','frete','motivo']].copy()
    df_e['sku_original'] = df_e['sku'].copy()
    df_e['data_venda'] = pd.to_datetime(df_e['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
    df_e.insert(0, 'Sel', False)

    df_ed = st.data_editor(df_e, column_config={
        'Sel': st.column_config.CheckboxColumn("Sel", default=False),
        'sku': st.column_config.TextColumn("SKU (editável)"), 'sku_original': None,
        'valor_venda_efetivo': st.column_config.NumberColumn("Receita", format="%.2f"),
        'comissao': st.column_config.NumberColumn("Tarifa", format="%.2f"),
        'imposto': st.column_config.NumberColumn("Imposto", format="%.2f"),
        'frete': st.column_config.NumberColumn("Frete", format="%.2f"),
    }, use_container_width=True, height=400, hide_index=True, key="ed_pend_div")

    sels = df_ed[df_ed['Sel']==True]
    if len(sels) > 0:
        st.info(f"📌 {len(sels)} selecionada(s)")
        if st.button("🔄 Reprocessar Divergências", key="btn_div", type="primary"):
            with st.spinner("Reprocessando..."):
                itens = [{'id':r['id'],'sku':str(r['sku']).strip(),'sku_original':str(r['sku_original']).strip(),
                    'valor_venda_efetivo':r['valor_venda_efetivo'],'comissao':r['comissao'],'imposto':r['imposto'],
                    'frete':r['frete'],'quantidade':r['quantidade'],'marketplace_origem':r['marketplace_origem'],
                    'loja_origem':r['loja_origem'],'numero_pedido':r['numero_pedido'],
                    'data_venda':pd.to_datetime(r['data_venda'],format='%d/%m/%Y',errors='coerce'),
                    'codigo_anuncio':r.get('codigo_anuncio',''),'arquivo_origem':''} for _,r in sels.iterrows()]
                res = reprocessar_pendentes_manual(engine, itens)
                if res['sucesso'] > 0:
                    st.success(f"✅ {res['mensagem']}")
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else: st.error(f"❌ {res['mensagem']}")


def _exibir_historico(engine):
    with st.expander("✅ Histórico de reprocessadas", expanded=False):
        try:
            df_h = buscar_pendentes_revisados(engine, limit=100)
            if not df_h.empty:
                if 'data_venda' in df_h.columns:
                    df_h['data_venda'] = pd.to_datetime(df_h['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
                if 'valor_venda_efetivo' in df_h.columns:
                    df_h['valor_venda_efetivo'] = df_h['valor_venda_efetivo'].apply(formatar_valor)
                st.dataframe(df_h, use_container_width=True, height=300, hide_index=True)
                st.caption(f"Total: {len(df_h)} venda(s)")
            else: st.info("Nenhuma venda reprocessada ainda.")
        except Exception as e: st.error(f"Erro: {e}")


def main():
    st.title("💰 Central de Vendas")
    engine = get_engine()
    t1, t2, t3, t4 = st.tabs(["📤 Processar Upload","📊 Vendas Consolidadas","📚 Histórico","⏳ Vendas Pendentes"])
    with t1: tab_processar_upload(engine)
    with t2: tab_vendas_consolidadas(engine)
    with t3: tab_historico_uploads(engine)
    with t4: tab_vendas_pendentes(engine)


if __name__ == "__main__":
    main()
