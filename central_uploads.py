"""
CENTRAL DE UPLOADS - Sistema Nala
Interface principal para upload e processamento de vendas

VERSAO 3.2 (17/03/2026):
  - NOVO: Integracao processar_shein (XLSX, header linha 1, SKC como anuncio)
  - NOVO: Integracao processar_magalu (2 CSVs: pedidos + pacotes)
  - FIX: Historico reprocessados usa buscar_pendentes_revisados (corrige erro List argument)
  - FIX: Pendentes por tipo agora captura todos os motivos SKU/ASIN (ILIKE)
  - AJUSTE: Periodo obrigatorio para Amazon, Shein e Magalu
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import (
    get_engine,
    gravar_log_upload,
    buscar_pendentes,
    buscar_pendentes_resumo,
    reprocessar_pendentes_por_sku,
    recalcular_curva_abc,
    buscar_pendentes_por_tipo,
    reprocessar_pendentes_manual,
    gravar_mapeamento_sku,
    buscar_custos_skus,
    buscar_skus_validos,
    buscar_pendentes_revisados,
)
from processar_ml import processar_arquivo_ml, gravar_vendas_ml
from processar_shopee import processar_arquivo_shopee, gravar_vendas_shopee
from processar_amazon import processar_arquivo_amazon, gravar_vendas_amazon
from processar_shein import processar_arquivo_shein, gravar_vendas_shein
from processar_magalu import processar_arquivo_magalu, gravar_vendas_magalu


def _detectar_marketplace(mktp):
    mktp_upper = mktp.upper()
    if 'MERCADO' in mktp_upper and 'LIVRE' in mktp_upper:
        return 'ML'
    if 'SHOPEE' in mktp_upper:
        return 'SHOPEE'
    if 'AMAZON' in mktp_upper:
        return 'AMAZON'
    if 'SHEIN' in mktp_upper:
        return 'SHEIN'
    if 'MAGALU' in mktp_upper or 'MAGAZINE' in mktp_upper:
        return 'MAGALU'
    return 'DESCONHECIDO'


def _converter_data_br_para_banco(data_str):
    if not data_str or str(data_str).strip() == '':
        return None
    try:
        return datetime.strptime(str(data_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return str(data_str).strip()


def _exibir_alertas_comissao(alertas):
    if not alertas:
        return
    st.warning(
        f"**{len(alertas)} pedido(s) com comissao diferente da tabela vigente** "
        f"(pode indicar regra anterior ou promocao especial da Shopee)"
    )
    with st.expander(f"Ver detalhes das divergencias ({len(alertas)} pedidos)", expanded=False):
        df_alertas = pd.DataFrame(alertas)
        df_alertas = df_alertas.rename(columns={
            'pedido': 'Pedido', 'sku': 'SKU',
            'comissao_arquivo': 'Cobrado (R$)',
            'comissao_esperada': 'Esperado (R$)',
            'divergencia': 'Diferenca (R$)',
        })
        for c in ['Cobrado (R$)', 'Esperado (R$)', 'Diferenca (R$)']:
            df_alertas[c] = df_alertas[c].apply(formatar_valor)
        st.dataframe(df_alertas, use_container_width=True, hide_index=True)


def _buscar_skus_para_filtro(engine, texto_busca):
    if not texto_busca or not texto_busca.strip():
        return pd.DataFrame(columns=['sku', 'nome'])
    query = """
        SELECT sku, nome FROM dim_produtos
        WHERE status = 'Ativo' AND (sku ILIKE %s OR nome ILIKE %s)
        ORDER BY sku LIMIT 50
    """
    termo = f"%{texto_busca.strip()}%"
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (termo, termo))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception:
        return pd.DataFrame(columns=['sku', 'nome'])


def _buscar_vendas_parametrizada(engine, data_ini, data_fim, marketplace=None, loja=None, skus=None):
    query = "SELECT * FROM fact_vendas_snapshot WHERE data_venda BETWEEN %s AND %s"
    params = [str(data_ini), str(data_fim)]
    if marketplace:
        query += " AND marketplace_origem = %s"
        params.append(marketplace)
    if loja:
        query += " AND loja_origem = %s"
        params.append(loja)
    if skus and len(skus) > 0:
        placeholders = ','.join(['%s'] * len(skus))
        query += f" AND sku IN ({placeholders})"
        params.extend(skus)
    query += " ORDER BY data_venda DESC"
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception:
        return pd.DataFrame()


# ============================================================
# TAB 1: PROCESSAR UPLOAD
# ============================================================

def tab_processar_upload(engine):
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except Exception:
        st.error("Erro ao carregar lojas. Configure lojas em Config.")
        return
    if df_lojas.empty:
        st.warning("Cadastre lojas no modulo Config primeiro.")
        return

    col1, col2, col3 = st.columns(3)
    mktp = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
    lojas = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
    loja = col2.selectbox("Loja:", lojas)
    imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]
    st.info(f"Loja: {loja} | Imposto: {formatar_percentual(imposto)}")

    mp = _detectar_marketplace(mktp)

    data_ini = None
    data_fim = None

    if mp in ('AMAZON', 'SHEIN', 'MAGALU'):
        st.markdown(f"**Periodo do Relatorio (obrigatorio para {mp})**")
        col_d1, col_d2 = st.columns(2)
        data_ini = col_d1.date_input("Data Inicio:", value=None, key="periodo_data_ini")
        data_fim = col_d2.date_input("Data Fim:", value=None, key="periodo_data_fim")
        if not data_ini or not data_fim:
            st.warning("Selecione as datas de inicio e fim do periodo para continuar.")
            return
        if data_ini > data_fim:
            st.error("Data de inicio nao pode ser maior que data de fim.")
            return
        st.caption(f"Periodo: {data_ini.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

    if mp == 'AMAZON':
        tipos_aceitos = ['csv']
    elif mp == 'MAGALU':
        tipos_aceitos = ['csv']
    elif mp == 'SHEIN':
        tipos_aceitos = ['xlsx', 'csv']
    else:
        tipos_aceitos = ['xlsx']

    if mp == 'MAGALU':
        st.markdown("**A Magalu requer dois relatorios:**")
        col_up1, col_up2 = st.columns(2)
        arquivo_pedidos = col_up1.file_uploader("Relatorio de PEDIDOS (CSV)", type=['csv'], key="mglu_pedidos")
        arquivo_pacotes = col_up2.file_uploader("Relatorio de PACOTES (CSV)", type=['csv'], key="mglu_pacotes")
        arquivo = arquivo_pedidos
        arquivos_ok = arquivo_pedidos is not None and arquivo_pacotes is not None
    else:
        arquivo = st.file_uploader(f"Upload do arquivo de vendas", type=tipos_aceitos)
        arquivo_pedidos = None
        arquivo_pacotes = None
        arquivos_ok = arquivo is not None

    if arquivos_ok and st.button("ANALISAR ARQUIVO", type="primary"):
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
                st.error(f"Processador para '{mktp}' ainda nao implementado.")
                return

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
                st.error(f"{info}")

    if 'df_proc' in st.session_state:
        df_proc = st.session_state['df_proc']
        info = st.session_state['info']
        mktp = st.session_state['mktp']
        mp_key = st.session_state.get('mp_key', 'ML')
        loja = st.session_state['loja']
        arquivo_nome = st.session_state['arquivo_nome']

        st.success(f"{info['total_linhas']} vendas processadas com sucesso!")

        col_a, col_b, col_c = st.columns(3)
        col_a.info(f"Periodo: {info.get('periodo_inicio', '-')} a {info.get('periodo_fim', '-')}")
        col_b.info(f"Loja: {loja}")
        col_c.info(f"Arquivo: {arquivo_nome}")

        if info.get('linhas_descartadas', 0) > 0:
            st.warning(f"{info['linhas_descartadas']} linhas descartadas (canceladas, devolvidas ou sem receita)")
        if info.get('skus_sem_custo', 0) > 0:
            st.warning(f"{info['skus_sem_custo']} SKUs sem custo cadastrado (margem com custo = R$ 0,00)")
        if info.get('carrinhos_encontrados', 0) > 0:
            st.info(f"{info['carrinhos_encontrados']} carrinho(s) detectado(s) - receita distribuida proporcionalmente.")
        if info.get('skus_corrigidos', 0) > 0:
            st.info(f"{info['skus_corrigidos']} SKU(s) corrigido(s) automaticamente via mapeamento.")
        if info.get('descartes'):
            st.info(f"{len(info['descartes'])} linha(s) descartada(s) serao rastreadas em fact_vendas_descartadas.")
        if mp_key == 'ML' and info.get('pendentes_carrinho'):
            st.warning(f"{len(info['pendentes_carrinho'])} venda(s) de carrinho com divergencia financeira > R$ 5,00.")
        if mp_key == 'SHOPEE' and info.get('carrinhos', 0) > 0:
            st.info(f"{info['carrinhos']} pedido(s) de carrinho composto detectado(s).")
        if mp_key == 'SHOPEE':
            _exibir_alertas_comissao(info.get('alertas_comissao', []))
        if mp_key == 'AMAZON' and info.get('asins_sem_config'):
            st.warning(f"{len(info['asins_sem_config'])} ASIN(s) sem configuracao - vendas irao para pendentes.")

        if not df_proc.empty:
            st.subheader("Preview das Vendas (primeiras 20 linhas)")
            df_preview = df_proc.copy()
            colunas_valor = ['receita', 'tarifa', 'imposto', 'frete', 'custo', 'margem',
                             'comissao', 'taxa_fixa', 'taxa_estocagem', 'valor_liquido',
                             'desconto_parceiro', 'desconto_marketplace']
            for col in colunas_valor:
                if col in df_preview.columns:
                    df_preview[col] = df_preview[col].apply(formatar_valor)
            colunas_pct = ['margem_pct']
            for col in colunas_pct:
                if col in df_preview.columns:
                    df_preview[col] = df_preview[col].apply(formatar_percentual)
            st.dataframe(df_preview.head(20), use_container_width=True, height=400)
        else:
            st.info("Nenhuma venda normal para preview (apenas descartes e/ou pendentes).")

        st.divider()
        col_btn1, col_btn2 = st.columns([1, 3])

        if col_btn1.button("GRAVAR NO BANCO", type="primary", use_container_width=True):
            with st.spinner("Gravando vendas no banco..."):
                descartadas = 0
                atualizados = 0

                if mp_key == 'ML':
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_ml(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []),
                        pendentes_carrinho=info.get('pendentes_carrinho', [])
                    )
                elif mp_key == 'SHOPEE':
                    registros, erros, skus_invalidos, duplicatas, pendentes = gravar_vendas_shopee(
                        df_proc, mktp, loja, arquivo_nome, engine
                    )
                    descartadas = 0
                    atualizados = 0
                elif mp_key == 'AMAZON':
                    data_ini_s = st.session_state.get('data_ini')
                    data_fim_s = st.session_state.get('data_fim')
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_amazon(
                        df_proc, mktp, loja, arquivo_nome, engine, data_ini_s, data_fim_s,
                        descartes=info.get('descartes', []),
                        pendentes_carrinho=info.get('pendentes_carrinho', [])
                    )
                elif mp_key == 'SHEIN':
                    data_ini_s = st.session_state.get('data_ini')
                    data_fim_s = st.session_state.get('data_fim')
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_shein(
                        df_proc, mktp, loja, arquivo_nome, engine, data_ini_s, data_fim_s,
                        descartes=info.get('descartes', []),
                        pendentes_carrinho=info.get('pendentes_carrinho', [])
                    )
                elif mp_key == 'MAGALU':
                    data_ini_s = st.session_state.get('data_ini')
                    data_fim_s = st.session_state.get('data_fim')
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_magalu(
                        df_proc, mktp, loja, arquivo_nome, engine, data_ini_s, data_fim_s,
                        descartes=info.get('descartes', []),
                        pendentes_carrinho=info.get('pendentes_carrinho', [])
                    )
                else:
                    st.error("Processador nao identificado para gravacao.")
                    return

                try:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    periodo_ini_banco = _converter_data_br_para_banco(info.get('periodo_inicio'))
                    periodo_fim_banco = _converter_data_br_para_banco(info.get('periodo_fim'))
                    if not periodo_ini_banco and st.session_state.get('data_ini'):
                        periodo_ini_banco = str(st.session_state['data_ini'])
                    if not periodo_fim_banco and st.session_state.get('data_fim'):
                        periodo_fim_banco = str(st.session_state['data_fim'])
                    sql_log = """
                        INSERT INTO log_uploads (data_upload, marketplace, loja, arquivo_nome,
                            periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status)
                        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_log, (
                        mktp, loja, arquivo_nome, periodo_ini_banco, periodo_fim_banco,
                        info['total_linhas'], registros, erros,
                        'SUCESSO' if registros > 0 else 'ERRO'
                    ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception as e:
                    st.error(f"Erro ao gravar log: {e}")

                if registros > 0:
                    st.success(f"{registros} vendas gravadas com sucesso!")
                    st.balloons()
                if duplicatas > 0:
                    st.info(f"{duplicatas} venda(s) ignorada(s) - duplicatas")
                if pendentes > 0:
                    st.warning(f"{pendentes} venda(s) salva(s) como pendentes (SKU nao cadastrado ou ASIN nao configurado).")
                if descartadas > 0:
                    st.info(f"{descartadas} venda(s) cancelada(s)/devolvida(s) rastreadas.")
                if atualizados > 0:
                    st.info(f"{atualizados} registro(s) do periodo anterior substituido(s).")
                if erros > 0:
                    st.warning(f"{erros} linha(s) com erro")
                if skus_invalidos:
                    lista_skus = ', '.join(sorted(list(skus_invalidos))[:10])
                    if len(skus_invalidos) > 10:
                        lista_skus += f" ... (+{len(skus_invalidos) - 10} SKUs)"
                    st.error(f"SKUs nao cadastrados: {lista_skus}")
                if registros > 0:
                    try:
                        recalcular_curva_abc(engine, dias=30)
                    except Exception:
                        pass
                for key in ['df_proc', 'info', 'mktp', 'mp_key', 'loja', 'arquivo_nome', 'data_ini', 'data_fim']:
                    st.session_state.pop(key, None)


# ============================================================
# TAB 2: VENDAS CONSOLIDADAS
# ============================================================

def tab_vendas_consolidadas(engine):
    st.subheader("Vendas Consolidadas")

    col1, col2, col3 = st.columns(3)
    periodo = col1.selectbox("Periodo:", ["Hoje", "Ontem", "Ultimos 7 dias", "Ultimos 15 dias", "Ultimos 30 dias", "Personalizado"])
    hoje = datetime.now().date()

    if periodo == "Hoje":
        data_ini = data_fim = hoje
    elif periodo == "Ontem":
        data_ini = data_fim = hoje - timedelta(days=1)
    elif "7" in periodo:
        data_ini = hoje - timedelta(days=7); data_fim = hoje
    elif "15" in periodo:
        data_ini = hoje - timedelta(days=15); data_fim = hoje
    elif "30" in periodo:
        data_ini = hoje - timedelta(days=30); data_fim = hoje
    else:
        data_ini = col2.date_input("De:", hoje - timedelta(days=30))
        data_fim = col3.date_input("Ate:", hoje)
        col2.caption(f"Selecionado: {data_ini.strftime('%d/%m/%Y')}")
        col3.caption(f"Selecionado: {data_fim.strftime('%d/%m/%Y')}")

    col_f1, col_f2 = st.columns(2)
    df_lojas = pd.read_sql("SELECT DISTINCT marketplace, loja FROM dim_lojas", engine)
    mktp_filtro = col_f1.selectbox("Marketplace:", ["Todos"] + sorted(df_lojas['marketplace'].unique().tolist()))
    if mktp_filtro != "Todos":
        lojas_disponiveis = df_lojas[df_lojas['marketplace'] == mktp_filtro]['loja'].tolist()
    else:
        lojas_disponiveis = df_lojas['loja'].tolist()
    loja_filtro = col_f2.selectbox("Loja:", ["Todas"] + sorted(lojas_disponiveis))

    st.markdown("**Filtrar por SKU ou Nome do Produto**")
    texto_busca_sku = st.text_input("Buscar por SKU ou Nome do Produto:", placeholder="Ex: 321, escova, kit jogo", key="busca_sku_consolidadas")
    skus_selecionados = []
    if texto_busca_sku.strip():
        df_skus_encontrados = _buscar_skus_para_filtro(engine, texto_busca_sku)
        if not df_skus_encontrados.empty:
            opcoes_sku = []
            mapa_opcao_para_sku = {}
            for _, row_sku in df_skus_encontrados.iterrows():
                nome_curto = str(row_sku['nome'])[:60] if row_sku['nome'] else ''
                opcao = f"{row_sku['sku']} - {nome_curto}"
                opcoes_sku.append(opcao)
                mapa_opcao_para_sku[opcao] = row_sku['sku']
            st.caption(f"Encontrados {len(opcoes_sku)} SKU(s) correspondentes:")
            selecionados = st.multiselect("Selecionar SKU(s):", options=opcoes_sku,
                default=opcoes_sku if len(opcoes_sku) <= 5 else [], key="multiselect_sku_consolidadas")
            skus_selecionados = [mapa_opcao_para_sku[s] for s in selecionados]
        else:
            st.info(f"Nenhum SKU ativo encontrado com '{texto_busca_sku}'.")

    mktp_param = mktp_filtro if mktp_filtro != "Todos" else None
    loja_param = loja_filtro if loja_filtro != "Todas" else None
    skus_param = skus_selecionados if skus_selecionados else None

    if texto_busca_sku.strip() and not skus_selecionados:
        st.warning("Nenhum SKU selecionado.")
        return

    df_vendas = _buscar_vendas_parametrizada(engine, data_ini, data_fim, marketplace=mktp_param, loja=loja_param, skus=skus_param)
    if df_vendas.empty:
        st.warning("Nenhuma venda encontrada com os filtros selecionados.")
        return

    df_com_custo = df_vendas[df_vendas['custo_total'] > 0]
    df_sem_custo = df_vendas[df_vendas['custo_total'] == 0]

    dias_diff = (data_fim - data_ini).days
    data_ini_ant = data_ini - timedelta(days=dias_diff + 1)
    data_fim_ant = data_fim - timedelta(days=dias_diff + 1)
    df_ant = _buscar_vendas_parametrizada(engine, data_ini_ant, data_fim_ant, marketplace=mktp_param, loja=loja_param, skus=skus_param)
    df_ant_com_custo = df_ant[df_ant['custo_total'] > 0] if not df_ant.empty else pd.DataFrame()

    st.markdown("### Indicadores do Periodo")
    c1, c2, c3, c4 = st.columns(4)

    receita_atual = df_com_custo['valor_venda_efetivo'].sum()
    receita_ant = df_ant_com_custo['valor_venda_efetivo'].sum() if not df_ant_com_custo.empty else 0
    var_receita = ((receita_atual - receita_ant) / receita_ant * 100) if receita_ant > 0 else 0
    c1.metric("Receita Total", formatar_valor(receita_atual), f"{formatar_percentual(var_receita)} vs periodo anterior")

    pedidos_atual = len(df_com_custo)
    pedidos_ant = len(df_ant_com_custo) if not df_ant_com_custo.empty else 0
    var_pedidos = ((pedidos_atual - pedidos_ant) / pedidos_ant * 100) if pedidos_ant > 0 else 0
    c2.metric("Pedidos", formatar_quantidade(pedidos_atual), formatar_percentual(var_pedidos))

    margem_atual = df_com_custo['margem_percentual'].mean() if not df_com_custo.empty else 0
    margem_ant = df_ant_com_custo['margem_percentual'].mean() if not df_ant_com_custo.empty else 0
    c3.metric("Margem Media", formatar_percentual(margem_atual), formatar_percentual(margem_atual - margem_ant))

    c4.metric("Pendentes", formatar_quantidade(len(df_sem_custo)),
        formatar_valor(df_sem_custo['valor_venda_efetivo'].sum()) + " nao contabilizados", delta_color="off")

    st.divider()
    st.subheader("Detalhamento de Vendas")

    df_display = df_vendas.copy()
    df_display['data_venda'] = pd.to_datetime(df_display['data_venda']).dt.strftime('%d/%m/%Y')
    df_display['valor_venda_efetivo'] = df_display['valor_venda_efetivo'].apply(formatar_valor)
    df_display['custo_total'] = df_display['custo_total'].apply(formatar_valor)
    df_display['margem_percentual'] = df_display['margem_percentual'].apply(formatar_percentual)

    st.dataframe(df_display[['data_venda', 'loja_origem', 'numero_pedido', 'sku', 'codigo_anuncio',
        'quantidade', 'valor_venda_efetivo', 'custo_total', 'margem_percentual']],
        use_container_width=True, height=600)

    if st.button("Download Excel"):
        buffer = io.BytesIO()
        df_excel = df_vendas.copy()
        df_excel['data_venda'] = pd.to_datetime(df_excel['data_venda']).dt.strftime('%d/%m/%Y')
        colunas_valor = ['preco_venda', 'valor_venda_efetivo', 'custo_unitario', 'custo_total',
            'imposto', 'comissao', 'frete', 'total_tarifas', 'valor_liquido', 'margem_total']
        for col in colunas_valor:
            if col in df_excel.columns:
                df_excel[col] = df_excel[col].apply(lambda x: f"{float(x):.2f}".replace('.', ','))
        if 'margem_percentual' in df_excel.columns:
            df_excel['margem_percentual'] = df_excel['margem_percentual'].apply(lambda x: f"{float(x):.2f}".replace('.', ','))
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_excel.to_excel(writer, index=False, sheet_name='Vendas')
        st.download_button("Baixar Relatorio", buffer.getvalue(),
            f"vendas_{data_ini.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.divider()
    with st.expander("Deletar Vendas (ADMIN)", expanded=False):
        st.warning("ATENCAO: Esta acao e irreversivel!")
        modo_delete = st.radio("Modo de exclusao:", ["Selecionar vendas individuais", "Deletar marketplace inteiro"], horizontal=True)
        if modo_delete == "Selecionar vendas individuais":
            df_delete = df_vendas[['id', 'data_venda', 'numero_pedido', 'sku', 'codigo_anuncio',
                'quantidade', 'valor_venda_efetivo', 'margem_percentual']].copy()
            df_delete['data_venda'] = pd.to_datetime(df_delete['data_venda']).dt.strftime('%d/%m/%Y')
            df_delete.insert(0, 'Excluir', False)
            df_editado = st.data_editor(df_delete, column_config={
                'Excluir': st.column_config.CheckboxColumn("Excluir?", default=False),
                'id': st.column_config.NumberColumn("ID", disabled=True),
                'valor_venda_efetivo': st.column_config.NumberColumn("Receita (R$)", format="%.2f", disabled=True),
            }, use_container_width=True, height=400, hide_index=True, key="delete_editor")
            ids_selecionados = df_editado[df_editado['Excluir'] == True]['id'].tolist()
            if ids_selecionados:
                st.info(f"{len(ids_selecionados)} venda(s) selecionada(s) para exclusao.")
                confirmar = st.checkbox(f"Confirmo excluir {len(ids_selecionados)} venda(s) permanentemente")
                if st.button("EXCLUIR SELECIONADAS", type="secondary"):
                    if confirmar:
                        try:
                            conn = engine.raw_connection()
                            cursor = conn.cursor()
                            placeholders = ','.join(['%s'] * len(ids_selecionados))
                            cursor.execute(f"DELETE FROM fact_vendas_snapshot WHERE id IN ({placeholders})", ids_selecionados)
                            conn.commit(); cursor.close(); conn.close()
                            st.success(f"{cursor.rowcount} venda(s) excluida(s)!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro: {e}")
                    else:
                        st.warning("Marque a confirmacao antes de excluir.")
        else:
            st.error("Esta opcao apaga TODAS as vendas de um marketplace!")
            df_lojas_del = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)
            col_del1, col_del2 = st.columns(2)
            confirm_delete = col_del1.text_input("Digite 'DELETAR' para confirmar:")
            marketplace_del = col_del2.selectbox("Marketplace a deletar:", [""] + df_lojas_del['marketplace'].tolist())
            if st.button("DELETAR TODAS DO MARKETPLACE", type="secondary"):
                if confirm_delete == "DELETAR" and marketplace_del:
                    try:
                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM fact_vendas_snapshot WHERE marketplace_origem = %s", (marketplace_del,))
                        deletados = cursor.rowcount
                        cursor.execute("DELETE FROM log_uploads WHERE marketplace = %s", (marketplace_del,))
                        conn.commit(); cursor.close(); conn.close()
                        st.success(f"{deletados} vendas de {marketplace_del} deletadas!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")


# ============================================================
# TAB 3: HISTORICO
# ============================================================

def tab_historico_uploads(engine):
    st.subheader("Historico de Importacoes")
    try:
        df_log = pd.read_sql("""SELECT data_upload, marketplace, loja, arquivo_nome,
            periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status
            FROM log_uploads ORDER BY data_upload DESC LIMIT 200""", engine)
        if not df_log.empty:
            df_log['data_upload'] = pd.to_datetime(df_log['data_upload']).dt.strftime('%d/%m/%Y %H:%M')
            for col_data in ['periodo_inicio', 'periodo_fim']:
                if col_data in df_log.columns:
                    df_log[col_data] = pd.to_datetime(df_log[col_data], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
            st.dataframe(df_log, use_container_width=True, height=600)
        else:
            st.info("Nenhuma importacao registrada ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar historico: {e}")


# ============================================================
# TAB 4: VENDAS PENDENTES (v3.2)
# ============================================================

def tab_vendas_pendentes(engine):
    st.subheader("Vendas Pendentes")
    st.markdown("Vendas que precisam de revisao. Podem ser por **SKU nao cadastrado**, **ASIN nao configurado** ou **divergencia financeira**.")

    df_resumo = buscar_pendentes_resumo(engine)
    if df_resumo.empty:
        st.success("Nenhuma venda pendente!")
        _exibir_historico_reprocessados(engine)
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("SKUs Pendentes", formatar_quantidade(len(df_resumo)))
    col2.metric("Vendas Pendentes", formatar_quantidade(int(df_resumo['total_vendas'].sum())))
    col3.metric("Receita Nao Contabilizada", formatar_valor(df_resumo['receita_total'].sum()))

    st.divider()
    _secao_pendentes_sku(engine)
    st.divider()
    _secao_pendentes_divergencia(engine)
    st.divider()
    _exibir_historico_reprocessados(engine)


def _secao_pendentes_sku(engine):
    st.markdown("### Pendentes por SKU nao cadastrado")
    st.caption("Corrija o SKU na tabela ou cadastre no modulo Gestao de SKUs. SKUs corrigidos serao lembrados.")

    df_sku = buscar_pendentes_por_tipo(engine, tipo='sku')
    if df_sku.empty:
        st.success("Nenhuma venda pendente por SKU.")
        return

    skus_validos = buscar_skus_validos(engine)
    df_edit = df_sku[['id', 'sku', 'numero_pedido', 'data_venda', 'loja_origem',
        'marketplace_origem', 'valor_venda_efetivo', 'codigo_anuncio',
        'quantidade', 'comissao', 'imposto', 'frete', 'motivo']].copy()
    df_edit['sku_original'] = df_edit['sku'].copy()
    df_edit['data_venda'] = pd.to_datetime(df_edit['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
    df_edit.insert(0, 'Selecionar', False)

    df_editado = st.data_editor(df_edit, column_config={
        'Selecionar': st.column_config.CheckboxColumn("Selecionar", default=False),
        'id': st.column_config.NumberColumn("ID", disabled=True),
        'sku': st.column_config.TextColumn("SKU (editavel)"),
        'sku_original': None,
        'numero_pedido': st.column_config.TextColumn("Pedido", disabled=True),
        'data_venda': st.column_config.TextColumn("Data", disabled=True),
        'loja_origem': st.column_config.TextColumn("Loja", disabled=True),
        'marketplace_origem': st.column_config.TextColumn("Marketplace", disabled=True),
        'valor_venda_efetivo': st.column_config.NumberColumn("Receita (R$)", format="%.2f", disabled=True),
        'codigo_anuncio': st.column_config.TextColumn("Anuncio", disabled=True),
        'quantidade': st.column_config.NumberColumn("Qtd", disabled=True),
        'comissao': st.column_config.NumberColumn("Tarifa (R$)", format="%.2f", disabled=True),
        'imposto': st.column_config.NumberColumn("Imposto (R$)", format="%.2f", disabled=True),
        'frete': st.column_config.NumberColumn("Frete (R$)", format="%.2f", disabled=True),
        'motivo': st.column_config.TextColumn("Motivo", disabled=True),
    }, use_container_width=True, height=400, hide_index=True, key="editor_pendentes_sku")

    selecionados = df_editado[df_editado['Selecionar'] == True]
    if len(selecionados) > 0:
        skus_mod = selecionados[selecionados['sku'] != selecionados['sku_original']]
        if len(skus_mod) > 0:
            st.info(f"{len(skus_mod)} SKU(s) corrigido(s). Mapeamento sera salvo automaticamente.")
        skus_nao = [str(r['sku']).strip() for _, r in selecionados.iterrows() if str(r['sku']).strip() not in skus_validos]
        if skus_nao:
            st.warning(f"SKU(s) nao cadastrado(s): {', '.join(skus_nao)}. Cadastre antes de reprocessar.")
        st.info(f"{len(selecionados)} venda(s) selecionada(s) para reprocessamento.")

        if st.button("Reprocessar SKUs Selecionados", key="btn_repro_sku", type="primary"):
            with st.spinner("Reprocessando..."):
                itens = []
                for _, r in selecionados.iterrows():
                    itens.append({
                        'id': r['id'], 'sku': str(r['sku']).strip(),
                        'sku_original': str(r['sku_original']).strip(),
                        'valor_venda_efetivo': r['valor_venda_efetivo'],
                        'comissao': r['comissao'], 'imposto': r['imposto'], 'frete': r['frete'],
                        'quantidade': r['quantidade'], 'marketplace_origem': r['marketplace_origem'],
                        'loja_origem': r['loja_origem'], 'numero_pedido': r['numero_pedido'],
                        'data_venda': pd.to_datetime(r['data_venda'], format='%d/%m/%Y', errors='coerce'),
                        'codigo_anuncio': r.get('codigo_anuncio', ''), 'arquivo_origem': '',
                    })
                resultado = reprocessar_pendentes_manual(engine, itens)
                if resultado['sucesso'] > 0:
                    st.success(f"{resultado['mensagem']}")
                    if resultado['mapeados'] > 0:
                        st.info(f"{resultado['mapeados']} mapeamento(s) salvo(s).")
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else:
                    st.error(f"{resultado['mensagem']}")


def _secao_pendentes_divergencia(engine):
    st.markdown("### Pendentes por Divergencia Financeira")
    st.caption("Vendas de carrinho com divergencia > R$ 5,00. Ajuste valores e reprocesse.")

    df_div = buscar_pendentes_por_tipo(engine, tipo='divergencia')
    if df_div.empty:
        st.success("Nenhuma venda pendente por divergencia financeira.")
        return

    df_edit = df_div[['id', 'sku', 'numero_pedido', 'data_venda', 'loja_origem',
        'marketplace_origem', 'valor_venda_efetivo', 'codigo_anuncio',
        'quantidade', 'comissao', 'imposto', 'frete', 'motivo']].copy()
    df_edit['sku_original'] = df_edit['sku'].copy()
    df_edit['data_venda'] = pd.to_datetime(df_edit['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
    df_edit.insert(0, 'Selecionar', False)

    df_editado = st.data_editor(df_edit, column_config={
        'Selecionar': st.column_config.CheckboxColumn("Selecionar", default=False),
        'id': st.column_config.NumberColumn("ID", disabled=True),
        'sku': st.column_config.TextColumn("SKU (editavel)"),
        'sku_original': None,
        'valor_venda_efetivo': st.column_config.NumberColumn("Receita (R$)", format="%.2f"),
        'comissao': st.column_config.NumberColumn("Tarifa (R$)", format="%.2f"),
        'imposto': st.column_config.NumberColumn("Imposto (R$)", format="%.2f"),
        'frete': st.column_config.NumberColumn("Frete (R$)", format="%.2f"),
    }, use_container_width=True, height=400, hide_index=True, key="editor_pendentes_div")

    selecionados = df_editado[df_editado['Selecionar'] == True]
    if len(selecionados) > 0:
        st.info(f"{len(selecionados)} venda(s) selecionada(s).")
        if st.button("Reprocessar Divergencias Selecionadas", key="btn_repro_div", type="primary"):
            with st.spinner("Reprocessando..."):
                itens = []
                for _, r in selecionados.iterrows():
                    itens.append({
                        'id': r['id'], 'sku': str(r['sku']).strip(),
                        'sku_original': str(r['sku_original']).strip(),
                        'valor_venda_efetivo': r['valor_venda_efetivo'],
                        'comissao': r['comissao'], 'imposto': r['imposto'], 'frete': r['frete'],
                        'quantidade': r['quantidade'], 'marketplace_origem': r['marketplace_origem'],
                        'loja_origem': r['loja_origem'], 'numero_pedido': r['numero_pedido'],
                        'data_venda': pd.to_datetime(r['data_venda'], format='%d/%m/%Y', errors='coerce'),
                        'codigo_anuncio': r.get('codigo_anuncio', ''), 'arquivo_origem': '',
                    })
                resultado = reprocessar_pendentes_manual(engine, itens)
                if resultado['sucesso'] > 0:
                    st.success(f"{resultado['mensagem']}")
                    try: recalcular_curva_abc(engine, dias=30)
                    except: pass
                    st.rerun()
                else:
                    st.error(f"{resultado['mensagem']}")


def _exibir_historico_reprocessados(engine):
    """FIX v3.2: Usa buscar_pendentes_revisados() - corrige erro List argument."""
    with st.expander("Historico de vendas reprocessadas / revisadas", expanded=False):
        try:
            df_historico = buscar_pendentes_revisados(engine, limit=100)
            if not df_historico.empty:
                if 'data_venda' in df_historico.columns:
                    df_historico['data_venda'] = pd.to_datetime(df_historico['data_venda'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
                if 'valor_venda_efetivo' in df_historico.columns:
                    df_historico['valor_venda_efetivo'] = df_historico['valor_venda_efetivo'].apply(formatar_valor)
                st.dataframe(df_historico, use_container_width=True, height=300, hide_index=True)
                st.caption(f"Total: {len(df_historico)} venda(s)")
            else:
                st.info("Nenhuma venda reprocessada ou revisada ainda.")
        except Exception as e:
            st.error(f"Erro ao buscar historico: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    st.title("Central de Vendas")
    engine = get_engine()

    tab1, tab2, tab3, tab4 = st.tabs([
        "Processar Upload", "Vendas Consolidadas", "Historico", "Vendas Pendentes"
    ])

    with tab1: tab_processar_upload(engine)
    with tab2: tab_vendas_consolidadas(engine)
    with tab3: tab_historico_uploads(engine)
    with tab4: tab_vendas_pendentes(engine)


if __name__ == "__main__":
    main()
