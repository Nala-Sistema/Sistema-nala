"""
CENTRAL DE UPLOADS - Sistema Nala
VERSAO 3.3 (17/03/2026):
  - FIX: Periodo obrigatorio APENAS para Amazon (Shein/Magalu auto-detectam)
  - FIX: Preview com valores formatados (2 casas decimais)
  - FIX: Historico reprocessados usa buscar_pendentes_revisados
  - NOVO: Pedido original exibido na tabela de vendas consolidadas
  - NOVO: Integracao Shein e Magalu
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

            # Colunas financeiras para formatar
            colunas_valor = ['receita', 'tarifa', 'imposto', 'frete', 'custo', 'margem',
                             'comissao', 'taxa_fixa', 'taxa_estocagem', 'valor_liquido',
                             'preco_venda', 'desconto_parceiro', 'desconto_marketplace', 'total_tarifas']
            for col in colunas_valor:
                if col in df_preview.columns:
                    df_preview[col] = df_preview[col].apply(formatar_valor)
            if 'margem_pct' in df_preview.columns:
                df_preview['margem_pct'] = df_preview['margem_pct'].apply(formatar_percentual)

            # Escolher colunas relevantes para exibicao
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
                    # Shein: datas auto-detectadas (data_ini/data_fim opcionais)
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_shein(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                elif mp_key == 'MAGALU':
                    # Magalu: datas auto-detectadas
                    registros, erros, skus_invalidos, duplicatas, pendentes, descartadas, atualizados = gravar_vendas_magalu(
                        df_proc, mktp, loja, arquivo_nome, engine,
                        descartes=info.get('descartes', []), pendentes_carrinho=info.get('pendentes_carrinho', []))
                else:
                    st.error("⚠️ Processador não identificado."); return

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
# TAB 2: VENDAS CONSOLIDADAS
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

    # FIX v3.3: Mostrar pedido_original se existir
    cols_exibir = ['data_venda', 'loja_origem', 'sku', 'codigo_anuncio', 'quantidade',
                   'valor_venda_efetivo', 'custo_total', 'margem_percentual']
    if 'pedido_original' in df_d.columns:
        cols_exibir.insert(2, 'pedido_original')
    else:
        cols_exibir.insert(2, 'numero_pedido')

    st.dataframe(df_d[cols_exibir], use_container_width=True, height=600)

    if st.button("📥 Download Excel"):
        buffer = io.BytesIO()
        df_e = df_vendas.copy()
        df_e['data_venda'] = pd.to_datetime(df_e['data_venda']).dt.strftime('%d/%m/%Y')
        for col in ['preco_venda','valor_venda_efetivo','custo_unitario','custo_total','imposto','comissao','frete','total_tarifas','valor_liquido','margem_total']:
            if col in df_e.columns: df_e[col] = df_e[col].apply(lambda x: f"{float(x):.2f}".replace('.',','))
        if 'margem_percentual' in df_e.columns: df_e['margem_percentual'] = df_e['margem_percentual'].apply(lambda x: f"{float(x):.2f}".replace('.',','))
        with pd.ExcelWriter(buffer, engine='openpyxl') as w: df_e.to_excel(w, index=False, sheet_name='Vendas')
        st.download_button("⬇️ Baixar", buffer.getvalue(),
            f"vendas_{data_ini.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
# TAB 3: HISTORICO
# ============================================================

def tab_historico_uploads(engine):
    st.subheader("📚 Histórico de Importações")
    try:
        df_log = pd.read_sql("""SELECT data_upload, marketplace, loja, arquivo_nome,
            periodo_inicio, periodo_fim, total_linhas, linhas_importadas, linhas_erro, status
            FROM log_uploads ORDER BY data_upload DESC LIMIT 200""", engine)
        if not df_log.empty:
            df_log['data_upload'] = pd.to_datetime(df_log['data_upload']).dt.strftime('%d/%m/%Y %H:%M')
            for c in ['periodo_inicio','periodo_fim']:
                if c in df_log.columns: df_log[c] = pd.to_datetime(df_log[c], errors='coerce').dt.strftime('%d/%m/%Y').fillna('-')
            st.dataframe(df_log, use_container_width=True, height=600)
        else: st.info("Nenhuma importação registrada.")
    except Exception as e: st.error(f"⚠️ Erro: {e}")


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
