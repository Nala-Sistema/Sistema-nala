"""
CENTRAL DE UPLOADS - Sistema Nala
Interface principal para upload e processamento de vendas

VERSÃO 2.1 (11/03/2026):
  - Tab 2: CORREÇÃO filtro SKU — query parametrizada (Bug 4)
  - Tab 2: Busca inteligente por SKU ou Nome do Produto (Bug 5)
  - Tab 2: Todas as queries agora usam parâmetros seguros (sem f-string SQL)

VERSÃO 2.0 (10/03/2026):
  - Tab 2: Filtros por loja e SKU adicionados
  - Tab 4: Vendas Pendentes (SKUs não cadastrados) com reprocessamento por SKU
  - Proteção contra duplicatas: mensagem com contagem de ignoradas
  - Recálculo automático da Curva ABC após gravação (30 dias)
  - Mensagens de resultado expandidas (duplicatas, pendentes)

VERSÃO anterior:
  - Histórico: datas convertidas para formato banco antes de gravar log
  - Deletar vendas: agora permite selecionar vendas individuais
  - Download Excel: datas garantidas em dd/mm/aaaa
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

# Imports dos módulos
from formatadores import formatar_valor, formatar_percentual, formatar_quantidade
from database_utils import (
    get_engine,
    gravar_log_upload,
    buscar_pendentes,
    buscar_pendentes_resumo,
    reprocessar_pendentes_por_sku,
    recalcular_curva_abc,
)
from processar_ml import processar_arquivo_ml, gravar_vendas_ml
from processar_shopee import processar_arquivo_shopee, gravar_vendas_shopee


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _detectar_marketplace(mktp: str) -> str:
    """
    Normaliza o nome do marketplace para roteamento interno.
    Retorna: 'ML', 'SHOPEE', ou 'DESCONHECIDO'
    """
    mktp_upper = mktp.upper()
    if 'MERCADO' in mktp_upper and 'LIVRE' in mktp_upper:
        return 'ML'
    if 'SHOPEE' in mktp_upper:
        return 'SHOPEE'
    return 'DESCONHECIDO'


def _converter_data_br_para_banco(data_str):
    """
    Converte data do formato brasileiro (dd/mm/aaaa) para formato banco (aaaa-mm-dd).
    Necessário porque PostgreSQL espera ISO 8601 em colunas DATE.
    Se receber None ou string vazia, retorna None.
    """
    if not data_str or str(data_str).strip() == '':
        return None
    try:
        return datetime.strptime(str(data_str).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        # Se já estiver em formato ISO ou outro formato, tenta retornar como está
        return str(data_str).strip()


def _exibir_alertas_comissao(alertas: list):
    """
    Exibe alertas de divergência de comissão no painel da Shopee.
    Mostra tabela compacta com os pedidos divergentes.
    """
    if not alertas:
        return

    st.warning(
        f"⚠️ **{len(alertas)} pedido(s) com comissão diferente da tabela vigente** "
        f"(pode indicar regra anterior ou promoção especial da Shopee)"
    )

    with st.expander(f"🔍 Ver detalhes das divergências ({len(alertas)} pedidos)", expanded=False):
        df_alertas = pd.DataFrame(alertas)
        df_alertas = df_alertas.rename(columns={
            'pedido':            'Pedido',
            'sku':               'SKU',
            'comissao_arquivo':  'Cobrado (R$)',
            'comissao_esperada': 'Esperado (R$)',
            'divergencia':       'Diferença (R$)',
        })
        # Formatar valores
        df_alertas['Cobrado (R$)']   = df_alertas['Cobrado (R$)'].apply(formatar_valor)
        df_alertas['Esperado (R$)']  = df_alertas['Esperado (R$)'].apply(formatar_valor)
        df_alertas['Diferença (R$)'] = df_alertas['Diferença (R$)'].apply(formatar_valor)

        st.dataframe(df_alertas, use_container_width=True, hide_index=True)
        st.caption(
            "Tabela vigente (01/03/2026): até R$79,99 → 20%+R$4 | "
            "R$80-99,99 → 14%+R$16 | R$100-199,99 → 14%+R$20 | "
            "R$200-499,99 → 14%+R$26 | acima R$500 → 14%+R$26"
        )


def _buscar_skus_para_filtro(engine, texto_busca):
    """
    Busca SKUs em dim_produtos que contenham o texto digitado
    no campo sku OU no campo nome. Retorna lista para o multiselect.

    Args:
        engine: SQLAlchemy engine
        texto_busca: texto digitado pelo usuário (ex: "321" ou "escova")

    Retorna:
        DataFrame com colunas: sku, nome (filtrados)
    """
    if not texto_busca or not texto_busca.strip():
        return pd.DataFrame(columns=['sku', 'nome'])

    query = """
        SELECT sku, nome
        FROM dim_produtos
        WHERE status = 'Ativo'
          AND (
              sku ILIKE %s
              OR nome ILIKE %s
          )
        ORDER BY sku
        LIMIT 50
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
    """
    Busca vendas no fact_vendas_snapshot com query 100% parametrizada.
    Resolve o Bug 4: f-string com ILIKE causava falha no filtro SKU.

    Args:
        engine: SQLAlchemy engine
        data_ini: data início (date)
        data_fim: data fim (date)
        marketplace: filtro marketplace (None = todos)
        loja: filtro loja (None = todas)
        skus: lista de SKUs para filtrar (None = todos)

    Retorna:
        DataFrame com as vendas
    """
    # Montar query com parâmetros seguros
    query = "SELECT * FROM fact_vendas_snapshot WHERE data_venda BETWEEN %s AND %s"
    params = [str(data_ini), str(data_fim)]

    if marketplace:
        query += " AND marketplace_origem = %s"
        params.append(marketplace)

    if loja:
        query += " AND loja_origem = %s"
        params.append(loja)

    if skus and len(skus) > 0:
        # Filtro por lista de SKUs selecionados
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
    """Tab 1: Upload e processamento de arquivos"""

    # 1. BUSCAR LOJAS DO BANCO
    try:
        df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas", engine)
    except Exception:
        st.error("⚠️ Erro ao carregar lojas. Configure lojas em Config.")
        return

    if df_lojas.empty:
        st.warning("⚠️ Cadastre lojas no módulo Config primeiro.")
        return

    # 2. SELEÇÃO DE MARKETPLACE E LOJA
    col1, col2, col3 = st.columns(3)

    mktp   = col1.selectbox("Marketplace:", sorted(df_lojas['marketplace'].unique()))
    lojas  = df_lojas[df_lojas['marketplace'] == mktp]['loja'].tolist()
    loja   = col2.selectbox("Loja:", lojas)
    imposto = df_lojas[df_lojas['loja'] == loja]['imposto'].values[0]

    st.info(f"📍 {loja} | Imposto: {formatar_percentual(imposto)}")

    # 3. UPLOAD DE ARQUIVO
    arquivo = st.file_uploader("📂 Upload do arquivo de vendas (XLSX)", type=['xlsx'])

    # 4. BOTÃO ANALISAR
    if arquivo and st.button("🔍 ANALISAR ARQUIVO", type="primary"):
        with st.spinner("Processando arquivo..."):

            mp = _detectar_marketplace(mktp)

            if mp == 'ML':
                df_proc, info = processar_arquivo_ml(arquivo, loja, imposto, engine)
            elif mp == 'SHOPEE':
                df_proc, info = processar_arquivo_shopee(arquivo, loja, imposto, engine)
            else:
                st.error(f"⚠️ Processador para '{mktp}' ainda não implementado.")
                return

            if df_proc is not None:
                st.session_state['df_proc']       = df_proc
                st.session_state['info']          = info
                st.session_state['mktp']          = mktp
                st.session_state['mp_key']        = mp
                st.session_state['loja']          = loja
                st.session_state['arquivo_nome']  = arquivo.name
                st.rerun()
            else:
                st.error(f"❌ {info}")

    # 5. PREVIEW (se já processou)
    if 'df_proc' in st.session_state:
        df_proc      = st.session_state['df_proc']
        info         = st.session_state['info']
        mktp         = st.session_state['mktp']
        mp_key       = st.session_state.get('mp_key', 'ML')
        loja         = st.session_state['loja']
        arquivo_nome = st.session_state['arquivo_nome']

        # Mensagem de sucesso
        st.success(f"✅ {info['total_linhas']} vendas processadas com sucesso!")

        # Informações do arquivo
        col_a, col_b, col_c = st.columns(3)
        col_a.info(f"📅 Período: {info['periodo_inicio']} a {info['periodo_fim']}")
        col_b.info(f"🏪 Loja: {loja}")
        col_c.info(f"📦 Arquivo: {arquivo_nome}")

        # Alertas gerais
        if info.get('linhas_descartadas', 0) > 0:
            st.warning(
                f"⚠️ {info['linhas_descartadas']} linhas descartadas "
                f"(canceladas, devolvidas ou sem receita)"
            )

        if info.get('skus_sem_custo', 0) > 0:
            st.warning(
                f"⚠️ {info['skus_sem_custo']} SKUs sem custo cadastrado "
                f"(margem calculada com custo = R$ 0,00)"
            )

        # Alerta Shopee: carrinhos compostos
        if mp_key == 'SHOPEE' and info.get('carrinhos', 0) > 0:
            st.info(
                f"🛒 {info['carrinhos']} pedido(s) de carrinho composto detectado(s) — "
                f"comissão calculada pela tabela oficial por item."
            )

        # Alerta Shopee: divergências de comissão
        if mp_key == 'SHOPEE':
            _exibir_alertas_comissao(info.get('alertas_comissao', []))

        # PREVIEW COM FORMATAÇÃO BR
        st.subheader("📋 Preview das Vendas (primeiras 20 linhas)")

        df_preview = df_proc.copy()
        df_preview['receita']    = df_preview['receita'].apply(formatar_valor)
        df_preview['tarifa']     = df_preview['tarifa'].apply(formatar_valor)
        df_preview['imposto']    = df_preview['imposto'].apply(formatar_valor)
        df_preview['frete']      = df_preview['frete'].apply(formatar_valor)
        df_preview['custo']      = df_preview['custo'].apply(formatar_valor)
        df_preview['margem']     = df_preview['margem'].apply(formatar_valor)
        df_preview['margem_pct'] = df_preview['margem_pct'].apply(formatar_percentual)

        st.dataframe(
            df_preview.head(20),
            use_container_width=True,
            height=400
        )

        # 6. BOTÃO GRAVAR
        st.divider()

        col_btn1, col_btn2 = st.columns([1, 3])

        if col_btn1.button("💾 GRAVAR NO BANCO", type="primary", use_container_width=True):

            with st.spinner("Gravando vendas no banco..."):

                # Gravar vendas no processador correto (RETORNO EXPANDIDO v2.0)
                if mp_key == 'ML':
                    registros, erros, skus_invalidos, duplicatas, pendentes = gravar_vendas_ml(
                        df_proc, mktp, loja, arquivo_nome, engine
                    )
                elif mp_key == 'SHOPEE':
                    registros, erros, skus_invalidos, duplicatas, pendentes = gravar_vendas_shopee(
                        df_proc, mktp, loja, arquivo_nome, engine
                    )
                else:
                    st.error("⚠️ Processador não identificado para gravação.")
                    return

                # GRAVAR LOG DE IMPORTAÇÃO
                try:
                    conn   = engine.raw_connection()
                    cursor = conn.cursor()

                    # Converter datas de dd/mm/aaaa para aaaa-mm-dd
                    periodo_ini_banco = _converter_data_br_para_banco(info['periodo_inicio'])
                    periodo_fim_banco = _converter_data_br_para_banco(info['periodo_fim'])

                    sql_log = """
                        INSERT INTO log_uploads (
                            data_upload, marketplace, loja, arquivo_nome,
                            periodo_inicio, periodo_fim, total_linhas,
                            linhas_importadas, linhas_erro, status
                        ) VALUES (
                            NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    cursor.execute(sql_log, (
                        mktp, loja, arquivo_nome,
                        periodo_ini_banco, periodo_fim_banco,
                        info['total_linhas'], registros, erros,
                        'SUCESSO' if registros > 0 else 'ERRO'
                    ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception as e:
                    st.error(f"⚠️ Erro ao gravar log de importação: {e}")

                # MENSAGENS DE RESULTADO (expandidas v2.0)
                if registros > 0:
                    st.success(f"✅ {registros} vendas gravadas com sucesso!")
                    st.balloons()

                if duplicatas > 0:
                    st.info(f"🔄 {duplicatas} venda(s) ignorada(s) — já existiam no banco (duplicatas)")

                if pendentes > 0:
                    st.warning(
                        f"⏳ {pendentes} venda(s) salva(s) como **pendentes** "
                        f"(SKU não cadastrado). Vá na tab 'Vendas Pendentes' para reprocessar."
                    )

                if erros > 0:
                    st.warning(f"⚠️ {erros} linha(s) com erro")

                if skus_invalidos:
                    lista_skus = ', '.join(sorted(list(skus_invalidos))[:10])
                    if len(skus_invalidos) > 10:
                        lista_skus += f" ... (+{len(skus_invalidos) - 10} SKUs)"
                    st.error(f"❌ SKUs não cadastrados: {lista_skus}")

                # RECALCULAR CURVA ABC (automático, 30 dias)
                if registros > 0:
                    try:
                        resultado_abc = recalcular_curva_abc(engine, dias=30)
                        if resultado_abc['total_anuncios'] > 0:
                            st.info(
                                f"📊 Curva ABC atualizada: "
                                f"{resultado_abc['a']} anúncio(s) A, "
                                f"{resultado_abc['b']} B, "
                                f"{resultado_abc['c']} C "
                                f"(últimos 30 dias)"
                            )
                    except Exception:
                        pass  # Não bloquear gravação se ABC falhar

                # Limpar session_state
                for key in ['df_proc', 'info', 'mktp', 'mp_key', 'loja', 'arquivo_nome']:
                    st.session_state.pop(key, None)


# ============================================================
# TAB 2: VENDAS CONSOLIDADAS
# v2.1: Query parametrizada + Busca inteligente de SKU
# ============================================================

def tab_vendas_consolidadas(engine):
    """Tab 2: Visualização de vendas consolidadas"""

    st.subheader("📊 Vendas Consolidadas")

    # FILTROS DE PERÍODO
    col1, col2, col3 = st.columns(3)

    periodo = col1.selectbox(
        "Período:",
        ["Hoje", "Ontem", "Últimos 7 dias", "Últimos 15 dias", "Últimos 30 dias", "Personalizado"]
    )

    hoje = datetime.now().date()

    if periodo == "Hoje":
        data_ini = data_fim = hoje
    elif periodo == "Ontem":
        data_ini = data_fim = hoje - timedelta(days=1)
    elif "7" in periodo:
        data_ini = hoje - timedelta(days=7)
        data_fim = hoje
    elif "15" in periodo:
        data_ini = hoje - timedelta(days=15)
        data_fim = hoje
    elif "30" in periodo:
        data_ini = hoje - timedelta(days=30)
        data_fim = hoje
    else:
        data_ini = col2.date_input("De:", hoje - timedelta(days=30))
        data_fim = col3.date_input("Até:", hoje)
        col2.caption(f"Selecionado: {data_ini.strftime('%d/%m/%Y')}")
        col3.caption(f"Selecionado: {data_fim.strftime('%d/%m/%Y')}")

    # FILTROS: MARKETPLACE, LOJA
    col_f1, col_f2 = st.columns(2)

    # Marketplace
    df_lojas     = pd.read_sql("SELECT DISTINCT marketplace, loja FROM dim_lojas", engine)
    mktp_filtro  = col_f1.selectbox("Marketplace:", ["Todos"] + sorted(df_lojas['marketplace'].unique().tolist()))

    # Loja (depende do marketplace selecionado)
    if mktp_filtro != "Todos":
        lojas_disponiveis = df_lojas[df_lojas['marketplace'] == mktp_filtro]['loja'].tolist()
    else:
        lojas_disponiveis = df_lojas['loja'].tolist()
    loja_filtro = col_f2.selectbox("Loja:", ["Todas"] + sorted(lojas_disponiveis))

    # ============================================================
    # BUSCA INTELIGENTE DE SKU (v2.1 — igual ao Gestão de SKUs)
    # Passo 1: Digitar parte do SKU ou nome do produto
    # Passo 2: Selecionar um ou mais SKUs encontrados
    # ============================================================
    st.markdown("**🔍 Filtrar por SKU ou Nome do Produto**")

    texto_busca_sku = st.text_input(
        "Buscar por SKU ou Nome do Produto:",
        placeholder="Ex: 321, escova, kit jogo",
        key="busca_sku_consolidadas"
    )

    # Lista de SKUs selecionados para filtrar as vendas
    skus_selecionados = []

    if texto_busca_sku.strip():
        # Buscar SKUs que combinam com o texto digitado
        df_skus_encontrados = _buscar_skus_para_filtro(engine, texto_busca_sku)

        if not df_skus_encontrados.empty:
            # Montar opções para o multiselect: "SKU — Nome do Produto"
            opcoes_sku = []
            mapa_opcao_para_sku = {}
            for _, row_sku in df_skus_encontrados.iterrows():
                nome_curto = str(row_sku['nome'])[:60] if row_sku['nome'] else ''
                opcao = f"{row_sku['sku']} — {nome_curto}"
                opcoes_sku.append(opcao)
                mapa_opcao_para_sku[opcao] = row_sku['sku']

            st.caption(f"Encontrados {len(opcoes_sku)} SKU(s) correspondentes:")

            # Multiselect para escolher um ou mais SKUs
            selecionados = st.multiselect(
                "Selecionar SKU(s):",
                options=opcoes_sku,
                default=opcoes_sku if len(opcoes_sku) <= 5 else [],
                key="multiselect_sku_consolidadas"
            )

            # Extrair os SKUs puros das opções selecionadas
            skus_selecionados = [mapa_opcao_para_sku[s] for s in selecionados]
        else:
            st.info(f"ℹ️ Nenhum SKU ativo encontrado com '{texto_busca_sku}'.")

    # PREPARAR PARÂMETROS PARA A QUERY
    mktp_param = mktp_filtro if mktp_filtro != "Todos" else None
    loja_param = loja_filtro if loja_filtro != "Todas" else None
    skus_param = skus_selecionados if skus_selecionados else None

    # Se digitou texto mas não selecionou nenhum SKU, não buscar vendas
    if texto_busca_sku.strip() and not skus_selecionados:
        st.warning("⚠️ Nenhum SKU selecionado. Selecione pelo menos um SKU para filtrar as vendas.")
        return

    # QUERY PRINCIPAL (parametrizada — corrige Bug 4)
    df_vendas = _buscar_vendas_parametrizada(
        engine, data_ini, data_fim,
        marketplace=mktp_param,
        loja=loja_param,
        skus=skus_param
    )

    if df_vendas.empty:
        st.warning("⚠️ Nenhuma venda encontrada com os filtros selecionados.")
        return

    # Separar com/sem custo
    df_com_custo = df_vendas[df_vendas['custo_total'] > 0]
    df_sem_custo = df_vendas[df_vendas['custo_total'] == 0]

    # Período anterior para comparação (mesma query parametrizada)
    dias_diff    = (data_fim - data_ini).days
    data_ini_ant = data_ini - timedelta(days=dias_diff + 1)
    data_fim_ant = data_fim - timedelta(days=dias_diff + 1)

    df_ant = _buscar_vendas_parametrizada(
        engine, data_ini_ant, data_fim_ant,
        marketplace=mktp_param,
        loja=loja_param,
        skus=skus_param
    )
    df_ant_com_custo = df_ant[df_ant['custo_total'] > 0] if not df_ant.empty else pd.DataFrame()

    # INDICADORES
    st.markdown("### 📈 Indicadores do Período")
    c1, c2, c3, c4 = st.columns(4)

    # Receita
    receita_atual = df_com_custo['valor_venda_efetivo'].sum()
    receita_ant   = df_ant_com_custo['valor_venda_efetivo'].sum() if not df_ant_com_custo.empty else 0
    var_receita   = ((receita_atual - receita_ant) / receita_ant * 100) if receita_ant > 0 else 0
    c1.metric("Receita Total", formatar_valor(receita_atual), f"{formatar_percentual(var_receita)} vs período anterior")

    # Pedidos
    pedidos_atual = len(df_com_custo)
    pedidos_ant   = len(df_ant_com_custo) if not df_ant_com_custo.empty else 0
    var_pedidos   = ((pedidos_atual - pedidos_ant) / pedidos_ant * 100) if pedidos_ant > 0 else 0
    c2.metric("Pedidos", formatar_quantidade(pedidos_atual), formatar_percentual(var_pedidos))

    # Margem
    margem_atual = df_com_custo['margem_percentual'].mean() if not df_com_custo.empty else 0
    margem_ant   = df_ant_com_custo['margem_percentual'].mean() if not df_ant_com_custo.empty else 0
    var_margem   = margem_atual - margem_ant
    c3.metric("Margem Média", formatar_percentual(margem_atual), formatar_percentual(var_margem))

    # Pendentes (sem custo)
    c4.metric(
        "Pendentes",
        formatar_quantidade(len(df_sem_custo)),
        formatar_valor(df_sem_custo['valor_venda_efetivo'].sum()) + " não contabilizados",
        delta_color="off"
    )

    # TABELA DETALHADA
    st.divider()
    st.subheader("📋 Detalhamento de Vendas")

    df_display = df_vendas.copy()
    df_display['data_venda'] = pd.to_datetime(df_display['data_venda']).dt.strftime('%d/%m/%Y')

    # Formatação BR para exibição
    df_display['valor_venda_efetivo'] = df_display['valor_venda_efetivo'].apply(formatar_valor)
    df_display['custo_total']         = df_display['custo_total'].apply(formatar_valor)
    df_display['margem_percentual']   = df_display['margem_percentual'].apply(formatar_percentual)

    st.dataframe(
        df_display[[
            'data_venda', 'loja_origem', 'numero_pedido', 'sku', 'codigo_anuncio',
            'quantidade', 'valor_venda_efetivo', 'custo_total', 'margem_percentual'
        ]],
        use_container_width=True,
        height=600
    )

    # DOWNLOAD EXCEL
    if st.button("📥 Download Excel"):
        buffer   = io.BytesIO()
        df_excel = df_vendas.copy()

        # Garantir data em dd/mm/aaaa no Excel
        df_excel['data_venda'] = pd.to_datetime(df_excel['data_venda']).dt.strftime('%d/%m/%Y')

        # Formatar valores monetários com vírgula decimal (padrão BR)
        colunas_valor = [
            'preco_venda', 'valor_venda_efetivo', 'custo_unitario', 'custo_total',
            'imposto', 'comissao', 'frete', 'total_tarifas', 'valor_liquido', 'margem_total'
        ]
        for col in colunas_valor:
            if col in df_excel.columns:
                df_excel[col] = df_excel[col].apply(lambda x: f"{float(x):.2f}".replace('.', ','))

        if 'margem_percentual' in df_excel.columns:
            df_excel['margem_percentual'] = df_excel['margem_percentual'].apply(
                lambda x: f"{float(x):.2f}".replace('.', ',')
            )

        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_excel.to_excel(writer, index=False, sheet_name='Vendas')

        st.download_button(
            "⬇️ Baixar Relatório",
            buffer.getvalue(),
            f"vendas_{data_ini.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ============================================================
    # DELETAR VENDAS (ADMIN)
    # ============================================================
    st.divider()
    with st.expander("🗑️ Deletar Vendas (ADMIN)", expanded=False):
        st.warning("⚠️ **ATENÇÃO:** Esta ação é irreversível!")

        modo_delete = st.radio(
            "Modo de exclusão:",
            ["Selecionar vendas individuais", "Deletar marketplace inteiro"],
            horizontal=True
        )

        # ----- MODO 1: DELETAR VENDAS INDIVIDUAIS -----
        if modo_delete == "Selecionar vendas individuais":
            st.markdown("Marque as vendas que deseja excluir:")

            # Preparar dados para seleção (usar dados originais, não formatados)
            df_delete = df_vendas[['id', 'data_venda', 'numero_pedido', 'sku',
                                    'codigo_anuncio', 'quantidade', 'valor_venda_efetivo',
                                    'margem_percentual']].copy()

            df_delete['data_venda'] = pd.to_datetime(df_delete['data_venda']).dt.strftime('%d/%m/%Y')

            # Adicionar coluna de seleção
            df_delete.insert(0, '🗑️ Excluir', False)

            # Exibir tabela editável
            df_editado = st.data_editor(
                df_delete,
                column_config={
                    '🗑️ Excluir': st.column_config.CheckboxColumn(
                        "Excluir?",
                        help="Marque para excluir esta venda",
                        default=False,
                    ),
                    'id': st.column_config.NumberColumn("ID", disabled=True),
                    'data_venda': st.column_config.TextColumn("Data", disabled=True),
                    'numero_pedido': st.column_config.TextColumn("Pedido", disabled=True),
                    'sku': st.column_config.TextColumn("SKU", disabled=True),
                    'codigo_anuncio': st.column_config.TextColumn("Anúncio", disabled=True),
                    'quantidade': st.column_config.NumberColumn("Qtd", disabled=True),
                    'valor_venda_efetivo': st.column_config.NumberColumn(
                        "Receita (R$)", format="%.2f", disabled=True
                    ),
                    'margem_percentual': st.column_config.NumberColumn(
                        "Margem %", format="%.2f", disabled=True
                    ),
                },
                use_container_width=True,
                height=400,
                hide_index=True,
                key="delete_editor"
            )

            # Contar selecionadas
            ids_selecionados = df_editado[df_editado['🗑️ Excluir'] == True]['id'].tolist()
            qtd_selecionadas = len(ids_selecionados)

            if qtd_selecionadas > 0:
                st.info(f"📌 {qtd_selecionadas} venda(s) selecionada(s) para exclusão.")

                # Confirmação em dois passos
                confirmar = st.checkbox(
                    f"✅ Confirmo que desejo excluir {qtd_selecionadas} venda(s) permanentemente"
                )

                if st.button("🗑️ EXCLUIR SELECIONADAS", type="secondary"):
                    if confirmar:
                        try:
                            conn   = engine.raw_connection()
                            cursor = conn.cursor()

                            # Deletar por lista de IDs
                            placeholders = ','.join(['%s'] * len(ids_selecionados))
                            cursor.execute(
                                f"DELETE FROM fact_vendas_snapshot WHERE id IN ({placeholders})",
                                ids_selecionados
                            )

                            deletados = cursor.rowcount
                            conn.commit()
                            cursor.close()
                            conn.close()

                            st.success(f"✅ {deletados} venda(s) excluída(s) com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Erro ao excluir vendas: {e}")
                    else:
                        st.warning("⚠️ Marque a confirmação antes de excluir.")

        # ----- MODO 2: DELETAR MARKETPLACE INTEIRO -----
        else:
            st.error(
                "⛔ Esta opção apaga TODAS as vendas de um marketplace. "
                "Use apenas em casos extremos!"
            )

            df_lojas_del = pd.read_sql("SELECT DISTINCT marketplace FROM dim_lojas", engine)

            col_del1, col_del2 = st.columns(2)
            confirm_delete  = col_del1.text_input("Digite 'DELETAR' para confirmar:")
            marketplace_del = col_del2.selectbox(
                "Marketplace a deletar:", [""] + df_lojas_del['marketplace'].tolist()
            )

            if st.button("🗑️ DELETAR TODAS DO MARKETPLACE", type="secondary"):
                if confirm_delete == "DELETAR" and marketplace_del:
                    try:
                        conn   = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "DELETE FROM fact_vendas_snapshot WHERE marketplace_origem = %s",
                            (marketplace_del,)
                        )
                        deletados = cursor.rowcount
                        cursor.execute(
                            "DELETE FROM log_uploads WHERE marketplace = %s",
                            (marketplace_del,)
                        )
                        conn.commit()
                        cursor.close()
                        conn.close()
                        st.success(f"✅ {deletados} vendas de {marketplace_del} deletadas!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro ao deletar: {e}")
                else:
                    st.warning("⚠️ Confirme digitando 'DELETAR' e selecionando marketplace.")


# ============================================================
# TAB 3: HISTÓRICO DE UPLOADS
# ============================================================

def tab_historico_uploads(engine):
    """Tab 3: Histórico de importações"""

    st.subheader("📚 Histórico de Importações")

    try:
        df_log = pd.read_sql(
            """SELECT
                data_upload, marketplace, loja, arquivo_nome,
                periodo_inicio, periodo_fim, total_linhas,
                linhas_importadas, linhas_erro, status
               FROM log_uploads
               ORDER BY data_upload DESC
               LIMIT 200""",
            engine
        )

        if not df_log.empty:
            # Formatar datas para exibição BR
            df_log['data_upload'] = pd.to_datetime(df_log['data_upload']).dt.strftime('%d/%m/%Y %H:%M')

            # Converter periodo_inicio e periodo_fim para exibição BR (podem estar em formato ISO)
            for col_data in ['periodo_inicio', 'periodo_fim']:
                if col_data in df_log.columns:
                    df_log[col_data] = pd.to_datetime(
                        df_log[col_data], errors='coerce'
                    ).dt.strftime('%d/%m/%Y').fillna('-')

            st.dataframe(df_log, use_container_width=True, height=600)
        else:
            st.info("ℹ️ Nenhuma importação registrada ainda.")

    except Exception as e:
        st.error(f"⚠️ Erro ao carregar histórico: {e}")
        # Mostrar erro detalhado para diagnóstico
        with st.expander("🔍 Detalhes do erro"):
            st.code(str(e))
            st.markdown(
                "Se o erro mencionar 'relation log_uploads does not exist', "
                "a tabela precisa ser criada no banco.\n\n"
                "SQL para criar:\n"
            )
            st.code("""
CREATE TABLE IF NOT EXISTS log_uploads (
    id SERIAL PRIMARY KEY,
    data_upload TIMESTAMP DEFAULT NOW(),
    marketplace VARCHAR(100),
    loja VARCHAR(100),
    arquivo_nome VARCHAR(500),
    periodo_inicio DATE,
    periodo_fim DATE,
    total_linhas INTEGER DEFAULT 0,
    linhas_importadas INTEGER DEFAULT 0,
    linhas_erro INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'PENDENTE'
);
            """, language="sql")


# ============================================================
# TAB 4: VENDAS PENDENTES (NOVO v2.0)
# ============================================================

def tab_vendas_pendentes(engine):
    """
    Tab 4: Vendas com SKU não cadastrado.
    Permite visualizar e reprocessar após cadastro do SKU.
    Reprocessamento por SKU individual.
    """

    st.subheader("⏳ Vendas Pendentes (SKU não cadastrado)")

    st.markdown(
        "Vendas que foram importadas mas cujo SKU não existia em **dim_produtos** "
        "no momento do upload. Cadastre o SKU no módulo **Gestão de SKUs** e "
        "clique em **Reprocessar** para gravá-las."
    )

    # RESUMO POR SKU
    df_resumo = buscar_pendentes_resumo(engine)

    if df_resumo.empty:
        st.success("✅ Nenhuma venda pendente! Todos os SKUs estão cadastrados.")
        return

    # Indicadores
    col1, col2, col3 = st.columns(3)
    total_vendas = int(df_resumo['total_vendas'].sum())
    total_skus = len(df_resumo)
    receita_total = df_resumo['receita_total'].sum()

    col1.metric("SKUs Pendentes", formatar_quantidade(total_skus))
    col2.metric("Vendas Pendentes", formatar_quantidade(total_vendas))
    col3.metric("Receita Não Contabilizada", formatar_valor(receita_total))

    st.divider()

    # TABELA DE SKUs PENDENTES COM BOTÃO REPROCESSAR
    st.markdown("### SKUs aguardando cadastro")

    for _, row_resumo in df_resumo.iterrows():
        sku = row_resumo['sku']
        qtd = int(row_resumo['total_vendas'])
        receita = float(row_resumo['receita_total'])
        mktps = row_resumo['marketplaces']
        lojas = row_resumo['lojas']

        # Formatar datas
        try:
            primeira = pd.to_datetime(row_resumo['primeira_venda']).strftime('%d/%m/%Y')
            ultima = pd.to_datetime(row_resumo['ultima_venda']).strftime('%d/%m/%Y')
            periodo_txt = f"{primeira} a {ultima}"
        except:
            periodo_txt = "-"

        # Card do SKU
        with st.container():
            col_info, col_btn = st.columns([4, 1])

            col_info.markdown(
                f"**{sku}** — {qtd} venda(s), {formatar_valor(receita)} receita | "
                f"{mktps} | {lojas} | Período: {periodo_txt}"
            )

            # Botão reprocessar com key única
            if col_btn.button("🔄 Reprocessar", key=f"repro_{sku}", use_container_width=True):
                with st.spinner(f"Reprocessando {sku}..."):
                    resultado = reprocessar_pendentes_por_sku(engine, sku)

                    if resultado['sucesso'] > 0:
                        st.success(f"✅ {resultado['mensagem']}")

                        # Recalcular ABC após reprocessamento
                        try:
                            recalcular_curva_abc(engine, dias=30)
                        except:
                            pass

                        st.rerun()
                    elif resultado['erros'] > 0:
                        st.warning(f"⚠️ {resultado['mensagem']}")
                    else:
                        st.error(f"❌ {resultado['mensagem']}")

            st.markdown("---")

    # DETALHES EXPANDÍVEIS
    with st.expander("📋 Ver todas as vendas pendentes em detalhe", expanded=False):
        df_todas = buscar_pendentes(engine, status='Pendente')

        if not df_todas.empty:
            df_exibir = df_todas[[
                'sku', 'numero_pedido', 'data_venda', 'loja_origem',
                'marketplace_origem', 'valor_venda_efetivo', 'codigo_anuncio'
            ]].copy()

            # Formatar
            df_exibir['data_venda'] = pd.to_datetime(
                df_exibir['data_venda'], errors='coerce'
            ).dt.strftime('%d/%m/%Y').fillna('-')

            df_exibir['valor_venda_efetivo'] = df_exibir['valor_venda_efetivo'].apply(formatar_valor)

            st.dataframe(df_exibir, use_container_width=True, height=400, hide_index=True)

    # HISTÓRICO DE REPROCESSADOS
    with st.expander("✅ Ver vendas já reprocessadas", expanded=False):
        df_repro = buscar_pendentes(engine, status='Reprocessado')

        if not df_repro.empty:
            df_repro_exibir = df_repro[[
                'sku', 'numero_pedido', 'data_venda', 'loja_origem',
                'marketplace_origem', 'valor_venda_efetivo'
            ]].copy()

            df_repro_exibir['data_venda'] = pd.to_datetime(
                df_repro_exibir['data_venda'], errors='coerce'
            ).dt.strftime('%d/%m/%Y').fillna('-')

            df_repro_exibir['valor_venda_efetivo'] = df_repro_exibir['valor_venda_efetivo'].apply(
                formatar_valor
            )

            st.dataframe(df_repro_exibir, use_container_width=True, height=300, hide_index=True)
            st.caption(f"Total: {len(df_repro)} venda(s) reprocessada(s)")
        else:
            st.info("Nenhuma venda reprocessada ainda.")


# ============================================================
# MAIN
# ============================================================

def main():
    """Função principal do módulo"""

    st.title("💰 Central de Vendas")

    engine = get_engine()

    tab1, tab2, tab3, tab4 = st.tabs([
        "📤 Processar Upload",
        "📊 Vendas Consolidadas",
        "📚 Histórico",
        "⏳ Vendas Pendentes"
    ])

    with tab1:
        tab_processar_upload(engine)

    with tab2:
        tab_vendas_consolidadas(engine)

    with tab3:
        tab_historico_uploads(engine)

    with tab4:
        tab_vendas_pendentes(engine)


if __name__ == "__main__":
    main()
