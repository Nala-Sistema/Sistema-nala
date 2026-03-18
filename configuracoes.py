"""
CONFIGURAÇÕES - Sistema Nala
Versão: 3.0 (17/03/2026)

CHANGELOG v3.0:
  - REWRITE: Tab Amazon completamente refeita
  - NOVO: Campo SKU inteligente com autocomplete (busca por SKU ou nome do produto)
  - NOVO: Modo "Cadastrar Novo" / "Editar Existente" no Vincular Manual
  - NOVO: Botões Editar e Excluir na Lista de Anúncios
  - FIX: Campo SKU não apaga mais ao digitar (removido clear_on_submit, usa session_state)
  - Tabs Frete ML, Impostos e Usuários mantidas intactas

CHANGELOG v2.1:
  - FIX: Todas queries usam raw_connection via _query_to_df
  - FIX: Tab Amazon Importar agora tem botão de processar com UPSERT

Tabs:
  1. Amazon — Vincular ASINs, taxas, importação massiva
  2. Frete ML (FLEX) — Custo FLEX por loja Mercado Livre
  3. Impostos & Lojas — Gestão das 14 lojas (imposto + custo_flex)
  4. Gestão de Usuários — Criar, editar, ativar/desativar usuários
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"


def get_engine():
    return create_engine(DB_URL)


def _query_to_df(engine, query, params=None):
    """Executa query e retorna DataFrame (raw_connection para SQLAlchemy 2.x)."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception:
        return pd.DataFrame()


def main():
    st.header("⚙️ Configurações Nala")
    engine = get_engine()

    t_amz, t_ml, t_fisc, t_users = st.tabs([
        "📦 Amazon",
        "🚚 Frete ML (FLEX)",
        "💰 Impostos & Lojas",
        "👤 Gestão de Usuários"
    ])

    with t_amz:
        _tab_amazon(engine)

    with t_ml:
        _tab_frete_ml(engine)

    with t_fisc:
        _tab_impostos_lojas(engine)

    with t_users:
        _tab_usuarios(engine)


# ============================================================
# TAB 1: AMAZON (v3.0 — REWRITE COMPLETO)
# ============================================================

def _buscar_produtos_para_autocomplete(engine, termo):
    """Busca produtos em dim_produtos por SKU ou nome (ILIKE)."""
    if not termo or len(termo.strip()) < 2:
        return []
    query = """
        SELECT sku, nome 
        FROM dim_produtos 
        WHERE status = 'Ativo' 
          AND (sku ILIKE %s OR nome ILIKE %s)
        ORDER BY sku ASC
        LIMIT 15
    """
    padrao = f"%{termo.strip()}%"
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, (padrao, padrao))
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(zip(colunas, row)) for row in rows]
    except Exception:
        return []


def _carregar_configs_amazon(engine):
    """Carrega todas as configs Amazon como DataFrame."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, asin, sku, logistica, comissao_percentual, taxa_fixa, frete_estimado 
            FROM dim_config_marketplace 
            WHERE marketplace = 'AMAZON' AND ativo = true
            ORDER BY asin, logistica
        """)
        colunas = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(rows, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao listar anúncios: {e}")
        return pd.DataFrame()


def _salvar_config_amazon(engine, asin, sku, logistica, comissao, taxa_fixa, frete_est):
    """Salva config Amazon (DELETE+INSERT por asin+logistica)."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM dim_config_marketplace WHERE asin = %s AND marketplace = 'AMAZON' AND logistica = %s",
            (asin.strip(), logistica.strip())
        )
        cursor.execute("""
            INSERT INTO dim_config_marketplace 
                (asin, sku, marketplace, loja, logistica, 
                 comissao_percentual, taxa_fixa, frete_estimado, ativo, data_vigencia)
            VALUES (%s, %s, 'AMAZON', 'AMAZON', %s, %s, %s, %s, TRUE, CURRENT_DATE)
        """, (asin.strip(), sku.strip(), logistica.strip(), comissao, taxa_fixa, frete_est))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False


def _deletar_config_amazon(engine, asin, logistica):
    """Exclui config Amazon por asin+logistica."""
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM dim_config_marketplace WHERE asin = %s AND marketplace = 'AMAZON' AND logistica = %s",
            (asin.strip(), logistica.strip())
        )
        deletados = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return deletados > 0
    except Exception as e:
        st.error(f"Erro ao excluir: {e}")
        return False


def _tab_amazon(engine):
    """Configuração de anúncios Amazon (ASINs, taxas, De-Para) — v3.0"""

    s1, s2, s3 = st.tabs(["📋 Lista de Anúncios", "➕ Vincular Manual", "📥 Importar"])

    # ================================================================
    # SUB-TAB 1: LISTA DE ANÚNCIOS (com Editar e Excluir)
    # ================================================================
    with s1:
        df_amz = _carregar_configs_amazon(engine)

        if not df_amz.empty:
            st.success(f"✅ {len(df_amz)} anúncio(s) Amazon cadastrado(s)")

            # Exibir tabela (sem coluna id)
            df_exibir = df_amz.drop(columns=['id'], errors='ignore')
            st.dataframe(df_exibir, use_container_width=True, hide_index=True)

            st.divider()

            # ---- AÇÕES: EDITAR / EXCLUIR ----
            st.subheader("🔧 Ações")

            # Criar opções para selecionar (ASIN + Logística)
            df_amz['_label'] = df_amz['asin'] + ' | ' + df_amz['sku'] + ' | ' + df_amz['logistica']
            opcoes_lista = df_amz['_label'].tolist()

            selecionado = st.selectbox(
                "Selecione o anúncio:",
                opcoes_lista,
                key="sel_anuncio_acoes"
            )

            if selecionado:
                idx_sel = opcoes_lista.index(selecionado)
                row_sel = df_amz.iloc[idx_sel]

                col_edit, col_del = st.columns(2)

                # ---- EDITAR ----
                with col_edit:
                    with st.expander("✏️ Editar este anúncio", expanded=False):
                        with st.form("form_editar_anuncio", clear_on_submit=False):
                            st.text_input("ASIN", value=row_sel['asin'], disabled=True, key="edit_asin_display")
                            st.text_input("Logística", value=row_sel['logistica'], disabled=True, key="edit_log_display")

                            # Campo SKU com busca
                            edit_busca = st.text_input(
                                "🔍 Buscar novo SKU (ou manter atual)",
                                value=row_sel['sku'],
                                key="edit_busca_sku"
                            )

                            e_com = st.text_input(
                                "Comissão %",
                                value=str(row_sel['comissao_percentual']).replace('.', ','),
                                key="edit_comissao"
                            )
                            e_tax = st.text_input(
                                "Taxa Fixa R$",
                                value=str(row_sel['taxa_fixa']).replace('.', ','),
                                key="edit_taxa"
                            )
                            e_fre = st.text_input(
                                "Frete Est. R$",
                                value=str(row_sel['frete_estimado']).replace('.', ','),
                                key="edit_frete"
                            )

                            if st.form_submit_button("💾 Salvar Alteração"):
                                # Resolver SKU: se digitou algo diferente, buscar
                                sku_final = edit_busca.strip()
                                if ' — ' in sku_final:
                                    sku_final = sku_final.split(' — ')[0].strip()

                                if not sku_final:
                                    st.error("SKU não pode ser vazio.")
                                else:
                                    ok = _salvar_config_amazon(
                                        engine,
                                        row_sel['asin'],
                                        sku_final,
                                        row_sel['logistica'],
                                        float(e_com.replace(',', '.')),
                                        float(e_tax.replace(',', '.')),
                                        float(e_fre.replace(',', '.'))
                                    )
                                    if ok:
                                        st.success("✅ Anúncio atualizado!")
                                        st.rerun()

                # ---- EXCLUIR ----
                with col_del:
                    with st.expander("🗑️ Excluir este anúncio", expanded=False):
                        st.warning(
                            f"Tem certeza que deseja excluir?\n\n"
                            f"**ASIN:** {row_sel['asin']}\n\n"
                            f"**SKU:** {row_sel['sku']}\n\n"
                            f"**Logística:** {row_sel['logistica']}"
                        )
                        if st.button("❌ Confirmar Exclusão", key="btn_excluir_anuncio", type="primary"):
                            ok = _deletar_config_amazon(engine, row_sel['asin'], row_sel['logistica'])
                            if ok:
                                st.success("✅ Anúncio excluído!")
                                st.rerun()
                            else:
                                st.error("Erro ao excluir.")
        else:
            st.info("Nenhum anúncio Amazon cadastrado.")

    # ================================================================
    # SUB-TAB 2: VINCULAR MANUAL (com autocomplete e modos)
    # ================================================================
    with s2:
        st.subheader("Vincular Anúncio Amazon")

        # ---- MODO: Cadastrar ou Editar ----
        modo = st.radio(
            "Modo de Operação:",
            ["Cadastrar Novo Anúncio", "Editar Anúncio Existente"],
            key="modo_vincular_amz",
            horizontal=True
        )

        # ---- Se EDITAR: selecionar anúncio existente para pré-preencher ----
        prefill = {}
        if modo == "Editar Anúncio Existente":
            df_amz = _carregar_configs_amazon(engine)
            if df_amz.empty:
                st.info("Nenhum anúncio cadastrado para editar.")
                return

            df_amz['_label'] = df_amz['asin'] + ' | ' + df_amz['sku'] + ' | ' + df_amz['logistica']
            sel_editar = st.selectbox(
                "Selecione o anúncio para editar:",
                df_amz['_label'].tolist(),
                key="sel_editar_vincular"
            )
            if sel_editar:
                idx = df_amz['_label'].tolist().index(sel_editar)
                r = df_amz.iloc[idx]
                prefill = {
                    'asin': r['asin'],
                    'sku': r['sku'],
                    'logistica': r['logistica'],
                    'comissao': str(r['comissao_percentual']).replace('.', ','),
                    'taxa_fixa': str(r['taxa_fixa']).replace('.', ','),
                    'frete': str(r['frete_estimado']).replace('.', ','),
                }

        st.divider()

        # ---- CAMPO SKU INTELIGENTE (fora do form para ser dinâmico) ----
        st.markdown("**1. Selecionar Produto (SKU)**")

        # Inicializar session state
        if 'sku_vinc_selecionado' not in st.session_state:
            st.session_state['sku_vinc_selecionado'] = prefill.get('sku', '')

        # Se modo editar mudou o prefill, atualizar
        if prefill.get('sku') and modo == "Editar Anúncio Existente":
            # Só atualizar se o select mudou
            if st.session_state.get('_ultimo_prefill_sku') != prefill.get('sku'):
                st.session_state['sku_vinc_selecionado'] = prefill['sku']
                st.session_state['_ultimo_prefill_sku'] = prefill['sku']

        busca_termo = st.text_input(
            "🔍 Digite parte do SKU ou nome do produto:",
            key="busca_sku_vincular",
            placeholder="Ex: L-0152, Anel Tonificador, LWI..."
        )

        if busca_termo and len(busca_termo.strip()) >= 2:
            resultados = _buscar_produtos_para_autocomplete(engine, busca_termo)
            if resultados:
                opcoes_display = [f"{p['sku']} — {p['nome']}" for p in resultados]
                escolha = st.selectbox(
                    "Selecione o produto:",
                    opcoes_display,
                    key="sel_produto_vincular"
                )
                if escolha:
                    sku_escolhido = escolha.split(' — ')[0].strip()
                    st.session_state['sku_vinc_selecionado'] = sku_escolhido
            else:
                st.warning("Nenhum produto encontrado. Verifique o termo de busca.")

        # Mostrar SKU selecionado
        sku_atual = st.session_state.get('sku_vinc_selecionado', '')
        if sku_atual:
            st.info(f"✅ SKU selecionado: **{sku_atual}**")

        st.divider()

        # ---- FORMULÁRIO DE DADOS ----
        st.markdown("**2. Dados do Anúncio**")

        # Determinar logística padrão para o selectbox
        opcoes_logistica = ["FBA", "DBA", "DBA PF", "Crossdocking"]
        idx_logistica = 0
        if prefill.get('logistica'):
            try:
                idx_logistica = opcoes_logistica.index(prefill['logistica'])
            except ValueError:
                # Se a logística do prefill não está na lista, adiciona
                opcoes_logistica.append(prefill['logistica'])
                idx_logistica = len(opcoes_logistica) - 1

        with st.form("form_vincular_amazon", clear_on_submit=False):
            c1, c2 = st.columns(2)
            f_asin = c1.text_input(
                "ASIN *",
                value=prefill.get('asin', ''),
                key="vinc_asin",
                disabled=(modo == "Editar Anúncio Existente"),
            )
            f_log = c2.selectbox(
                "Logística *",
                opcoes_logistica,
                index=idx_logistica,
                key="vinc_logistica",
                disabled=(modo == "Editar Anúncio Existente"),
            )

            c3, c4, c5 = st.columns(3)
            f_com = c3.text_input("Comissão %", value=prefill.get('comissao', '12,00'), key="vinc_comissao")
            f_tax = c4.text_input("Taxa Fixa R$", value=prefill.get('taxa_fixa', '0,00'), key="vinc_taxa")
            f_fre = c5.text_input("Frete Est. R$", value=prefill.get('frete', '0,00'), key="vinc_frete")

            # Mostrar SKU escolhido dentro do form (informativo)
            st.text_input(
                "SKU Nala (selecionado acima)",
                value=sku_atual,
                disabled=True,
                key="vinc_sku_display"
            )

            submitted = st.form_submit_button("💾 Salvar Anúncio Amazon", type="primary")

            if submitted:
                asin_val = f_asin.strip() if modo == "Cadastrar Novo Anúncio" else prefill.get('asin', f_asin).strip()
                log_val = f_log if modo == "Cadastrar Novo Anúncio" else prefill.get('logistica', f_log)
                sku_val = st.session_state.get('sku_vinc_selecionado', '').strip()

                if not asin_val:
                    st.error("❌ ASIN é obrigatório.")
                elif not sku_val:
                    st.error("❌ Selecione um SKU no campo de busca acima.")
                else:
                    try:
                        ok = _salvar_config_amazon(
                            engine,
                            asin_val,
                            sku_val,
                            log_val,
                            float(f_com.replace(',', '.')),
                            float(f_tax.replace(',', '.')),
                            float(f_fre.replace(',', '.'))
                        )
                        if ok:
                            acao = "atualizado" if modo == "Editar Anúncio Existente" else "cadastrado"
                            st.success(f"✅ Anúncio {acao} com sucesso!")
                            # Limpar seleção
                            st.session_state['sku_vinc_selecionado'] = ''
                            st.session_state['_ultimo_prefill_sku'] = ''
                            st.rerun()
                    except ValueError:
                        st.error("❌ Verifique os valores numéricos (comissão, taxa, frete).")

    # ================================================================
    # SUB-TAB 3: IMPORTAÇÃO MASSIVA (mantida da v2.1)
    # ================================================================
    with s3:
        _tab_amazon_importar(engine)


def _tab_amazon_importar(engine):
    """Importação massiva de configs Amazon via Excel."""
    st.subheader("📥 Importação Massiva Amazon")

    cols_amz = ["asin", "sku", "logistica", "comissao_percentual", "taxa_fixa", "frete_estimado"]

    # Template
    tmpl_amz = pd.DataFrame(columns=cols_amz)
    buf_amz = io.BytesIO()
    with pd.ExcelWriter(buf_amz, engine='openpyxl') as wr:
        tmpl_amz.to_excel(wr, index=False)
    st.download_button("📄 Baixar Template Amazon", data=buf_amz.getvalue(),
                       file_name="template_amazon.xlsx")

    # Upload
    arquivo_amz = st.file_uploader("Subir arquivo preenchido (.xlsx)",
                                   type=["xlsx"], key="up_amz_file")

    if arquivo_amz and st.button("📥 Processar Importação Amazon", type="primary"):
        try:
            df_import = pd.read_excel(arquivo_amz)
            st.info(f"📄 Arquivo lido: {len(df_import)} linhas, colunas: {list(df_import.columns)}")

            colunas_esperadas = {'asin', 'sku'}
            if not colunas_esperadas.issubset(set(df_import.columns)):
                st.error(f"❌ Colunas obrigatórias: {colunas_esperadas}. Encontrado: {set(df_import.columns)}")
            else:
                conn = engine.raw_connection()
                cursor = conn.cursor()
                importados = 0
                atualizados = 0
                erros_imp = 0
                primeiro_erro = None

                total = len(df_import)
                progress = st.progress(0)
                status = st.empty()

                def _safe_float(val, default=0.0):
                    try:
                        s = str(val).replace(',', '.').strip()
                        if s in ('nan', '', 'None', 'none'):
                            return default
                        return float(s)
                    except (ValueError, TypeError):
                        return default

                for i, (_, row) in enumerate(df_import.iterrows()):
                    progress.progress(min((i + 1) / total, 1.0))
                    status.text(f"Processando {i + 1} de {total}...")

                    try:
                        asin = str(row.get('asin', '')).strip()
                        sku = str(row.get('sku', '')).strip()

                        if not asin or not sku or asin == 'nan' or sku == 'nan':
                            erros_imp += 1
                            continue

                        logistica = str(row.get('logistica', 'FBA')).strip()
                        if logistica in ('nan', '', 'None'):
                            logistica = 'FBA'

                        comissao = _safe_float(row.get('comissao_percentual', 0))
                        taxa = _safe_float(row.get('taxa_fixa', 0))
                        frete = _safe_float(row.get('frete_estimado', 0))

                        cursor.execute(
                            "DELETE FROM dim_config_marketplace WHERE asin = %s AND marketplace = 'AMAZON' AND logistica = %s",
                            (asin, logistica)
                        )
                        deletados = cursor.rowcount
                        if deletados > 0:
                            atualizados += deletados

                        cursor.execute("""
                            INSERT INTO dim_config_marketplace 
                                (asin, sku, marketplace, loja, logistica, 
                                 comissao_percentual, taxa_fixa, frete_estimado, ativo, data_vigencia)
                            VALUES (%s, %s, 'AMAZON', 'AMAZON', %s, %s, %s, %s, TRUE, CURRENT_DATE)
                        """, (asin, sku, logistica, comissao, taxa, frete))
                        importados += 1

                    except Exception as e:
                        erros_imp += 1
                        if primeiro_erro is None:
                            primeiro_erro = str(e)[:300]

                conn.commit()
                cursor.close()
                conn.close()
                progress.empty()
                status.empty()

                if importados > 0:
                    st.success(f"✅ {importados} anúncio(s) importado(s)!")
                if atualizados > 0:
                    st.info(f"🔄 {atualizados} anúncio(s) atualizado(s) (já existiam)")
                if erros_imp > 0:
                    st.warning(f"⚠️ {erros_imp} linha(s) com erro")
                    if primeiro_erro:
                        st.error(f"Primeiro erro: {primeiro_erro}")
                if importados == 0 and erros_imp == 0:
                    st.warning("⚠️ Nenhuma linha processada.")

        except Exception as e:
            st.error(f"❌ Erro ao processar arquivo: {e}")


# ============================================================
# TAB 2: FRETE ML (FLEX) — sem mudanças
# ============================================================

def _tab_frete_ml(engine):
    """Configuração do custo FLEX por loja do Mercado Livre."""

    st.subheader("🚚 Custo FLEX por Loja (Mercado Livre)")

    st.markdown(
        "Configure o custo do frete FLEX para cada loja ML. "
        "Este valor é usado no cálculo de margem durante o processamento de vendas. "
        "O valor padrão é **R$ 12,90**."
    )

    df_ml = _query_to_df(engine,
        """SELECT loja, imposto, custo_flex 
           FROM dim_lojas 
           WHERE UPPER(marketplace) LIKE '%%MERCADO%%LIVRE%%'
           ORDER BY loja"""
    )

    if df_ml.empty:
        st.warning("Nenhuma loja Mercado Livre cadastrada. Cadastre na tab 'Impostos & Lojas'.")
        return

    df_ml['custo_flex'] = df_ml['custo_flex'].fillna(12.90)

    st.info(f"📍 {len(df_ml)} loja(s) Mercado Livre encontrada(s)")

    df_editado = st.data_editor(
        df_ml,
        column_config={
            'loja': st.column_config.TextColumn("Loja", disabled=True),
            'imposto': st.column_config.NumberColumn(
                "Imposto (%)", format="%.2f", disabled=True,
                help="Edite na tab 'Impostos & Lojas'"
            ),
            'custo_flex': st.column_config.NumberColumn(
                "Custo FLEX (R$)", format="%.2f", min_value=0.0,
                help="Custo do frete FLEX cobrado pela transportadora"
            ),
        },
        use_container_width=True,
        hide_index=True,
        key="editor_flex_ml"
    )

    if st.button("💾 Salvar Custos FLEX", key="btn_salvar_flex", type="primary"):
        try:
            conn = engine.raw_connection()
            cursor = conn.cursor()

            atualizados = 0
            for _, row in df_editado.iterrows():
                custo = float(str(row['custo_flex']).replace(',', '.')) if row['custo_flex'] else 12.90
                cursor.execute(
                    "UPDATE dim_lojas SET custo_flex = %s WHERE loja = %s",
                    (custo, row['loja'])
                )
                atualizados += cursor.rowcount

            conn.commit()
            cursor.close()
            conn.close()

            st.success(f"✅ Custo FLEX atualizado para {atualizados} loja(s)!")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

    with st.expander("ℹ️ Como funciona o frete FLEX", expanded=False):
        st.markdown("""
**Frete FLEX (Mercado Livre):**

O frete FLEX é quando o Mercado Livre usa uma transportadora terceirizada para entregas rápidas.

**Cálculo no Sistema Nala:**
- Custo do frete FLEX = `custo_flex` (configurado aqui) − `receita_envio` (o que o comprador pagou)
- Imposto sobre FLEX = R$ 0,00 (sem incidência)

**Exemplo:** Se o custo FLEX é R$ 12,90 e o comprador pagou R$ 8,00 de frete, o custo líquido para você é R$ 4,90.

**Frete Normal** (não FLEX): custo = `tarifa_envio` − `receita_envio`, com imposto normal.
        """)


# ============================================================
# TAB 3: IMPOSTOS & LOJAS — sem mudanças
# ============================================================

def _tab_impostos_lojas(engine):
    """Gestão das 14 lojas — imposto e custo_flex."""

    st.subheader("Gerenciamento das 14 Lojas")

    df_lojas = _query_to_df(engine,
        "SELECT marketplace, loja, imposto, custo_flex FROM dim_lojas ORDER BY marketplace ASC"
    )

    if df_lojas.empty:
        data_nala = [
            ["MERCADO LIVRE", "ML-Nala", 10.00, 12.90],
            ["MERCADO LIVRE", "ML-LPT", 10.00, 12.90],
            ["MERCADO LIVRE", "ML-YanniRJ", 10.00, 12.90],
            ["MERCADO LIVRE", "ML-YanniSP", 10.00, 12.90],
            ["AMAZON", "AMZ-Innovare(CPF)", 0.00, 0.00],
            ["AMAZON", "AMZ-Nala", 10.00, 0.00],
            ["AMAZON", "AMZ-LPT", 10.00, 0.00],
            ["AMAZON", "AMZ-Yanni", 10.00, 0.00],
            ["SHOPEE", "Shopee Lithouse(Nala)", 10.00, 0.00],
            ["SHOPEE", "Shopee Litstore(Yanni)", 10.00, 0.00],
            ["SHOPEE", "Shopee-LPT", 10.00, 0.00],
            ["SHEIN", "Shein Yanni", 10.00, 0.00],
            ["SHEIN", "Shein LPT", 10.00, 0.00],
            ["MAGALU", "Magalu-Nala", 10.00, 0.00],
        ]
        df_lojas = pd.DataFrame(data_nala, columns=["marketplace", "loja", "imposto", "custo_flex"])

    if 'custo_flex' not in df_lojas.columns:
        df_lojas['custo_flex'] = 0.00
    df_lojas['custo_flex'] = df_lojas['custo_flex'].fillna(0.00)

    st.info("Ajuste as alíquotas e custo FLEX, depois clique em salvar.")

    df_editado = st.data_editor(
        df_lojas,
        column_config={
            'marketplace': st.column_config.TextColumn("Marketplace"),
            'loja': st.column_config.TextColumn("Loja"),
            'imposto': st.column_config.NumberColumn("Imposto (%)", format="%.2f", min_value=0.0),
            'custo_flex': st.column_config.NumberColumn(
                "Custo FLEX (R$)", format="%.2f", min_value=0.0,
                help="Usado no cálculo de frete FLEX (Mercado Livre)"
            ),
        },
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="editor_lojas_fiscal"
    )

    if st.button("💾 Salvar Estrutura Fiscal no Banco", key="btn_salvar_fiscal"):
        try:
            conn = engine.raw_connection()
            cursor = conn.cursor()

            cursor.execute("TRUNCATE TABLE dim_lojas")

            for _, row in df_editado.iterrows():
                val_imposto = float(str(row['imposto']).replace(',', '.')) if row['imposto'] else 0.0
                val_flex = float(str(row['custo_flex']).replace(',', '.')) if row['custo_flex'] else 0.0

                cursor.execute(
                    "INSERT INTO dim_lojas (marketplace, loja, imposto, custo_flex) VALUES (%s, %s, %s, %s)",
                    (row['marketplace'], row['loja'], val_imposto, val_flex)
                )

            conn.commit()
            cursor.close()
            conn.close()

            st.success(f"✅ {len(df_editado)} lojas salvas com sucesso!")
            st.rerun()
        except Exception as e:
            st.error(f"Erro: {e}")


# ============================================================
# TAB 4: GESTÃO DE USUÁRIOS — sem mudanças
# ============================================================

def _tab_usuarios(engine):
    """Gestão de usuários do sistema (RBAC: ADMIN, COMPRAS, GESTOR)."""

    st.subheader("👤 Gestão de Usuários")

    usuario_logado = st.session_state.get('usuario', {})
    role_logado = usuario_logado.get('role', '')

    if role_logado != 'ADMIN' and role_logado != '':
        st.warning("⚠️ Apenas administradores podem gerenciar usuários.")
        return

    # ---- LISTAR USUÁRIOS ----
    st.markdown("### Usuários Cadastrados")

    df_users = _query_to_df(engine,
        """SELECT username, role, ativo, created_at 
           FROM dim_usuarios 
           ORDER BY created_at DESC"""
    )

    if not df_users.empty:
        df_display = df_users.copy()
        df_display['created_at'] = pd.to_datetime(df_display['created_at']).dt.strftime('%d/%m/%Y %H:%M')
        df_display['ativo'] = df_display['ativo'].apply(lambda x: "✅ Ativo" if x else "❌ Inativo")
        df_display = df_display.rename(columns={
            'username': 'Usuário', 'role': 'Perfil',
            'ativo': 'Status', 'created_at': 'Criado em'
        })
        st.dataframe(df_display, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum usuário cadastrado.")

    st.divider()

    # ---- CRIAR NOVO USUÁRIO ----
    st.markdown("### ➕ Criar Novo Usuário")

    with st.form("form_novo_usuario", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        novo_username = col1.text_input("Username")
        novo_role = col2.selectbox("Perfil", ["ADMIN", "COMPRAS", "GESTOR"])
        novo_senha = col3.text_input("Senha", type="password")
        novo_senha_confirm = st.text_input("Confirmar Senha", type="password")

        if st.form_submit_button("💾 Criar Usuário", type="primary"):
            if not novo_username or not novo_username.strip():
                st.error("❌ Username não pode ser vazio.")
            elif not novo_senha or len(novo_senha) < 6:
                st.error("❌ Senha deve ter pelo menos 6 caracteres.")
            elif novo_senha != novo_senha_confirm:
                st.error("❌ As senhas não coincidem.")
            else:
                try:
                    import bcrypt
                    password_hash = bcrypt.hashpw(
                        novo_senha.encode('utf-8'), bcrypt.gensalt()
                    ).decode('utf-8')

                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO dim_usuarios (username, password_hash, role) VALUES (%s, %s, %s)",
                        (novo_username.strip(), password_hash, novo_role)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                    st.success(f"✅ Usuário '{novo_username}' criado com perfil {novo_role}!")
                    st.rerun()
                except Exception as e:
                    if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                        st.error(f"❌ Usuário '{novo_username}' já existe!")
                    else:
                        st.error(f"❌ Erro ao criar usuário: {e}")

    st.divider()

    # ---- ALTERAR SENHA ----
    st.markdown("### 🔑 Alterar Senha")

    if not df_users.empty:
        with st.form("form_alterar_senha", clear_on_submit=True):
            col1, col2 = st.columns(2)
            user_senha = col1.selectbox("Usuário", df_users['username'].tolist(), key="sel_user_senha")
            nova_senha = col2.text_input("Nova Senha", type="password", key="nova_senha_input")
            nova_senha_conf = st.text_input("Confirmar Nova Senha", type="password", key="nova_senha_conf")

            if st.form_submit_button("🔑 Alterar Senha"):
                if not nova_senha or len(nova_senha) < 6:
                    st.error("❌ Senha deve ter pelo menos 6 caracteres.")
                elif nova_senha != nova_senha_conf:
                    st.error("❌ As senhas não coincidem.")
                else:
                    try:
                        import bcrypt
                        password_hash = bcrypt.hashpw(
                            nova_senha.encode('utf-8'), bcrypt.gensalt()
                        ).decode('utf-8')

                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE dim_usuarios SET password_hash = %s WHERE username = %s",
                            (password_hash, user_senha)
                        )
                        conn.commit()
                        cursor.close()
                        conn.close()
                        st.success(f"✅ Senha alterada para '{user_senha}'!")
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")

    st.divider()

    # ---- ATIVAR / DESATIVAR ----
    st.markdown("### 🔄 Ativar / Desativar Usuário")

    if not df_users.empty:
        col1, col2 = st.columns(2)

        with col1:
            users_ativos = df_users[(df_users['ativo'] == True) & (df_users['username'] != 'admin')]['username'].tolist()
            if users_ativos:
                user_desativar = st.selectbox("Desativar:", users_ativos, key="sel_desativar")
                if st.button("❌ Desativar Usuário", key="btn_desativar"):
                    try:
                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute("UPDATE dim_usuarios SET ativo = FALSE WHERE username = %s", (user_desativar,))
                        conn.commit()
                        cursor.close()
                        conn.close()
                        st.success(f"✅ '{user_desativar}' desativado!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")
            else:
                st.info("Nenhum usuário ativo para desativar (exceto admin).")

        with col2:
            users_inativos = df_users[df_users['ativo'] == False]['username'].tolist()
            if users_inativos:
                user_ativar = st.selectbox("Ativar:", users_inativos, key="sel_ativar")
                if st.button("✅ Ativar Usuário", key="btn_ativar"):
                    try:
                        conn = engine.raw_connection()
                        cursor = conn.cursor()
                        cursor.execute("UPDATE dim_usuarios SET ativo = TRUE WHERE username = %s", (user_ativar,))
                        conn.commit()
                        cursor.close()
                        conn.close()
                        st.success(f"✅ '{user_ativar}' ativado!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")
            else:
                st.info("Nenhum usuário inativo para ativar.")


if __name__ == "__main__":
    main()
