"""
CONFIGURAÇÕES - Sistema Nala
Versão: 2.1 (11/03/2026)

CHANGELOG v2.1:
  - FIX: Todas queries usam raw_connection via _query_to_df (compatível SQLAlchemy 2.x)
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
    """
    Executa query e retorna DataFrame.
    Usa raw_connection para compatibilidade com SQLAlchemy 2.x.
    Evita erro 'immutabledict is not a sequence'.
    """
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
# TAB 1: AMAZON
# ============================================================

def _tab_amazon(engine):
    """Configuração de anúncios Amazon (ASINs, taxas, De-Para)"""

    s1, s2, s3 = st.tabs(["📋 Lista de Anúncios", "➕ Vincular Manual", "📥 Importar"])
    cols_amz = ["asin", "sku", "logistica", "comissao_percentual", "taxa_fixa", "frete_estimado"]

    with s1:
        df_amz = _query_to_df(engine,
            """SELECT id_plataforma as asin, sku, logistica, 
                      comissao_percentual, taxa_fixa, frete_estimado 
               FROM dim_config_marketplace 
               WHERE marketplace = 'AMAZON'"""
        )
        if not df_amz.empty:
            st.dataframe(df_amz, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum anúncio Amazon cadastrado.")
            st.dataframe(pd.DataFrame(columns=cols_amz), use_container_width=True, hide_index=True)

    with s2:
        st.subheader("Vincular Manualmente (Amazon)")
        with st.form("f_amz_manual", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            f_asin = c1.text_input("ASIN")
            f_sku = c2.text_input("SKU Nala")
            f_log = c3.selectbox("Logística", ["FBA", "DBA", "Crossdocking"])

            c4, c5, c6 = st.columns(3)
            f_com = c4.text_input("Comissão %", value="0,00")
            f_tax = c5.text_input("Taxa Fixa R$", value="0,00")
            f_fre = c6.text_input("Frete Est. R$", value="0,00")

            if st.form_submit_button("💾 Salvar Anúncio Amazon"):
                try:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO dim_config_marketplace 
                            (id_plataforma, sku, marketplace, logistica, 
                             comissao_percentual, taxa_fixa, frete_estimado)
                        VALUES (%s, %s, 'AMAZON', %s, %s, %s, %s)
                        ON CONFLICT (id_plataforma) DO UPDATE SET 
                            sku=EXCLUDED.sku, 
                            logistica=EXCLUDED.logistica,
                            comissao_percentual=EXCLUDED.comissao_percentual, 
                            taxa_fixa=EXCLUDED.taxa_fixa, 
                            frete_estimado=EXCLUDED.frete_estimado
                    """, (
                        f_asin, f_sku, f_log,
                        float(f_com.replace(',', '.')),
                        float(f_tax.replace(',', '.')),
                        float(f_fre.replace(',', '.'))
                    ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    st.success("Vinculado com sucesso!")
                    st.rerun()
                except Exception:
                    st.error("Erro ao salvar. Verifique se os campos numéricos estão corretos.")

    with s3:
        st.subheader("📥 Importação Massiva Amazon")

        # Template para download
        tmpl_amz = pd.DataFrame(columns=cols_amz)
        buf_amz = io.BytesIO()
        with pd.ExcelWriter(buf_amz, engine='openpyxl') as wr:
            tmpl_amz.to_excel(wr, index=False)
        st.download_button("📄 Baixar Template Amazon", data=buf_amz.getvalue(),
                           file_name="template_amazon.xlsx")

        # Upload
        arquivo_amz = st.file_uploader("Subir arquivo preenchido (.xlsx)",
                                       type=["xlsx"], key="up_amz_file")

        # Botão processar (NOVO v2.1)
        if arquivo_amz and st.button("📥 Processar Importação Amazon", type="primary"):
            try:
                df_import = pd.read_excel(arquivo_amz)

                # Validar colunas mínimas
                colunas_esperadas = {'asin', 'sku'}
                if not colunas_esperadas.issubset(set(df_import.columns)):
                    st.error(f"❌ Colunas obrigatórias não encontradas: {colunas_esperadas}")
                else:
                    conn = engine.raw_connection()
                    cursor = conn.cursor()
                    importados = 0
                    erros_imp = 0

                    for _, row in df_import.iterrows():
                        try:
                            asin = str(row.get('asin', '')).strip()
                            sku = str(row.get('sku', '')).strip()

                            if not asin or not sku:
                                erros_imp += 1
                                continue

                            logistica = str(row.get('logistica', 'FBA')).strip()
                            comissao = float(str(row.get('comissao_percentual', 0)).replace(',', '.'))
                            taxa = float(str(row.get('taxa_fixa', 0)).replace(',', '.'))
                            frete = float(str(row.get('frete_estimado', 0)).replace(',', '.'))

                            cursor.execute("""
                                INSERT INTO dim_config_marketplace 
                                    (id_plataforma, sku, marketplace, logistica, 
                                     comissao_percentual, taxa_fixa, frete_estimado)
                                VALUES (%s, %s, 'AMAZON', %s, %s, %s, %s)
                                ON CONFLICT (id_plataforma) DO UPDATE SET 
                                    sku=EXCLUDED.sku,
                                    logistica=EXCLUDED.logistica,
                                    comissao_percentual=EXCLUDED.comissao_percentual, 
                                    taxa_fixa=EXCLUDED.taxa_fixa, 
                                    frete_estimado=EXCLUDED.frete_estimado
                            """, (asin, sku, logistica, comissao, taxa, frete))
                            importados += 1
                        except Exception:
                            erros_imp += 1

                    conn.commit()
                    cursor.close()
                    conn.close()

                    if importados > 0:
                        st.success(f"✅ {importados} anúncio(s) importado(s) com sucesso!")
                    if erros_imp > 0:
                        st.warning(f"⚠️ {erros_imp} linha(s) com erro (ASIN ou SKU vazio)")
                    st.rerun()

            except Exception as e:
                st.error(f"❌ Erro ao processar arquivo: {e}")


# ============================================================
# TAB 2: FRETE ML (FLEX)
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
# TAB 3: IMPOSTOS & LOJAS
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
# TAB 4: GESTÃO DE USUÁRIOS
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
