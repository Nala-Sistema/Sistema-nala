import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

# 1. CONEXÃO E CONFIGURAÇÃO
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
engine = create_engine(DB_URL)

def formatar_moeda(valor):
    """Formata valor para moeda brasileira (sempre 2 casas decimais com vírgula)"""
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def formatar_data(data):
    """Formata data para padrão brasileiro dd/mm/aaaa"""
    if isinstance(data, str):
        return pd.to_datetime(data).strftime('%d/%m/%Y')
    return data.strftime('%d/%m/%Y')

def main():
    st.header("🛒 Registro de Compras & Gestão de Custos")
    
    # ========== CSS MELHORADO - ALINHAMENTO PERFEITO ==========
    st.markdown("""
        <style>
        /* Cards de métricas com altura fixa */
        div[data-testid="stMetric"] { 
            background-color: #f8f9fa; 
            border: 1px solid #e0e0e0; 
            padding: 12px; 
            border-radius: 8px; 
            height: 85px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        
        /* Formulário com altura mínima para alinhar com cards */
        .stForm { 
            border: 1px solid #e0e0e0; 
            padding: 20px; 
            border-radius: 8px; 
            background-color: #ffffff;
            min-height: 580px;
            display: flex;
            flex-direction: column;
        }
        
        /* Container de métricas com altura fixa */
        div[data-testid="column"]:has(div[data-testid="stMetric"]) {
            min-height: 580px;
        }
        
        /* Botão de submit sempre no final */
        .stForm > div:last-child {
            margin-top: auto;
        }
        
        /* Botões de ação */
        .stButton > button {
            font-weight: 600;
        }
        </style>
    """, unsafe_allow_html=True)

    # ========== BLOCO 1: BUSCA E DADOS ==========
    try:
        df_p = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
        df_p['display'] = df_p['sku'] + " - " + df_p['nome']
        df_f = pd.read_sql("SELECT DISTINCT fornecedor FROM fato_compras ORDER BY fornecedor", engine)
        lista_forn = df_f['fornecedor'].tolist()
    except Exception as e:
        st.error(f"❌ Erro de conexão: {e}")
        return

    escolha = st.selectbox("🔍 Selecione o Produto (SKU ou Nome)", options=[""] + df_p['display'].tolist())
    sku_sel = escolha.split(" - ")[0] if escolha != "" else ""

    if not sku_sel:
        st.info("👆 Selecione um produto para começar o registro de compra.")
        return

    st.divider()

    # ========== BLOCO 2: LAYOUT PRINCIPAL ==========
    col_form, col_metrics = st.columns([1.8, 1.2])

    # ========== COLUNA ESQUERDA: FORMULÁRIO ==========
    with col_form:
        st.markdown("### 📝 Detalhes da Compra")
        
        with st.form("form_compras", clear_on_submit=True):
            # --- DATA E NF ---
            col_dt, col_nf = st.columns(2)
            data_compra = col_dt.date_input("Data", value=date.today(), format="DD/MM/YYYY")
            num_nf = col_nf.text_input("NF nº", placeholder="Opcional")
            
            # --- FORNECEDOR ---
            st.markdown("**Fornecedor**")
            
            # Usar session_state para manter a escolha
            if 'tipo_fornecedor' not in st.session_state:
                st.session_state.tipo_fornecedor = "Selecionar Existente"
            
            tipo_forn = st.radio(
                "Escolha o tipo",
                ["Selecionar Existente", "Cadastrar Novo"],
                horizontal=True,
                label_visibility="collapsed",
                key="radio_fornecedor"
            )
            
            forn_final = ""
            if tipo_forn == "Selecionar Existente":
                if lista_forn:
                    forn_final = st.selectbox(
                        "Fornecedor (Histórico)", 
                        options=[""] + lista_forn, 
                        key="select_forn_existente"
                    )
                else:
                    st.info("Nenhum fornecedor cadastrado ainda. Use 'Cadastrar Novo'.")
            else:
                forn_final = st.text_input(
                    "Nome do Novo Fornecedor", 
                    placeholder="Digite o nome completo",
                    key="input_forn_novo"
                )
            
            # --- QUANTIDADE E VALORES ---
            st.markdown("**Valores**")
            col_q, col_pnf = st.columns(2)
            qtd = col_q.number_input("Qtd", min_value=1, value=1, step=1)
            preco_nf_txt = col_pnf.text_input("Preço Unit. NF (R$)", placeholder="Ex: 15,50")
            
            custo_cons_txt = st.text_input(
                "Custo Considerado (R$)", 
                placeholder="Opcional - Se vazio, usará o Preço da NF",
                help="💡 Se deixar vazio, o sistema usará o mesmo valor da NF como custo."
            )
            
            # --- BOTÃO DE SUBMISSÃO ---
            submitted = st.form_submit_button("🚀 Gravar Compra", use_container_width=True, type="primary")
            
            if submitted:
                # ========== VALIDAÇÕES ==========
                erros = []
                
                # Fornecedor obrigatório
                if not forn_final or forn_final.strip() == "":
                    erros.append("⚠️ Fornecedor não pode estar vazio")
                
                # Preço NF obrigatório
                if not preco_nf_txt or preco_nf_txt.strip() == "":
                    erros.append("⚠️ Preço da NF é obrigatório")
                
                # NF não é obrigatória (removido validação)
                
                if erros:
                    for erro in erros:
                        st.error(erro)
                else:
                    try:
                        # Conversão de valores
                        v_nf = float(preco_nf_txt.replace(',', '.').strip())
                        v_cons = float(custo_cons_txt.replace(',', '.').strip()) if custo_cons_txt.strip() else v_nf
                        
                        # Validação de valores positivos
                        if v_nf <= 0:
                            st.error("❌ Preço da NF deve ser maior que zero")
                        elif v_cons <= 0:
                            st.error("❌ Custo Considerado deve ser maior que zero")
                        else:
                            # ========== GRAVAÇÃO NO BANCO ==========
                            with engine.connect() as conn:
                                # 1. Insere histórico na fato_compras
                                conn.execute(text("""
                                    INSERT INTO fato_compras 
                                    (data_compra, sku, fornecedor, quantidade, preco_unitario, custo_considerado, valor_total, numero_nf)
                                    VALUES (:d, :s, :f, :q, :p, :cc, :t, :nf)
                                """), {
                                    "d": data_compra, 
                                    "s": sku_sel, 
                                    "f": forn_final.strip(), 
                                    "q": qtd, 
                                    "p": v_nf, 
                                    "cc": v_cons, 
                                    "t": qtd * v_nf,
                                    "nf": num_nf.strip() if num_nf else None
                                })
                                
                                # 2. Atualiza CUSTO OPERACIONAL (dim_produtos_custos)
                                # preco_compra = custo_considerado (para cálculo operacional)
                                conn.execute(text("""
                                    UPDATE dim_produtos_custos 
                                    SET preco_compra = :cc,
                                        custo_final = :cc + COALESCE(embalagem, 0) + COALESCE(mdo, 0) + COALESCE(custo_ads, 0)
                                    WHERE sku = :s
                                """), {"cc": v_cons, "s": sku_sel})
                                
                                # 3. Atualiza PREÇO DE VENDA (dim_produtos) se necessário
                                # Aqui você pode adicionar lógica de atualização de preço de venda se quiser
                                
                                conn.commit()
                            
                            st.success(f"✅ Compra gravada! Custo operacional atualizado: {formatar_moeda(v_cons)}")
                            st.rerun()
                            
                    except ValueError:
                        st.error("❌ Erro ao converter valores. Use apenas números (ex: 15,50 ou 15.50)")
                    except Exception as e:
                        st.error(f"❌ Erro ao gravar: {e}")

    # ========== COLUNA DIREITA: INDICADORES ==========
    with col_metrics:
        st.markdown(f"### 📊 Indicadores Atuais: **{sku_sel}**")
        
        # Query para buscar ÚLTIMA COMPRA (preco_unitario da NF) + CUSTOS OPERACIONAIS
        query_indicadores = text("""
            WITH ultima_compra AS (
                SELECT preco_unitario, custo_considerado
                FROM fato_compras
                WHERE sku = :s
                ORDER BY id DESC
                LIMIT 1
            )
            SELECT 
                COALESCE(uc.preco_unitario, 0) as ultimo_preco_nf,
                COALESCE(c.embalagem, 0) as embalagem,
                COALESCE(c.mdo, 0) as mdo,
                COALESCE(c.custo_ads, 0) as custo_ads,
                COALESCE(c.preco_compra, 0) as custo_considerado_atual,
                COALESCE(c.custo_final, 0) as custo_total_operacional,
                COALESCE(p.preco_a_ser_considerado, 0) as preco_venda
            FROM dim_produtos_custos c
            LEFT JOIN dim_produtos p ON c.sku = p.sku
            LEFT JOIN ultima_compra uc ON true
            WHERE c.sku = :s
        """)
        
        try:
            with engine.connect() as conn:
                resultado = conn.execute(query_indicadores, {"s": sku_sel}).fetchone()
            
            if resultado:
                ultimo_preco_nf, emb, mdo, ads, custo_cons_atual, total_op, p_venda = resultado
                
                # Cards de métricas (SEMPRE 2 CASAS DECIMAIS COM VÍRGULA)
                col_m1, col_m2 = st.columns(2)
                col_m1.metric("Compra NF Atual", formatar_moeda(ultimo_preco_nf))
                col_m2.metric("Embalagem", formatar_moeda(emb))
                
                col_m3, col_m4 = st.columns(2)
                col_m3.metric("MDO", formatar_moeda(mdo))
                col_m4.metric("ADS", formatar_moeda(ads))
                
                st.markdown("---")
                st.metric("📦 CUSTO OPERACIONAL TOTAL", formatar_moeda(total_op))
                st.metric("🏷️ PREÇO VENDA ATUAL", formatar_moeda(p_venda))
            else:
                st.warning("⚠️ Produto sem dados de custo cadastrados")
                
        except Exception as e:
            st.error(f"❌ Erro ao buscar indicadores: {e}")

    # ========== BLOCO 3: HISTÓRICO ==========
    st.markdown("---")
    st.subheader(f"🕒 Histórico de Compras - {sku_sel}")
    
    try:
        df_historico = pd.read_sql(
            text("""
                SELECT id, data_compra, fornecedor, numero_nf, quantidade, preco_unitario, custo_considerado, valor_total
                FROM fato_compras 
                WHERE sku = :s 
                ORDER BY id DESC
            """), 
            engine, 
            params={"s": sku_sel}
        )
        
        if not df_historico.empty:
            # Formatação da tabela (SEMPRE dd/mm/aaaa e vírgula nos valores)
            df_display = df_historico.copy()
            df_display['data_compra'] = df_display['data_compra'].apply(formatar_data)
            df_display['numero_nf'] = df_display['numero_nf'].fillna('-')
            
            # Renomear colunas para exibição
            df_display = df_display.rename(columns={
                'id': 'ID',
                'data_compra': 'Data',
                'fornecedor': 'Fornecedor',
                'numero_nf': 'NF nº',
                'quantidade': 'Qtd',
                'preco_unitario': 'Preço NF',
                'custo_considerado': 'Custo Considerado',
                'valor_total': 'Valor Total'
            })
            
            st.dataframe(
                df_display.style.format({
                    "Preço NF": lambda x: formatar_moeda(x),
                    "Custo Considerado": lambda x: formatar_moeda(x),
                    "Valor Total": lambda x: formatar_moeda(x)
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # ========== EXCLUSÃO COM EXPANDER ==========
            with st.expander("🗑️ Excluir Registro de Compra", expanded=False):
                st.warning("⚠️ **ATENÇÃO:** Ao excluir, o custo será restaurado para a compra anterior.")
                
                col_select, col_btn = st.columns([2, 1])
                
                # Criar labels descritivos
                opcoes_exclusao = []
                for _, row in df_historico.iterrows():
                    data_fmt = formatar_data(row['data_compra'])
                    nf = row['numero_nf'] if pd.notna(row['numero_nf']) else 'S/NF'
                    label = f"ID {row['id']} - {data_fmt} - {row['fornecedor']} - {nf} - {formatar_moeda(row['custo_considerado'])}"
                    opcoes_exclusao.append((row['id'], label))
                
                id_selecionado = col_select.selectbox(
                    "Selecione o registro para excluir",
                    options=[opt[0] for opt in opcoes_exclusao],
                    format_func=lambda x: [opt[1] for opt in opcoes_exclusao if opt[0] == x][0]
                )
                
                if col_btn.button("🗑️ Confirmar Exclusão", type="secondary"):
                    try:
                        with engine.connect() as conn:
                            # 1. Exclui o registro
                            conn.execute(text("DELETE FROM fato_compras WHERE id = :id"), {"id": id_selecionado})
                            
                            # 2. RESTAURAÇÃO: Busca o último registro restante
                            ultimo_registro = conn.execute(
                                text("""
                                    SELECT custo_considerado 
                                    FROM fato_compras 
                                    WHERE sku = :s 
                                    ORDER BY id DESC 
                                    LIMIT 1
                                """), 
                                {"s": sku_sel}
                            ).fetchone()
                            
                            # 3. Atualiza o custo (ou zera se não houver mais registros)
                            novo_custo = float(ultimo_registro[0]) if ultimo_registro else 0.0
                            
                            conn.execute(text("""
                                UPDATE dim_produtos_custos 
                                SET preco_compra = :c, 
                                    custo_final = :c + COALESCE(embalagem, 0) + COALESCE(mdo, 0) + COALESCE(custo_ads, 0)
                                WHERE sku = :s
                            """), {"c": novo_custo, "s": sku_sel})
                            
                            conn.commit()
                        
                        st.success(f"✅ Registro excluído! Custo restaurado: {formatar_moeda(novo_custo)}")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Erro ao excluir: {e}")
        else:
            st.info("📭 Nenhuma compra registrada para este produto ainda.")
            
    except Exception as e:
        st.error(f"❌ Erro ao carregar histórico: {e}")

if __name__ == "__main__":
    main()