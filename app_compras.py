import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

# CONEXÃO
DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
engine = create_engine(DB_URL)

def fmt_moeda(v):
    """R$ 15,50 (sempre 2 casas)"""
    return f"R$ {float(v):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def fmt_data(d):
    """dd/mm/aaaa"""
    if isinstance(d, str):
        return pd.to_datetime(d).strftime('%d/%m/%Y')
    return d.strftime('%d/%m/%Y')

def main():
    st.markdown("<h4 style='margin:0;padding:5px 0;'>🛒 Registro de Compras</h4>", unsafe_allow_html=True)
    
    # CSS ULTRA COMPACTO
    st.markdown("""
        <style>
        /* Remover espaços */
        .main { padding-top: 0.3rem !important; }
        .block-container { padding-top: 0.5rem !important; padding-bottom: 0.3rem !important; }
        h1, h2, h3, h4 { margin: 0 !important; padding: 0.2rem 0 !important; font-size: 0.95rem !important; }
        
        /* Cards compactos */
        div[data-testid="stMetric"] { 
            background-color: #f8f9fa; 
            border: 1px solid #dee2e6; 
            padding: 5px 8px !important; 
            border-radius: 4px; 
            height: 55px !important;
            display: flex;
            flex-direction: column;
            justify-content: center;
            margin: 1px 0 !important;
        }
        div[data-testid="stMetric"] label { font-size: 0.68rem !important; margin: 0 !important; }
        div[data-testid="stMetric"] div { font-size: 0.9rem !important; font-weight: 600; margin: 0 !important; }
        
        /* Form compacto com altura fixa */
        .stForm { 
            border: 1px solid #dee2e6; 
            padding: 8px !important; 
            border-radius: 4px; 
            background-color: #fff;
            min-height: 440px !important;
        }
        
        /* Container de cards alinhado */
        div[data-testid="column"]:has(div[data-testid="stMetric"]) {
            min-height: 440px !important;
        }
        
        /* Labels menores */
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stDateInput"] label,
        div[data-testid="stNumberInput"] label { 
            font-size: 0.72rem !important; 
            font-weight: 500;
            margin-bottom: 1px !important;
        }
        
        /* Inputs menores */
        input, select { 
            font-size: 0.82rem !important; 
            padding: 3px 6px !important;
            min-height: 30px !important;
        }
        
        /* Tabela */
        .stDataFrame { font-size: 0.78rem !important; }
        
        /* Botões */
        .stButton button { 
            padding: 5px 10px !important; 
            font-size: 0.82rem !important;
        }
        
        /* Espaçamento entre elementos */
        div[data-testid="column"] > div { margin-bottom: 0 !important; }
        hr { margin: 6px 0 !important; }
        
        /* Mensagens de sucesso/erro */
        .stSuccess, .stError, .stWarning { 
            padding: 6px 10px !important; 
            font-size: 0.82rem !important;
            margin: 4px 0 !important;
        }
        
        /* Caption (nome do produto) */
        .stCaption { 
            font-size: 0.7rem !important;
            color: #666 !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # DADOS
    try:
        df_p = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
        df_p['display'] = df_p['sku'] + " - " + df_p['nome']
        df_f = pd.read_sql("SELECT DISTINCT fornecedor FROM fato_compras ORDER BY fornecedor", engine)
        lista_forn = df_f['fornecedor'].tolist()
    except Exception as e:
        st.error(f"❌ Erro: {e}")
        return

    escolha = st.selectbox("🔍 Produto", [""] + df_p['display'].tolist(), label_visibility="collapsed")
    sku_sel = escolha.split(" - ")[0] if escolha else ""

    if not sku_sel:
        st.info("Selecione um produto.")
        return
    
    # Buscar nome do produto UMA VEZ
    nome_produto = df_p[df_p['sku'] == sku_sel]['nome'].iloc[0] if not df_p[df_p['sku'] == sku_sel].empty else ""

    st.markdown("---")

    # LAYOUT
    col_form, col_ind = st.columns([1.6, 1.4])

    # FORMULÁRIO
    with col_form:
        st.markdown(f"**📝 Nova Compra - {sku_sel}** - {nome_produto[:50]}")
        
        # ===== FORNECEDOR (FORA DO FORM) =====
        st.markdown("**Fornecedor**")
        
        col_sel, col_novo = st.columns([1.2, 1])
        
        # Selectbox: fornecedores existentes
        forn_existente = col_sel.selectbox(
            "Selecionar existente",
            [""] + lista_forn,
            key="sel_forn_existente"
        )
        
        # Text input: cadastrar novo
        forn_novo = col_novo.text_input(
            "Ou cadastrar novo",
            placeholder="Digite novo",
            key="txt_forn_novo"
        )
        
        # Determinar qual usar (novo sobrepõe existente)
        if forn_novo.strip():
            forn_final = forn_novo.strip()
        elif forn_existente:
            forn_final = forn_existente
        else:
            forn_final = ""
        
        # ===== FIM FORNECEDOR =====
        
        with st.form("frm", clear_on_submit=True):
            c1, c2 = st.columns(2)
            dt = c1.date_input("Data", date.today(), format="DD/MM/YYYY")
            nf = c2.text_input("NF nº", placeholder="Opcional")
            
            c3, c4 = st.columns(2)
            qtd = c3.number_input("Qtd", 1, step=1)
            pnf = c4.text_input("Preço NF", placeholder="Ex: 15,50")
            
            custo = st.text_input("Custo Considerado", placeholder="Opcional - se vazio = Preço NF")
            
            if st.form_submit_button("💾 Gravar", use_container_width=True, type="primary"):
                # VALIDAÇÕES
                if not forn_final:
                    st.error("⚠️ Selecione ou cadastre um fornecedor")
                elif not pnf or not pnf.strip():
                    st.error("⚠️ Preencha o Preço NF")
                else:
                    try:
                        v_nf = float(pnf.replace(',', '.').strip())
                        v_custo = float(custo.replace(',', '.').strip()) if custo.strip() else v_nf
                        
                        if v_nf <= 0 or v_custo <= 0:
                            st.error("❌ Valores devem ser positivos")
                        else:
                            with engine.begin() as conn:  # Auto-commit
                                # Gravar histórico
                                conn.execute(text("""
                                    INSERT INTO fato_compras 
                                    (data_compra, sku, fornecedor, quantidade, preco_unitario, custo_considerado, valor_total, numero_nf)
                                    VALUES (:d, :s, :f, :q, :p, :c, :t, :nf)
                                """), {
                                    "d": dt, "s": sku_sel, "f": forn_final, "q": qtd, 
                                    "p": v_nf, "c": v_custo, "t": qtd * v_nf, "nf": nf.strip() or None
                                })
                            
                            st.success(f"✅ Gravado! Preço NF: {fmt_moeda(v_nf)} | Custo Considerado: {fmt_moeda(v_custo)}")
                            st.rerun()
                            
                    except ValueError:
                        st.error("❌ Valores inválidos. Use: 15,50")
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")

    # INDICADORES
    with col_ind:
        st.markdown(f"**📊 {sku_sel}** - {nome_produto[:50]}")
        
        query = text("""
            WITH ult AS (
                SELECT preco_unitario, custo_considerado
                FROM fato_compras
                WHERE sku = :s
                ORDER BY id DESC
                LIMIT 1
            )
            SELECT 
                COALESCE(u.preco_unitario, 0) as nf,
                COALESCE(c.embalagem, 0) as emb,
                COALESCE(c.mdo, 0) as mdo,
                COALESCE(c.custo_ads, 0) as ads,
                COALESCE(u.custo_considerado, 0) as custo_cons
            FROM dim_produtos_custos c
            LEFT JOIN ult u ON true
            WHERE c.sku = :s
        """)
        
        try:
            with engine.connect() as conn:
                r = conn.execute(query, {"s": sku_sel}).fetchone()
            
            if r:
                nf_val, emb, mdo, ads, custo_cons = [float(x) for x in r]
                
                # CUSTO TOTAL = Preço NF + Custos Fixos
                custo_total = nf_val + emb + mdo + ads
                
                c1, c2 = st.columns(2)
                c1.metric("NF Atual", fmt_moeda(nf_val))
                c2.metric("Embalagem", fmt_moeda(emb))
                
                c3, c4 = st.columns(2)
                c3.metric("MDO", fmt_moeda(mdo))
                c4.metric("ADS", fmt_moeda(ads))
                
                st.metric("📦 CUSTO TOTAL", fmt_moeda(custo_total))
                st.metric("💎 Custo Considerado", fmt_moeda(custo_cons))
            else:
                st.warning("Sem dados de custo")
        except Exception as e:
            st.error(f"❌ {e}")

    # HISTÓRICO
    st.markdown("---")
    st.markdown(f"**🕒 Histórico - {sku_sel} - {nome_produto}**")
    
    try:
        # Buscar custos fixos do produto
        with engine.connect() as conn:
            custos_fixos = conn.execute(text("""
                SELECT COALESCE(embalagem, 0), COALESCE(mdo, 0), COALESCE(custo_ads, 0)
                FROM dim_produtos_custos
                WHERE sku = :s
            """), {"s": sku_sel}).fetchone()
        
        if custos_fixos:
            emb_fix, mdo_fix, ads_fix = float(custos_fixos[0]), float(custos_fixos[1]), float(custos_fixos[2])
        else:
            emb_fix, mdo_fix, ads_fix = 0.0, 0.0, 0.0
        
        df_h = pd.read_sql(text("""
            SELECT id, data_compra, fornecedor, numero_nf, quantidade, preco_unitario, custo_considerado
            FROM fato_compras 
            WHERE sku = :s 
            ORDER BY id DESC
        """), engine, params={"s": sku_sel})
        
        if not df_h.empty:
            df_show = df_h.copy()
            df_show['Data'] = df_show['data_compra'].apply(fmt_data)
            df_show['NF nº'] = df_show['numero_nf'].fillna('-')
            df_show['Preço NF'] = df_show['preco_unitario'].apply(lambda x: fmt_moeda(float(x)))
            df_show['Custo Considerado'] = df_show['custo_considerado'].apply(lambda x: fmt_moeda(float(x)))
            # CUSTO TOTAL = Preço NF + Custos Fixos (converter para float)
            df_show['Custo Total'] = df_show['preco_unitario'].apply(lambda x: fmt_moeda(float(x) + emb_fix + mdo_fix + ads_fix))
            
            st.dataframe(
                df_show[['id', 'Data', 'fornecedor', 'NF nº', 'quantidade', 'Preço NF', 'Custo Considerado', 'Custo Total']],
                use_container_width=True,
                hide_index=True
            )
            
            # EXCLUSÃO
            with st.expander("🗑️ Excluir Registro"):
                st.warning("⚠️ Esta ação não pode ser desfeita.")
                
                cx, cy = st.columns([3, 1])
                opts = [(r['id'], f"ID {r['id']} - {fmt_data(r['data_compra'])} - {r['fornecedor']} - {fmt_moeda(r['custo_considerado'])}") 
                        for _, r in df_h.iterrows()]
                
                sel = cx.selectbox("Registro", [o[0] for o in opts], format_func=lambda x: [o[1] for o in opts if o[0]==x][0])
                
                if cy.button("🗑️ Excluir"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("DELETE FROM fato_compras WHERE id = :id"), {"id": sel})
                        
                        st.success("✅ Registro excluído!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")
        else:
            st.info("Sem compras registradas.")
    except Exception as e:
        st.error(f"❌ {e}")

if __name__ == "__main__":
    main()
