import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def main():
    st.header("⚙️ Configurações Nala")
    engine = get_engine()
    
    t_amz, t_ml, t_fisc = st.tabs(["📦 Amazon", "🤝 Mercado Livre", "💰 Impostos & Lojas"])

    # --- 1. AMAZON ---
    with t_amz:
        s1, s2, s3 = st.tabs(["📋 Lista de Anúncios", "➕ Vincular Manual", "📥 Importar"])
        cols_amz = ["asin", "sku", "logistica", "comissao_percentual", "taxa_fixa", "frete_estimado"]
        
        with s1:
            try:
                df_amz = pd.read_sql("SELECT id_plataforma as asin, sku, logistica, comissao_percentual, taxa_fixa, frete_estimado FROM dim_config_marketplace WHERE marketplace = 'AMAZON'", engine)
                st.dataframe(df_amz if not df_amz.empty else pd.DataFrame(columns=cols_amz), use_container_width=True, hide_index=True)
            except: 
                st.info("Nenhum anúncio Amazon no banco.")
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
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO dim_config_marketplace (id_plataforma, sku, marketplace, logistica, comissao_percentual, taxa_fixa, frete_estimado) 
                                VALUES (:id, :s, 'AMAZON', :l, :c, :t, :f) 
                                ON CONFLICT (id_plataforma) DO UPDATE SET sku=EXCLUDED.sku, comissao_percentual=EXCLUDED.comissao_percentual, taxa_fixa=EXCLUDED.taxa_fixa, frete_estimado=EXCLUDED.frete_estimado"""),
                                {"id":f_asin, "s":f_sku, "l":f_log, "c":float(f_com.replace(',','.')), "t":float(f_tax.replace(',','.')), "f":float(f_fre.replace(',','.'))})
                            conn.commit()
                        st.success("Vinculado com sucesso!"); st.rerun()
                    except: st.error("Erro ao salvar. Verifique se os campos numéricos estão corretos.")

        with s3:
            st.subheader("📥 Importação Massiva Amazon")
            tmpl_amz = pd.DataFrame(columns=cols_amz)
            buf_amz = io.BytesIO()
            with pd.ExcelWriter(buf_amz, engine='openpyxl') as wr: tmpl_amz.to_excel(wr, index=False)
            st.download_button("📄 Baixar Template Amazon", data=buf_amz.getvalue(), file_name="template_amazon.xlsx")
            st.file_uploader("Subir arquivo preenchido (.xlsx)", type=["xlsx"], key="up_amz_file")

    # --- 2. MERCADO LIVRE ---
    with t_ml:
        m1, m2, m3 = st.tabs(["📋 Lista de Anúncios", "➕ Vincular Manual", "📥 Importar"])
        cols_ml = ["mlb", "sku", "tipo_anuncio", "comissao_percentual", "valor_frete"]
        
        with m1:
            try:
                df_ml = pd.read_sql("SELECT id_plataforma as mlb, sku, tipo_anuncio, comissao_percentual, frete_estimado as valor_frete FROM dim_config_marketplace WHERE marketplace = 'MERCADO_LIVRE'", engine)
                st.dataframe(df_ml if not df_ml.empty else pd.DataFrame(columns=cols_ml), use_container_width=True, hide_index=True)
            except: 
                st.info("Nenhum anúncio ML no banco.")
                st.dataframe(pd.DataFrame(columns=cols_ml), use_container_width=True, hide_index=True)

        with m2:
            st.subheader("Vincular Manualmente (Mercado Livre)")
            with st.form("f_ml_manual", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                f_mlb = c1.text_input("MLB")
                f_sku_ml = c2.text_input("SKU Nala")
                f_tipo = c3.selectbox("Tipo", ["Clássico", "Premium"])
                
                c4, c5 = st.columns(2)
                f_com_ml = c4.text_input("Comissão %", value="0,00")
                f_fre_ml = c5.text_input("Custo Frete R$", value="0,00")
                
                if st.form_submit_button("💾 Salvar Anúncio ML"):
                    try:
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO dim_config_marketplace (id_plataforma, sku, marketplace, tipo_anuncio, comissao_percentual, frete_estimado) 
                                VALUES (:id, :s, 'MERCADO_LIVRE', :t, :c, :f) 
                                ON CONFLICT (id_plataforma) DO UPDATE SET sku=EXCLUDED.sku, comissao_percentual=EXCLUDED.comissao_percentual, frete_estimado=EXCLUDED.frete_estimado"""),
                                {"id":f_mlb, "s":f_sku_ml, "t":f_tipo, "c":float(f_com_ml.replace(',','.')), "f":float(f_fre_ml.replace(',','.'))})
                            conn.commit()
                        st.success("Vinculado com sucesso!"); st.rerun()
                    except: st.error("Erro ao salvar.")

        with m3:
            st.subheader("📥 Importação Massiva ML")
            tmpl_ml = pd.DataFrame(columns=cols_ml)
            buf_ml = io.BytesIO()
            with pd.ExcelWriter(buf_ml, engine='openpyxl') as wr: tmpl_ml.to_excel(wr, index=False)
            st.download_button("📄 Baixar Template ML", data=buf_ml.getvalue(), file_name="template_ml.xlsx")
            st.file_uploader("Subir arquivo preenchido (.xlsx)", type=["xlsx"], key="up_ml_file")

    # --- 3. IMPOSTOS & LOJAS ---
    with t_fisc:
        st.subheader("Gerenciamento das 14 Lojas")
        try:
            df_lojas = pd.read_sql("SELECT marketplace, loja, imposto FROM dim_lojas ORDER BY marketplace ASC", engine)
        except:
            df_lojas = pd.DataFrame()

        if df_lojas.empty:
            # Lista definitiva corrigida do Grupo Nala
            data_nala = [
                ["MERCADO LIVRE", "ML-Nala", 10.00], ["MERCADO LIVRE", "ML-LPT", 10.00],
                ["MERCADO LIVRE", "ML-YanniRJ", 10.00], ["MERCADO LIVRE", "ML-YanniSP", 10.00],
                ["AMAZON", "AMZ-Innovare(CPF)", 0.00], ["AMAZON", "AMZ-Nala", 10.00],
                ["AMAZON", "AMZ-LPT", 10.00], ["AMAZON", "AMZ-Yanni", 10.00],
                ["SHOPEE", "Shopee Lithouse(Nala)", 10.00], ["SHOPEE", "Shopee Litstore(Yanni)", 10.00],
                ["SHOPEE", "Shopee-LPT", 10.00], ["SHEIN", "Shein Yanni", 10.00],
                ["SHEIN", "Shein LPT", 10.00], ["MAGALU", "Magalu-Nala", 10.00]
            ]
            df_lojas = pd.DataFrame(data_nala, columns=["marketplace", "loja", "imposto"])

        st.info("Ajuste as alíquotas e clique em salvar para registrar no banco Neon.")
        df_editado = st.data_editor(df_lojas, use_container_width=True, num_rows="dynamic", hide_index=True)

        if st.button("💾 Salvar Estrutura Fiscal no Banco"):
            try:
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE dim_lojas"))
                    for _, row in df_editado.iterrows():
                        val_i = float(str(row['imposto']).replace(',', '.')) if row['imposto'] else 0.0
                        conn.execute(text("INSERT INTO dim_lojas (marketplace, loja, imposto) VALUES (:m, :l, :i)"),
                                     {"m": row['marketplace'], "l": row['loja'], "i": val_i})
                    conn.commit()
                st.success("✅ As 14 lojas do Grupo Nala foram salvas!"); st.rerun()
            except Exception as e: st.error(f"Erro: {e}")

if __name__ == "__main__":
    main()