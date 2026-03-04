import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

DB_URL = "postgresql://neondb_owner:npg_fplFq8iAR4Ur@ep-long-unit-acfema6a-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

def get_engine():
    return create_engine(DB_URL)

def formatar_valor_br(valor):
    """Converte float para string no formato brasileiro (ex: 1.234,56)"""
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

def converter_valor_para_float(valor_str):
    """Converte string brasileira para float (ex: '1.234,56' -> 1234.56)"""
    try:
        if isinstance(valor_str, (int, float)):
            return float(valor_str)
        # Remove pontos de milhar e substitui vírgula por ponto
        valor_limpo = str(valor_str).replace(".", "").replace(",", ".")
        return float(valor_limpo)
    except:
        return 0.0

def main():
    st.header("📦 Gestão de SKUs Nala")
    engine = get_engine()
    
    user_role = st.session_state.get('perfil', 'Admin') 
    is_admin = user_role in ['Admin', 'Controladoria']

    t1, t2, t3 = st.tabs(["📋 Lista e Busca", "⚙️ Gerenciar SKU", "📥 Importação"])
    
    # ============================================================
    # TAB 1: LISTA E BUSCA + DOWNLOAD
    # ============================================================
    with t1:
        col1, col2, col3 = st.columns([2, 1, 1])
        
        # Busca inteligente
        busca = col1.text_input("🔍 Buscar por SKU ou Nome do Produto", 
                                placeholder="Digite parte do nome ou código...")
        
        if col2.button("🔄 Atualizar Base"):
            st.rerun()

        try:
            # Query completa com JOIN
            query = """
                SELECT p.sku, p.nome, p.categoria, p.status, 
                       c.cod_fornecedor, c.preco_compra, c.embalagem, c.mdo, c.custo_ads, 
                       (c.preco_compra + c.embalagem + c.mdo + c.custo_ads) as custo_final,
                       p.preco_a_ser_considerado 
                FROM dim_produtos p
                LEFT JOIN dim_produtos_custos c ON p.sku = c.sku
                ORDER BY p.sku ASC
            """ if is_admin else """
                SELECT sku, nome, categoria, status, preco_a_ser_considerado 
                FROM dim_produtos 
                ORDER BY sku ASC
            """
            
            df = pd.read_sql(query, engine)
            
            if not df.empty:
                # Filtro inteligente
                if busca:
                    mask = df['sku'].str.contains(busca, case=False, na=False) | \
                           df['nome'].str.contains(busca, case=False, na=False)
                    df_filtrado = df[mask]
                else:
                    df_filtrado = df

                st.write(f"Mostrando {len(df_filtrado)} de {len(df)} itens.")
                
                # Exibir tabela com formatação BR
                colunas_moeda = ['preco_compra', 'embalagem', 'mdo', 'custo_ads', 'custo_final', 'preco_a_ser_considerado']
                df_display = df_filtrado.copy()
                
                st.dataframe(
                    df_display.style.format({
                        col: lambda x: formatar_valor_br(x) 
                        for col in colunas_moeda if col in df_display.columns
                    }),
                    use_container_width=True, 
                    hide_index=True
                )
                
                # BOTÃO DOWNLOAD EXCEL
                if col3.button("📥 Download Excel"):
                    # Preparar dados para Excel com formatação BR
                    df_excel = df_filtrado.copy()
                    
                    # Formatar colunas numéricas para Excel
                    for col in colunas_moeda:
                        if col in df_excel.columns:
                            df_excel[col] = df_excel[col].apply(formatar_valor_br)
                    
                    # Gerar arquivo Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_excel.to_excel(writer, index=False, sheet_name='SKUs')
                    
                    output.seek(0)
                    
                    st.download_button(
                        label="📥 Baixar Arquivo",
                        data=output,
                        file_name=f"skus_nala_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.info("Base de dados vazia.")
                
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")

    # ============================================================
    # TAB 2: GERENCIAR SKU (CADASTRO/EDIÇÃO COMPLETO)
    # ============================================================
    with t2:
        if not is_admin:
            st.warning("⚠️ Acesso restrito a administradores.")
        else:
            # EXPANDER: CADASTRAR/EDITAR
            with st.expander("➕ Cadastrar / Editar SKU", expanded=True):
                st.markdown("**Modo de Operação:**")
                modo = st.radio("", ["Cadastrar Novo SKU", "Editar SKU Existente"], horizontal=True)
                
                # Inicializar valores padrão
                valores_padrao = {
                    'sku': '',
                    'nome': '',
                    'categoria': '',
                    'status': 'Ativo',
                    'cod_fornecedor': '',
                    'preco_compra': 0.0,
                    'embalagem': 0.0,
                    'mdo': 0.0,
                    'custo_ads': 0.0,
                    'preco_a_ser_considerado': 0.0
                }
                
                # Se modo edição, buscar SKU existente
                if modo == "Editar SKU Existente":
                    try:
                        # Buscar lista de SKUs
                        df_skus = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
                        lista_skus = ["Selecione..."] + [f"{row['sku']} - {row['nome']}" for _, row in df_skus.iterrows()]
                        
                        sku_selecionado = st.selectbox("🔍 Selecione o SKU para editar:", lista_skus)
                        
                        if sku_selecionado != "Selecione...":
                            sku_code = sku_selecionado.split(" - ")[0]
                            
                            # Buscar dados completos
                            query_sku = text("""
                                SELECT p.sku, p.nome, p.categoria, p.status, p.preco_a_ser_considerado,
                                       c.cod_fornecedor, c.preco_compra, c.embalagem, c.mdo, c.custo_ads
                                FROM dim_produtos p
                                LEFT JOIN dim_produtos_custos c ON p.sku = c.sku
                                WHERE p.sku = :sku
                            """)
                            
                            with engine.connect() as conn:
                                result = conn.execute(query_sku, {"sku": sku_code}).fetchone()
                                
                                if result:
                                    valores_padrao = {
                                        'sku': result[0] or '',
                                        'nome': result[1] or '',
                                        'categoria': result[2] or '',
                                        'status': result[3] or 'Ativo',
                                        'preco_a_ser_considerado': float(result[4] or 0.0),
                                        'cod_fornecedor': result[5] or '',
                                        'preco_compra': float(result[6] or 0.0),
                                        'embalagem': float(result[7] or 0.0),
                                        'mdo': float(result[8] or 0.0),
                                        'custo_ads': float(result[9] or 0.0)
                                    }
                                    st.success(f"✅ SKU {sku_code} carregado para edição!")
                    except Exception as e:
                        st.error(f"Erro ao carregar SKUs: {e}")
                
                # FORMULÁRIO DE CADASTRO/EDIÇÃO
                with st.form("form_sku", clear_on_submit=False):
                    st.markdown("### Dados Básicos")
                    col1, col2 = st.columns(2)
                    
                    v_sku = col1.text_input("SKU (ID único)*", 
                                            value=valores_padrao['sku'],
                                            disabled=(modo == "Editar SKU Existente"))
                    v_nome = col2.text_input("Nome do Produto*", 
                                             value=valores_padrao['nome'])
                    
                    col3, col4 = st.columns(2)
                    v_categoria = col3.text_input("Categoria", 
                                                  value=valores_padrao['categoria'])
                    v_status = col4.selectbox("Status", 
                                             ["Ativo", "Inativo"],
                                             index=0 if valores_padrao['status'] == 'Ativo' else 1)
                    
                    v_cod_fornecedor = st.text_input("Código Fornecedor", 
                                                     value=valores_padrao['cod_fornecedor'])
                    
                    st.markdown("### Custos Explodidos")
                    col5, col6, col7, col8 = st.columns(4)
                    
                    v_preco_compra = col5.text_input("Preço Compra (R$)", 
                                                     value=formatar_valor_br(valores_padrao['preco_compra']))
                    v_embalagem = col6.text_input("Embalagem (R$)", 
                                                  value=formatar_valor_br(valores_padrao['embalagem']))
                    v_mdo = col7.text_input("MDO (R$)", 
                                           value=formatar_valor_br(valores_padrao['mdo']))
                    v_custo_ads = col8.text_input("Custo Ads (R$)", 
                                                  value=formatar_valor_br(valores_padrao['custo_ads']))
                    
                    st.markdown("### Preço Final")
                    v_preco_final = st.text_input("Preço a Ser Considerado (R$)*", 
                                                  value=formatar_valor_br(valores_padrao['preco_a_ser_considerado']))
                    
                    st.info("💡 **Dica:** O preço a ser considerado pode ser editado manualmente ou calculado automaticamente pelo módulo de compras.")
                    
                    # BOTÃO SALVAR
                    submitted = st.form_submit_button("💾 Salvar Produto", type="primary")
                    
                    if submitted:
                        if not v_sku or not v_nome:
                            st.error("❌ SKU e Nome são obrigatórios!")
                        else:
                            try:
                                # Converter valores
                                preco_compra_float = converter_valor_para_float(v_preco_compra)
                                embalagem_float = converter_valor_para_float(v_embalagem)
                                mdo_float = converter_valor_para_float(v_mdo)
                                custo_ads_float = converter_valor_para_float(v_custo_ads)
                                preco_final_float = converter_valor_para_float(v_preco_final)
                                
                                with engine.connect() as conn:
                                    # 1. Inserir/Atualizar em dim_produtos
                                    query_produtos = text("""
                                        INSERT INTO dim_produtos (sku, nome, categoria, status, preco_a_ser_considerado)
                                        VALUES (:sku, :nome, :categoria, :status, :preco)
                                        ON CONFLICT (sku) 
                                        DO UPDATE SET
                                            nome = EXCLUDED.nome,
                                            categoria = EXCLUDED.categoria,
                                            status = EXCLUDED.status,
                                            preco_a_ser_considerado = EXCLUDED.preco_a_ser_considerado
                                    """)
                                    
                                    conn.execute(query_produtos, {
                                        "sku": v_sku,
                                        "nome": v_nome,
                                        "categoria": v_categoria,
                                        "status": v_status,
                                        "preco": preco_final_float
                                    })
                                    
                                    # 2. Inserir/Atualizar em dim_produtos_custos
                                    query_custos = text("""
                                        INSERT INTO dim_produtos_custos 
                                        (sku, cod_fornecedor, preco_compra, embalagem, mdo, custo_ads)
                                        VALUES (:sku, :cod_forn, :preco_compra, :embalagem, :mdo, :custo_ads)
                                        ON CONFLICT (sku)
                                        DO UPDATE SET
                                            cod_fornecedor = EXCLUDED.cod_fornecedor,
                                            preco_compra = EXCLUDED.preco_compra,
                                            embalagem = EXCLUDED.embalagem,
                                            mdo = EXCLUDED.mdo,
                                            custo_ads = EXCLUDED.custo_ads
                                    """)
                                    
                                    conn.execute(query_custos, {
                                        "sku": v_sku,
                                        "cod_forn": v_cod_fornecedor,
                                        "preco_compra": preco_compra_float,
                                        "embalagem": embalagem_float,
                                        "mdo": mdo_float,
                                        "custo_ads": custo_ads_float
                                    })
                                    
                                    conn.commit()
                                
                                st.success(f"✅ SKU {v_sku} salvo com sucesso!")
                                st.balloons()
                                
                            except Exception as e:
                                st.error(f"❌ Erro ao salvar: {e}")
            
            # ZONA DE PERIGO - EXCLUSÃO
            st.markdown("---")
            with st.expander("🗑️ ZONA DE PERIGO - Excluir SKU"):
                st.warning("⚠️ A exclusão é permanente e removerá também os custos associados.")
                
                try:
                    df_skus = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
                    lista_delete = ["Selecione..."] + [f"{row['sku']} - {row['nome']}" for _, row in df_skus.iterrows()]
                    
                    sku_para_deletar = st.selectbox("Selecione o SKU para remover:", lista_delete)
                    
                    if sku_para_deletar != "Selecione...":
                        sku_code_delete = sku_para_deletar.split(" - ")[0]
                        confirmar = st.checkbox(f"✅ Confirmo que desejo excluir o SKU {sku_code_delete}")
                        
                        if st.button("❌ EXCLUIR DEFINITIVAMENTE", type="primary"):
                            if confirmar:
                                try:
                                    with engine.connect() as conn:
                                        # Deleta primeiro da tabela de custos
                                        conn.execute(text("DELETE FROM dim_produtos_custos WHERE sku = :s"), {"s": sku_code_delete})
                                        # Depois deleta da tabela principal
                                        conn.execute(text("DELETE FROM dim_produtos WHERE sku = :s"), {"s": sku_code_delete})
                                        conn.commit()
                                    
                                    st.error(f"🗑️ SKU {sku_code_delete} removido com sucesso!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Erro ao excluir: {e}")
                            else:
                                st.info("⚠️ Marque a confirmação para proceder com a exclusão.")
                except Exception as e:
                    st.error(f"Erro ao carregar lista: {e}")

    # ============================================================
    # TAB 3: IMPORTAÇÃO EM MASSA
    # ============================================================
    with t3:
        st.subheader("📥 Sincronização em Massa")
        
        if not is_admin:
            st.warning("⚠️ Acesso restrito a administradores.")
        else:
            col1, col2 = st.columns(2)
            
            # DOWNLOAD TEMPLATE
            with col1:
                st.markdown("### 📥 Download Template")
                st.info("Baixe o modelo Excel para preencher seus SKUs")
                
                if st.button("📥 Baixar Template Excel"):
                    # Criar DataFrame template
                    template_data = {
                        'sku': ['EXEMPLO-001', 'EXEMPLO-002'],
                        'nome': ['Produto Exemplo 1', 'Produto Exemplo 2'],
                        'categoria': ['CATEGORIA 1', 'CATEGORIA 2'],
                        'status': ['Ativo', 'Ativo'],
                        'cod_fornecedor': ['FORN-001', 'FORN-002'],
                        'preco_compra': ['15,50', '20,00'],
                        'embalagem': ['0,50', '1,00'],
                        'mdo': ['0,88', '0,88'],
                        'custo_ads': ['0,50', '0,00'],
                        'preco_a_ser_considerado': ['17,38', '21,88']
                    }
                    
                    df_template = pd.DataFrame(template_data)
                    
                    # Gerar Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_template.to_excel(writer, index=False, sheet_name='Template SKUs')
                        
                        # Ajustar largura das colunas
                        worksheet = writer.sheets['Template SKUs']
                        for idx, col in enumerate(df_template.columns):
                            max_length = max(
                                df_template[col].astype(str).apply(len).max(),
                                len(col)
                            )
                            worksheet.column_dimensions[chr(65 + idx)].width = max_length + 2
                    
                    output.seek(0)
                    
                    st.download_button(
                        label="📥 Download Template.xlsx",
                        data=output,
                        file_name="template_skus_nala.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            # UPLOAD E PROCESSAR
            with col2:
                st.markdown("### 📤 Upload Arquivo")
                st.info("Envie o arquivo Excel preenchido para importar em massa")
                
                uploaded_file = st.file_uploader("Selecione o arquivo Excel", type=['xlsx', 'xls'])
                
                if uploaded_file is not None:
                    try:
                        # Ler Excel
                        df_import = pd.read_excel(uploaded_file)
                        
                        # Validar colunas
                        colunas_obrigatorias = ['sku', 'nome', 'categoria', 'status', 'cod_fornecedor', 
                                               'preco_compra', 'embalagem', 'mdo', 'custo_ads', 
                                               'preco_a_ser_considerado']
                        
                        colunas_faltando = [col for col in colunas_obrigatorias if col not in df_import.columns]
                        
                        if colunas_faltando:
                            st.error(f"❌ Colunas faltando no arquivo: {', '.join(colunas_faltando)}")
                        else:
                            st.success(f"✅ Arquivo válido! {len(df_import)} registros encontrados.")
                            
                            # Mostrar preview
                            st.markdown("**Preview dos dados:**")
                            st.dataframe(df_import.head(10), use_container_width=True)
                            
                            if st.button("🚀 Importar Dados", type="primary"):
                                sucesso = 0
                                erros = 0
                                
                                progress_bar = st.progress(0)
                                status_text = st.empty()
                                
                                for idx, row in df_import.iterrows():
                                    try:
                                        # Converter valores
                                        preco_compra = converter_valor_para_float(row['preco_compra'])
                                        embalagem = converter_valor_para_float(row['embalagem'])
                                        mdo = converter_valor_para_float(row['mdo'])
                                        custo_ads = converter_valor_para_float(row['custo_ads'])
                                        preco_final = converter_valor_para_float(row['preco_a_ser_considerado'])
                                        
                                        with engine.connect() as conn:
                                            # Inserir em dim_produtos
                                            query_prod = text("""
                                                INSERT INTO dim_produtos (sku, nome, categoria, status, preco_a_ser_considerado)
                                                VALUES (:sku, :nome, :cat, :status, :preco)
                                                ON CONFLICT (sku) 
                                                DO UPDATE SET
                                                    nome = EXCLUDED.nome,
                                                    categoria = EXCLUDED.categoria,
                                                    status = EXCLUDED.status,
                                                    preco_a_ser_considerado = EXCLUDED.preco_a_ser_considerado
                                            """)
                                            
                                            conn.execute(query_prod, {
                                                "sku": str(row['sku']),
                                                "nome": str(row['nome']),
                                                "cat": str(row['categoria']),
                                                "status": str(row['status']),
                                                "preco": preco_final
                                            })
                                            
                                            # Inserir em dim_produtos_custos
                                            query_custos = text("""
                                                INSERT INTO dim_produtos_custos 
                                                (sku, cod_fornecedor, preco_compra, embalagem, mdo, custo_ads)
                                                VALUES (:sku, :cod, :pc, :emb, :mdo, :ads)
                                                ON CONFLICT (sku)
                                                DO UPDATE SET
                                                    cod_fornecedor = EXCLUDED.cod_fornecedor,
                                                    preco_compra = EXCLUDED.preco_compra,
                                                    embalagem = EXCLUDED.embalagem,
                                                    mdo = EXCLUDED.mdo,
                                                    custo_ads = EXCLUDED.custo_ads
                                            """)
                                            
                                            conn.execute(query_custos, {
                                                "sku": str(row['sku']),
                                                "cod": str(row['cod_fornecedor']),
                                                "pc": preco_compra,
                                                "emb": embalagem,
                                                "mdo": mdo,
                                                "ads": custo_ads
                                            })
                                            
                                            conn.commit()
                                        
                                        sucesso += 1
                                        
                                    except Exception as e:
                                        erros += 1
                                        st.warning(f"Erro na linha {idx + 2} (SKU: {row['sku']}): {e}")
                                    
                                    # Atualizar progresso
                                    progress = (idx + 1) / len(df_import)
                                    progress_bar.progress(progress)
                                    status_text.text(f"Processando... {idx + 1}/{len(df_import)}")
                                
                                # Resultado final
                                st.success(f"✅ Importação concluída! Sucesso: {sucesso} | Erros: {erros}")
                                st.balloons()
                                
                    except Exception as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")

if __name__ == "__main__":
    main()
