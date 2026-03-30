"""
REGISTRO DE COMPRAS - Sistema Nala
Versão: 2.0 (29/03/2026)

CHANGELOG v2.0:
  - FIX: DB_URL hardcoded removido — usa get_engine() de database_utils.py
  - FIX: Tela não limpava após gravar compra — corrigido com session_state reset
  - NOVO: Campo "Outros Custos" editável (dim_produtos_custos.outros_custos)
  - NOVO: Todos os campos de custo são editáveis (embalagem, mdo, ads, outros)
  - NOVO: Ao gravar, salva todos os custos editados na dim_produtos_custos
  - NOVO: Ao gravar, atualiza preco_a_ser_considerado na dim_produtos automaticamente
  - NOVO: Ao abrir SKU sem histórico, mostra custos cadastrados em dim_produtos_custos
  - MELHORIA: Calculadora ao vivo — custo total recalcula em tempo real ao digitar qualquer campo

CHANGELOG v1.0:
  - Versão inicial com formulário de compra, histórico e exclusão
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import date

# v2.0: Usa get_engine de database_utils (respeita ambiente Produção/Dev)
from database_utils import get_engine


def fmt_moeda(v):
    """R$ 15,50 (sempre 2 casas)"""
    return f"R$ {float(v):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def fmt_data(d):
    """dd/mm/aaaa"""
    if isinstance(d, str):
        return pd.to_datetime(d).strftime('%d/%m/%Y')
    return d.strftime('%d/%m/%Y')


def _safe_float(valor_str, default=0.0):
    """Converte string BR ou número para float seguro."""
    try:
        if isinstance(valor_str, (int, float)):
            return float(valor_str)
        s = str(valor_str).replace(".", "").replace(",", ".").strip()
        if s in ('', 'nan', 'None', 'none'):
            return default
        return float(s)
    except:
        return default


def formatar_valor_br(valor):
    """Converte float para string no formato brasileiro (ex: 15,50)"""
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"


def main():
    st.markdown("<h4 style='margin:0;padding:5px 0;'>🛒 Registro de Compras</h4>",
                unsafe_allow_html=True)

    # CSS ULTRA COMPACTO
    st.markdown("""
        <style>
        .main { padding-top: 0.3rem !important; }
        .block-container { padding-top: 0.5rem !important; padding-bottom: 0.3rem !important; }
        h1, h2, h3, h4 { margin: 0 !important; padding: 0.2rem 0 !important; font-size: 0.95rem !important; }
        div[data-testid="stMetric"] { 
            background-color: #f8f9fa; border: 1px solid #dee2e6; 
            padding: 5px 8px !important; border-radius: 4px; height: 55px !important;
            display: flex; flex-direction: column; justify-content: center; margin: 1px 0 !important;
        }
        div[data-testid="stMetric"] label { font-size: 0.68rem !important; margin: 0 !important; }
        div[data-testid="stMetric"] div { font-size: 0.9rem !important; font-weight: 600; margin: 0 !important; }
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stDateInput"] label,
        div[data-testid="stNumberInput"] label { 
            font-size: 0.72rem !important; font-weight: 500; margin-bottom: 1px !important;
        }
        input, select { font-size: 0.82rem !important; padding: 3px 6px !important; min-height: 30px !important; }
        .stDataFrame { font-size: 0.78rem !important; }
        .stButton button { padding: 5px 10px !important; font-size: 0.82rem !important; }
        div[data-testid="column"] > div { margin-bottom: 0 !important; }
        hr { margin: 6px 0 !important; }
        .stSuccess, .stError, .stWarning { 
            padding: 6px 10px !important; font-size: 0.82rem !important; margin: 4px 0 !important;
        }
        .stCaption { font-size: 0.7rem !important; color: #666 !important; margin: 0 !important; padding: 0 !important; }
        </style>
    """, unsafe_allow_html=True)

    engine = get_engine()

    # ========================================================================
    # VERIFICAR SE PRECISA LIMPAR APÓS GRAVAR
    # ========================================================================
    if st.session_state.get('_compra_salva'):
        # Limpar todos os campos do formulário
        for key in ['sel_forn_existente', 'txt_forn_novo', 'preco_nf',
                     'num_nf', 'qtd_compra', 'data_compra',
                     'edit_embalagem', 'edit_mdo', 'edit_ads', 'edit_outros',
                     'custo_consid']:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state['_compra_salva'] = False

    # ========================================================================
    # DADOS BASE
    # ========================================================================
    try:
        df_p = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
        df_p['display'] = df_p['sku'] + " - " + df_p['nome']
        df_f = pd.read_sql("SELECT DISTINCT fornecedor FROM fato_compras ORDER BY fornecedor", engine)
        lista_forn = df_f['fornecedor'].tolist()
    except Exception as e:
        st.error(f"❌ Erro: {e}")
        return

    escolha = st.selectbox("🔍 Produto", [""] + df_p['display'].tolist(),
                           label_visibility="collapsed")
    sku_sel = escolha.split(" - ")[0] if escolha else ""

    if not sku_sel:
        st.info("Selecione um produto.")
        return

    nome_produto = df_p[df_p['sku'] == sku_sel]['nome'].iloc[0] \
        if not df_p[df_p['sku'] == sku_sel].empty else ""

    st.markdown("---")

    # ========================================================================
    # BUSCAR CUSTOS CADASTRADOS (dim_produtos_custos)
    # ========================================================================
    try:
        with engine.connect() as conn:
            custos_db = conn.execute(text("""
                SELECT COALESCE(preco_compra, 0), COALESCE(embalagem, 0), 
                       COALESCE(mdo, 0), COALESCE(custo_ads, 0), COALESCE(outros_custos, 0)
                FROM dim_produtos_custos WHERE sku = :s
            """), {"s": sku_sel}).fetchone()
    except:
        custos_db = None

    if custos_db:
        db_preco_compra = float(custos_db[0])
        db_embalagem = float(custos_db[1])
        db_mdo = float(custos_db[2])
        db_ads = float(custos_db[3])
        db_outros = float(custos_db[4])
    else:
        db_preco_compra, db_embalagem, db_mdo, db_ads, db_outros = 0.0, 0.0, 0.0, 0.0, 0.0

    # ========================================================================
    # BUSCAR ÚLTIMO PREÇO DE COMPRA (histórico)
    # ========================================================================
    try:
        with engine.connect() as conn:
            ult = conn.execute(text("""
                SELECT preco_unitario FROM fato_compras 
                WHERE sku = :s ORDER BY id DESC LIMIT 1
            """), {"s": sku_sel}).fetchone()
        ultimo_preco_hist = float(ult[0]) if ult else None
    except:
        ultimo_preco_hist = None

    # Preço base para exibição inicial: último histórico > cadastrado > 0
    preco_base = ultimo_preco_hist if ultimo_preco_hist is not None else db_preco_compra

    # ========================================================================
    # LAYOUT PRINCIPAL
    # ========================================================================
    col_form, col_ind = st.columns([1.6, 1.4])

    # ====================================================================
    # COLUNA ESQUERDA: FORMULÁRIO DE COMPRA
    # ====================================================================
    with col_form:
        st.markdown(f"**📝 Nova Compra - {sku_sel}** - {nome_produto[:50]}")

        # Fornecedor
        st.markdown("**Fornecedor**")
        col_sel, col_novo = st.columns([1.2, 1])

        forn_existente = col_sel.selectbox("Selecionar existente", [""] + lista_forn,
                                           key="sel_forn_existente")
        forn_novo = col_novo.text_input("Ou cadastrar novo", placeholder="Digite novo",
                                         key="txt_forn_novo")

        forn_final = forn_novo.strip() if forn_novo.strip() else forn_existente

        # Dados da compra
        c1, c2 = st.columns(2)
        dt = c1.date_input("Data", date.today(), format="DD/MM/YYYY", key="data_compra")
        nf = c2.text_input("NF nº", placeholder="Opcional", key="num_nf")

        c3, c4 = st.columns(2)
        qtd = c3.number_input("Qtd", 1, step=1, key="qtd_compra")
        pnf = c4.text_input("Preço NF", placeholder="Ex: 15,50", key="preco_nf",
                            value="" if 'preco_nf' not in st.session_state else st.session_state.get('preco_nf', ''))

        custo_input = st.text_input("Custo Considerado (opcional — se vazio = Custo Total)",
                                     placeholder="Sobrescrever custo total manualmente",
                                     key="custo_consid")

    # ====================================================================
    # COLUNA DIREITA: CUSTOS EDITÁVEIS + CALCULADORA AO VIVO
    # ====================================================================
    with col_ind:
        st.markdown(f"**📊 Custos - {sku_sel}**")

        # Ler Preço NF digitado (para calculadora ao vivo)
        preco_digitado = _safe_float(pnf) if pnf else 0.0

        # Se não digitou, usar preço base (histórico ou cadastrado)
        preco_para_calculo = preco_digitado if preco_digitado > 0 else preco_base

        # Campos editáveis de custo
        c_e1, c_e2 = st.columns(2)
        edit_emb = c_e1.text_input("📦 Embalagem (R$)",
                                    value=formatar_valor_br(db_embalagem),
                                    key="edit_embalagem")
        edit_mdo = c_e2.text_input("👷 MDO (R$)",
                                    value=formatar_valor_br(db_mdo),
                                    key="edit_mdo")

        c_e3, c_e4 = st.columns(2)
        edit_ads = c_e3.text_input("📢 ADS (R$)",
                                    value=formatar_valor_br(db_ads),
                                    key="edit_ads")
        edit_outros = c_e4.text_input("📋 Outros Custos (R$)",
                                       value=formatar_valor_br(db_outros),
                                       key="edit_outros")

        # Converter valores editados para float (ao vivo)
        v_emb = _safe_float(edit_emb)
        v_mdo = _safe_float(edit_mdo)
        v_ads = _safe_float(edit_ads)
        v_outros = _safe_float(edit_outros)

        # CÁLCULO AO VIVO
        custo_total_calc = preco_para_calculo + v_emb + v_mdo + v_ads + v_outros

        # Custo Considerado: se vazio, usa custo total calculado
        custo_consid_valor = _safe_float(custo_input) if custo_input and custo_input.strip() else custo_total_calc

        # Exibir resultados
        st.metric("💵 Preço NF (digitado/histórico)", fmt_moeda(preco_para_calculo))
        st.metric("📊 CUSTO TOTAL", fmt_moeda(custo_total_calc))
        st.metric("💎 Custo Considerado", fmt_moeda(custo_consid_valor))

        # Dica visual
        if preco_digitado > 0:
            if custo_input and custo_input.strip():
                st.caption("✅ Custo Considerado definido manualmente")
            else:
                st.caption("ℹ️ Custo Considerado = Custo Total (automático)")
        elif preco_base > 0:
            if ultimo_preco_hist is not None:
                st.caption("ℹ️ Exibindo último preço do histórico. Digite o Preço NF para recalcular.")
            else:
                st.caption("ℹ️ Exibindo preço cadastrado. Digite o Preço NF para recalcular.")
        else:
            st.caption("ℹ️ Digite o Preço NF para calcular")

    # ========================================================================
    # BOTÃO GRAVAR (fora das colunas, largura total)
    # ========================================================================
    if st.button("💾 Gravar Compra", use_container_width=True, type="primary", key="btn_gravar"):
        if not forn_final:
            st.error("⚠️ Selecione ou cadastre um fornecedor")
        elif not pnf or not pnf.strip():
            st.error("⚠️ Preencha o Preço NF")
        else:
            try:
                v_nf = _safe_float(pnf)

                if v_nf <= 0:
                    st.error("❌ Preço NF deve ser positivo")
                else:
                    # Recalcular custo total com o preço NF digitado
                    custo_total_final = v_nf + v_emb + v_mdo + v_ads + v_outros

                    # Custo considerado final
                    if custo_input and custo_input.strip():
                        custo_consid_final = _safe_float(custo_input)
                    else:
                        custo_consid_final = custo_total_final

                    if custo_consid_final <= 0:
                        st.error("❌ Custo Considerado deve ser positivo")
                    else:
                        with engine.begin() as conn:
                            # 1. Gravar histórico na fato_compras
                            conn.execute(text("""
                                INSERT INTO fato_compras 
                                (data_compra, sku, fornecedor, quantidade, preco_unitario, 
                                 custo_considerado, valor_total, numero_nf)
                                VALUES (:d, :s, :f, :q, :p, :c, :t, :nf)
                            """), {
                                "d": dt, "s": sku_sel, "f": forn_final, "q": qtd,
                                "p": v_nf, "c": custo_consid_final, "t": qtd * v_nf,
                                "nf": nf.strip() or None
                            })

                            # 2. SINCRONIZAR dim_produtos_custos (todos os campos editáveis)
                            conn.execute(text("""
                                UPDATE dim_produtos_custos 
                                SET preco_compra = :pc,
                                    embalagem = :emb,
                                    mdo = :mdo,
                                    custo_ads = :ads,
                                    outros_custos = :outros
                                WHERE sku = :s
                            """), {
                                "pc": v_nf, "emb": v_emb, "mdo": v_mdo,
                                "ads": v_ads, "outros": v_outros, "s": sku_sel
                            })

                            # 3. SINCRONIZAR dim_produtos.preco_a_ser_considerado
                            conn.execute(text("""
                                UPDATE dim_produtos 
                                SET preco_a_ser_considerado = :custo
                                WHERE sku = :s
                            """), {"custo": custo_consid_final, "s": sku_sel})

                        st.success(
                            f"✅ Gravado! Preço NF: {fmt_moeda(v_nf)} | "
                            f"Custo Total: {fmt_moeda(custo_total_final)} | "
                            f"Considerado: {fmt_moeda(custo_consid_final)}"
                        )

                        # Sinalizar para limpar na próxima execução
                        st.session_state['_compra_salva'] = True
                        st.rerun()

            except ValueError:
                st.error("❌ Valores inválidos. Use formato: 15,50")
            except Exception as e:
                st.error(f"❌ Erro: {e}")

    # ========================================================================
    # HISTÓRICO DE COMPRAS
    # ========================================================================
    st.markdown("---")
    st.markdown(f"**🕒 Histórico - {sku_sel} - {nome_produto}**")

    try:
        df_h = pd.read_sql(text("""
            SELECT id, data_compra, fornecedor, numero_nf, quantidade, 
                   preco_unitario, custo_considerado
            FROM fato_compras 
            WHERE sku = :s 
            ORDER BY id DESC
        """), engine, params={"s": sku_sel})

        if not df_h.empty:
            df_show = df_h.copy()
            df_show['Data'] = df_show['data_compra'].apply(fmt_data)
            df_show['NF nº'] = df_show['numero_nf'].fillna('-')
            df_show['Preço NF'] = df_show['preco_unitario'].apply(
                lambda x: fmt_moeda(float(x)))
            df_show['Custo Considerado'] = df_show['custo_considerado'].apply(
                lambda x: fmt_moeda(float(x)))
            # Custo Total = Preço NF + custos fixos atuais
            df_show['Custo Total'] = df_show['preco_unitario'].apply(
                lambda x: fmt_moeda(float(x) + v_emb + v_mdo + v_ads + v_outros))

            st.dataframe(
                df_show[['id', 'Data', 'fornecedor', 'NF nº', 'quantidade',
                         'Preço NF', 'Custo Considerado', 'Custo Total']],
                use_container_width=True,
                hide_index=True
            )

            # EXCLUSÃO
            with st.expander("🗑️ Excluir Registro"):
                st.warning("⚠️ Esta ação não pode ser desfeita.")

                cx, cy = st.columns([3, 1])
                opts = [
                    (r['id'], f"ID {r['id']} - {fmt_data(r['data_compra'])} - "
                              f"{r['fornecedor']} - {fmt_moeda(r['custo_considerado'])}")
                    for _, r in df_h.iterrows()
                ]

                sel = cx.selectbox("Registro",
                                   [o[0] for o in opts],
                                   format_func=lambda x: [o[1] for o in opts if o[0] == x][0])

                if cy.button("🗑️ Excluir"):
                    try:
                        with engine.begin() as conn:
                            # 1. Deletar o registro
                            conn.execute(text("DELETE FROM fato_compras WHERE id = :id"),
                                         {"id": sel})

                            # 2. Buscar última compra restante
                            ultimo_registro = conn.execute(text("""
                                SELECT preco_unitario 
                                FROM fato_compras 
                                WHERE sku = :s 
                                ORDER BY id DESC 
                                LIMIT 1
                            """), {"s": sku_sel}).fetchone()

                            # 3. Atualizar dim_produtos_custos.preco_compra
                            novo_preco = float(ultimo_registro[0]) if ultimo_registro else 0.0

                            conn.execute(text("""
                                UPDATE dim_produtos_custos 
                                SET preco_compra = :p
                                WHERE sku = :s
                            """), {"p": novo_preco, "s": sku_sel})

                            # 4. Recalcular e atualizar preco_a_ser_considerado
                            custos_atuais = conn.execute(text("""
                                SELECT COALESCE(embalagem, 0), COALESCE(mdo, 0),
                                       COALESCE(custo_ads, 0), COALESCE(outros_custos, 0)
                                FROM dim_produtos_custos WHERE sku = :s
                            """), {"s": sku_sel}).fetchone()

                            if custos_atuais:
                                novo_considerado = (novo_preco
                                                    + float(custos_atuais[0])
                                                    + float(custos_atuais[1])
                                                    + float(custos_atuais[2])
                                                    + float(custos_atuais[3]))
                            else:
                                novo_considerado = novo_preco

                            conn.execute(text("""
                                UPDATE dim_produtos SET preco_a_ser_considerado = :c
                                WHERE sku = :s
                            """), {"c": novo_considerado, "s": sku_sel})

                        st.success(
                            f"✅ Registro excluído! Preço restaurado: {fmt_moeda(novo_preco)} | "
                            f"Considerado: {fmt_moeda(novo_considerado)}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")
        else:
            st.info("Sem compras registradas.")
    except Exception as e:
        st.error(f"❌ {e}")


if __name__ == "__main__":
    main()
