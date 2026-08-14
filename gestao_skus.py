"""
GESTÃO DE SKUs - Sistema Nala
Versão: 2.1 (30/03/2026)

CHANGELOG v2.1:
  - NOVO: Campos de dimensão e peso (largura, comprimento, altura, peso_bruto)
        Aparecem no formulário de cadastro/edição, template e importação.
        NÃO aparecem na lista principal (regra de UX).

CHANGELOG v2.0:
  - FIX: DB_URL hardcoded removido — usa get_engine() de database_utils.py
  - FIX: Campos não limpavam após salvar cadastro/edição — corrigido com session_state reset
  - NOVO: Campo "Outros Custos" (dim_produtos_custos.outros_custos)
  - NOVO: Campos "Margem Mínima %" e "Margem Desejável %" (dim_produtos)
  - NOVO: Custo Final calculado (soma de todos os custos) exibido na lista
  - MELHORIA: Template de importação e download incluem novos campos
  - MELHORIA: Importação em massa processa novos campos

CHANGELOG v1.0:
  - Versão inicial com lista, busca, cadastro, edição, exclusão e importação
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text
import io

# v2.0: Usa get_engine de database_utils (respeita ambiente Produção/Dev)
from database_utils import get_engine


def formatar_valor_br(valor):
    """Converte float para string no formato brasileiro (ex: 1.234,56)"""
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"


def converter_valor_para_float(valor_str):
    """
    Converte string brasileira para float (ex: '1.234,56' -> 1234.56).

    Usado nos formulários manuais (Tab 2), onde campo vazio = zero é o esperado.
    v2.2 FIX: NaN não passa mais direto para o banco — vira 0.0.
    """
    valor = converter_valor_planilha(valor_str)
    return 0.0 if valor is None else valor


def converter_valor_planilha(valor_str):
    """
    Converte valor de planilha para float, ou None quando não há valor.

    v2.2: usado na importação em massa. A diferença para
    converter_valor_para_float é crítica: célula vazia devolve None
    ("não informado", mantém o que está no banco) em vez de 0.0
    ("custo é zero", apaga o cadastro).

    Trata os dois formatos decimais:
      '1.234,56' -> 1234.56   (BR)
      '1,234.56' -> 1234.56   (US)
      '134.71'   -> 134.71    (US simples — antes virava 13471.0)
      '134,71'   -> 134.71    (BR simples)
    """
    if valor_str is None:
        return None

    # NaN do pandas passa no isinstance(float) — precisa ser barrado antes
    if isinstance(valor_str, float) and valor_str != valor_str:
        return None

    if isinstance(valor_str, (int, float)):
        return float(valor_str)

    texto = str(valor_str).strip()
    if texto == "" or texto.lower() in ("nan", "none", "-", "r$"):
        return None

    texto = texto.replace("R$", "").replace(" ", "").replace("\xa0", "")

    tem_ponto = "." in texto
    tem_virgula = "," in texto

    if tem_ponto and tem_virgula:
        # O separador decimal é o que aparece por último
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")   # BR: 1.234,56
        else:
            texto = texto.replace(",", "")                     # US: 1,234.56
    elif tem_virgula:
        texto = texto.replace(",", ".")                        # BR: 134,71
    elif tem_ponto:
        # Só ponto: milhar (1.234) ou decimal (134.71)?
        # Milhar sempre tem exatamente 3 dígitos depois do último ponto.
        depois = texto.rsplit(".", 1)[-1]
        if len(depois) == 3 and texto.count(".") >= 1 and len(texto.replace(".", "")) > 3:
            texto = texto.replace(".", "")                     # 1.234 -> 1234
        # senão mantém como decimal: 134.71 -> 134.71

    try:
        return float(texto)
    except (ValueError, TypeError):
        return None


# ============================================================
# IMPORTAÇÃO EM MASSA — BLINDAGEM (v2.2)
# ============================================================
# Contexto: em 13/08/2026 uma importação sobrescreveu o custo de 948
# dos 951 SKUs ativos com valores errados, sem aviso nenhum. As margens
# de ML e Amazon foram para 36% e ninguém percebeu no ato.
#
# Três defesas foram adicionadas:
#   1. Célula vazia / coluna ausente PRESERVA o valor do banco (COALESCE)
#   2. Diff obrigatório na tela antes de gravar
#   3. Circuit breaker: queda de custo em massa bloqueia a importação
#   4. Transação única — ou grava tudo, ou não grava nada

COLUNAS_OBRIGATORIAS = ['sku', 'nome']

# Campos que vão para dim_produtos
CAMPOS_PRODUTO = ['nome', 'categoria', 'status', 'preco_a_ser_considerado',
                  'margem_minima', 'margem_desejavel',
                  'largura', 'comprimento', 'altura', 'peso_bruto']

# Campos que vão para dim_produtos_custos
CAMPOS_CUSTO = ['cod_fornecedor', 'preco_compra', 'embalagem', 'mdo',
                'custo_ads', 'outros_custos']

TODOS_CAMPOS = CAMPOS_PRODUTO + CAMPOS_CUSTO

CAMPOS_TEXTO = {'nome', 'categoria', 'status', 'cod_fornecedor'}

# Circuit breaker: bloqueia se mais de 20% dos SKUs caírem mais de 30% de custo
LIMIAR_PCT_SKUS_AFETADOS = 20.0
LIMIAR_QUEDA_CUSTO = 30.0


def _preparar_registros(df_import):
    """
    Converte o DataFrame da planilha em registros normalizados.

    Cada campo vira None quando a coluna não existe ou a célula está vazia —
    None significa "não informado, mantenha o banco", nunca "zere o valor".
    """
    registros = []
    erros = []

    for idx, row in df_import.iterrows():
        linha_planilha = idx + 2  # +1 do header, +1 porque Excel começa em 1

        sku = str(row.get('sku', '')).strip()
        if sku == '' or sku.lower() == 'nan':
            erros.append({'linha': linha_planilha, 'sku': '(vazio)',
                          'problema': 'SKU vazio'})
            continue

        nome = str(row.get('nome', '')).strip()
        if nome == '' or nome.lower() == 'nan':
            erros.append({'linha': linha_planilha, 'sku': sku,
                          'problema': 'Nome vazio'})
            continue

        registro = {'sku': sku, '_linha': linha_planilha}

        for campo in TODOS_CAMPOS:
            if campo not in df_import.columns:
                registro[campo] = None
                continue

            bruto = row.get(campo)

            if campo in CAMPOS_TEXTO:
                texto = '' if bruto is None else str(bruto).strip()
                registro[campo] = None if texto in ('', 'nan', 'None') else texto
            else:
                valor = converter_valor_planilha(bruto)
                if valor is not None and valor < 0:
                    erros.append({'linha': linha_planilha, 'sku': sku,
                                  'problema': f'{campo} negativo ({valor})'})
                registro[campo] = valor

        registros.append(registro)

    return registros, erros


def _carregar_estado_atual(engine, skus):
    """Busca os valores hoje no banco para os SKUs da planilha."""
    if not skus:
        return {}

    query = text("""
        SELECT p.sku, p.nome, p.preco_a_ser_considerado,
               c.preco_compra, c.embalagem, c.mdo, c.custo_ads, c.outros_custos
        FROM dim_produtos p
        LEFT JOIN dim_produtos_custos c ON c.sku = p.sku
        WHERE p.sku = ANY(:skus)
    """)

    with engine.connect() as conn:
        linhas = conn.execute(query, {"skus": list(skus)}).fetchall()

    def _num(valor):
        """
        NULL e NaN viram None.

        O banco tem NaN real em colunas numeric (herança da importação de
        13/08). Se um NaN chegasse ao _avaliar_risco, toda comparação com ele
        seria False e o circuit breaker deixaria passar sem bloquear.
        """
        if valor is None:
            return None
        try:
            convertido = float(valor)
        except (TypeError, ValueError):
            return None
        return None if convertido != convertido else convertido

    return {
        linha[0]: {
            'nome': linha[1],
            'preco_a_ser_considerado': _num(linha[2]),
            'preco_compra': _num(linha[3]),
            'embalagem': _num(linha[4]),
            'mdo': _num(linha[5]),
            'custo_ads': _num(linha[6]),
            'outros_custos': _num(linha[7]),
        }
        for linha in linhas
    }


def _montar_diff(registros, estado_atual):
    """Monta o comparativo linha a linha entre planilha e banco."""
    diff = []

    def _valido(valor):
        """None para NULL e NaN — NaN faria toda comparação virar False."""
        return valor if (valor is not None and valor == valor) else None

    for registro in registros:
        sku = registro['sku']
        atual = estado_atual.get(sku)
        novo_custo = _valido(registro.get('preco_a_ser_considerado'))

        if atual is None:
            diff.append({
                'sku': sku, 'nome': registro.get('nome') or '',
                'situacao': 'NOVO', 'custo_atual': None,
                'custo_novo': novo_custo, 'variacao_pct': None,
            })
            continue

        custo_atual = _valido(atual['preco_a_ser_considerado'])

        if novo_custo is None:
            variacao = None
            situacao = 'PRESERVA CUSTO'
        elif custo_atual is None or custo_atual == 0:
            variacao = None
            situacao = 'DEFINE CUSTO'
        else:
            variacao = (novo_custo - custo_atual) / custo_atual * 100
            if abs(variacao) < 0.01:
                situacao = 'SEM MUDANÇA'
            elif variacao < 0:
                situacao = 'REDUZ CUSTO'
            else:
                situacao = 'AUMENTA CUSTO'

        diff.append({
            'sku': sku, 'nome': registro.get('nome') or atual['nome'] or '',
            'situacao': situacao, 'custo_atual': custo_atual,
            'custo_novo': novo_custo, 'variacao_pct': variacao,
        })

    return diff


def _avaliar_risco(diff):
    """
    Circuit breaker. Devolve None se a importação parece segura,
    ou um dict com o motivo do bloqueio.
    """
    comparaveis = [d for d in diff if d['variacao_pct'] is not None]
    if not comparaveis:
        return None

    quedas = [d for d in comparaveis if d['variacao_pct'] <= -LIMIAR_QUEDA_CUSTO]
    pct_afetado = len(quedas) / len(comparaveis) * 100

    if pct_afetado <= LIMIAR_PCT_SKUS_AFETADOS:
        return None

    queda_media = sum(d['variacao_pct'] for d in quedas) / len(quedas)

    return {
        'n_quedas': len(quedas),
        'mensagem': (
            f"**{len(quedas)} de {len(comparaveis)} SKUs ({pct_afetado:.1f}%) "
            f"teriam o custo reduzido em mais de {LIMIAR_QUEDA_CUSTO:.0f}%.**\n\n"
            f"Queda média nesses SKUs: {queda_media:.1f}%. "
            f"O limite de segurança é {LIMIAR_PCT_SKUS_AFETADOS:.0f}% dos SKUs."
        ),
    }


def _render_diff(diff, colunas_no_arquivo):
    """Mostra o resumo e a tabela de impacto antes de gravar."""
    novos = [d for d in diff if d['situacao'] == 'NOVO']
    reduz = [d for d in diff if d['situacao'] == 'REDUZ CUSTO']
    aumenta = [d for d in diff if d['situacao'] == 'AUMENTA CUSTO']
    preserva = [d for d in diff if d['situacao'] == 'PRESERVA CUSTO']

    st.markdown("### 🔍 Impacto no custo")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKUs novos", len(novos))
    c2.metric("Custo ↓", len(reduz))
    c3.metric("Custo ↑", len(aumenta))
    c4.metric("Preservados", len(preserva))

    if 'preco_a_ser_considerado' not in colunas_no_arquivo:
        st.success("✅ A planilha não traz `preco_a_ser_considerado` — "
                   "nenhum custo será alterado.")

    mudancas = [d for d in diff if d['variacao_pct'] is not None
                and abs(d['variacao_pct']) >= 0.01]

    if not mudancas:
        st.info("Nenhuma alteração de custo em SKUs já cadastrados.")
        return

    ordenado = sorted(mudancas, key=lambda d: d['variacao_pct'])

    # Sinalizador em texto: st.style.background_gradient exigiria matplotlib,
    # que não está no requirements.txt — importar quebraria a página inteira.
    def _sinal(variacao):
        if variacao <= -LIMIAR_QUEDA_CUSTO:
            return '🔴 queda forte'
        if variacao < 0:
            return '🟡 queda'
        return '🟢 aumento'

    df_diff = pd.DataFrame([{
        'SKU': d['sku'],
        'Produto': (d['nome'] or '')[:45],
        'Custo atual': formatar_valor_br(d['custo_atual']),
        'Custo novo': formatar_valor_br(d['custo_novo']),
        'Variação %': f"{d['variacao_pct']:+.1f}%",
        'Sinal': _sinal(d['variacao_pct']),
    } for d in ordenado])

    st.markdown("**Maiores quedas primeiro** — confira se fazem sentido:")
    st.dataframe(df_diff, use_container_width=True, height=320, hide_index=True)


def _executar_importacao(engine, registros, colunas_no_arquivo):
    """
    Grava tudo numa transação única.

    O COALESCE é o que impede o acidente de 13/08: parâmetro None
    (coluna ausente ou célula vazia) mantém o valor que já está no banco.
    """
    query_prod = text("""
        INSERT INTO dim_produtos
            (sku, nome, categoria, status, preco_a_ser_considerado,
             margem_minima, margem_desejavel,
             largura, comprimento, altura, peso_bruto)
        VALUES (:sku, :nome, :categoria, COALESCE(:status, 'Ativo'),
                :preco_a_ser_considerado, :margem_minima, :margem_desejavel,
                :largura, :comprimento, :altura, :peso_bruto)
        ON CONFLICT (sku) DO UPDATE SET
            nome = COALESCE(EXCLUDED.nome, dim_produtos.nome),
            categoria = COALESCE(EXCLUDED.categoria, dim_produtos.categoria),
            status = COALESCE(EXCLUDED.status, dim_produtos.status),
            preco_a_ser_considerado = COALESCE(EXCLUDED.preco_a_ser_considerado,
                                               dim_produtos.preco_a_ser_considerado),
            margem_minima = COALESCE(EXCLUDED.margem_minima, dim_produtos.margem_minima),
            margem_desejavel = COALESCE(EXCLUDED.margem_desejavel, dim_produtos.margem_desejavel),
            largura = COALESCE(EXCLUDED.largura, dim_produtos.largura),
            comprimento = COALESCE(EXCLUDED.comprimento, dim_produtos.comprimento),
            altura = COALESCE(EXCLUDED.altura, dim_produtos.altura),
            peso_bruto = COALESCE(EXCLUDED.peso_bruto, dim_produtos.peso_bruto)
    """)

    query_custos = text("""
        INSERT INTO dim_produtos_custos
            (sku, cod_fornecedor, preco_compra, embalagem, mdo, custo_ads, outros_custos)
        VALUES (:sku, :cod_fornecedor, :preco_compra, :embalagem,
                :mdo, :custo_ads, :outros_custos)
        ON CONFLICT (sku) DO UPDATE SET
            cod_fornecedor = COALESCE(EXCLUDED.cod_fornecedor, dim_produtos_custos.cod_fornecedor),
            preco_compra = COALESCE(EXCLUDED.preco_compra, dim_produtos_custos.preco_compra),
            embalagem = COALESCE(EXCLUDED.embalagem, dim_produtos_custos.embalagem),
            mdo = COALESCE(EXCLUDED.mdo, dim_produtos_custos.mdo),
            custo_ads = COALESCE(EXCLUDED.custo_ads, dim_produtos_custos.custo_ads),
            outros_custos = COALESCE(EXCLUDED.outros_custos, dim_produtos_custos.outros_custos)
    """)

    tem_campo_custo = any(c in colunas_no_arquivo for c in CAMPOS_CUSTO)

    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(registros)

    try:
        # engine.begin() = transação única: erro em qualquer linha desfaz tudo
        with engine.begin() as conn:
            for i, registro in enumerate(registros):
                params_prod = {'sku': registro['sku']}
                params_prod.update({c: registro.get(c) for c in CAMPOS_PRODUTO})
                conn.execute(query_prod, params_prod)

                if tem_campo_custo:
                    params_custo = {'sku': registro['sku']}
                    params_custo.update({c: registro.get(c) for c in CAMPOS_CUSTO})
                    conn.execute(query_custos, params_custo)

                if i % 25 == 0 or i == total - 1:
                    progress_bar.progress((i + 1) / total)
                    status_text.text(f"Gravando... {i + 1}/{total}")

        progress_bar.progress(1.0)
        status_text.empty()
        st.success(f"✅ Importação concluída: {total} SKUs gravados.")
        st.balloons()

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Importação revertida — nada foi gravado.\n\nErro: {e}")


def main():
    st.header("📦 Gestão de SKUs Nala")
    engine = get_engine()

    user_role = st.session_state.get('perfil', 'Admin')
    is_admin = user_role in ['Admin', 'Controladoria', 'ADMIN', 'CONTROLADORIA']

    t1, t2, t3 = st.tabs(["📋 Lista e Busca", "⚙️ Gerenciar SKU", "📥 Importação"])

    # ============================================================
    # TAB 1: LISTA E BUSCA + DOWNLOAD
    # ============================================================
    with t1:
        col1, col2, col3 = st.columns([2, 1, 1])

        busca = col1.text_input("🔍 Buscar por SKU ou Nome do Produto",
                                placeholder="Digite parte do nome ou código...")

        if col2.button("🔄 Atualizar Base"):
            st.rerun()

        try:
            if is_admin:
                query = """
                    SELECT p.sku, p.nome, p.categoria, p.status,
                           c.cod_fornecedor, c.preco_compra, c.embalagem, c.mdo, 
                           c.custo_ads, c.outros_custos,
                           (COALESCE(c.preco_compra, 0) + COALESCE(c.embalagem, 0) 
                            + COALESCE(c.mdo, 0) + COALESCE(c.custo_ads, 0) 
                            + COALESCE(c.outros_custos, 0)) as custo_final,
                           p.preco_a_ser_considerado,
                           p.margem_minima, p.margem_desejavel
                    FROM dim_produtos p
                    LEFT JOIN dim_produtos_custos c ON p.sku = c.sku
                    ORDER BY p.sku ASC
                """
            else:
                query = """
                    SELECT sku, nome, categoria, status, preco_a_ser_considerado
                    FROM dim_produtos
                    ORDER BY sku ASC
                """

            df = pd.read_sql(query, engine)

            if not df.empty:
                if busca:
                    mask = df['sku'].str.contains(busca, case=False, na=False) | \
                           df['nome'].str.contains(busca, case=False, na=False)
                    df_filtrado = df[mask]
                else:
                    df_filtrado = df

                st.write(f"Mostrando {len(df_filtrado)} de {len(df)} itens.")

                colunas_moeda = ['preco_compra', 'embalagem', 'mdo', 'custo_ads',
                                 'outros_custos', 'custo_final', 'preco_a_ser_considerado']
                colunas_pct = ['margem_minima', 'margem_desejavel']

                df_display = df_filtrado.copy()

                format_dict = {}
                for col in colunas_moeda:
                    if col in df_display.columns:
                        format_dict[col] = lambda x: formatar_valor_br(x)
                for col in colunas_pct:
                    if col in df_display.columns:
                        format_dict[col] = lambda x: f"{float(x or 0):,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")

                st.dataframe(
                    df_display.style.format(format_dict),
                    use_container_width=True,
                    hide_index=True
                )

                # BOTÃO DOWNLOAD EXCEL
                if col3.button("📥 Download Excel"):
                    df_excel = df_filtrado.copy()

                    for col in colunas_moeda:
                        if col in df_excel.columns:
                            df_excel[col] = df_excel[col].apply(formatar_valor_br)
                    for col in colunas_pct:
                        if col in df_excel.columns:
                            df_excel[col] = df_excel[col].apply(
                                lambda x: f"{float(x or 0):.1f}".replace(".", ",")
                            )

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
            with st.expander("➕ Cadastrar / Editar SKU", expanded=True):
                st.markdown("**Modo de Operação:**")
                modo = st.radio("", ["Cadastrar Novo SKU", "Editar SKU Existente"],
                                horizontal=True, key="modo_sku_operacao")

                # Inicializar valores padrão
                valores_padrao = {
                    'sku': '', 'nome': '', 'categoria': '', 'status': 'Ativo',
                    'cod_fornecedor': '',
                    'preco_compra': 0.0, 'embalagem': 0.0, 'mdo': 0.0,
                    'custo_ads': 0.0, 'outros_custos': 0.0,
                    'preco_a_ser_considerado': 0.0,
                    'margem_minima': 0.0, 'margem_desejavel': 0.0,
                    'largura': 0.0, 'comprimento': 0.0, 'altura': 0.0, 'peso_bruto': 0.0,
                }

                # Se modo edição, buscar SKU existente
                if modo == "Editar SKU Existente":
                    try:
                        df_skus = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
                        lista_skus = ["Selecione..."] + [
                            f"{row['sku']} - {row['nome']}" for _, row in df_skus.iterrows()
                        ]

                        sku_selecionado = st.selectbox("🔍 Selecione o SKU para editar:",
                                                       lista_skus, key="sel_sku_editar")

                        if sku_selecionado != "Selecione...":
                            sku_code = sku_selecionado.split(" - ")[0]

                            query_sku = text("""
                                SELECT p.sku, p.nome, p.categoria, p.status, 
                                       p.preco_a_ser_considerado, p.margem_minima, p.margem_desejavel,
                                       c.cod_fornecedor, c.preco_compra, c.embalagem, c.mdo, 
                                       c.custo_ads, c.outros_custos,
                                       p.largura, p.comprimento, p.altura, p.peso_bruto
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
                                        'margem_minima': float(result[5] or 0.0),
                                        'margem_desejavel': float(result[6] or 0.0),
                                        'cod_fornecedor': result[7] or '',
                                        'preco_compra': float(result[8] or 0.0),
                                        'embalagem': float(result[9] or 0.0),
                                        'mdo': float(result[10] or 0.0),
                                        'custo_ads': float(result[11] or 0.0),
                                        'outros_custos': float(result[12] or 0.0),
                                        'largura': float(result[13] or 0.0),
                                        'comprimento': float(result[14] or 0.0),
                                        'altura': float(result[15] or 0.0),
                                        'peso_bruto': float(result[16] or 0.0),
                                    }
                                    st.success(f"✅ SKU {sku_code} carregado para edição!")
                    except Exception as e:
                        st.error(f"Erro ao carregar SKUs: {e}")

                # FORMULÁRIO DE CADASTRO/EDIÇÃO
                # Usar key com counter para forçar limpeza após salvar
                form_counter = st.session_state.get('_form_sku_counter', 0)

                with st.form(f"form_sku_{form_counter}", clear_on_submit=False):
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
                    col5, col6, col7, col8, col9 = st.columns(5)

                    v_preco_compra = col5.text_input("Preço Compra (R$)",
                                                     value=formatar_valor_br(valores_padrao['preco_compra']))
                    v_embalagem = col6.text_input("Embalagem (R$)",
                                                  value=formatar_valor_br(valores_padrao['embalagem']))
                    v_mdo = col7.text_input("MDO (R$)",
                                           value=formatar_valor_br(valores_padrao['mdo']))
                    v_custo_ads = col8.text_input("Custo Ads (R$)",
                                                  value=formatar_valor_br(valores_padrao['custo_ads']))
                    v_outros_custos = col9.text_input("Outros Custos (R$)",
                                                      value=formatar_valor_br(valores_padrao['outros_custos']))

                    # Custo Final (informativo — soma calculada)
                    try:
                        custo_calc = (converter_valor_para_float(valores_padrao['preco_compra'])
                                      + converter_valor_para_float(valores_padrao['embalagem'])
                                      + converter_valor_para_float(valores_padrao['mdo'])
                                      + converter_valor_para_float(valores_padrao['custo_ads'])
                                      + converter_valor_para_float(valores_padrao['outros_custos']))
                    except:
                        custo_calc = 0.0

                    st.text_input("📊 Custo Final (soma calculada)",
                                  value=formatar_valor_br(custo_calc),
                                  disabled=True,
                                  help="Soma automática: Compra + Embalagem + MDO + Ads + Outros")

                    st.markdown("### Preço Final")
                    v_preco_final = st.text_input("Preço a Ser Considerado (R$)*",
                                                  value=formatar_valor_br(valores_padrao['preco_a_ser_considerado']))

                    st.info("💡 **Dica:** O preço a ser considerado pode ser editado manualmente "
                            "ou atualizado automaticamente pelo módulo de compras.")

                    st.markdown("### Margens de Referência")
                    col_mg1, col_mg2 = st.columns(2)
                    v_margem_min = col_mg1.text_input(
                        "Margem Mínima Aceitável (%)",
                        value=str(valores_padrao['margem_minima']).replace('.', ','),
                        help="Margem percentual mínima aceitável para este SKU"
                    )
                    v_margem_des = col_mg2.text_input(
                        "Margem Desejável (%)",
                        value=str(valores_padrao['margem_desejavel']).replace('.', ','),
                        help="Margem percentual desejável/ideal para este SKU"
                    )

                    st.markdown("### Dimensões e Peso")
                    st.caption("Usado no cálculo de frete (peso cubado) no módulo Tabela de Preço.")
                    col_d1, col_d2, col_d3, col_d4 = st.columns(4)
                    v_largura = col_d1.text_input(
                        "Largura (cm)",
                        value=str(valores_padrao['largura']).replace('.', ','),
                    )
                    v_comprimento = col_d2.text_input(
                        "Comprimento (cm)",
                        value=str(valores_padrao['comprimento']).replace('.', ','),
                    )
                    v_altura = col_d3.text_input(
                        "Altura (cm)",
                        value=str(valores_padrao['altura']).replace('.', ','),
                    )
                    v_peso_bruto = col_d4.text_input(
                        "Peso Bruto (kg)",
                        value=str(valores_padrao['peso_bruto']).replace('.', ','),
                    )

                    # BOTÃO SALVAR
                    submitted = st.form_submit_button("💾 Salvar Produto", type="primary")

                    if submitted:
                        if not v_sku or not v_nome:
                            st.error("❌ SKU e Nome são obrigatórios!")
                        else:
                            try:
                                preco_compra_float = converter_valor_para_float(v_preco_compra)
                                embalagem_float = converter_valor_para_float(v_embalagem)
                                mdo_float = converter_valor_para_float(v_mdo)
                                custo_ads_float = converter_valor_para_float(v_custo_ads)
                                outros_custos_float = converter_valor_para_float(v_outros_custos)
                                preco_final_float = converter_valor_para_float(v_preco_final)
                                margem_min_float = converter_valor_para_float(v_margem_min)
                                margem_des_float = converter_valor_para_float(v_margem_des)
                                largura_float = converter_valor_para_float(v_largura)
                                comprimento_float = converter_valor_para_float(v_comprimento)
                                altura_float = converter_valor_para_float(v_altura)
                                peso_bruto_float = converter_valor_para_float(v_peso_bruto)

                                with engine.connect() as conn:
                                    # 1. Inserir/Atualizar em dim_produtos
                                    query_produtos = text("""
                                        INSERT INTO dim_produtos 
                                            (sku, nome, categoria, status, preco_a_ser_considerado,
                                             margem_minima, margem_desejavel,
                                             largura, comprimento, altura, peso_bruto)
                                        VALUES (:sku, :nome, :categoria, :status, :preco,
                                                :margem_min, :margem_des,
                                                :largura, :comprimento, :altura, :peso_bruto)
                                        ON CONFLICT (sku) 
                                        DO UPDATE SET
                                            nome = EXCLUDED.nome,
                                            categoria = EXCLUDED.categoria,
                                            status = EXCLUDED.status,
                                            preco_a_ser_considerado = EXCLUDED.preco_a_ser_considerado,
                                            margem_minima = EXCLUDED.margem_minima,
                                            margem_desejavel = EXCLUDED.margem_desejavel,
                                            largura = EXCLUDED.largura,
                                            comprimento = EXCLUDED.comprimento,
                                            altura = EXCLUDED.altura,
                                            peso_bruto = EXCLUDED.peso_bruto
                                    """)

                                    conn.execute(query_produtos, {
                                        "sku": v_sku,
                                        "nome": v_nome,
                                        "categoria": v_categoria,
                                        "status": v_status,
                                        "preco": preco_final_float,
                                        "margem_min": margem_min_float,
                                        "margem_des": margem_des_float,
                                        "largura": largura_float,
                                        "comprimento": comprimento_float,
                                        "altura": altura_float,
                                        "peso_bruto": peso_bruto_float,
                                    })

                                    # 2. Inserir/Atualizar em dim_produtos_custos
                                    query_custos = text("""
                                        INSERT INTO dim_produtos_custos 
                                        (sku, cod_fornecedor, preco_compra, embalagem, mdo, 
                                         custo_ads, outros_custos)
                                        VALUES (:sku, :cod_forn, :preco_compra, :embalagem, :mdo, 
                                                :custo_ads, :outros_custos)
                                        ON CONFLICT (sku)
                                        DO UPDATE SET
                                            cod_fornecedor = EXCLUDED.cod_fornecedor,
                                            preco_compra = EXCLUDED.preco_compra,
                                            embalagem = EXCLUDED.embalagem,
                                            mdo = EXCLUDED.mdo,
                                            custo_ads = EXCLUDED.custo_ads,
                                            outros_custos = EXCLUDED.outros_custos
                                    """)

                                    conn.execute(query_custos, {
                                        "sku": v_sku,
                                        "cod_forn": v_cod_fornecedor,
                                        "preco_compra": preco_compra_float,
                                        "embalagem": embalagem_float,
                                        "mdo": mdo_float,
                                        "custo_ads": custo_ads_float,
                                        "outros_custos": outros_custos_float,
                                    })

                                    conn.commit()

                                st.success(f"✅ SKU {v_sku} salvo com sucesso!")
                                # Incrementar counter para forçar limpeza do formulário
                                st.session_state['_form_sku_counter'] = form_counter + 1
                                st.balloons()
                                st.rerun()

                            except Exception as e:
                                st.error(f"❌ Erro ao salvar: {e}")

            # ZONA DE PERIGO - EXCLUSÃO
            st.markdown("---")
            with st.expander("🗑️ ZONA DE PERIGO - Excluir SKU"):
                st.warning("⚠️ A exclusão é permanente e removerá também os custos associados.")

                try:
                    df_skus = pd.read_sql("SELECT sku, nome FROM dim_produtos ORDER BY sku", engine)
                    lista_delete = ["Selecione..."] + [
                        f"{row['sku']} - {row['nome']}" for _, row in df_skus.iterrows()
                    ]

                    sku_para_deletar = st.selectbox("Selecione o SKU para remover:", lista_delete)

                    if sku_para_deletar != "Selecione...":
                        sku_code_delete = sku_para_deletar.split(" - ")[0]
                        confirmar = st.checkbox(
                            f"✅ Confirmo que desejo excluir o SKU {sku_code_delete}")

                        if st.button("❌ EXCLUIR DEFINITIVAMENTE", type="primary"):
                            if confirmar:
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text(
                                            "DELETE FROM dim_produtos_custos WHERE sku = :s"),
                                            {"s": sku_code_delete})
                                        conn.execute(text(
                                            "DELETE FROM dim_produtos WHERE sku = :s"),
                                            {"s": sku_code_delete})
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
                        'outros_custos': ['0,00', '0,00'],
                        'preco_a_ser_considerado': ['17,38', '21,88'],
                        'margem_minima': ['10,0', '12,0'],
                        'margem_desejavel': ['25,0', '30,0'],
                        'largura': ['20,0', '15,0'],
                        'comprimento': ['30,0', '25,0'],
                        'altura': ['10,0', '8,0'],
                        'peso_bruto': ['0,50', '0,35'],
                    }

                    df_template = pd.DataFrame(template_data)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_template.to_excel(writer, index=False, sheet_name='Template SKUs')

                        worksheet = writer.sheets['Template SKUs']
                        for idx, col_name in enumerate(df_template.columns):
                            max_length = max(
                                df_template[col_name].astype(str).apply(len).max(),
                                len(col_name)
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
                st.info("Envie o arquivo Excel preenchido para importar em massa. "
                        "Colunas ausentes ou células vazias **preservam** o valor "
                        "cadastrado — só sobrescreve o que vier preenchido.")

                uploaded_file = st.file_uploader("Selecione o arquivo Excel",
                                                 type=['xlsx', 'xls'])

                if uploaded_file is not None:
                    try:
                        df_import = pd.read_excel(uploaded_file)

                        colunas_faltando = [c for c in COLUNAS_OBRIGATORIAS
                                            if c not in df_import.columns]

                        if colunas_faltando:
                            st.error(f"❌ Colunas obrigatórias faltando: {', '.join(colunas_faltando)}")
                        else:
                            registros, erros_parse = _preparar_registros(df_import)

                            if erros_parse:
                                st.error(f"❌ {len(erros_parse)} linha(s) com problema — corrija a planilha:")
                                st.dataframe(pd.DataFrame(erros_parse), use_container_width=True)

                            elif not registros:
                                st.warning("⚠️ Nenhuma linha válida encontrada no arquivo.")

                            else:
                                st.success(f"✅ Arquivo lido: {len(registros)} SKUs.")

                                colunas_no_arquivo = [c for c in TODOS_CAMPOS
                                                      if c in df_import.columns]
                                colunas_ausentes = [c for c in TODOS_CAMPOS
                                                    if c not in df_import.columns]
                                if colunas_ausentes:
                                    st.info("ℹ️ Colunas ausentes na planilha (serão preservadas no "
                                            f"banco): {', '.join(colunas_ausentes)}")

                                # ---- DIFF CONTRA O BANCO ----
                                estado_atual = _carregar_estado_atual(
                                    engine, [r['sku'] for r in registros])
                                diff = _montar_diff(registros, estado_atual)

                                _render_diff(diff, colunas_no_arquivo)

                                # ---- CIRCUIT BREAKER ----
                                bloqueio = _avaliar_risco(diff)

                                pode_importar = True
                                if bloqueio:
                                    st.error(
                                        f"🛑 **IMPORTAÇÃO BLOQUEADA POR SEGURANÇA**\n\n"
                                        f"{bloqueio['mensagem']}\n\n"
                                        f"Isso costuma indicar coluna trocada ou formato decimal "
                                        f"errado na planilha. Confira o diff acima antes de liberar."
                                    )
                                    frase = f"CONFIRMO REDUZIR CUSTO DE {bloqueio['n_quedas']} SKUS"
                                    st.markdown(
                                        f"Para liberar mesmo assim, digite exatamente: `{frase}`")
                                    digitado = st.text_input(
                                        "Confirmação de segurança",
                                        key="confirma_import_skus",
                                        label_visibility="collapsed")
                                    pode_importar = (digitado.strip() == frase)

                                if st.button("🚀 Importar Dados", type="primary",
                                             disabled=not pode_importar):
                                    _executar_importacao(engine, registros, colunas_no_arquivo)

                    except Exception as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")


if __name__ == "__main__":
    main()
