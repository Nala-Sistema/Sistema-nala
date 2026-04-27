"""
analise_ads_shopee.py — Módulo de Análise de Ads da Shopee
Sistema Nala — Parte 3A + 3B (completo)

Conteúdo:
  - Tab Upload: preview com banner destacado das datas detectadas
  - Tab Dashboard TACOS: query corrigida + tradução loja + dias pago/orgânico +
    botão "🤖 Gerar Insights com IA" (Gemini com 7 regras e faixas calibradas)
  - Tab Match SKU: data_editor inline + download/upload XLSX (a_preencher/matches_ativos)
  - Tab Histórico: preservado

Regras do projeto:
  - raw_connection() + cursor em TODAS as queries (nunca pd.read_sql com engine)
  - SAVEPOINT por linha em loops de INSERT/UPDATE
  - Formatação BR: R$ 1.234,56 | dd/mm/aaaa | 18,50%
  - Chave da IA: st.secrets["GEMINI_API_KEY"]
"""

import io
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from processar_ads_shopee import (
    processar_csv_ads_shopee, gravar_ads_shopee, extrair_metadados_csv,
    buscar_skus_match, atualizar_matches_sku, calcular_tacos,
    data_fim_efetiva, LOJA_ADS_PARA_ORIGEM
)

try:
    import google.generativeai as genai
    GEMINI_DISPONIVEL = True
except Exception:
    GEMINI_DISPONIVEL = False


# ============================================================
# CONSTANTES
# ============================================================

LOJAS_SHOPEE = {
    "Shopee Nala/Lithouse": "Nala-Lit",
    "Shopee Litstore (Yanni)": "litstoreshop",
    "Shopee LPT": "LPT Store",
}

MAX_SKUS_POR_ANUNCIO = 3  # número máximo de SKUs que um anúncio pode ter


# ============================================================
# HELPERS DE FORMATAÇÃO (padrão BR)
# ============================================================

def fmt_brl(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"{valor:.2f}%".replace(".", ",")


def fmt_int(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "N/A"
    return f"{int(valor):,}".replace(",", ".")


def fmt_data_br(d):
    if d is None or pd.isna(d):
        return "—"
    if isinstance(d, str):
        return d
    try:
        return d.strftime("%d/%m/%Y")
    except Exception:
        return str(d)


def cor_tacos(tacos):
    if tacos is None:
        return "⚫"
    if tacos <= 3:
        return "✅"
    elif tacos <= 5:
        return "🟡"
    elif tacos <= 10:
        return "🟠"
    return "🔴"


# ============================================================
# HELPERS DE BANCO (raw_connection sempre)
# ============================================================

def _query_df(engine, query, params=None):
    """Executa query e retorna DataFrame — USA raw_connection SEMPRE"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        colunas = [desc[0] for desc in cursor.description]
        dados = cursor.fetchall()
        return pd.DataFrame(dados, columns=colunas)
    except Exception as e:
        st.error(f"Erro na query: {str(e)[:200]}")
        return pd.DataFrame()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _query_scalar(engine, query, params=None):
    """Executa query e retorna valor único — USA raw_connection SEMPRE"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        st.error(f"Erro na query: {str(e)[:200]}")
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# HELPERS DE XLSX (download/upload de matches)
# ============================================================

def _gerar_xlsx_matches(engine, loja_nome):
    """
    Gera XLSX com 2 abas:
      - 'a_preencher'     → anúncios SEM match ativo hoje (usuário preenche SKUs)
      - 'matches_ativos'  → todos os matches vigentes hoje (editáveis)

    Retorna: bytes do xlsx (para st.download_button)
    """
    hoje = date.today()

    # Anúncios totais da loja
    df_anuncios = _query_df(engine, """
        SELECT DISTINCT nome_anuncio, id_produto
        FROM fact_ads_shopee
        WHERE loja = %s
        ORDER BY nome_anuncio
    """, [loja_nome])

    # Matches ativos hoje
    df_ativos = _query_df(engine, """
        SELECT nome_produto_ads, id_produto_ads, sku, data_inicio
        FROM dim_ads_produto_sku
        WHERE marketplace = 'Shopee'
          AND loja = %s
          AND data_inicio <= %s
          AND (data_fim IS NULL OR data_fim >= %s)
        ORDER BY nome_produto_ads, data_inicio DESC, sku
    """, [loja_nome, hoje, hoje])

    # Conjunto de anúncios que já têm ao menos 1 match ativo
    anuncios_com_match = set(df_ativos['nome_produto_ads'].tolist()) if not df_ativos.empty else set()

    # ---- ABA 1: a_preencher ----
    linhas_preencher = []
    for _, r in df_anuncios.iterrows():
        if r['nome_anuncio'] not in anuncios_com_match:
            linhas_preencher.append({
                'nome_anuncio': r['nome_anuncio'],
                'id_produto': str(r.get('id_produto', '') or ''),
                'sku_1': '',
                'sku_2': '',
                'sku_3': '',
            })
    df_preencher = pd.DataFrame(linhas_preencher) if linhas_preencher else pd.DataFrame(
        columns=['nome_anuncio', 'id_produto', 'sku_1', 'sku_2', 'sku_3']
    )

    # ---- ABA 2: matches_ativos ----
    # Agrupar por anúncio: até MAX_SKUS_POR_ANUNCIO colunas de SKU
    linhas_ativos = []
    if not df_ativos.empty:
        agrupado = df_ativos.groupby('nome_produto_ads', sort=False)
        for nome, grupo in agrupado:
            id_p = grupo['id_produto_ads'].iloc[0] if 'id_produto_ads' in grupo.columns else ''
            skus = grupo['sku'].tolist()[:MAX_SKUS_POR_ANUNCIO]
            data_inicio_min = grupo['data_inicio'].min()
            linha = {
                'nome_anuncio': nome,
                'id_produto': str(id_p or ''),
                'sku_atual_1': skus[0] if len(skus) >= 1 else '',
                'sku_atual_2': skus[1] if len(skus) >= 2 else '',
                'sku_atual_3': skus[2] if len(skus) >= 3 else '',
                'novo_sku_1': '',
                'novo_sku_2': '',
                'novo_sku_3': '',
                'data_inicio_mais_antiga': fmt_data_br(data_inicio_min),
            }
            linhas_ativos.append(linha)
    df_ativos_export = pd.DataFrame(linhas_ativos) if linhas_ativos else pd.DataFrame(columns=[
        'nome_anuncio', 'id_produto',
        'sku_atual_1', 'sku_atual_2', 'sku_atual_3',
        'novo_sku_1', 'novo_sku_2', 'novo_sku_3',
        'data_inicio_mais_antiga'
    ])

    # ---- Montar XLSX em memória ----
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_preencher.to_excel(writer, sheet_name='a_preencher', index=False)
        df_ativos_export.to_excel(writer, sheet_name='matches_ativos', index=False)

        # Aba de instruções
        instrucoes = pd.DataFrame({
            'Instruções de uso': [
                '1. Aba "a_preencher": anúncios SEM SKU vinculado. Preencha sku_1, sku_2, sku_3.',
                '2. Aba "matches_ativos": anúncios COM SKU vinculado hoje.',
                '   Para ALTERAR os SKUs de um anúncio, preencha novo_sku_1/2/3.',
                '   - Se preencher, o sistema FECHA os SKUs atuais em CURRENT_DATE',
                '     e ABRE novos registros com data_inicio = CURRENT_DATE.',
                '   - Se deixar novo_sku_1/2/3 em branco, MANTÉM como está.',
                '3. Não altere as colunas nome_anuncio nem id_produto.',
                '4. Salve o arquivo e faça upload na tela "Upload de matches".',
                '5. Deixe campos em branco para remover um SKU (quando aplicável).',
            ]
        })
        instrucoes.to_excel(writer, sheet_name='instrucoes', index=False)

    output.seek(0)
    return output.getvalue()


def _processar_upload_xlsx_matches(engine, loja_nome, arquivo_xlsx):
    """
    Lê o XLSX enviado pelo usuário e aplica as mudanças em dim_ads_produto_sku.

    Regras:
      - Aba 'a_preencher': para cada linha com sku_1/2/3 preenchidos, cria match
        novo com data_inicio = CURRENT_DATE.
      - Aba 'matches_ativos': para cada linha com novo_sku_1/2/3 preenchidos,
        fecha os atuais (data_fim = CURRENT_DATE) e abre novos
        (data_inicio = CURRENT_DATE). Se novo_sku_* em branco, ignora.

    Retorna: (adicionados, alterados, ignorados, erros)
    """
    try:
        xls = pd.ExcelFile(arquivo_xlsx)
    except Exception as e:
        return 0, 0, 0, [f"Não foi possível abrir o XLSX: {str(e)[:100]}"]

    adicionados = 0
    alterados = 0
    ignorados = 0
    erros = []

    def _coletar_skus(row, prefix):
        """Coleta sku_prefix_1..3 limpos, únicos, em ordem"""
        lista = []
        seen = set()
        for i in (1, 2, 3):
            col = f'{prefix}_{i}'
            if col not in row.index:
                continue
            val = row[col]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            txt = str(val).strip()
            if not txt or txt.lower() in ('nan', 'none'):
                continue
            if txt not in seen:
                seen.add(txt)
                lista.append(txt)
        return lista

    # ---- ABA a_preencher ----
    if 'a_preencher' in xls.sheet_names:
        df_ap = pd.read_excel(xls, sheet_name='a_preencher')
        for idx, row in df_ap.iterrows():
            nome = str(row.get('nome_anuncio', '') or '').strip()
            id_prod = str(row.get('id_produto', '') or '').strip()
            if not nome:
                continue
            skus_novos = _coletar_skus(row, 'sku')
            if not skus_novos:
                ignorados += 1
                continue
            ok = atualizar_matches_sku(engine, loja_nome, nome, id_prod, skus_novos)
            if ok:
                _sync_fact_sku_match(engine, loja_nome, nome, skus_novos[0])
                adicionados += 1
            else:
                erros.append(f"[a_preencher] {nome[:40]}: falha ao gravar")

    # ---- ABA matches_ativos ----
    if 'matches_ativos' in xls.sheet_names:
        df_ma = pd.read_excel(xls, sheet_name='matches_ativos')
        for idx, row in df_ma.iterrows():
            nome = str(row.get('nome_anuncio', '') or '').strip()
            id_prod = str(row.get('id_produto', '') or '').strip()
            if not nome:
                continue
            novos = _coletar_skus(row, 'novo_sku')
            if not novos:
                ignorados += 1
                continue
            atuais = _coletar_skus(row, 'sku_atual')
            if sorted(atuais) == sorted(novos):
                ignorados += 1
                continue
            ok = atualizar_matches_sku(engine, loja_nome, nome, id_prod, novos)
            if ok:
                _sync_fact_sku_match(engine, loja_nome, nome, novos[0])
                alterados += 1
            else:
                erros.append(f"[matches_ativos] {nome[:40]}: falha ao gravar")

    return adicionados, alterados, ignorados, erros


# ============================================================
# HELPERS DE IA (Gemini)
# ============================================================

# Faixas calibradas com dados reais dez/2025–abr/2026
_FAIXAS_PROMPT = """
**Faixas ACOS:**
- 0–8%: Excelente → manter, considerar aumentar budget se TACOS bom
- 8–15%: Saudável → manter e monitorar
- 15–25%: Alerta → avaliar % orgânico; se > 50%, pode ser aceitável
- 25–50%: Crítico → reduzir budget ou pausar
- >50%: Drenar → pausar imediatamente (exceto Fase 1+2 com prazo)

**Faixas TACOS:**
- 0–3%: Excelente
- 3–8%: Saudável
- 8–15%: Elevado → avaliar dependência
- >15%: Insustentável

**Faixas CVR:**
- >5%: Excelente
- 2–5%: Normal
- 1–2%: Baixo → verificar preço/fotos/reviews
- <1%: Problema grave → pausar ads, otimizar página

**Faixas % Orgânico:**
- >70%: Forte
- 40–70%: Equilibrado
- 10–40%: Dependente
- <10%: Viciado em ads

**7 Regras de decisão (em ordem de prioridade):**
1. ENCERRAR: investimento > R$30 E conversões = 0 E dias ativos > 7 → PAUSAR
2. DRENAGEM: ACOS > 25% E % orgânico < 30% → ALERTA VERMELHO
3. FASE 1+2 ACEITÁVEL SE: TACOS < 8% E % orgânico > 60%
4. CANIBALIZAÇÃO: % orgânico atual < % orgânico baseline − 30pp E TACOS subiu
5. CVR CRÍTICO: CVR < 0,5% E cliques > 500 → problema de página
6. ESTRELA: ACOS < 10% E TACOS < 5% E % orgânico > 40% → aumentar budget 10–20%
7. ORGÂNICO PURO: receita > R$500/quinzena E investimento = 0 → sinalizar como ativo
"""


def _coletar_dados_para_ia(engine, loja_nome, dt_inicio, dt_fim):
    """
    Retorna lista de dicts (um por anúncio) com as métricas necessárias para a IA.
    """
    df_ads = _query_df(engine, """
        SELECT nome_anuncio, id_produto, status_anuncio,
               data_inicio_anuncio, data_fim_anuncio,
               metodo_lance, impressoes, cliques, ctr,
               conversoes_diretas, taxa_conversao_direta,
               itens_vendidos_diretos, receita_direta,
               despesas, gmv, acos_direto, roas_direto,
               sku_match
        FROM fact_ads_shopee
        WHERE loja = %s
          AND periodo_inicio = %s
          AND periodo_fim = %s
        ORDER BY despesas DESC
    """, [loja_nome, dt_inicio, dt_fim])

    if df_ads.empty:
        return []

    dados = []
    for _, ad in df_ads.iterrows():
        lista_skus = buscar_skus_match(engine, loja_nome, ad['nome_anuncio'])
        tacos_data = None
        if lista_skus:
            tacos_data = calcular_tacos(engine, loja_nome, lista_skus, dt_inicio, dt_fim)
            if tacos_data and 'erro' in tacos_data:
                tacos_data = None

        # Dias ads efetivos
        fim_efetivo = data_fim_efetiva(ad.get('data_fim_anuncio'), dt_fim)
        inicio = ad.get('data_inicio_anuncio')
        if inicio is not None and not pd.isna(inicio):
            inicio_efetivo = max(inicio, dt_inicio)
        else:
            inicio_efetivo = dt_inicio
        if fim_efetivo and inicio_efetivo and fim_efetivo >= inicio_efetivo:
            dias_ads = (fim_efetivo - inicio_efetivo).days + 1
        else:
            dias_ads = 0

        dados.append({
            'produto': str(ad['nome_anuncio'])[:80],
            'skus_vinculados': ', '.join(lista_skus) if lista_skus else 'NÃO VINCULADO',
            'status': str(ad.get('status_anuncio', '')),
            'metodo_lance': str(ad.get('metodo_lance', '')),
            'investimento': float(ad['despesas'] or 0),
            'gmv_painel': float(ad['gmv'] or 0),
            'receita_direta': float(ad['receita_direta'] or 0),
            'acos_direto_pct': float(ad['acos_direto'] or 0),
            'roas_direto': float(ad['roas_direto'] or 0),
            'cliques': int(ad['cliques'] or 0),
            'impressoes': int(ad['impressoes'] or 0),
            'ctr_pct': float(ad['ctr'] or 0),
            'conversoes_diretas': int(ad['conversoes_diretas'] or 0),
            'cvr_direta_pct': float(ad['taxa_conversao_direta'] or 0),
            'itens_vendidos_diretos': int(ad['itens_vendidos_diretos'] or 0),
            'dias_ads_efetivos': dias_ads,
            'tacos_pct': tacos_data['tacos'] if tacos_data else None,
            'pct_organico': tacos_data['pct_organico'] if tacos_data else None,
            'receita_total_sku': tacos_data['receita_total'] if tacos_data else None,
            'qtd_total_sku': tacos_data['qtd_total'] if tacos_data else None,
        })
    return dados


def _montar_prompt_ia(loja_nome, dt_inicio, dt_fim, dados_periodo, dt_ini_comp, dt_fim_comp, dados_comp):
    """Monta o prompt completo para a IA analista."""
    linhas = [
        "Você é um Senior Market Intelligence Auditor especialista em Shopee Ads.",
        "Analise os dados da loja **{loja}** para o período **{di} a {df}**.".format(
            loja=loja_nome,
            di=fmt_data_br(dt_inicio), df=fmt_data_br(dt_fim)
        ),
        "",
        "Responda em **PORTUGUÊS DO BRASIL** com a seguinte estrutura:",
        "1. **Resumo geral da loja** (3–5 linhas)",
        "2. **Ranking por TACOS** (pior → melhor) — use bullets",
        "3. **Lista de ações** (Pausar / Reduzir / Manter / Escalar) — um bullet por anúncio com ação clara",
        "4. **Comparação com período anterior** (se fornecido abaixo)",
        "",
        "Use formatação em markdown. Valores em R$ com formato BR (R$ 1.234,56) e percentuais com vírgula (13,57%).",
        "",
        "---",
        _FAIXAS_PROMPT,
        "",
        "---",
        "### Dados do período analisado",
        "",
    ]
    if not dados_periodo:
        linhas.append("_(sem dados no período)_")
    else:
        for i, d in enumerate(dados_periodo, 1):
            linhas.append(
                f"**{i}. {d['produto']}** | SKU(s): {d['skus_vinculados']} | Status: {d['status']}\n"
                f"   - Método: {d['metodo_lance']} | Dias ads efetivos: {d['dias_ads_efetivos']}\n"
                f"   - Investimento: R$ {d['investimento']:.2f} | GMV painel: R$ {d['gmv_painel']:.2f} "
                f"| Receita direta: R$ {d['receita_direta']:.2f}\n"
                f"   - ACOS direto: {d['acos_direto_pct']:.2f}% | ROAS direto: {d['roas_direto']:.2f}\n"
                f"   - Impressões: {d['impressoes']} | Cliques: {d['cliques']} | CTR: {d['ctr_pct']:.2f}%\n"
                f"   - Conversões diretas: {d['conversoes_diretas']} | CVR direta: {d['cvr_direta_pct']:.2f}%\n"
                f"   - Itens vendidos diretos: {d['itens_vendidos_diretos']}\n"
                f"   - TACOS calculado: {('%.2f%%' % d['tacos_pct']) if d['tacos_pct'] is not None else 'N/A (sem SKU vinculado)'}\n"
                f"   - % Orgânico: {('%.2f%%' % d['pct_organico']) if d['pct_organico'] is not None else 'N/A'}\n"
                f"   - Receita total SKU: {('R$ %.2f' % d['receita_total_sku']) if d['receita_total_sku'] is not None else 'N/A'}\n"
            )

    if dados_comp is not None and dt_ini_comp and dt_fim_comp:
        linhas.append("")
        linhas.append("---")
        linhas.append(
            f"### Dados do período anterior para comparação "
            f"({fmt_data_br(dt_ini_comp)} a {fmt_data_br(dt_fim_comp)})"
        )
        linhas.append("")
        if not dados_comp:
            linhas.append("_(sem dados no período de comparação)_")
        else:
            for i, d in enumerate(dados_comp, 1):
                linhas.append(
                    f"**{i}. {d['produto']}** — Invest: R$ {d['investimento']:.2f} | "
                    f"ACOS: {d['acos_direto_pct']:.2f}% | "
                    f"TACOS: {('%.2f%%' % d['tacos_pct']) if d['tacos_pct'] is not None else 'N/A'} | "
                    f"% Orgânico: {('%.2f%%' % d['pct_organico']) if d['pct_organico'] is not None else 'N/A'} | "
                    f"Receita direta: R$ {d['receita_direta']:.2f}"
                )

    return "\n".join(linhas)


def _chamar_gemini(prompt):
    """Chama o Gemini e retorna o texto da resposta, ou mensagem de erro."""
    if not GEMINI_DISPONIVEL:
        return "❌ Biblioteca google-generativeai não está instalada."
    try:
        api_key = st.secrets.get("GEMINI_API_KEY", None)
    except Exception:
        api_key = None
    if not api_key:
        return "❌ `GEMINI_API_KEY` não configurada em `st.secrets`."
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"❌ Erro ao chamar o Gemini: {str(e)[:300]}"


# ============================================================
# ENTRY POINT DO MÓDULO
# ============================================================

def modulo_ads_shopee(engine):
    """Ponto de entrada do módulo Shopee Ads (chamado pelo orquestrador)."""
    subtab_upload, subtab_dashboard, subtab_match, subtab_historico = st.tabs([
        "📤 Upload", "📈 Dashboard TACOS", "🔗 Match SKU", "📋 Histórico"
    ])
    with subtab_upload:
        _shopee_upload(engine)
    with subtab_dashboard:
        _shopee_dashboard(engine)
    with subtab_match:
        _shopee_match_sku(engine)
    with subtab_historico:
        _shopee_historico(engine)


# ============================================================
# TAB 1: UPLOAD
# ============================================================

def _shopee_upload(engine):
    st.subheader("Upload de Relatório de Ads")
    st.caption("Importe CSVs exportados da Central de Marketing da Shopee.")

    col1, col2 = st.columns(2)
    with col1:
        loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="ads_loja")
    with col2:
        tipo = st.selectbox("Tipo de relatório", [
            "Geral (Todos os Anúncios)", "Grupo de Anúncios", "Produto Individual"
        ], key="ads_tipo")

    arquivos = st.file_uploader(
        "Selecione o(s) CSV(s) de ads",
        type=['csv'], accept_multiple_files=True, key="ads_files"
    )

    if arquivos and st.button("🔍 ANALISAR", key="ads_analisar", type="primary"):
        nomes_arquivos = []
        for arquivo in arquivos:
            st.markdown(f"**📄 Arquivo:** `{arquivo.name}`")
            loja_nome = LOJAS_SHOPEE[loja]
            df, meta = processar_csv_ads_shopee(arquivo, loja_override=loja_nome)
            if df is None:
                st.error(f"❌ {meta}")
                continue

            # Banner destacado com o período do relatório
            pi = meta.get('periodo_inicio')
            pf = meta.get('periodo_fim')
            if pi and pf:
                dias = (pf - pi).days + 1
                st.success(
                    f"📅 **Período do relatório: {fmt_data_br(pi)} a {fmt_data_br(pf)}** "
                    f"({dias} dias)  —  Confira antes de gravar!"
                )
            else:
                st.warning("⚠️ Período do relatório não detectado — verifique o CSV antes de gravar.")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Tipo", meta.get('tipo_relatorio', '?'))
            c2.metric("Registros", meta.get('total_registros', 0))
            c3.metric("Investimento", fmt_brl(meta.get('total_despesas', 0)))
            c4.metric("GMV Ads", fmt_brl(meta.get('total_gmv', 0)))
            if meta.get('nome_grupo'):
                st.caption(f"Grupo: {meta['nome_grupo']}")

            # Preview com as datas de vigência de cada anúncio
            preview_cols = [
                'nome_anuncio', 'status_anuncio',
                'data_inicio_anuncio', 'data_fim_anuncio',
                'despesas', 'gmv', 'acos_direto',
                'itens_vendidos_diretos', 'metodo_lance'
            ]
            preview_cols = [c for c in preview_cols if c in df.columns]
            preview = df[preview_cols].copy()
            if 'nome_anuncio' in preview.columns:
                preview['nome_anuncio'] = preview['nome_anuncio'].str[:60]
            if 'data_inicio_anuncio' in preview.columns:
                preview['data_inicio_anuncio'] = preview['data_inicio_anuncio'].apply(fmt_data_br)
            if 'data_fim_anuncio' in preview.columns:
                preview['data_fim_anuncio'] = preview['data_fim_anuncio'].apply(
                    lambda x: fmt_data_br(x) if x is not None and not pd.isna(x) else "Ilimitado"
                )
            if 'despesas' in preview.columns:
                preview['despesas'] = preview['despesas'].apply(fmt_brl)
            if 'gmv' in preview.columns:
                preview['gmv'] = preview['gmv'].apply(fmt_brl)
            if 'acos_direto' in preview.columns:
                preview['acos_direto'] = preview['acos_direto'].apply(fmt_pct)

            preview.columns = [
                {
                    'nome_anuncio': 'Anúncio',
                    'status_anuncio': 'Status',
                    'data_inicio_anuncio': 'Início',
                    'data_fim_anuncio': 'Encerramento',
                    'despesas': 'Investimento',
                    'gmv': 'GMV',
                    'acos_direto': 'ACOS Direto',
                    'itens_vendidos_diretos': 'Itens Diretos',
                    'metodo_lance': 'Método',
                }.get(c, c) for c in preview.columns
            ]

            st.dataframe(preview, use_container_width=True, hide_index=True)

            st.session_state[f'ads_df_{arquivo.name}'] = df
            st.session_state[f'ads_meta_{arquivo.name}'] = meta
            nomes_arquivos.append(arquivo.name)
        st.session_state['ads_arquivos_analisados'] = nomes_arquivos

    # Bloco de gravação
    nomes_pendentes = st.session_state.get('ads_arquivos_analisados', [])
    if nomes_pendentes:
        st.divider()
        st.info(f"📄 {len(nomes_pendentes)} arquivo(s) analisado(s) aguardando gravação.")
        if st.button("💾 GRAVAR NO BANCO", key="ads_gravar", type="primary"):
            for nome_arq in nomes_pendentes:
                key_df = f'ads_df_{nome_arq}'
                key_meta = f'ads_meta_{nome_arq}'
                if key_df not in st.session_state:
                    continue
                df = st.session_state[key_df]
                meta = st.session_state[key_meta]
                with st.spinner(f"Gravando {nome_arq}..."):
                    gravados, erros, duplicatas = gravar_ads_shopee(df, nome_arq, engine)
                    _auto_match_skus(engine, df)
                    _registrar_log_ads(engine, meta, nome_arq, gravados, len(erros))
                if gravados > 0:
                    st.success(f"✅ `{nome_arq}` — {gravados} registros gravados")
                if duplicatas > 0:
                    st.info(f"ℹ️ {duplicatas} registros atualizados (já existentes)")
                if erros:
                    st.warning(f"⚠️ {len(erros)} erros:")
                    for e in erros[:5]:
                        st.caption(e)
            for nome_arq in nomes_pendentes:
                st.session_state.pop(f'ads_df_{nome_arq}', None)
                st.session_state.pop(f'ads_meta_{nome_arq}', None)
            st.session_state.pop('ads_arquivos_analisados', None)


def _auto_match_skus(engine, df_ads):
    """Ao gravar ads, tenta preencher sku_match com o primeiro SKU vigente hoje"""
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        for _, row in df_ads.iterrows():
            lista_skus = buscar_skus_match(engine, row['loja'], row['nome_anuncio'])
            if lista_skus:
                # grava o primeiro SKU (compatibilidade com o schema atual de fact_ads_shopee)
                cursor.execute("""
                    UPDATE fact_ads_shopee SET sku_match = %s, match_confirmado = TRUE
                    WHERE loja = %s AND nome_anuncio = %s AND (sku_match IS NULL OR sku_match = '')
                """, (lista_skus[0], row['loja'], row['nome_anuncio']))
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _registrar_log_ads(engine, meta, arquivo_nome, gravados, erros):
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO log_uploads_ads (
                marketplace, loja, tipo_relatorio, arquivo_nome,
                periodo_inicio, periodo_fim,
                total_linhas, linhas_importadas, linhas_erro
            )
            VALUES ('Shopee', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            meta.get('loja', ''), meta.get('tipo_relatorio', ''), arquivo_nome,
            meta.get('periodo_inicio'), meta.get('periodo_fim'),
            meta.get('total_registros', 0), gravados, erros
        ))
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# TAB 2: DASHBOARD TACOS
# ============================================================

def _shopee_dashboard(engine):
    st.subheader("Dashboard TACOS")
    st.caption("Meta TACOS: máximo 3% | 🟢 até 3% • 🟡 3–5% • 🟠 5–10% • 🔴 acima de 10%")

    # Dropdown de loja
    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="dash_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    # Dropdown de períodos já importados (mais prático que date_input manual)
    df_periodos = _query_df(engine, """
        SELECT DISTINCT periodo_inicio, periodo_fim,
               COUNT(*) AS anuncios,
               ROUND(SUM(despesas)::numeric, 2) AS invest
        FROM fact_ads_shopee
        WHERE loja = %s
        GROUP BY periodo_inicio, periodo_fim
        ORDER BY periodo_fim DESC, periodo_inicio DESC
    """, [loja_nome])

    if df_periodos.empty:
        st.warning("Nenhum relatório de ads importado para esta loja ainda.")
        st.caption("Importe primeiro um CSV na aba Upload.")
        return

    opcoes_periodos = []
    for _, r in df_periodos.iterrows():
        pi = r['periodo_inicio']
        pf = r['periodo_fim']
        label = (
            f"{fmt_data_br(pi)} a {fmt_data_br(pf)}  "
            f"— {int(r['anuncios'])} anúncios | {fmt_brl(float(r['invest']))}"
        )
        opcoes_periodos.append((label, pi, pf))

    sel = st.selectbox(
        "Período do relatório",
        options=range(len(opcoes_periodos)),
        format_func=lambda i: opcoes_periodos[i][0],
        key="dash_periodo"
    )
    _, dt_inicio, dt_fim = opcoes_periodos[sel]

    if not st.button("📊 CALCULAR", key="dash_calc", type="primary"):
        return

    with st.spinner("Calculando TACOS..."):
        # Ads do período
        df_ads = _query_df(engine, """
            SELECT nome_anuncio, id_produto, status_anuncio,
                   data_inicio_anuncio, data_fim_anuncio,
                   metodo_lance, impressoes, cliques, ctr,
                   conversoes_diretas, taxa_conversao_direta,
                   custo_por_conversao_direta,
                   itens_vendidos_diretos, receita_direta,
                   despesas, gmv, acos_direto, roas_direto,
                   sku_match
            FROM fact_ads_shopee
            WHERE loja = %s
              AND periodo_inicio = %s
              AND periodo_fim = %s
            ORDER BY despesas DESC
        """, [loja_nome, dt_inicio, dt_fim])

        if df_ads.empty:
            st.warning("Nenhum dado de ads encontrado para este período/loja.")
            return

        # Receita total da loja no período — QUERY CORRIGIDA
        # - marketplace_origem = 'SHOPEE' (MAIÚSCULAS, como está no banco)
        # - loja_origem traduzida via LOJA_ADS_PARA_ORIGEM
        loja_origem = LOJA_ADS_PARA_ORIGEM.get(loja_nome, loja_nome)

        vendas_totais = _query_scalar(engine, """
            SELECT COALESCE(SUM(valor_venda_efetivo), 0)
            FROM fact_vendas_snapshot
            WHERE UPPER(marketplace_origem) = 'SHOPEE'
              AND loja_origem = %s
              AND data_venda BETWEEN %s AND %s
        """, [loja_origem, dt_inicio, dt_fim]) or 0
        vendas_totais = float(vendas_totais)

        invest_total = float(df_ads['despesas'].sum())
        gmv_total = float(df_ads['gmv'].sum())
        tacos_loja = (invest_total / vendas_totais * 100) if vendas_totais > 0 else None
        acos_loja = (invest_total / gmv_total * 100) if gmv_total > 0 else None

        st.markdown("### Indicadores da Loja")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Receita Total", fmt_brl(vendas_totais))
        c2.metric("Investimento Ads", fmt_brl(invest_total))
        c3.metric("TACOS Loja", fmt_pct(tacos_loja))
        c4.metric("ACOS Painel", fmt_pct(acos_loja))
        c5.metric("Anúncios", len(df_ads))

        if vendas_totais == 0:
            st.warning(
                f"⚠️ Nenhuma venda encontrada em `fact_vendas_snapshot` para "
                f"`loja_origem = '{loja_origem}'` entre {fmt_data_br(dt_inicio)} e {fmt_data_br(dt_fim)}. "
                f"Confira se os uploads de vendas Shopee cobrem este período."
            )

        st.markdown("### TACOS por Produto")
        st.caption(
            "Produtos com SKU vinculado mostram TACOS real. Sem SKU = apenas ACOS direto do painel.  "
            "**Dias Ads** = até a menor data entre encerramento do anúncio e fim do relatório."
        )

        resultados = []
        for _, ad in df_ads.iterrows():
            # Busca TODOS os SKUs vigentes hoje para este anúncio (match múltiplo)
            lista_skus = buscar_skus_match(engine, loja_nome, ad['nome_anuncio'])

            # Data efetiva de fim do anúncio (MIN entre encerramento e fim do relatório)
            data_fim_anuncio = ad.get('data_fim_anuncio')
            fim_efetivo = data_fim_efetiva(data_fim_anuncio, dt_fim)

            # Dias em que o anúncio rodou dentro do relatório
            inicio_anuncio = ad.get('data_inicio_anuncio')
            if inicio_anuncio is not None and not pd.isna(inicio_anuncio):
                inicio_efetivo = max(inicio_anuncio, dt_inicio)
            else:
                inicio_efetivo = dt_inicio

            if fim_efetivo and inicio_efetivo and fim_efetivo >= inicio_efetivo:
                dias_ads = (fim_efetivo - inicio_efetivo).days + 1
            else:
                dias_ads = 0

            dias_relatorio = (dt_fim - dt_inicio).days + 1
            dias_organico = max(0, dias_relatorio - dias_ads)

            # Cálculo TACOS usando a lista de SKUs (pode ser múltipla)
            tacos_data = None
            if lista_skus:
                tacos_data = calcular_tacos(engine, loja_nome, lista_skus, dt_inicio, dt_fim)
                if tacos_data and 'erro' in tacos_data:
                    tacos_data = None

            sku_display = ", ".join(lista_skus) if lista_skus else "❌ não vinculado"

            resultados.append({
                'Status': cor_tacos(tacos_data['tacos'] if tacos_data else None),
                'Produto': str(ad['nome_anuncio'])[:45],
                'Investimento': float(ad['despesas']),
                'ACOS Direto': float(ad['acos_direto']) if ad['acos_direto'] is not None else 0.0,
                'TACOS': tacos_data['tacos'] if tacos_data else None,
                'Dias Ads': dias_ads,
                'Dias Orgânico': dias_organico,
                'Itens Diretos': int(ad['itens_vendidos_diretos']) if ad['itens_vendidos_diretos'] is not None else 0,
                'Itens Total': tacos_data['qtd_total'] if tacos_data else None,
                '% Orgânico': tacos_data['pct_organico'] if tacos_data else None,
                'Receita Total': tacos_data['receita_total'] if tacos_data else None,
                'Método': str(ad.get('metodo_lance', ''))[:25],
                'SKU(s)': sku_display,
            })

        df_res = pd.DataFrame(resultados).sort_values('Investimento', ascending=False)
        df_exib = df_res.copy()
        df_exib['Investimento'] = df_exib['Investimento'].apply(fmt_brl)
        df_exib['ACOS Direto'] = df_exib['ACOS Direto'].apply(fmt_pct)
        df_exib['TACOS'] = df_exib['TACOS'].apply(fmt_pct)
        df_exib['% Orgânico'] = df_exib['% Orgânico'].apply(fmt_pct)
        df_exib['Receita Total'] = df_exib['Receita Total'].apply(fmt_brl)
        st.dataframe(df_exib, use_container_width=True, hide_index=True)

        # Alertas automáticos
        st.markdown("### ⚠️ Alertas")
        tem_alerta = False
        for _, r in df_res.iterrows():
            if r['TACOS'] is not None and r['TACOS'] > 10:
                st.error(f"🔴 **{r['Produto']}** — TACOS {fmt_pct(r['TACOS'])}. Ação necessária!")
                tem_alerta = True
            elif (r['ACOS Direto'] or 0) > 50 and r['Investimento'] > 10:
                st.warning(
                    f"⚠️ **{r['Produto']}** — ACOS Direto {fmt_pct(r['ACOS Direto'])}. "
                    f"Vincule SKU para calcular TACOS."
                )
                tem_alerta = True
        if not tem_alerta:
            st.success("Nenhum alerta crítico no período.")

        # Persistir contexto do último cálculo para o bloco da IA
        st.session_state['dash_ads_ultimo_calc'] = {
            'loja_nome': loja_nome,
            'dt_inicio': dt_inicio,
            'dt_fim': dt_fim,
        }

    # ---- BLOCO DA IA (fora do "if CALCULAR", mas dependente do último cálculo) ----
    ultimo = st.session_state.get('dash_ads_ultimo_calc')
    if ultimo and ultimo.get('loja_nome') == loja_nome and ultimo.get('dt_inicio') == dt_inicio and ultimo.get('dt_fim') == dt_fim:
        st.divider()
        _bloco_ia_insights(engine, loja_nome, dt_inicio, dt_fim)


def _bloco_ia_insights(engine, loja_nome, dt_inicio, dt_fim):
    """Renderiza o botão e painel de insights da IA."""
    st.markdown("### 🤖 Insights com IA")
    st.caption(
        "Análise automática com Gemini usando as 7 regras de decisão calibradas. "
        "Opcionalmente, escolha um período anterior para comparação."
    )

    # Sugestão padrão de período anterior: mesma janela imediatamente antes
    dias_janela = (dt_fim - dt_inicio).days + 1
    sug_fim = dt_inicio - timedelta(days=1)
    sug_ini = sug_fim - timedelta(days=dias_janela - 1)

    comparar = st.checkbox(
        "🔁 Comparar com período anterior",
        value=False,
        key="ia_comparar",
        help=f"Sugestão: {fmt_data_br(sug_ini)} a {fmt_data_br(sug_fim)} (mesma janela de {dias_janela} dias)"
    )
    dt_ini_comp = None
    dt_fim_comp = None
    if comparar:
        col1, col2 = st.columns(2)
        with col1:
            dt_ini_comp = st.date_input("Início comparação", value=sug_ini, key="ia_dt_ini")
        with col2:
            dt_fim_comp = st.date_input("Fim comparação", value=sug_fim, key="ia_dt_fim")

    if st.button("🤖 Gerar Insights com IA", key="ia_gerar", type="primary"):
        if not GEMINI_DISPONIVEL:
            st.error("Biblioteca `google-generativeai` não está instalada no ambiente.")
            return
        with st.spinner("Coletando dados..."):
            dados_periodo = _coletar_dados_para_ia(engine, loja_nome, dt_inicio, dt_fim)
            dados_comp = None
            if comparar and dt_ini_comp and dt_fim_comp:
                dados_comp = _coletar_dados_para_ia(engine, loja_nome, dt_ini_comp, dt_fim_comp)

        if not dados_periodo:
            st.warning("Nenhum dado encontrado no período principal para analisar.")
            return

        with st.spinner("Montando prompt..."):
            prompt = _montar_prompt_ia(
                loja_nome, dt_inicio, dt_fim,
                dados_periodo, dt_ini_comp, dt_fim_comp, dados_comp
            )

        with st.spinner("Consultando Gemini..."):
            resposta = _chamar_gemini(prompt)

        st.session_state['ia_ultima_resposta'] = resposta
        st.session_state['ia_ultimo_prompt'] = prompt

    # Exibir última resposta se houver
    resposta = st.session_state.get('ia_ultima_resposta')
    if resposta:
        st.markdown("---")
        st.markdown(resposta)
        with st.expander("🔎 Ver prompt enviado à IA (debug)"):
            st.code(st.session_state.get('ia_ultimo_prompt', ''), language='markdown')


# ============================================================
# TAB 3: MATCH SKU (múltiplos SKUs por anúncio, com data_editor)
# ============================================================

def _shopee_match_sku(engine):
    st.subheader("Vincular Produto Ads → SKU")
    st.caption(
        "Cada anúncio pode ter até 3 SKUs vinculados. "
        "Alterações criam um novo registro temporal (SKU antigo é encerrado na data atual, "
        "novo SKU começa hoje) — igual ao snapshot de custos do sistema."
    )

    loja = st.selectbox("Loja", list(LOJAS_SHOPEE.keys()), key="match_loja")
    loja_nome = LOJAS_SHOPEE[loja]

    # 1) Carregar todos os anúncios da loja (com ou sem match)
    df_anuncios = _query_df(engine, """
        SELECT DISTINCT nome_anuncio, id_produto
        FROM fact_ads_shopee
        WHERE loja = %s
        ORDER BY nome_anuncio
    """, [loja_nome])

    if df_anuncios.empty:
        st.info("Nenhum anúncio importado para esta loja ainda. Faça upload de um CSV primeiro.")
        return

    # 2) Carregar SKUs vigentes hoje para cada anúncio
    hoje = date.today()
    matches_atuais = {}  # {nome_anuncio: [sku1, sku2, ...]}
    for _, r in df_anuncios.iterrows():
        lista = buscar_skus_match(engine, loja_nome, r['nome_anuncio'], data_ref=hoje)
        matches_atuais[r['nome_anuncio']] = lista

    # 3) Carregar lista de SKUs disponíveis (para a dica no topo)
    df_skus = _query_df(engine, """
        SELECT sku, nome_produto FROM dim_skus
        WHERE ativo = TRUE ORDER BY sku
    """)
    total_skus = len(df_skus)

    # 4) Métricas de topo
    com_match = sum(1 for skus in matches_atuais.values() if skus)
    sem_match = len(matches_atuais) - com_match

    c1, c2, c3 = st.columns(3)
    c1.metric("Total de anúncios", len(df_anuncios))
    c2.metric("Com SKU vinculado", com_match)
    c3.metric("Sem vínculo", sem_match)

    # ---- BLOCO XLSX: download para preenchimento + upload ----
    with st.expander("📎 Trabalhar via planilha (XLSX)", expanded=False):
        st.caption(
            "Baixe o XLSX com 2 abas: **`a_preencher`** (anúncios sem SKU) e "
            "**`matches_ativos`** (vigentes hoje). Preencha fora do sistema e faça upload "
            "de volta — novos matches entram com `data_inicio = hoje` e antigos (alterados) "
            "fecham com `data_fim = hoje`."
        )
        colx1, colx2 = st.columns(2)
        with colx1:
            if st.button("📥 Gerar XLSX para preenchimento", key="match_xlsx_gen"):
                with st.spinner("Gerando XLSX..."):
                    xlsx_bytes = _gerar_xlsx_matches(engine, loja_nome)
                st.session_state[f'match_xlsx_bytes_{loja_nome}'] = xlsx_bytes

            xlsx_bytes = st.session_state.get(f'match_xlsx_bytes_{loja_nome}')
            if xlsx_bytes:
                nome_arq = f"matches_ads_{loja_nome.replace(' ', '_')}_{date.today().strftime('%Y%m%d')}.xlsx"
                st.download_button(
                    "⬇️ Baixar XLSX gerado",
                    data=xlsx_bytes,
                    file_name=nome_arq,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="match_xlsx_dl",
                )

        with colx2:
            arq_up = st.file_uploader(
                "📤 Upload de matches preenchidos",
                type=['xlsx'], key="match_xlsx_up", accept_multiple_files=False
            )
            if arq_up and st.button("💾 Processar upload", key="match_xlsx_proc", type="primary"):
                with st.spinner("Processando upload..."):
                    adic, alter, ign, errs = _processar_upload_xlsx_matches(
                        engine, loja_nome, arq_up
                    )
                if adic:
                    st.success(f"✅ {adic} vínculo(s) novo(s) adicionado(s).")
                if alter:
                    st.success(f"✅ {alter} vínculo(s) alterado(s) (antigo fechado, novo aberto hoje).")
                if ign:
                    st.info(f"ℹ️ {ign} linha(s) ignorada(s) (sem alteração ou campos em branco).")
                if errs:
                    st.error(f"❌ {len(errs)} erro(s):")
                    for e in errs[:10]:
                        st.caption(e)

    st.divider()

    # 5) Construir DataFrame editável
    # Colunas: ID, Anúncio, SKU 1, SKU 2, SKU 3
    linhas = []
    for _, r in df_anuncios.iterrows():
        nome = r['nome_anuncio']
        id_prod = r.get('id_produto', '') or ''
        skus = matches_atuais.get(nome, [])
        # Preencher até MAX_SKUS_POR_ANUNCIO
        skus_padded = list(skus) + [""] * (MAX_SKUS_POR_ANUNCIO - len(skus))
        skus_padded = skus_padded[:MAX_SKUS_POR_ANUNCIO]
        linhas.append({
            'ID Produto': str(id_prod),
            'Anúncio': nome,
            'SKU 1': skus_padded[0],
            'SKU 2': skus_padded[1],
            'SKU 3': skus_padded[2],
        })

    df_editor = pd.DataFrame(linhas)

    st.markdown("### 📝 Edite os SKUs abaixo e clique em Salvar")
    st.caption(
        "💡 **Dica:** preencha os SKUs exatamente como cadastrados no sistema "
        f"(exemplos dos seus {total_skus} SKUs ativos disponíveis em `dim_skus`). "
        "Deixe em branco os campos não usados. Para remover um SKU, apague o valor do campo."
    )

    # st.data_editor — edição inline como planilha
    df_editado = st.data_editor(
        df_editor,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=['ID Produto', 'Anúncio'],
        key="match_editor",
        column_config={
            'ID Produto': st.column_config.TextColumn(width="small"),
            'Anúncio': st.column_config.TextColumn(width="large"),
            'SKU 1': st.column_config.TextColumn(width="small", help="SKU principal"),
            'SKU 2': st.column_config.TextColumn(width="small", help="SKU adicional (opcional)"),
            'SKU 3': st.column_config.TextColumn(width="small", help="SKU adicional (opcional)"),
        }
    )

    if st.button("💾 Salvar alterações", key="match_salvar", type="primary"):
        _salvar_matches_em_lote(engine, loja_nome, df_editado, matches_atuais)


def _salvar_matches_em_lote(engine, loja_nome, df_editado, matches_atuais):
    """Processa o DataFrame editado e atualiza a dim_ads_produto_sku com lógica temporal."""
    alterados = 0
    inalterados = 0
    erros = []

    progress = st.progress(0, text="Salvando alterações...")
    total = len(df_editado)

    for idx, row in df_editado.iterrows():
        nome = str(row['Anúncio']).strip()
        id_prod = str(row.get('ID Produto', '')).strip()

        # Coletar SKUs novos (não-vazios, normalizados)
        skus_novos = []
        for col in ['SKU 1', 'SKU 2', 'SKU 3']:
            val = row.get(col, '')
            if val is not None and not pd.isna(val):
                txt = str(val).strip()
                if txt and txt.lower() not in ('nan', 'none'):
                    skus_novos.append(txt)

        # Remover duplicatas preservando ordem
        seen = set()
        skus_novos_unicos = []
        for s in skus_novos:
            if s not in seen:
                seen.add(s)
                skus_novos_unicos.append(s)

        # Comparar com o estado atual
        atuais = matches_atuais.get(nome, [])
        if sorted(atuais) == sorted(skus_novos_unicos):
            inalterados += 1
        else:
            # Aplicar mudanças via função temporal
            sucesso = atualizar_matches_sku(
                engine, loja_nome, nome, id_prod, skus_novos_unicos
            )
            if sucesso:
                # Atualizar sku_match em fact_ads_shopee (pega o primeiro da lista)
                primeiro_sku = skus_novos_unicos[0] if skus_novos_unicos else None
                _sync_fact_sku_match(engine, loja_nome, nome, primeiro_sku)
                alterados += 1
            else:
                erros.append(nome[:40])

        progress.progress((idx + 1) / total, text=f"Processando {idx + 1}/{total}...")

    progress.empty()

    if alterados > 0:
        st.success(f"✅ {alterados} anúncio(s) atualizados com vigência temporal a partir de hoje.")
    if inalterados > 0:
        st.info(f"ℹ️ {inalterados} anúncio(s) sem alteração.")
    if erros:
        st.error(f"❌ {len(erros)} erros: {', '.join(erros[:5])}")

    if alterados > 0:
        st.caption("Clique em **Rerun** ou mude de aba para recarregar a tela.")


def _sync_fact_sku_match(engine, loja, nome_anuncio, primeiro_sku):
    """
    Sincroniza a coluna fact_ads_shopee.sku_match com o primeiro SKU ativo.
    Se primeiro_sku for None, apaga o vínculo em fact_ads_shopee.
    """
    conn = None
    cursor = None
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        if primeiro_sku:
            cursor.execute("""
                UPDATE fact_ads_shopee
                SET sku_match = %s, match_confirmado = TRUE
                WHERE loja = %s AND nome_anuncio = %s
            """, (primeiro_sku, loja, nome_anuncio))
        else:
            cursor.execute("""
                UPDATE fact_ads_shopee
                SET sku_match = NULL, match_confirmado = FALSE
                WHERE loja = %s AND nome_anuncio = %s
            """, (loja, nome_anuncio))
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# TAB 4: HISTÓRICO
# ============================================================

def _shopee_historico(engine):
    st.subheader("Histórico de Uploads")
    df = _query_df(engine, """
        SELECT data_upload, loja, tipo_relatorio, arquivo_nome,
               periodo_inicio, periodo_fim,
               linhas_importadas, linhas_erro, status
        FROM log_uploads_ads
        WHERE marketplace = 'Shopee'
        ORDER BY data_upload DESC LIMIT 50
    """)
    if df.empty:
        st.info("Nenhum upload de ads registrado ainda.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("🔍 Diagnóstico do banco"):
        total = _query_scalar(engine, "SELECT COUNT(*) FROM fact_ads_shopee")
        st.caption(f"Total de registros em `fact_ads_shopee`: {total or 0}")

        total_matches_ativos = _query_scalar(engine, """
            SELECT COUNT(*) FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee' AND data_fim IS NULL
        """)
        st.caption(f"Total de matches ativos (data_fim IS NULL): {total_matches_ativos or 0}")

        total_matches_historico = _query_scalar(engine, """
            SELECT COUNT(*) FROM dim_ads_produto_sku
            WHERE marketplace = 'Shopee'
        """)
        st.caption(f"Total histórico em `dim_ads_produto_sku`: {total_matches_historico or 0}")

        df_resumo = _query_df(engine, """
            SELECT loja, periodo_inicio, periodo_fim,
                   COUNT(*) AS registros,
                   ROUND(SUM(despesas)::numeric, 2) AS invest_total
            FROM fact_ads_shopee
            GROUP BY loja, periodo_inicio, periodo_fim
            ORDER BY periodo_fim DESC
        """)
        if not df_resumo.empty:
            st.caption("Dados importados por loja/período:")
            st.dataframe(df_resumo, use_container_width=True, hide_index=True)
        else:
            st.warning("Tabela `fact_ads_shopee` está vazia.")
