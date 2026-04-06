"""
PROCESSADOR SHOPEE - Sistema Nala
Processa arquivos de vendas da Shopee (.xlsx exportado do painel)

VERSÃO 2.2 (06/04/2026):
  - FIX: renomear_colunas_shopee agora exclui colunas 'bruta' (Shopee adicionou
         'Taxa de comissão bruta' e 'Taxa de serviço bruta' ao export, causando
         duplicata de coluna e KeyError: 0)
  - FIX: Cashback rename restrito a 'coin cashback' (evita duplicata com 'Compensar Moedas')

VERSÃO 2.1 (18/03/2026):
  - NOVO: Salva pedido_original no banco (pedido real da Shopee)
  - FIX: Barra de progresso agora mostra texto com contagem de pedidos
  - Mantido: Toda lógica v2.0 intacta

REGRAS DE NEGÓCIO:
- Receita = Subtotal do produto (Preço acordado × Qtd)
- Comissão pedidos simples: Net Commission Fee + Taxa de serviço líquida (valor do arquivo)
- Comissão carrinhos compostos: calculada pela tabela oficial (arquivo repete valor total em cada linha)
- Imposto: Subtotal × alíquota da loja (dim_lojas)
- Cupom do vendedor: deduzido da margem quando > 0
- Frete: IGNORADO (Shopee retém, não passa ao vendedor)
- Verificação de comissão: alerta quando valor cobrado diverge da tabela vigente

TABELA DE COMISSÕES (válida a partir de 01/03/2026, aplicada POR ITEM):
- Até R$ 79,99:         20% + R$ 4,00
- R$ 80,00 a R$ 99,99:  14% + R$ 16,00
- R$ 100,00 a R$ 199,99: 14% + R$ 20,00
- R$ 200,00 a R$ 499,99: 14% + R$ 26,00
- Acima de R$ 500,00:   14% + R$ 26,00

CARRINHOS COMPOSTOS:
- Detectados quando o mesmo ID do pedido aparece em múltiplas linhas
- O arquivo Shopee repete as comissões do pedido inteiro em cada linha (não divide)
- Para carrinhos: comissão calculada pela tabela por item (não usa o valor do arquivo)

CORREÇÃO 10/03/2026:
- _buscar_custos_skus agora lê de dim_produtos.preco_a_ser_considerado (não dim_produtos_custos)
- _buscar_skus_validos agora lê de dim_produtos (não dim_skus)

VERSÃO 2.0 (10/03/2026):
- Proteção contra duplicatas: pré-carrega (pedido, sku) existentes
- Vendas pendentes: SKU não cadastrado vai para fact_vendas_pendentes (não descarta)
- Retorno expandido: (registros, erros, skus_invalidos, duplicatas, pendentes)
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime

from formatadores import formatar_valor, formatar_percentual
from database_utils import (
    get_engine,
    buscar_duplicatas_loja,
    gravar_venda_pendente,
)


# ============================================================
# TABELA OFICIAL DE COMISSÕES SHOPEE
# Válida a partir de 01/03/2026
# Aplicada POR ITEM: (preco_unitario × taxa + fixo) × quantidade
# ============================================================
TABELA_COMISSAO_SHOPEE = [
    (79.99,       0.20, 4.00),
    (99.99,       0.14, 16.00),
    (199.99,      0.14, 20.00),
    (499.99,      0.14, 26.00),
    (float('inf'), 0.14, 26.00),
]


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def calcular_comissao_shopee(preco_unitario: float, quantidade: int) -> float:
    """
    Calcula comissão esperada pela tabela oficial da Shopee.
    Regra aplicada por item (preço unitário), multiplicada pela quantidade.
    A parte fixa (R$4, R$16, etc.) também é multiplicada pela quantidade.
    """
    for limite, taxa, fixo in TABELA_COMISSAO_SHOPEE:
        if preco_unitario <= limite:
            return round((preco_unitario * taxa + fixo) * quantidade, 2)
    # fallback (não deve ocorrer com float('inf') na tabela)
    return round((preco_unitario * 0.14 + 26.00) * quantidade, 2)


def _limpar_numero(valor) -> float:
    """Converte valor para float de forma segura."""
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return 0.0
    try:
        s = str(valor).strip()
        if s in ('', 'nan', 'NaN', 'None', '-'):
            return 0.0
        return float(s.replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0


def _buscar_custos_skus(skus: list, engine) -> dict:
    """
    Busca custo dos SKUs no banco.
    CORREÇÃO: Fonte principal é dim_produtos.preco_a_ser_considerado
    (onde gestao_skus.py e app_compras.py atualizam).
    Fallback para soma dos componentes em dim_produtos_custos.

    Retorna dict {sku: custo}.
    """
    if not skus:
        return {}
    try:
        placeholders = ','.join(['%s'] * len(skus))
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT 
                    p.sku,
                    COALESCE(
                        NULLIF(p.preco_a_ser_considerado, 0),
                        NULLIF(pc.preco_compra + pc.embalagem + pc.mdo + pc.custo_ads, 0),
                        pc.preco_compra,
                        0
                    ) as custo
                FROM dim_produtos p
                LEFT JOIN dim_produtos_custos pc ON p.sku = pc.sku
                WHERE p.sku IN ({placeholders})
                  AND p.status = 'Ativo'""",
            skus
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0]: float(row[1]) if row[1] is not None else 0.0 for row in rows}
    except Exception:
        return {}


def _buscar_skus_validos(skus: list, engine) -> set:
    """
    Retorna conjunto de SKUs cadastrados.
    CORREÇÃO: Busca de dim_produtos (onde gestao_skus.py cadastra)
    em vez de dim_skus.
    """
    if not skus:
        return set()
    try:
        placeholders = ','.join(['%s'] * len(skus))
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT sku FROM dim_produtos WHERE sku IN ({placeholders}) AND status = 'Ativo'",
            skus
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0] for row in rows}
    except Exception:
        return set(skus)


# ============================================================
# DETECÇÃO DE HEADER
# ============================================================

def detectar_header_shopee(arquivo) -> int:
    """
    Detecta a linha do header no arquivo Shopee.
    Procura pela coluna 'ID do pedido'.
    Retorna o índice da linha (0-based) a ser usada como skiprows.
    Retorna 0 como padrão caso não encontre.
    """
    try:
        df_raw = pd.read_excel(arquivo, header=None, nrows=15)
        for i, row in df_raw.iterrows():
            for val in row.values:
                if 'ID do pedido' in str(val):
                    return i
    except Exception:
        pass
    return 0


# ============================================================
# PADRONIZAÇÃO DE COLUNAS
# ============================================================

def renomear_colunas_shopee(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes de colunas do arquivo Shopee (v2.2).
    Suporta variações em Inglês e Português para evitar erros de mapeamento.

    v2.2 FIX: Shopee passou a exportar colunas 'bruta' (Taxa de comissão bruta,
    Taxa de serviço bruta). A condição genérica com 'in' renomeava tanto bruta
    quanto líquida para o mesmo nome, gerando duplicata e KeyError: 0.
    Agora exclui explicitamente colunas contendo 'bruta'.
    """
    rename_map = {}
    for col in df.columns:
        col_norm = str(col).lower().strip()
        
        # Mapeamento: Net Commission Fee / Taxa de comissão líquida
        if col_norm in ['net commission fee', 'taxa de comissão líquida', 'taxa de comissao liquida']:
            rename_map[col] = 'Net Commission Fee'
            
        # Mapeamento: Taxa de serviço líquida (exclui 'bruta' — Shopee passou a exportar ambas)
        elif ('taxa de serviço' in col_norm or 'taxa de servico' in col_norm) and 'bruta' not in col_norm:
            rename_map[col] = 'Taxa de serviço líquida'
            
        # Mapeamento: ID do Pedido (Garante que espaços não quebrem a detecção)
        elif col_norm == 'id do pedido':
            rename_map[col] = 'ID do pedido'

        # Mapeamento: Cashback — só 'Coin Cashback' (Shopee adicionou 'Compensar Moedas' separado)
        elif 'coin cashback' in col_norm:
            rename_map[col] = 'Seller Absorbed Coin Cashback'
            
    return df.rename(columns=rename_map)


# ============================================================
# PROCESSADOR PRINCIPAL
# ============================================================

def processar_arquivo_shopee(arquivo, loja: str, imposto: float, engine):
    """
    Processa arquivo de vendas da Shopee.

    Parâmetros:
        arquivo   : arquivo XLSX enviado via st.file_uploader
        loja      : nome da loja (ex: 'Shopee Litstore')
        imposto   : alíquota de imposto da loja em % (ex: 10.0)
        engine    : conexão SQLAlchemy

    Retorna:
        (df_processado, info_dict)  → sucesso
        (None, mensagem_erro)       → falha
    """
    try:
        # --------------------------------------------------
        # 1. LER ARQUIVO
        # --------------------------------------------------
        arquivo.seek(0)
        skiprows = detectar_header_shopee(arquivo)
        arquivo.seek(0)

        df = pd.read_excel(arquivo, skiprows=skiprows)
        df = renomear_colunas_shopee(df)

        if df.empty:
            return None, "Arquivo vazio ou sem dados válidos."

        # --------------------------------------------------
        # 2. VERIFICAR COLUNAS OBRIGATÓRIAS
        # --------------------------------------------------
        colunas_obrigatorias = [
            'ID do pedido',
            'Status do pedido',
            'Número de referência SKU',
            'Preço acordado',
            'Quantidade',
            'Subtotal do produto',
            'Net Commission Fee',
        ]
        faltando = [c for c in colunas_obrigatorias if c not in df.columns]
        if faltando:
            return None, f"Colunas obrigatórias não encontradas: {', '.join(faltando)}"

        # --------------------------------------------------
        # 3. CONVERTER TIPOS
        # --------------------------------------------------
        df['Preço acordado']        = df['Preço acordado'].apply(_limpar_numero)
        df['Quantidade']            = df['Quantidade'].apply(_limpar_numero).astype(int)
        df['Subtotal do produto']   = df['Subtotal do produto'].apply(_limpar_numero)
        df['Net Commission Fee']    = df['Net Commission Fee'].apply(_limpar_numero)
        df['Taxa de serviço líquida'] = df['Taxa de serviço líquida'].apply(_limpar_numero)
        df['Total global']          = df['Total global'].apply(_limpar_numero) \
                                        if 'Total global' in df.columns \
                                        else pd.Series(1.0, index=df.index)

        # Cupom do vendedor (opcional)
        if 'Cupom do vendedor' in df.columns:
            df['Cupom do vendedor'] = df['Cupom do vendedor'].apply(_limpar_numero)
        else:
            df['Cupom do vendedor'] = 0.0

        # --------------------------------------------------
        # 4. FILTRAR REGISTROS INVÁLIDOS
        # --------------------------------------------------
        total_original = len(df)

        # Cancelados
        mask_cancelado = df['Status do pedido'].astype(str).str.contains(
            'cancelad', case=False, na=False
        )

        # Devoluções / reembolsos
        if 'Status da Devolução / Reembolso' in df.columns:
            mask_devolucao = (
                df['Status da Devolução / Reembolso'].notna()
                & (df['Status da Devolução / Reembolso'].astype(str).str.strip() != '')
                & (~df['Status da Devolução / Reembolso'].astype(str).str.lower().isin(
                    ['nan', 'none', '-', '']
                ))
            )
        else:
            mask_devolucao = pd.Series(False, index=df.index)

        # Sem receita (Total global = 0 — pedidos sem pagamento real)
        mask_sem_receita = df['Total global'] == 0.0

        mask_descartar = mask_cancelado | mask_devolucao | mask_sem_receita
        df_valido = df[~mask_descartar].copy()
        linhas_descartadas = total_original - len(df_valido)

        if df_valido.empty:
            return None, "Nenhuma venda válida encontrada após filtros."

        # --------------------------------------------------
        # 5. DETECTAR CARRINHOS COMPOSTOS
        # --------------------------------------------------
        contagem_pedidos = df_valido['ID do pedido'].value_counts()
        ids_carrinho = set(contagem_pedidos[contagem_pedidos > 1].index)

        # --------------------------------------------------
        # 6. PROCESSAR LINHA A LINHA
        # --------------------------------------------------
        resultados      = []
        alertas_comissao = []

        # Identificar coluna de data disponível
        colunas_data_candidatas = [
            'Data de criação do pedido',
            'Hora do pagamento do pedido',
            'Data',
        ]

        for _, row in df_valido.iterrows():
            pedido_id  = str(row['ID do pedido']).strip()
            is_carrinho = pedido_id in ids_carrinho

            # SKU
            sku = str(row['Número de referência SKU']).strip()
            if not sku or sku.lower() in ('nan', 'none', ''):
                continue

            # Código do anúncio (SKU pai / agrupador)
            codigo_anuncio = ''
            if 'Nº de referência do SKU principal' in df.columns:
                val_anuncio = str(row.get('Nº de referência do SKU principal', '')).strip()
                codigo_anuncio = '' if val_anuncio.lower() in ('nan', 'none', '') else val_anuncio
            if not codigo_anuncio:
                codigo_anuncio = sku

            # Valores financeiros
            preco_unitario  = _limpar_numero(row['Preço acordado'])
            quantidade      = int(_limpar_numero(row['Quantidade']))
            subtotal        = _limpar_numero(row['Subtotal do produto'])
            cupom_vendedor  = _limpar_numero(row.get('Cupom do vendedor', 0))

            if subtotal == 0 and preco_unitario > 0:
                subtotal = preco_unitario * quantidade

            # Data da venda
            data_venda = None
            for col_data in colunas_data_candidatas:
                if col_data in df_valido.columns:
                    val_data = row.get(col_data)
                    if pd.notna(val_data) and str(val_data).strip() not in ('', 'nan'):
                        try:
                            data_venda = pd.to_datetime(val_data).date()
                            break
                        except Exception:
                            continue
            if data_venda is None:
                data_venda = datetime.now().date()

            # --------------------------------------------------
            # 7. CALCULAR COMISSÃO
            # --------------------------------------------------
            if is_carrinho:
                # Arquivo repete o total do pedido em cada linha — usar tabela por item
                comissao       = calcular_comissao_shopee(preco_unitario, quantidade)
                fonte_comissao = 'calculada_tabela'
            else:
                # Pedido simples: valor direto do arquivo é confiável
                comissao       = _limpar_numero(row['Net Commission Fee']) + \
                                 _limpar_numero(row['Taxa de serviço líquida'])
                fonte_comissao = 'arquivo'

                # Verificar divergência com tabela oficial
                comissao_esperada = calcular_comissao_shopee(preco_unitario, quantidade)
                divergencia       = comissao - comissao_esperada
                tolerancia        = max(0.50, comissao_esperada * 0.05)  # 5% ou R$0,50

                if abs(divergencia) > tolerancia:
                    alertas_comissao.append({
                        'pedido':             pedido_id,
                        'sku':                sku,
                        'comissao_arquivo':   round(comissao, 2),
                        'comissao_esperada':  round(comissao_esperada, 2),
                        'divergencia':        round(divergencia, 2),
                    })

            # --------------------------------------------------
            # 8. IMPOSTO
            # --------------------------------------------------
            imposto_valor = round(subtotal * (imposto / 100), 2)

            resultados.append({
                'pedido':          pedido_id,
                'pedido_original': pedido_id,  # v2.1: NOVO — pedido real da Shopee
                'data':            data_venda,
                'sku':             sku,
                'codigo_anuncio':  codigo_anuncio,
                'qtd':             quantidade,
                'preco_unit':      preco_unitario,
                'receita':         subtotal,
                'tarifa':          round(comissao, 2),
                'imposto':         imposto_valor,
                'cupom_vendedor':  cupom_vendedor,
                'frete':           0.0,
                'custo':           0.0,       # preenchido após busca no banco
                'custo_unit':      0.0,
                'tem_custo':       False,
                'fonte_comissao':  fonte_comissao,
                'is_carrinho':     is_carrinho,
            })

        if not resultados:
            return None, "Nenhuma linha processada com sucesso."

        df_proc = pd.DataFrame(resultados)

        # --------------------------------------------------
        # 9. BUSCAR CUSTOS NO BANCO (snapshot)
        # --------------------------------------------------
        skus_unicos = df_proc['sku'].unique().tolist()
        custos_db   = _buscar_custos_skus(skus_unicos, engine)

        df_proc['custo_unit'] = df_proc['sku'].map(custos_db).fillna(0.0)
        df_proc['custo']      = (df_proc['custo_unit'] * df_proc['qtd']).round(2)
        df_proc['tem_custo']  = df_proc['custo'] > 0

        # --------------------------------------------------
        # 10. CALCULAR MARGEM
        # --------------------------------------------------
        df_proc['margem'] = (
            df_proc['receita']
            - df_proc['tarifa']
            - df_proc['imposto']
            - df_proc['cupom_vendedor']
            - df_proc['custo']
        ).round(2)

        df_proc['margem_pct'] = df_proc.apply(
            lambda r: round((r['margem'] / r['receita'] * 100), 2) if r['receita'] > 0 else 0.0,
            axis=1
        )

        # --------------------------------------------------
        # 11. MONTAR INFO DICT
        # --------------------------------------------------
        skus_sem_custo = int((~df_proc['tem_custo']).sum())

        try:
            periodo_inicio = df_proc['data'].min().strftime('%d/%m/%Y')
            periodo_fim    = df_proc['data'].max().strftime('%d/%m/%Y')
        except Exception:
            hoje = datetime.now().strftime('%d/%m/%Y')
            periodo_inicio = periodo_fim = hoje

        info = {
            'total_linhas':       len(df_proc),
            'periodo_inicio':     periodo_inicio,
            'periodo_fim':        periodo_fim,
            'linhas_descartadas': linhas_descartadas,
            'skus_sem_custo':     skus_sem_custo,
            'carrinhos':          len(ids_carrinho),
            'alertas_comissao':   alertas_comissao,
        }

        return df_proc, info

    except Exception as e:
        return None, f"Erro ao processar arquivo Shopee: {str(e)}"


# ============================================================
# GRAVAÇÃO NO BANCO
# ============================================================

def gravar_vendas_shopee(df_vendas: pd.DataFrame, marketplace: str, loja: str,
                          arquivo_nome: str, engine):
    """
    Grava vendas da Shopee na tabela fact_vendas_snapshot.

    VERSÃO 2.1:
    - NOVO: Salva pedido_original no INSERT
    - FIX: Barra de progresso mostra texto com contagem
    - Mantido: Toda lógica v2.0 intacta

    VERSÃO 2.0:
    - Proteção contra duplicatas: pré-carrega (pedido, sku) existentes da loja
    - Vendas pendentes: SKU não cadastrado vai para fact_vendas_pendentes
    - Retorno expandido com contadores de duplicatas e pendentes

    Parâmetros:
        df_vendas    : DataFrame processado por processar_arquivo_shopee
        marketplace  : nome do marketplace (ex: 'Shopee')
        loja         : nome da loja (ex: 'Shopee Litstore')
        arquivo_nome : nome do arquivo original
        engine       : conexão SQLAlchemy

    Retorna:
        (registros_gravados, erros, skus_invalidos, duplicatas_count, pendentes_count)
    """
    registros      = 0
    erros          = 0
    skus_invalidos = set()
    duplicatas_count = 0
    pendentes_count  = 0

    if df_vendas.empty:
        return 0, 0, set(), 0, 0

    # Verificar SKUs cadastrados em dim_produtos (CORRIGIDO)
    skus_todos   = df_vendas['sku'].unique().tolist()
    skus_validos = _buscar_skus_validos(skus_todos, engine)

    # CARREGAR DUPLICATAS EXISTENTES (proteção contra reimportação)
    duplicatas_existentes = buscar_duplicatas_loja(engine, loja)

    total    = len(df_vendas)
    progress = st.progress(0)
    status_text = st.empty()  # v2.1: texto de status

    # v2.1: INSERT agora inclui pedido_original
    sql_insert = """
        INSERT INTO fact_vendas_snapshot (
            marketplace_origem,
            loja_origem,
            numero_pedido,
            pedido_original,
            data_venda,
            sku,
            codigo_anuncio,
            quantidade,
            preco_venda,
            desconto_parceiro,
            desconto_marketplace,
            valor_venda_efetivo,
            custo_unitario,
            custo_total,
            imposto,
            comissao,
            frete,
            tarifa_fixa,
            outros_custos,
            total_tarifas,
            valor_liquido,
            margem_total,
            margem_percentual,
            data_processamento,
            arquivo_origem
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, NOW(), %s
        )
    """

    conn   = engine.raw_connection()
    cursor = conn.cursor()

    for idx, (_, row) in enumerate(df_vendas.iterrows()):
        sku = row['sku']
        pedido = str(row['pedido']).strip()
        pedido_original = str(row.get('pedido_original', pedido)).strip()  # v2.1

        # v2.1: Atualizar texto do progresso
        progress.progress(min((idx + 1) / total, 1.0))
        status_text.text(f"Gravando venda {idx + 1} de {total}...")

        # ---- PROTEÇÃO DUPLICATA ----
        chave = (pedido, sku)
        if chave in duplicatas_existentes:
            duplicatas_count += 1
            continue

        # ---- SKU NÃO CADASTRADO → SALVAR COMO PENDENTE ----
        if sku not in skus_validos:
            skus_invalidos.add(sku)

            # Preparar dados financeiros para pendente
            receita         = float(row['receita'])
            comissao        = float(row['tarifa'])
            imposto_val     = float(row['imposto'])
            cupom_vendedor  = float(row.get('cupom_vendedor', 0.0))
            frete           = 0.0
            tarifa_fixa     = 0.0
            outros_custos   = cupom_vendedor
            total_tarifas   = comissao + imposto_val + outros_custos
            valor_liquido   = round(receita - total_tarifas, 2)

            dados_pendente = {
                'marketplace_origem': marketplace,
                'loja_origem': loja,
                'numero_pedido': pedido,
                'data_venda': row['data'],
                'sku': sku,
                'codigo_anuncio': row.get('codigo_anuncio', ''),
                'quantidade': int(row['qtd']),
                'preco_venda': float(row.get('preco_unit', 0)),
                'desconto_parceiro': 0,
                'desconto_marketplace': 0,
                'valor_venda_efetivo': receita,
                'imposto': imposto_val,
                'comissao': comissao,
                'frete': frete,
                'tarifa_fixa': tarifa_fixa,
                'outros_custos': outros_custos,
                'total_tarifas': total_tarifas,
                'valor_liquido': valor_liquido,
                'arquivo_origem': arquivo_nome,
            }

            if gravar_venda_pendente(cursor, dados_pendente):
                pendentes_count += 1
            else:
                erros += 1

            continue

        # ---- GRAVAÇÃO NORMAL ----
        try:
            # Valores financeiros
            receita         = float(row['receita'])
            comissao        = float(row['tarifa'])
            imposto_val     = float(row['imposto'])
            cupom_vendedor  = float(row.get('cupom_vendedor', 0.0))
            custo_unit      = float(row.get('custo_unit', 0.0))
            custo_total     = float(row['custo'])

            frete           = 0.0
            tarifa_fixa     = 0.0
            outros_custos   = cupom_vendedor          # cupom do vendedor sai do resultado
            total_tarifas   = comissao + imposto_val + outros_custos
            valor_liquido   = round(receita - total_tarifas, 2)
            margem_total    = float(row['margem'])
            margem_pct      = float(row['margem_pct'])

            # Savepoint individual — rollback só desta linha em caso de erro
            cursor.execute(f"SAVEPOINT sp_shopee_{idx}")
            cursor.execute(sql_insert, (
                marketplace,
                loja,
                pedido,
                pedido_original,  # v2.1: NOVO parâmetro
                row['data'],
                sku,
                row['codigo_anuncio'],
                int(row['qtd']),
                float(row['preco_unit']),   # preco_venda = preço unitário acordado
                0.0,                         # desconto_parceiro (já embutido no preço acordado)
                0.0,                         # desconto_marketplace (cupom Shopee — absorvido pela plataforma)
                receita,
                custo_unit,
                custo_total,
                imposto_val,
                comissao,
                frete,
                tarifa_fixa,
                outros_custos,
                total_tarifas,
                valor_liquido,
                margem_total,
                margem_pct,
                arquivo_nome,
            ))
            cursor.execute(f"RELEASE SAVEPOINT sp_shopee_{idx}")
            registros += 1

            # Adicionar ao set de duplicatas (evita duplicata intra-arquivo)
            duplicatas_existentes.add(chave)

        except Exception:
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT sp_shopee_{idx}")
            except:
                pass
            erros += 1

    # Commit único no final (vendas + pendentes)
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"❌ Erro ao commitar vendas Shopee: {e}")

    cursor.close()
    conn.close()
    progress.empty()
    status_text.empty()  # v2.1: limpar texto

    return registros, erros, skus_invalidos, duplicatas_count, pendentes_count
