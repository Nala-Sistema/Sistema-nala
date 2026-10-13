"""
FULL POR LOJA — aba do Sinais do Dia (frente [FULL POR LOJA], 12/10/2026)

Plano aprovado pelo Thiago em 12/10/2026. Uma aba "Full por loja" dentro do
Sinais do Dia, ML e Shopee na MESMA lógica, sempre POR LOJA (cobertura em peça
somada entre lojas foi "inútil"). Responde: o Full desta loja acaba antes do
próximo envio? quanto enviar? quanto R$ está parado no Full?

REGRAS
  - Dados: a MESMA leitura do dia do Sinais do Dia (obter_pacote, em memória):
    foto do Full por estoque_id (ML) / item:model (Shopee), ponte anúncio ×
    estoque, vendas por (loja, anúncio, SKU). Nada de v_cobertura_full (nunca
    auditada; divide pelos dias com estoque e só tem ML).
  - Full POR estoque_id, nunca somado por SKU (K-L-0421-A na ML-LPT: 6 un. no
    anúncio que vende, 89 no parado). Só quantidade, nunca availability_type.
  - Venda/dia = max(7 dias ÷ 7, 30 dias ÷ 30), janelas até ontem (dia fechado):
    sinais_dia.venda_do_estoque, a mesma conta do bloco FULL.
  - Dias de Full = (disponível + em transferência) ÷ venda/dia.
  - ALERTA "Full acabando": dias de Full < prazo. A API não dá a data de envio
    liberada (sonda de 12/10: ML 404, Shopee só status do FBS), então:
      * a gestora informa a próxima data liberada da loja (tabela
        full_proxima_data_envio, quem e quando ficam gravados); com data no
        futuro, prazo = dias até a data + PRAZO_CHEGADA_DIAS ("o Full acaba
        antes de o próximo envio chegar?");
      * sem data, ou com a data vencida, prazo = ANTECEDENCIA_DIAS (21).
  - QUANTO ENVIAR = venda/dia × DIAS_ALVO − disponível − em transferência,
    limitado ao galpão do estoque.
  - GALPÃO é físico e um só para todas as lojas (ML e Shopee), exceto a
    ML-YanniSP (prep center próprio). Mesmo SKU precisando de envio em mais de
    uma loja e a soma passando do galpão = CONFLITO: a tela mostra a
    necessidade de cada loja e NÃO divide (decisão do Thiago). Galpão nunca é
    somado (o UpSeller repete o mesmo número em todo anúncio do SKU; no ML o
    kit vem dividido): vale o maior número informado para o SKU.
    Kit não é múltiplo do item base: o conflito é por SKU; disputa entre kit e
    peça base fica fora desta versão.
  - "FULL PARADO" (sinal A1): Full ≥ SEMANAS_PARADO semanas de venda OU Full > 0
    sem venda em 30 dias. R$ parado = unidades no Full × custo (a mesma regra de
    custo da venda, estoque_peca.SQL_CUSTOS). Usa o Ciente do Sinais
    (sinal_ciente, regra 'full_parado', objeto = estoque_id) e NÃO acende no
    anúncio que está com "Full acabando" (outro estoque do mesmo anúncio, ou o
    mesmo com uma data liberada muito longe).
  - Shopee Litstore(Yanni) fica fora: não tem estoque no sistema (só upload).
"""

import math
from datetime import datetime, timedelta

import pandas as pd

import estoque_peca as ep
import sinais_dia as sd

# ============================================================
# CONSTANTES (calibrar aqui)
# ============================================================

DIAS_ALVO = 35              # cobertura que o envio deve deixar (abaixo das 6 semanas do "parado")
ANTECEDENCIA_DIAS = 21      # sem data liberada informada: alerta com 21 dias de Full
PRAZO_CHEGADA_DIAS = 4      # da coleta até o marketplace disponibilizar (3 a 4 dias)
SEMANAS_PARADO = 6          # Full ≥ 6 semanas de venda = parado
DIAS_DATA_MAX = 90          # data liberada informada: no máximo 90 dias à frente
LOJAS_PREP_CENTER = ('ML-YanniSP',)   # galpão próprio: fora do conflito
MARKETPLACES_FULL = (sd.MARKETPLACE, sd.SHOPEE)

REGRA_PARADO = 'full_parado'

# ============================================================
# SQL
# ============================================================

SQL_EXISTE_DATAS = "SELECT to_regclass('full_proxima_data_envio') IS NOT NULL"

# A data em vigor de cada loja = a última informada (NULL = a gestora limpou).
SQL_DATAS = """
    SELECT DISTINCT ON (marketplace, loja) marketplace, loja, data_envio,
           informado_por, informado_em
      FROM full_proxima_data_envio
     WHERE marketplace = ANY(%(mkts)s) AND loja = ANY(%(lojas)s)
     ORDER BY marketplace, loja, informado_em DESC, id DESC
"""

SQL_INSERIR_DATA = """
    INSERT INTO full_proxima_data_envio (marketplace, loja, data_envio, informado_por)
    VALUES (%(marketplace)s, %(loja)s, %(data)s, %(usuario)s)
"""

TODAS_AS_SQL = (SQL_EXISTE_DATAS, SQL_DATAS, SQL_INSERIR_DATA)


# ============================================================
# CONTAS (sem banco)
# ============================================================

def _teto(x):
    """ceil sem o ruído do float (35 × 0,2 = 7,000000001 não vira 8)."""
    return int(math.ceil(round(x, 6)))


def prazo_da_loja(info, hoje):
    """(prazo em dias, texto, data em vigor ou None). info = linha de SQL_DATAS
    como dict (data_envio, informado_por, informado_em) ou None."""
    data = (info or {}).get('data_envio')
    if data is not None and data > hoje:
        prazo = (data - hoje).days + PRAZO_CHEGADA_DIAS
        return prazo, (f"próxima data liberada {data:%d/%m} + {PRAZO_CHEGADA_DIAS} dias de "
                       f"chegada = {prazo} dias"), data
    if data is not None:
        return ANTECEDENCIA_DIAS, (f"data informada ({data:%d/%m}) já passou: vale "
                                   f"{ANTECEDENCIA_DIAS} dias"), None
    return ANTECEDENCIA_DIAS, f"sem data informada: vale {ANTECEDENCIA_DIAS} dias", None


def _custo(skus, custos):
    validos = [ep._custo_valido(custos.get(s)) for s in skus]
    validos = [float(c) for c in validos if c is not None]
    return max(validos) if validos else None


def avaliar(mkt, foto, ponte, por_sku, custos, prazos):
    """Um item por estoque da foto que existe na ponte. foto: [(loja, eid,
    data, full, transf, recebidas[, galpao])]; ponte já mapeada; por_sku de
    sd.agregar_vendas; custos {sku: custo}; prazos {loja: dias}."""
    est = sd.estoques(ponte)
    itens = []
    for linha in foto:
        loja, eid, data, full, transf = linha[:5]
        galpao = linha[6] if len(linha) > 6 else None
        e = est.get((loja, eid))
        if not e:
            continue
        v = sd.venda_do_estoque(loja, e, por_sku)
        full, transf, vd = int(full or 0), int(transf or 0), v['dia']
        prazo = prazos.get(loja, ANTECEDENCIA_DIAS)
        dias_full = (full + transf) / vd if vd > 0 else None
        acabando = bool(e['em_full']) and vd > 0 and dias_full < prazo
        necessidade = max(0, _teto(vd * DIAS_ALVO - full - transf)) if vd > 0 else 0
        parado = full > 0 and (vd == 0 or full >= SEMANAS_PARADO * 7 * vd)
        custo = _custo(e['skus'], custos)
        itens.append({
            'mkt': mkt, 'loja': loja, 'estoque_id': eid, 'data': data,
            'anuncios': tuple(sorted(e['anuncios'])), 'anuncio': v['anuncio'],
            'skus': tuple(sorted(e['skus'])), 'em_full': bool(e['em_full']),
            'full': full, 'transf': transf,
            'galpao': None if galpao is None else int(galpao),
            'qtd_7': v['qtd_ritmo'], 'qtd_30': v['qtd_30'], 'rec_30': v['rec_30'],
            'venda_dia': vd, 'dias_full': dias_full, 'prazo': prazo,
            'acabando': acabando, 'necessidade': necessidade if acabando else 0,
            'parado': parado, 'parado_suprimido': False, 'custo': custo,
            'rs_parado': full * custo if parado and custo else None,
            'semanas_full': (full / vd / 7) if vd > 0 else None,
            'enviar': None, 'obs': '',
        })
    return itens


def suprimir_parado_com_acabando(itens):
    """"Full parado" não acende no anúncio que está com "Full acabando"."""
    acabando = {(i['mkt'], i['loja'], a) for i in itens if i['acabando'] for a in i['anuncios']}
    for i in itens:
        if i['parado'] and any((i['mkt'], i['loja'], a) in acabando for a in i['anuncios']):
            i['parado_suprimido'] = True
    return itens


def _chave_sku(item):
    return ' + '.join(item['skus']) if item['skus'] else f"estoque {item['estoque_id']}"


def aplicar_galpao(itens):
    """Limita o envio ao galpão e marca o conflito entre lojas (mesmo SKU).
    Devolve a lista de conflitos [{sku, galpao, pedidos: [(mkt, loja, eid, n)]}].
    Nunca divide o galpão: no conflito, 'enviar' fica None."""
    compartilhado = [i for i in itens if i['loja'] not in LOJAS_PREP_CENTER]
    galpao_sku = {}
    for i in compartilhado:
        if i['galpao'] is not None:
            k = _chave_sku(i)
            galpao_sku[k] = max(galpao_sku.get(k, 0), i['galpao'])
    grupos = {}
    for i in compartilhado:
        if i['necessidade'] > 0:
            grupos.setdefault(_chave_sku(i), []).append(i)

    conflitos = []
    for k, g in grupos.items():
        galpao = galpao_sku.get(k)
        soma = sum(i['necessidade'] for i in g)
        if galpao is not None and len(g) > 1 and soma > galpao:
            for i in g:
                i['enviar'] = None
                i['obs'] = (f"CONFLITO: galpão {galpao} un. para {soma} un. pedidas por "
                            f"{len(g)} estoques — a divisão é decisão do Thiago")
            conflitos.append({'sku': k, 'skus': g[0]['skus'], 'galpao': galpao, 'soma': soma,
                              'pedidos': [(i['mkt'], i['loja'], i['estoque_id'],
                                           i['necessidade']) for i in g]})
            continue
        for i in g:
            _limitar(i, galpao)
    for i in itens:
        if i['loja'] in LOJAS_PREP_CENTER and i['necessidade'] > 0:
            _limitar(i, i['galpao'], prep_center=True)
    return conflitos


def _limitar(i, galpao, prep_center=False):
    nec = i['necessidade']
    if galpao is None:
        i['enviar'] = nec
        i['obs'] = 'galpão desconhecido: conferir antes de enviar'
    elif galpao <= 0:
        i['enviar'] = 0
        i['obs'] = 'galpão zerado' + (' (prep center)' if prep_center else '') + ': nada a enviar'
    elif galpao < nec:
        i['enviar'] = galpao
        i['obs'] = (f"limitado ao galpão{' do prep center' if prep_center else ''} "
                    f"({galpao} un.); faltam {nec - galpao}")
    else:
        i['enviar'] = nec
        if prep_center:
            i['obs'] = 'prep center próprio'


def sinais_parado(itens):
    """Sinais 'full_parado' (sd._sinal), objeto = estoque_id, medida = R$ parado.
    Sem custo cadastrado: aparece com R$ 0 e o texto diz."""
    out = []
    for i in itens:
        if not i['parado'] or i['parado_suprimido']:
            continue
        if i['venda_dia'] == 0:
            motivo = f"{i['full']} un. no Full sem venda em 30 dias"
        else:
            motivo = (f"{i['full']} un. no Full = {i['semanas_full']:.0f} semanas de venda "
                      f"({sd._dec(i['venda_dia'])}/dia)")
        rs = i['rs_parado']
        valor = (f" · {sd._brl(rs)} parados no custo" if rs
                 else " · SKU sem custo cadastrado (R$ fora da soma)")
        out.append({**sd._sinal(
            i['loja'], 'FULL', i['anuncio'], i['skus'], f"Full parado: {motivo}{valor}",
            "Não enviar mais; fazer girar (preço/ads) ou retirar do Full",
            rs or 0, regra=REGRA_PARADO, objeto=i['estoque_id'], medida=rs or 0,
            periodo=f"foto {sd._d(i['data'])} · venda 30 dias até ontem"),
            'mkt': i['mkt']})
    return out


def resumo_por_loja(itens, lojas):
    """[(mkt, loja)] -> linhas do quadro do topo (R$ parado por loja)."""
    linhas = []
    for mkt, loja in lojas:
        da_loja = [i for i in itens if i['mkt'] == mkt and i['loja'] == loja]
        parados = [i for i in da_loja if i['parado']]
        linhas.append({
            'Loja': loja,
            'R$ parado no Full (custo)': sd._brl(sum(i['rs_parado'] or 0 for i in parados)),
            'Estoques parados': len(parados),
            'Sem venda em 30 dias': sum(1 for i in parados if i['venda_dia'] == 0),
            'Sem custo cadastrado': sum(1 for i in parados if not i['custo']),
            'Full acabando': sum(1 for i in da_loja if i['acabando']),
            'Unidades a enviar': sum(i['enviar'] or 0 for i in da_loja if i['acabando']),
        })
    return linhas


def validar_data(data, hoje):
    if data is None:
        return []
    if data <= hoje:
        return ["A data liberada tem de ser depois de hoje."]
    if data > hoje + timedelta(days=DIAS_DATA_MAX):
        return [f"Data liberada de no máximo {DIAS_DATA_MAX} dias à frente."]
    return []


def gravar_data(conn, mkt, loja, data, usuario, hoje, lojas_permitidas):
    """Grava a próxima data liberada da loja (None = limpar). Só INSERT."""
    if not (usuario or '').strip():
        raise ValueError("Usuário não identificado.")
    if loja not in lojas_permitidas:
        raise PermissionError(f"Sem permissão na loja {loja}")
    erros = validar_data(data, hoje)
    if erros:
        raise ValueError(' '.join(erros))
    cur = conn.cursor()
    try:
        cur.execute(SQL_INSERIR_DATA, {'marketplace': mkt, 'loja': loja, 'data': data,
                                       'usuario': usuario})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ============================================================
# LEITURA
# ============================================================

def ler_datas(conn, lojas):
    """{(mkt, loja): info} ou None se a tabela ainda não existe (SQL não aplicado)."""
    if not sd._ler(conn, SQL_EXISTE_DATAS, {})[0][0]:
        return None
    linhas = sd._ler(conn, SQL_DATAS, {'mkts': list(MARKETPLACES_FULL), 'lojas': list(lojas)})
    return {(m, l): {'data_envio': d, 'informado_por': p, 'informado_em': em}
            for m, l, d, p, em in linhas}


def montar(pacotes, custos, datas, hoje):
    """pacotes {mkt: pacote do sd.obter_pacote}. Devolve (itens, conflitos,
    prazos {(mkt, loja): (dias, texto, data)}, lojas [(mkt, loja)] com foto)."""
    itens, prazos, lojas = [], {}, []
    for mkt, pacote in pacotes.items():
        dados = pacote['dados']
        foto = dados.get('foto') or []
        ponte = sd._ponte_mapeada(dados.get('ponte') or [], dados.get('mapa') or {})
        _an, por_sku = sd.agregar_vendas(dados.get('vendas') or [])
        lojas_mkt = sorted({r[0] for r in foto})
        for loja in lojas_mkt:
            prazos[(mkt, loja)] = prazo_da_loja((datas or {}).get((mkt, loja)), hoje)
            lojas.append((mkt, loja))
        itens += avaliar(mkt, foto, ponte, por_sku, custos,
                         {l: prazos[(mkt, l)][0] for l in lojas_mkt})
    suprimir_parado_com_acabando(itens)
    conflitos = aplicar_galpao(itens)
    return itens, conflitos, prazos, lojas


def skus_da_leitura(pacotes):
    out = set()
    for pacote in pacotes.values():
        dados = pacote['dados']
        mapa = dados.get('mapa') or {}
        for _l, _a, _e, s, _f in sd._ponte_mapeada(dados.get('ponte') or [], mapa):
            if s:
                out.add(s)
    return out


# ============================================================
# TELA
# ============================================================

def _usuario():
    import streamlit
    return (streamlit.session_state.get('usuario') or {}).get('username', '')


def _nome(skus, nomes):
    return ' / '.join(nomes.get(s) or '(sem cadastro)' for s in skus)


def tabela_acabando(itens, nomes):
    linhas = sorted((i for i in itens if i['acabando']), key=lambda i: i['dias_full'])
    return pd.DataFrame({
        'SKU': [', '.join(i['skus']) for i in linhas],
        'Produto': [_nome(i['skus'], nomes) for i in linhas],
        'Anúncio': [i['anuncio'] for i in linhas],
        'Full disp.': [i['full'] for i in linhas],
        'Em transf.': [i['transf'] for i in linhas],
        'Venda/dia': [sd._dec(i['venda_dia']) for i in linhas],
        'Dias de Full': [f"{i['dias_full']:.0f}" for i in linhas],
        'Prazo (dias)': [i['prazo'] for i in linhas],
        f'Precisa ({DIAS_ALVO} dias)': [i['necessidade'] for i in linhas],
        'Galpão': ['?' if i['galpao'] is None else i['galpao'] for i in linhas],
        'Enviar': ['decisão do Thiago' if i['enviar'] is None else i['enviar'] for i in linhas],
        'Observação': [i['obs'] for i in linhas],
    })


def tabela_conflitos(conflitos, nomes):
    linhas = []
    for c in sorted(conflitos, key=lambda c: -c['soma']):
        linhas.append({
            'SKU': c['sku'], 'Produto': _nome(c['skus'], nomes),
            'Galpão (un.)': c['galpao'], 'Pedido somado (un.)': c['soma'],
            'Necessidade por loja': '; '.join(f"{l}: {n}" for _m, l, _e, n in c['pedidos']),
        })
    return pd.DataFrame(linhas)


def render(st, engine):
    agora = datetime.now(sd.BRT)
    hoje = agora.date()
    st.caption(
        f"Por loja, nunca somado. Venda/dia = maior entre 7 dias ÷ 7 e 30 dias ÷ 30, até "
        f"ontem. Alerta: o Full acaba antes do próximo envio chegar (data liberada informada "
        f"+ {PRAZO_CHEGADA_DIAS} dias) ou, sem data, em menos de {ANTECEDENCIA_DIAS} dias. "
        f"Enviar = venda/dia × {DIAS_ALVO} dias − Full − em transferência, limitado ao "
        f"galpão. Parado = Full ≥ {SEMANAS_PARADO} semanas de venda ou sem venda em 30 dias.")

    restricao = sd.restricao_de_lojas(engine)
    if restricao is not None and not restricao:
        st.caption("Nenhuma loja atribuída ao seu perfil.")
        return
    pacotes, lojas_vis = {}, []
    for mkt in MARKETPLACES_FULL:
        try:
            lojas = sd.lojas_visiveis(sd.obter_lojas_ml(engine, mkt), restricao)
            if not lojas:
                continue
            pacote, _mem = sd.obter_pacote(engine, hoje, lojas, mkt)
        except Exception:  # noqa: BLE001
            st.error(f"Não consegui ler o {sd.MARKETPLACES[mkt]['aba']} agora.")
            continue
        faltam = [k for k in ('foto', 'ponte', 'vendas') if k in pacote['erros']]
        if faltam:
            st.error(f"{sd.MARKETPLACES[mkt]['aba']}: não consegui ler "
                     + ', '.join(sd.ROTULO_FONTE.get(k, k) for k in faltam)
                     + ". Este marketplace ficou de fora.")
            continue
        pacotes[mkt] = pacote
        lojas_vis += lojas
    if not pacotes:
        return

    datas, cientes, erro_ciente = None, {}, False
    conn = engine.raw_connection()
    try:
        try:
            datas = ler_datas(conn, lojas_vis)
        except Exception:  # noqa: BLE001
            datas = None
            st.warning("Não consegui ler as datas liberadas agora: vale "
                       f"{ANTECEDENCIA_DIAS} dias para todas as lojas.")
        custos = ep.ler_custos(conn, skus_da_leitura(pacotes))
        for mkt in pacotes:
            try:
                cientes[mkt] = sd.ler_cientes(conn, lojas_vis, mkt) or []
            except Exception:  # noqa: BLE001
                erro_ciente = True
        itens, conflitos, prazos, lojas = montar(pacotes, custos, datas, hoje)
        nomes = ep.ler_nomes(conn, {s for i in itens if i['acabando'] or i['parado']
                                    for s in i['skus']})
    finally:
        conn.close()

    sql_aplicado = datas is not None
    pode = sd.pode_dar_ciente() and sql_aplicado and not erro_ciente

    st.markdown("#### R$ parado no Full por loja")
    st.dataframe(pd.DataFrame(resumo_por_loja(itens, lojas)), hide_index=True,
                 use_container_width=True)
    if not sql_aplicado:
        st.caption("Data liberada e Ciente do \"Full parado\": aguardando o SQL "
                   "(sql/full_por_loja.sql). Até lá vale o prazo fixo de "
                   f"{ANTECEDENCIA_DIAS} dias.")
    if 'Shopee Litstore(Yanni)' in lojas_vis:
        st.caption("Shopee Litstore(Yanni): sem estoque no sistema (só upload), fora desta aba.")

    if conflitos:
        st.markdown("#### ⚠️ Galpão disputado entre lojas")
        st.caption("O mesmo SKU precisa de envio em mais de uma loja e o galpão não cobre "
                   "todas. A tela não divide: a divisão é decisão do Thiago. "
                   "ML-YanniSP fica fora (prep center próprio).")
        st.dataframe(tabela_conflitos(conflitos, nomes), hide_index=True,
                     use_container_width=True)

    datas_foto = {}
    for i in itens:
        datas_foto[i['loja']] = max(datas_foto.get(i['loja'], i['data']), i['data'])
    atrasadas = set(ep.lojas_atrasadas(datas_foto, hoje, agora))

    for mkt, loja in lojas:
        st.markdown("---")
        st.subheader(loja)
        try:
            _render_loja(st, engine, mkt, loja, [i for i in itens
                                                 if i['mkt'] == mkt and i['loja'] == loja],
                         nomes, prazos[(mkt, loja)], (datas or {}).get((mkt, loja)),
                         cientes.get(mkt, []), hoje, lojas_vis, pode, sql_aplicado,
                         loja in atrasadas, datas_foto.get(loja))
        except Exception:  # noqa: BLE001 — uma loja quebrada não esconde as outras
            st.error(f"{loja}: não consegui mostrar esta loja agora; as outras seguem.")


def _render_loja(st, engine, mkt, loja, itens, nomes, prazo, info, cientes_linhas, hoje,
                 lojas_vis, pode, sql_aplicado, atrasada, data_foto):
    import streamlit
    if atrasada and data_foto is not None:
        st.warning(f"⚠️ Foto do Full atrasada: {pd.Timestamp(data_foto):%d/%m}.")
    dias, texto, _data = prazo
    linha = f"**Prazo do alerta:** {texto}."
    if info and info.get('informado_por'):
        linha += (f" Informado por {info['informado_por']} em "
                  f"{pd.Timestamp(info['informado_em']):%d/%m %H:%M}.")
    st.markdown(linha)
    if sql_aplicado and sd.pode_dar_ciente():
        _render_form_data(st, engine, mkt, loja, info, hoje, lojas_vis)

    df = tabela_acabando(itens, nomes)
    if df.empty:
        st.success(f"Nenhum Full acabando antes do prazo ({dias} dias).")
    else:
        st.markdown(f"**📦 Full acabando ({len(df)})**: enviar para cobrir {DIAS_ALVO} dias")
        st.dataframe(df, hide_index=True, use_container_width=True)

    sinais = sinais_parado(itens)
    if not sinais:
        return
    ativos, silenciados = sd.aplicar_cientes(sinais, cientes_linhas, hoje)
    ativos = sorted(ativos, key=lambda s: -s['em_jogo'])
    versao = streamlit.session_state.get('sin_versao', 0)
    st.markdown(f"**🧊 Full parado ({len(ativos)})**")
    if ativos:
        df = sd.tabela_sinais(ativos, nomes).drop(
            columns=['Loja', 'Aparece desde', 'Família (mesma peça, mesma loja)'])
        if pode:
            df.insert(0, 'Ciente', False)
            ed = st.data_editor(df, key=f"fpl_ed_{mkt}_{loja}_{versao}", hide_index=True,
                                use_container_width=True,
                                disabled=[c for c in df.columns if c != 'Ciente'])
            marcados = [ativos[k] for k in range(len(ativos)) if bool(ed['Ciente'].iloc[k])]
            if marcados:
                _render_form_ciente(st, engine, mkt, loja, marcados, hoje, lojas_vis)
        else:
            st.dataframe(df, hide_index=True, use_container_width=True)
    if silenciados:
        cientes = [dict(zip(sd.CIENTE_COLS, r)) for r in cientes_linhas]
        cientes = [c for c in cientes if c['loja'] == loja and c['regra'] == REGRA_PARADO]
        with st.expander(f"👁 Parados em acompanhamento ({len(silenciados)})"):
            st.dataframe(sd.tabela_silenciados(silenciados, cientes, hoje, nomes, ativos),
                         hide_index=True, use_container_width=True)


def _render_form_data(st, engine, mkt, loja, info, hoje, lojas_vis):
    import streamlit
    atual = (info or {}).get('data_envio')
    padrao = atual if atual is not None and atual > hoje else hoje + timedelta(days=7)
    with st.form(key=f"fpl_data_{mkt}_{loja}"):
        data = st.date_input("Próxima data de envio liberada", value=padrao,
                             min_value=hoje + timedelta(days=1),
                             max_value=hoje + timedelta(days=DIAS_DATA_MAX),
                             format="DD/MM/YYYY", key=f"fpl_data_in_{mkt}_{loja}")
        c1, c2 = st.columns(2)
        salvar = c1.form_submit_button("Salvar data")
        limpar = c2.form_submit_button("Limpar data (volta aos 21 dias)")
    if not (salvar or limpar):
        return
    conn = engine.raw_connection()
    try:
        gravar_data(conn, mkt, loja, None if limpar else data, _usuario(), hoje, lojas_vis)
    except Exception as e:  # noqa: BLE001
        st.error(f"Não gravei: {e}" if isinstance(e, (ValueError, PermissionError))
                 else "Não consegui gravar agora; nada mudou.")
        return
    finally:
        conn.close()
    streamlit.session_state['sin_versao'] = streamlit.session_state.get('sin_versao', 0) + 1
    st.rerun()


def _render_form_ciente(st, engine, mkt, loja, marcados, hoje, lojas_vis):
    import streamlit
    with st.form(key=f"fpl_form_{mkt}_{loja}"):
        st.markdown(f"**Ciente em {len(marcados)} \"Full parado\" da {loja}**")
        motivo = st.selectbox("Motivo", list(sd.MOTIVOS), format_func=sd.MOTIVOS.get,
                              key=f"fpl_mot_{mkt}_{loja}")
        nota = st.text_input("Nota (obrigatória em \"Outro\")", key=f"fpl_nota_{mkt}_{loja}")
        ate = st.date_input("Silenciar até",
                            value=hoje + timedelta(days=sd.DIAS_SILENCIO_PADRAO),
                            min_value=hoje,
                            max_value=hoje + timedelta(days=sd.DIAS_SILENCIO_MAX),
                            format="DD/MM/YYYY", key=f"fpl_ate_{mkt}_{loja}")
        if not st.form_submit_button("Registrar ciente"):
            return
    erros = sd.validar_ciente(motivo, nota, ate, hoje)
    if erros:
        st.error(' '.join(erros))
        return
    conn = engine.raw_connection()
    try:
        n = sd.gravar_cientes(conn, [(x, motivo, nota, ate) for x in marcados], _usuario(),
                              hoje, lojas_vis, mkt)
    except Exception:  # noqa: BLE001
        st.error("Não consegui registrar o ciente agora; nada foi gravado. Tente de novo.")
        return
    finally:
        conn.close()
    streamlit.session_state['sin_versao'] = streamlit.session_state.get('sin_versao', 0) + 1
    st.success(f"Ciente registrado em {n} sinal(is).")
    st.rerun()
