/*
 * CAPTURADOR DE CAMPANHAS — Sistema Nala
 *
 * O QUE É
 *   Um "bookmarklet": um favorito do navegador que, em vez de abrir um site,
 *   executa uma instrução na página que já está aberta. Você abre o painel de
 *   campanhas do Mercado Livre, clica no favorito, e ele baixa um CSV com
 *   ROAS objetivo, orçamento diário e o sinal da plataforma de cada campanha.
 *
 * POR QUE ELE EXISTE
 *   Esses três campos não saem em relatório nenhum — só existem na tela. E são
 *   eles que dizem qual alavanca foi puxada: no ML se opera pelo ROAS
 *   objetivo, não pelo orçamento.
 *
 * POR QUE NÃO É UM ROBÔ QUE RODA SOZINHO
 *   Um robô precisaria guardar a senha, quebraria na verificação em duas
 *   etapas e de novo a cada mudança de tela do ML. Aqui quem clica é você, na
 *   sessão que já está aberta: nada de senha guardada, nada rodando sem
 *   você mandar. O custo é um clique; a troca vale a pena.
 *
 * COMO INSTALAR (uma vez só)
 *   1. Barra de favoritos do navegador → botão direito → "Adicionar página".
 *   2. Nome: "📸 Capturar campanhas".
 *   3. No campo URL, cole a LINHA ÚNICA gerada por `gerar_bookmarklet.py`
 *      (ela começa com "javascript:").
 *
 * COMO USAR (toda vez)
 *   1. Abra Publicidade → Campanhas, com as colunas Nome da campanha,
 *      Orçamento diário e ROAS Objetivo visíveis.
 *   2. Clique no favorito. Ele percorre as páginas sozinho.
 *   3. Baixa `campanhas_AAAA-MM-DD.csv` → suba na aba "🎯 ROAS objetivo".
 *
 * SE NÃO FUNCIONAR
 *   Alguns sites bloqueiam bookmarklet por política de segurança (CSP). Nesse
 *   caso: F12 → aba "Console" → cole o conteúdo deste arquivo → Enter. Faz
 *   exatamente a mesma coisa.
 */

(async function capturarCampanhas() {
  'use strict';

  const ESPERA_PAGINA = 1200;   // ms após clicar em "próxima"
  const MAX_PAGINAS = 40;       // trava de segurança contra laço infinito

  const norm = (s) => (s || '')
    .replace(/ /g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // O nome da campanha vem colado ao "1 anúncio patrocinado". Esse sufixo
  // precisa sair: é o nome limpo que casa com o relatório de ads, que é o que
  // liga a campanha ao MLB e ao SKU.
  const limparNome = (s) => norm(s)
    .replace(/\s*\d+\s+an[úu]ncios?\s+patrocinados?.*$/i, '')
    .replace(/^Selecionar campanha\s*/i, '')
    .trim();

  // O sinal da plataforma vem como "APRENDENDO Sua campanha vai se
  // estabilizar em 2 dias." Guardar a frase faria a comparação entre fotos
  // disparar sozinha quando "2 dias" virasse "1 dia".
  const rotulo = (s) => {
    const t = norm(s);
    for (const r of ['APRENDENDO', 'Excelente', 'Bom', 'Regular', 'Ruim']) {
      if (t.toUpperCase().startsWith(r.toUpperCase())) return r;
    }
    return t.slice(0, 40);
  };

  function acharTabela() {
    const tabelas = [...document.querySelectorAll('table')];
    // A tabela certa é a que tem cabeçalho de campanha E de ROAS objetivo.
    // Painéis costumam ter outras tabelas (resumo, tooltips) na mesma tela.
    return tabelas.find((t) => {
      const cab = norm(t.querySelector('tr')?.innerText || '').toLowerCase();
      return cab.includes('campanha') &&
             (cab.includes('roas objetivo') || cab.includes('roas alvo'));
    }) || null;
  }

  function indices(tabela) {
    const celulas = [...(tabela.querySelector('tr')?.children || [])];
    const achar = (...termos) => celulas.findIndex((c) => {
      const t = norm(c.innerText).toLowerCase();
      return termos.some((x) => t.includes(x));
    });
    return {
      nome: achar('nome da campanha', 'campanha'),
      roas: achar('roas objetivo', 'roas alvo', 'meta de roas'),
      orc: achar('orçamento diário', 'orcamento diario', 'orçamento'),
      diag: achar('diagnóstico', 'diagnostico'),
    };
  }

  function lerPagina(tabela, ix, destino, vistos) {
    const linhas = [...tabela.querySelectorAll('tr')].slice(1);
    let novas = 0;
    for (const tr of linhas) {
      const c = [...tr.children];
      if (ix.nome < 0 || !c[ix.nome]) continue;
      const nome = limparNome(c[ix.nome].innerText);
      if (!nome || vistos.has(nome)) continue;
      vistos.add(nome);
      novas++;
      destino.push({
        campanha: nome,
        // O ROAS objetivo fica dentro de um campo editável; pegar o innerText
        // da célula traz junto o rótulo do lápis de editar.
        roas: norm(c[ix.roas]?.innerText).replace(/[^\d.,]/g, ''),
        orc: norm(c[ix.orc]?.innerText).replace(/[^\d.,]/g, ''),
        diag: ix.diag >= 0 ? rotulo(c[ix.diag]?.innerText) : '',
      });
    }
    return novas;
  }

  function botaoProxima() {
    const candidatos = [...document.querySelectorAll(
      'button, a, li, [role="button"]')];
    return candidatos.find((b) => {
      if (b.getAttribute('aria-disabled') === 'true' || b.disabled) return false;
      const alvo = (b.getAttribute('aria-label') || b.innerText || '')
        .toLowerCase();
      return alvo.includes('próxima') || alvo.includes('proxima') ||
             alvo.includes('next') || alvo.includes('seguinte');
    }) || null;
  }

  // ---------------------------------------------------------------

  const tabela = acharTabela();
  if (!tabela) {
    alert('Não achei a tabela de campanhas nesta tela.\n\n' +
          'Abra Publicidade → Campanhas e confirme que as colunas ' +
          '"Nome da campanha" e "ROAS Objetivo" estão visíveis.');
    return;
  }

  const ix = indices(tabela);
  if (ix.nome < 0 || ix.roas < 0) {
    alert('A tabela está sem a coluna "Nome da campanha" ou "ROAS Objetivo".\n\n' +
          'Ative essas colunas no painel e clique de novo.');
    return;
  }

  const dados = [];
  const vistos = new Set();
  let pagina = 0;

  while (pagina < MAX_PAGINAS) {
    const atual = acharTabela();
    if (!atual) break;
    const novas = lerPagina(atual, indices(atual), dados, vistos);
    pagina++;

    const proxima = botaoProxima();
    // Sem botão, ou uma página inteira sem nome novo: acabou. A segunda
    // condição protege do caso em que o botão existe mas não avança de fato —
    // aí o laço rodaria até o teto relendo a mesma página.
    if (!proxima || (novas === 0 && pagina > 1)) break;
    proxima.click();
    await new Promise((r) => setTimeout(r, ESPERA_PAGINA));
  }

  if (!dados.length) {
    alert('A tabela foi encontrada, mas não li nenhuma campanha.');
    return;
  }

  const agora = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const carimbo = `${agora.getFullYear()}-${pad(agora.getMonth() + 1)}-` +
                  `${pad(agora.getDate())}T${pad(agora.getHours())}:` +
                  `${pad(agora.getMinutes())}:${pad(agora.getSeconds())}`;

  const mercado = location.hostname.includes('shopee') ? 'SHOPEE' : 'MERCADO LIVRE';
  const escapar = (v) => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;

  const linhas = [
    'data_captura;marketplace;campanha;roas_objetivo;orcamento_diario;diagnostico_ml',
    ...dados.map((d) => [carimbo, mercado, d.campanha, d.roas, d.orc, d.diag]
      .map(escapar).join(';')),
  ];

  // BOM na frente para o Excel abrir os acentos certos ao dar duplo clique.
  const blob = new Blob(['﻿' + linhas.join('\r\n')],
                        { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `campanhas_${carimbo.slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  alert(`✅ ${dados.length} campanha(s) capturada(s) em ${pagina} página(s).\n\n` +
        `Arquivo: campanhas_${carimbo.slice(0, 10)}.csv\n\n` +
        `Suba na aba "🎯 ROAS objetivo" do Sistema Nala.`);
})();
