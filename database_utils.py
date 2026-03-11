Relatório de Desenvolvimento – Projeto NALA (11/03/2026)
Este relatório resume as implementações e decisões arquiteturais tomadas para o sistema de gestão de e-commerce do Grupo Nala na data de hoje.

1. Alterações no Banco de Dados (PostgreSQL/Neon):

Tabela dim_lojas: Adição da coluna custo_flex (DECIMAL) para permitir a gestão dinâmica do custo logístico por loja através da interface.

Nova Tabela fact_vendas_descartadas: Criada para registar vendas com status "Cancelado", "Devolvido" ou "Mediação", permitindo a análise futura de faturamento perdido.

Tabela fact_vendas_pendentes: Atualizada para receber vendas com "Divergência Financeira" (quando o cálculo dos itens do carrinho difere do total do arquivo em mais de R$ 5,00).

2. Evolução do Processador Mercado Livre (processar_ml.py):

Lógica de Carrinhos (Parent-Child): Implementação de distribuição proporcional. O sistema identifica pedidos com múltiplos itens, captura os valores financeiros da linha mestre e distribui-os entre os SKUs do carrinho com base no peso do preco_unit_anuncio × quantidade.

FLEX Dinâmico: O custo do frete FLEX deixa de ser fixo (R$ 12,90) e passa a ser consultado na tabela de lojas via database_utils.py.

Integridade Estrutural: O arquivo deve manter a arquitetura original de loop manual e variáveis de estado (aproximadamente 480 linhas) para garantir o tratamento de todas as variações de colunas do Excel do ML.

3. Evolução do Processador Shopee (processar_shopee.py):

Chave de Duplicidade: Alterada de ID do pedido para a chave composta (ID do pedido, SKU). Esta mudança é vital para permitir que pedidos com múltiplos produtos diferentes sejam processados sem serem marcados erroneamente como duplicados.

4. Evolução do database_utils.py (v2.5):

Restauração integral de todas as funções de busca de custos e SKUs.

Inclusão da função gravar_venda_descartada e buscar_custo_flex.
