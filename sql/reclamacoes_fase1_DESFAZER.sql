-- =============================================================================
-- DESFAZER reclamacoes_fase1.sql  (so o que ele criou; nao toca em nada do sistema)
-- ARQUIVO: C:\Users\thiag\Sistema-nala\sql\reclamacoes_fase1_DESFAZER.sql
-- ONDE: "Gestão Marketplaces" (still-shape-14526725) / branch production / neondb
-- CREDENCIAL: DONO do banco (neondb_owner). Apaga os dados coletados.
-- =============================================================================
BEGIN;
  REVOKE ALL ON fact_reclamacoes, fact_reputacao_loja, dim_fonte_dados FROM coletor_reclamacoes;
  REVOKE ALL ON SCHEMA public FROM coletor_reclamacoes;
  DROP ROLE IF EXISTS coletor_reclamacoes;
  DROP TABLE IF EXISTS fact_reclamacoes;
  DROP TABLE IF EXISTS fact_reputacao_loja;
COMMIT;
