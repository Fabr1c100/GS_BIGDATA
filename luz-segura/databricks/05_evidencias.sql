-- 05 | Validação e auditoria das camadas

USE CATALOG workspace;

USE SCHEMA luz_segura;

-- Histórico de volumetria da camada Bronze por carga (lote 1, lote 2 e reexecução)
SELECT tabela, momento, linhas FROM evidencia_lotes ORDER BY tabela, momento;

-- Visão geral das camadas (Prata e Ouro)
SELECT 'prata_pontos' AS tabela, COUNT(*) AS linhas FROM prata_pontos
UNION ALL SELECT 'prata_clima', COUNT(*) FROM prata_clima
UNION ALL SELECT 'prata_sensor', COUNT(*) FROM prata_sensor
UNION ALL SELECT 'prata_relatos', COUNT(*) FROM prata_relatos
UNION ALL SELECT 'prata_chamados', COUNT(*) FROM prata_chamados
UNION ALL SELECT 'dim_local', COUNT(*) FROM dim_local
UNION ALL SELECT 'dim_tempo', COUNT(*) FROM dim_tempo
UNION ALL SELECT 'dim_clima', COUNT(*) FROM dim_clima
UNION ALL SELECT 'fato_ocorrencia', COUNT(*) FROM fato_ocorrencia
UNION ALL SELECT 'fato_chamado', COUNT(*) FROM fato_chamado;

-- Validação de anonimização (LGPD): confirma que a fato guarda apenas o relator_hash, sem nome ou contato
DESCRIBE fato_ocorrencia;

-- Conferência do hash por origem (relatos simulados têm hash; relatos do app ficam nulos por não coletarem contato)
SELECT origem_dado, COUNT(*) AS relatos, COUNT(relator_hash) AS com_hash
FROM fato_ocorrencia WHERE tipo_evento = 'relato' GROUP BY origem_dado;

-- Views consolidadas consumidas pelo painel
SELECT * FROM vw_painel_bairros ORDER BY pct_criticos DESC;

SELECT * FROM vw_painel_pontos ORDER BY lux_medio ASC LIMIT 20;