-- 04 | Consultas analíticas (camada Ouro)

USE CATALOG workspace;

USE SCHEMA luz_segura;

-- 1. Quais pontos são os mais escuros?
SELECT l.bairro, COALESCE(l.nome, 'Parada sem nome') AS parada, l.latitude, l.longitude,
       COUNT_IF(f.tipo_evento = 'relato' AND f.classe = 'escuro') AS relatos_escuro,
       ROUND(AVG(CASE WHEN f.tipo_evento = 'medicao' THEN f.lux_estimado END), 2) AS lux_medio
FROM fato_ocorrencia f JOIN dim_local l ON l.sk_local = f.sk_local
GROUP BY l.bairro, l.nome, l.latitude, l.longitude
ORDER BY lux_medio ASC
LIMIT 15;

-- 2. Resumo de criticidade e luminosidade por bairro (crítico: lux médio < 1,5 e ao menos 1 relato "escuro")
WITH ponto AS (
  SELECT l.bairro, l.sk_local,
         COUNT_IF(f.tipo_evento = 'relato' AND f.classe = 'escuro') AS escuro,
         AVG(CASE WHEN f.tipo_evento = 'medicao' THEN f.lux_estimado END) AS lux
  FROM fato_ocorrencia f JOIN dim_local l ON l.sk_local = f.sk_local
  GROUP BY l.bairro, l.sk_local)
SELECT bairro, COUNT(*) AS pontos,
       SUM(CASE WHEN lux < 1.5 AND escuro >= 1 THEN 1 ELSE 0 END) AS criticos,
       ROUND(100 * SUM(CASE WHEN lux < 1.5 AND escuro >= 1 THEN 1 ELSE 0 END) / COUNT(*), 0) AS pct_criticos,
       ROUND(AVG(lux), 1) AS lux_medio_bairro, ROUND(MIN(lux), 2) AS lux_pior_ponto
FROM ponto GROUP BY bairro ORDER BY pct_criticos DESC, lux_pior_ponto ASC;

-- 3. Em que faixa horária cada bairro concentra relatos "escuro"?
SELECT l.bairro, t.faixa_horaria, COUNT(*) AS relatos_escuro
FROM fato_ocorrencia f
JOIN dim_local l ON l.sk_local = f.sk_local
JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
WHERE f.tipo_evento = 'relato' AND f.classe = 'escuro'
GROUP BY l.bairro, t.faixa_horaria
ORDER BY l.bairro, t.faixa_horaria;

-- 4. Relação entre condições climáticas (Open-Meteo) e relatos
SELECT COALESCE(c.condicao_chuva, 'sem dado de clima') AS condicao,
       COUNT(*) AS relatos,
       COUNT_IF(f.classe = 'escuro') AS relatos_escuro,
       ROUND(100 * COUNT_IF(f.classe = 'escuro') / COUNT(*), 0) AS pct_escuro
FROM fato_ocorrencia f
LEFT JOIN dim_clima c ON c.sk_clima = f.sk_clima
WHERE f.tipo_evento = 'relato'
GROUP BY COALESCE(c.condicao_chuva, 'sem dado de clima');

-- 5. Taxa de resolução e maior tempo de espera dos chamados por bairro
SELECT l.bairro, COUNT(*) AS chamados,
       COUNT_IF(c.status = 'resolvido') AS resolvidos,
       ROUND(100 * COUNT_IF(c.status = 'resolvido') / COUNT(*), 0) AS pct_resolvidos,
       ROUND(AVG(c.dias_para_conserto), 1) AS dias_medios_conserto,
       COALESCE(MAX(c.dias_pendente), 0) AS maior_pendencia_dias
FROM fato_chamado c JOIN dim_local l ON l.sk_local = c.sk_local
GROUP BY l.bairro
ORDER BY maior_pendencia_dias DESC;

-- 6. Concordância entre os relatos dos cidadãos e as medições dos sensores
SELECT p.bairro, COUNT(*) AS relatos,
       COUNT_IF(LOWER(p.situacao) = f.classe) AS concordam,
       ROUND(100 * COUNT_IF(LOWER(p.situacao) = f.classe) / COUNT(*), 0) AS pct_concordancia
FROM fato_ocorrencia f
JOIN dim_local l ON l.sk_local = f.sk_local
JOIN vw_painel_pontos p ON p.id_local = l.id_local
WHERE f.tipo_evento = 'relato'
GROUP BY p.bairro ORDER BY pct_concordancia ASC;