# 03 | Camada Ouro (modelo dimensional e views)
# Cria as dimensões, tabelas fato e views analíticas. Dados pessoais do relator são anonimizados via hash.

CATALOGO, SCHEMA = "workspace", "luz_segura"
S = f"{CATALOGO}.{SCHEMA}"
LIMITE_ESCURO, LIMITE_FRACO = 1.5, 5  # faixas de luminosidade em lux

# Dimensões
spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_local AS
SELECT ROW_NUMBER() OVER (ORDER BY id_local) AS sk_local,
       id_local, nome, bairro, latitude, longitude, tipo_ponto, 'Osasco' AS municipio
FROM {S}.prata_pontos""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_tempo AS
SELECT CAST(date_format(h,'yyyyMMddHH') AS BIGINT) AS sk_tempo,
       CAST(h AS DATE) AS data, hour(h) AS hora,
       CASE WHEN hour(h) BETWEEN 18 AND 20 THEN '18h-20h'
            WHEN hour(h) BETWEEN 21 AND 23 THEN '21h-23h'
            ELSE 'fora do periodo noturno' END AS faixa_horaria,
       date_format(h,'EEEE') AS dia_semana
FROM (SELECT DISTINCT date_trunc('hour', data_hora) AS h FROM (
        SELECT data_hora FROM {S}.prata_clima
        UNION ALL SELECT data_hora FROM {S}.prata_relatos
        UNION ALL SELECT data_hora FROM {S}.prata_sensor))""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_clima AS
SELECT CAST(date_format(data_hora,'yyyyMMddHH') AS BIGINT) AS sk_clima,
       nebulosidade_pct, precipitacao_mm,
       CASE WHEN precipitacao_mm > 0 THEN 'com chuva' ELSE 'sem chuva' END AS condicao_chuva
FROM {S}.prata_clima""")

# Fatos
spark.sql(f"""CREATE OR REPLACE TABLE {S}.fato_ocorrencia AS
SELECT l.sk_local,
       CAST(date_format(r.data_hora,'yyyyMMddHH') AS BIGINT) AS sk_tempo,
       CAST(date_format(r.data_hora,'yyyyMMddHH') AS BIGINT) AS sk_clima,
       'relato' AS tipo_evento, r.id_relato AS id_origem, r.classe,
       CAST(NULL AS DOUBLE) AS lux_estimado,
       sha2(r.relator_contato, 256) AS relator_hash,
       r.origem AS origem_dado
FROM {S}.prata_relatos r JOIN {S}.dim_local l ON l.id_local = r.id_local
UNION ALL
SELECT l.sk_local,
       CAST(date_format(s.data_hora,'yyyyMMddHH') AS BIGINT),
       CAST(date_format(s.data_hora,'yyyyMMddHH') AS BIGINT),
       'medicao', s.id_leitura,
       CASE WHEN s.lux_estimado < {LIMITE_ESCURO} THEN 'escuro'
            WHEN s.lux_estimado < {LIMITE_FRACO} THEN 'fraco' ELSE 'iluminado' END,
       s.lux_estimado, CAST(NULL AS STRING), s.origem
FROM {S}.prata_sensor s JOIN {S}.dim_local l ON l.id_local = s.id_local""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.fato_chamado AS
SELECT c.id_chamado, l.sk_local, c.data_abertura, c.data_conserto,
       CASE WHEN c.data_conserto IS NULL THEN 'pendente' ELSE 'resolvido' END AS status,
       DATEDIFF(c.data_conserto, c.data_abertura) AS dias_para_conserto,
       CASE WHEN c.data_conserto IS NULL THEN DATEDIFF(current_date(), c.data_abertura) END AS dias_pendente
FROM {S}.prata_chamados c JOIN {S}.dim_local l ON l.id_local = c.id_local""")

# Views analíticas para o painel
spark.sql(f"""CREATE OR REPLACE VIEW {S}.vw_painel_pontos AS
WITH med AS (
  SELECT sk_local,
         AVG(CASE WHEN tipo_evento = 'medicao' THEN lux_estimado END) AS lux_medio,
         COUNT_IF(tipo_evento = 'relato') AS relatos_total,
         COUNT_IF(tipo_evento = 'relato' AND classe = 'escuro') AS relatos_escuro
  FROM {S}.fato_ocorrencia GROUP BY sk_local),
ch AS (
  SELECT sk_local, status AS chamado_status, dias_pendente,
         ROW_NUMBER() OVER (PARTITION BY sk_local ORDER BY data_abertura DESC) AS rn
  FROM {S}.fato_chamado)
SELECT l.id_local, l.nome, l.bairro, l.latitude, l.longitude, l.tipo_ponto,
       ROUND(m.lux_medio, 2) AS lux_medio,
       COALESCE(m.relatos_total, 0) AS relatos_total,
       COALESCE(m.relatos_escuro, 0) AS relatos_escuro,
       CASE WHEN m.lux_medio IS NULL THEN 'Sem medicao'
            WHEN m.lux_medio < {LIMITE_ESCURO} THEN 'Escuro'
            WHEN m.lux_medio < {LIMITE_FRACO} THEN 'Fraco' ELSE 'Iluminado' END AS situacao,
       CASE WHEN m.lux_medio < {LIMITE_ESCURO} AND COALESCE(m.relatos_escuro, 0) >= 1 THEN 1 ELSE 0 END AS critico,
       COALESCE(c.chamado_status, 'sem chamado') AS chamado_status,
       c.dias_pendente
FROM {S}.dim_local l
LEFT JOIN med m ON m.sk_local = l.sk_local
LEFT JOIN ch c ON c.sk_local = l.sk_local AND c.rn = 1""")

spark.sql(f"""CREATE OR REPLACE VIEW {S}.vw_painel_bairros AS
SELECT bairro,
       COUNT(*) AS pontos,
       SUM(critico) AS criticos,
       ROUND(100 * SUM(critico) / COUNT(*), 0) AS pct_criticos,
       ROUND(AVG(lux_medio), 1) AS lux_medio_bairro,
       ROUND(MIN(lux_medio), 2) AS lux_pior_ponto,
       COUNT_IF(chamado_status <> 'sem chamado') AS chamados,
       COUNT_IF(chamado_status = 'resolvido') AS resolvidos,
       COALESCE(MAX(dias_pendente), 0) AS maior_pendencia_dias
FROM {S}.vw_painel_pontos GROUP BY bairro""")