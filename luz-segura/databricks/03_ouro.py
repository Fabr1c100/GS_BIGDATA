S = "workspace.gs"

spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_local AS
SELECT ROW_NUMBER() OVER (ORDER BY id_local) AS sk_local,
       id_local, endereco, bairro, tipo_ponto, 'Osasco' AS municipio
FROM {S}.prata_pontos""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_tempo AS
SELECT CAST(date_format(data_hora,'yyyyMMddHH') AS BIGINT) AS sk_tempo,
       CAST(data_hora AS DATE) AS data, hour(data_hora) AS hora,
       CASE WHEN hour(data_hora) BETWEEN 18 AND 20 THEN '18h-20h'
            WHEN hour(data_hora) BETWEEN 21 AND 23 THEN '21h-23h'
            ELSE 'fora do periodo noturno' END AS faixa_horaria,
       date_format(data_hora,'EEEE') AS dia_semana
FROM (SELECT DISTINCT data_hora FROM {S}.prata_clima)""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.dim_clima AS
SELECT CAST(date_format(data_hora,'yyyyMMddHH') AS BIGINT) AS sk_clima,
       nebulosidade_pct, precipitacao_mm,
       CASE WHEN precipitacao_mm > 0 THEN 'com chuva' ELSE 'sem chuva' END AS condicao_chuva
FROM {S}.prata_clima""")

# LGPD: nome e contato do relator NAO entram na Ouro, so o hash do contato
spark.sql(f"""CREATE OR REPLACE TABLE {S}.fato_ocorrencia AS
SELECT l.sk_local,
       CAST(date_format(r.data_hora,'yyyyMMddHH') AS BIGINT) AS sk_tempo,
       CAST(date_format(r.data_hora,'yyyyMMddHH') AS BIGINT) AS sk_clima,
       'relato' AS tipo_evento, r.id_relato AS id_origem, r.classe,
       CAST(NULL AS DOUBLE) AS lux_estimado,
       sha2(r.relator_contato, 256) AS relator_hash
FROM {S}.prata_relatos r JOIN {S}.dim_local l ON l.id_local = r.id_local
UNION ALL
SELECT l.sk_local,
       CAST(date_format(s.data_hora,'yyyyMMddHH') AS BIGINT),
       CAST(date_format(s.data_hora,'yyyyMMddHH') AS BIGINT),
       'medicao', s.id_leitura,
       CASE WHEN s.lux_estimado < 1.5 THEN 'escuro'
            WHEN s.lux_estimado < 5 THEN 'fraco' ELSE 'iluminado' END,
       s.lux_estimado, CAST(NULL AS STRING)
FROM {S}.prata_sensor s JOIN {S}.dim_local l ON l.id_local = s.id_local""")

spark.sql(f"""CREATE OR REPLACE TABLE {S}.fato_chamado AS
SELECT c.id_chamado, l.sk_local, c.data_abertura, c.data_conserto,
       CASE WHEN c.data_conserto IS NULL THEN 'pendente' ELSE 'resolvido' END AS status,
       DATEDIFF(c.data_conserto, c.data_abertura) AS dias_para_conserto,
       CASE WHEN c.data_conserto IS NULL THEN DATEDIFF(current_date(), c.data_abertura) END AS dias_pendente
FROM {S}.prata_chamados c JOIN {S}.dim_local l ON l.id_local = c.id_local""")