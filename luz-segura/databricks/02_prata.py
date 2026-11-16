# 02 | Camada Prata (tratamento e qualidade)
# Tipagem, validação de regras de qualidade, auditoria de descartes e MERGE nas tabelas finais.

from pyspark.sql import functions as F, Window
from functools import reduce

CATALOGO, SCHEMA = "workspace", "luz_segura"
S = f"{CATALOGO}.{SCHEMA}"
FATOR_LUX = 0.008   # conversão do sensor: lux = valor_adc * 0.008
RAIO_MAX_M = 2100   # limite de 2,1 km da referência do bairro para considerar dentro da área

def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = F.sin((p2 - p1) / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(F.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371000 * F.asin(F.sqrt(a))

def marcar(df, regras):
    for r in regras:
        df = df.withColumn(r, F.coalesce(F.col(r), F.lit(False)))
    ruim = reduce(lambda a, b: a | b, [F.col(r) for r in regras])
    return df.withColumn("valido", ~ruim)

def contagem(df, regras, tabela):
    cols = [F.lit(tabela).alias("tabela"), F.count("*").alias("total_lido")]
    cols += [F.sum(F.col(r).cast("int")).alias(r) for r in regras]
    cols.append(F.sum((~F.col("valido")).cast("int")).alias("total_barrado"))
    display(df.agg(*cols))

def merge(df, tabela, chave):
    df.createOrReplaceTempView("lote")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {S}.{tabela} AS SELECT * FROM lote WHERE 1=0")
    spark.sql(f"""MERGE INTO {S}.{tabela} AS alvo USING lote AS novo
                  ON alvo.{chave} = novo.{chave}
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")

# Bairros de referência e pontos de parada (OpenStreetMap)
refs = (spark.table(f"{S}.bronze_bairros_referencia")
    .select("bairro", F.col("latitude").cast("double").alias("lat_ref"), F.col("longitude").cast("double").alias("lon_ref")))
refs.write.mode("overwrite").saveAsTable(f"{S}.prata_bairros_referencia")

bruto = spark.table(f"{S}.bronze_pontos_osm")
ultimo = Window.partitionBy("id_local").orderBy(F.col("carregado_em").desc())
pontos = (bruto.withColumn("rn", F.row_number().over(ultimo)).filter("rn = 1")
    .select("id_local", F.nullif(F.trim("nome"), F.lit("")).alias("nome"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"), "tipo_ponto", "origem"))

# Como a base de pontos chega completa todo dia, deduplicamos mantendo uma linha por parada
display(spark.createDataFrame([(bruto.count(), pontos.count())], ["linhas_na_bronze", "paradas_distintas"]))

# Associa cada parada ao bairro de referência mais próximo
cand = (pontos.crossJoin(refs)
    .withColumn("dist_ref_m", haversine_m(F.col("latitude"), F.col("longitude"), F.col("lat_ref"), F.col("lon_ref"))))
mais_perto = Window.partitionBy("id_local").orderBy("dist_ref_m")
pontos = (cand.withColumn("rn", F.row_number().over(mais_perto)).filter("rn = 1")
    .select("id_local", "nome", "latitude", "longitude", "tipo_ponto", "bairro",
            F.round("dist_ref_m", 0).alias("dist_ref_m"), "origem")
    .withColumn("r_id_nulo", F.col("id_local").isNull())
    .withColumn("r_coordenada_invalida", F.col("latitude").isNull() | F.col("longitude").isNull()
                | ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180))
    .withColumn("r_fora_da_area", F.col("dist_ref_m") > RAIO_MAX_M))
rp = ["r_id_nulo", "r_coordenada_invalida", "r_fora_da_area"]
pontos = marcar(pontos, rp); contagem(pontos, rp, "prata_pontos")
merge(pontos.filter("valido").select("id_local", "nome", "latitude", "longitude", "tipo_ponto",
                                     "bairro", "dist_ref_m", "origem"), "prata_pontos", "id_local")

ids = spark.table(f"{S}.prata_pontos").select("id_local").withColumn("existe", F.lit(True))

# Clima: faz o parse do JSON hourly e explode as listas paralelas de tempo, nuvens e chuva
schema_hourly = "struct<time:array<string>,cloud_cover:array<double>,precipitation:array<double>>"
clima = (spark.table(f"{S}.bronze_clima")
    .withColumn("h", F.from_json(F.col("hourly"), schema_hourly))
    .select(F.explode(F.arrays_zip(
        F.col("h.time").alias("time"),
        F.col("h.cloud_cover").alias("cloud_cover"),
        F.col("h.precipitation").alias("precipitation"))).alias("h"))
    .select(F.lit("Osasco").alias("cidade"),
            F.to_timestamp(F.col("h.time"), "yyyy-MM-dd'T'HH:mm").alias("data_hora"),
            F.col("h.cloud_cover").cast("int").alias("nebulosidade_pct"),
            F.col("h.precipitation").cast("double").alias("precipitacao_mm"))
    .dropDuplicates(["cidade", "data_hora"])
    .withColumn("chave", F.concat_ws("|", "cidade", F.col("data_hora").cast("string")))
    .withColumn("r_nebulosidade_invalida", (F.col("nebulosidade_pct") < 0) | (F.col("nebulosidade_pct") > 100))
    .withColumn("r_chuva_negativa", F.col("precipitacao_mm") < 0)
    .withColumn("r_valor_nulo", F.col("nebulosidade_pct").isNull() | F.col("precipitacao_mm").isNull()))
rc = ["r_nebulosidade_invalida", "r_chuva_negativa", "r_valor_nulo"]
clima = marcar(clima, rc); contagem(clima, rc, "prata_clima")
merge(clima.filter("valido").select("chave", "cidade", "data_hora", "nebulosidade_pct", "precipitacao_mm"),
      "prata_clima", "chave")

# Leituras do sensor de luminosidade
sensor = (spark.table(f"{S}.bronze_sensor").dropDuplicates(["id_leitura"])
    .select("id_leitura", "id_dispositivo", "id_local",
            F.to_timestamp("data_hora").alias("data_hora"),
            F.col("valor_adc").cast("int").alias("valor_adc"), "origem")
    .join(ids, "id_local", "left")
    .withColumn("r_adc_nulo", F.col("valor_adc").isNull())
    .withColumn("r_adc_fora_faixa", (F.col("valor_adc") < 0) | (F.col("valor_adc") > 4095))
    .withColumn("r_local_inexistente", F.col("existe").isNull())
    .withColumn("lux_estimado", F.round(F.col("valor_adc") * FATOR_LUX, 2)))
rs = ["r_adc_nulo", "r_adc_fora_faixa", "r_local_inexistente"]
sensor = marcar(sensor, rs); contagem(sensor, rs, "prata_sensor")
merge(sensor.filter("valido").select("id_leitura", "id_dispositivo", "id_local", "data_hora",
                                     "valor_adc", "lux_estimado", "origem"), "prata_sensor", "id_leitura")

# Relatos de usuários (simulados e app)
# Considera duplicado o relato no mesmo ponto, contato e classe dentro de 15 min (no app, sem contato, usa ponto + classe)
mesmo_relato = Window.partitionBy("id_local", F.coalesce(F.col("relator_contato"), F.lit("")), "classe").orderBy("data_hora")
relatos = (spark.table(f"{S}.bronze_relatos").dropDuplicates(["id_relato"])
    .select("id_relato", "id_local", F.to_timestamp("data_hora").alias("data_hora"),
            F.lower(F.trim("classe")).alias("classe"), "relator_nome", "relator_contato", "origem")
    .join(ids, "id_local", "left")
    .withColumn("min_desde_anterior",
                (F.col("data_hora").cast("long") - F.lag("data_hora").over(mesmo_relato).cast("long")) / 60)
    .withColumn("r_classe_invalida", ~F.col("classe").isin("iluminado", "fraco", "escuro"))
    .withColumn("r_local_inexistente", F.col("existe").isNull())
    .withColumn("r_duplicado", F.col("min_desde_anterior") < 15))
rr = ["r_classe_invalida", "r_local_inexistente", "r_duplicado"]
relatos = marcar(relatos, rr); contagem(relatos, rr, "prata_relatos")
merge(relatos.filter("valido").select("id_relato", "id_local", "data_hora", "classe", "relator_nome",
                                      "relator_contato", "origem"), "prata_relatos", "id_relato")

# Chamados de manutenção: mantém apenas o snapshot mais recente de cada chamado
snap = F.to_date(F.regexp_extract("arquivo_origem", r"chamados_(\d{4}-\d{2}-\d{2})", 1))
chamados = (spark.table(f"{S}.bronze_chamados")
    .withColumn("snapshot", snap)
    .withColumn("rn", F.row_number().over(Window.partitionBy("id_chamado").orderBy(F.col("snapshot").desc())))
    .filter("rn = 1")
    .select("id_chamado", "id_local", F.to_date("data_abertura").alias("data_abertura"),
            F.to_date("data_conserto").alias("data_conserto"), "origem")
    .join(ids, "id_local", "left")
    .withColumn("r_id_nulo", F.col("id_chamado").isNull())
    .withColumn("r_local_inexistente", F.col("existe").isNull())
    .withColumn("r_conserto_antes_abertura", F.col("data_conserto") < F.col("data_abertura")))
rch = ["r_id_nulo", "r_local_inexistente", "r_conserto_antes_abertura"]
chamados = marcar(chamados, rch); contagem(chamados, rch, "prata_chamados")
merge(chamados.filter("valido").select("id_chamado", "id_local", "data_abertura", "data_conserto", "origem"),
      "prata_chamados", "id_chamado")