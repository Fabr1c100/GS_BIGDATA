from pyspark.sql import functions as F, Window
from functools import reduce

S = "workspace.gs"
base = "/Volumes/workspace/gs/chegada"

def marcar(df, regras):
    for r in regras:
        df = df.withColumn(r, F.coalesce(F.col(r), F.lit(False)))
    ruim = reduce(lambda a, b: a | b, [F.col(r) for r in regras])
    return df.withColumn("valido", ~ruim)

def contagem(df, regras):
    cols = [F.count("*").alias("total_lido")]
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

# ---------- pontos (cadastro)
(spark.read.option("header", "true").csv(f"{base}/referencia/cadastro_pontos.csv")
     .write.mode("overwrite").saveAsTable(f"{S}.prata_pontos"))
ids = spark.table(f"{S}.prata_pontos").select("id_local").withColumn("existe", F.lit(True))

# ---------- clima (listas paralelas viram uma linha por hora)
clima = (spark.table(f"{S}.bronze_clima")
    .select(F.explode(F.arrays_zip(
        F.col("hourly.time").alias("time"),
        F.col("hourly.cloud_cover").alias("cloud_cover"),
        F.col("hourly.precipitation").alias("precipitation"))).alias("h"))
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
clima = marcar(clima, rc); contagem(clima, rc)
merge(clima.filter("valido").select("chave", "cidade", "data_hora", "nebulosidade_pct", "precipitacao_mm"), "prata_clima", "chave")

# ---------- sensor
sensor = (spark.table(f"{S}.bronze_sensor").dropDuplicates(["id_leitura"])
    .select("id_leitura", "id_dispositivo", "id_local",
            F.to_timestamp("data_hora").alias("data_hora"),
            F.col("valor_adc").cast("int").alias("valor_adc"), "origem")
    .join(ids, "id_local", "left")
    .withColumn("r_adc_nulo", F.col("valor_adc").isNull())
    .withColumn("r_adc_fora_faixa", (F.col("valor_adc") < 0) | (F.col("valor_adc") > 4095))
    .withColumn("r_local_inexistente", F.col("existe").isNull())
    .withColumn("lux_estimado", F.round(F.col("valor_adc") * 0.008, 2)))
rs = ["r_adc_nulo", "r_adc_fora_faixa", "r_local_inexistente"]
sensor = marcar(sensor, rs); contagem(sensor, rs)
merge(sensor.filter("valido").select("id_leitura", "id_dispositivo", "id_local", "data_hora", "valor_adc", "lux_estimado", "origem"),
      "prata_sensor", "id_leitura")

# ---------- relatos
w = Window.partitionBy("id_local", "relator_contato", "classe").orderBy("data_hora")
relatos = (spark.table(f"{S}.bronze_relatos").dropDuplicates(["id_relato"])
    .select("id_relato", "id_local", F.to_timestamp("data_hora").alias("data_hora"),
            F.lower(F.trim("classe")).alias("classe"), "relator_nome", "relator_contato", "origem")
    .join(ids, "id_local", "left")
    .withColumn("min_desde_anterior", (F.col("data_hora").cast("long") - F.lag("data_hora").over(w).cast("long")) / 60)
    .withColumn("r_classe_invalida", ~F.col("classe").isin("iluminado", "fraco", "escuro"))
    .withColumn("r_local_inexistente", F.col("existe").isNull())
    .withColumn("r_duplicado", F.col("min_desde_anterior") < 15))
rr = ["r_classe_invalida", "r_local_inexistente", "r_duplicado"]
relatos = marcar(relatos, rr); contagem(relatos, rr)
merge(relatos.filter("valido").select("id_relato", "id_local", "data_hora", "classe", "relator_nome", "relator_contato", "origem"),
      "prata_relatos", "id_relato")

# ---------- chamados (mantém a foto mais recente de cada chamado)
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
chamados = marcar(chamados, rch); contagem(chamados, rch)
merge(chamados.filter("valido").select("id_chamado", "id_local", "data_abertura", "data_conserto", "origem"),
      "prata_chamados", "id_chamado")