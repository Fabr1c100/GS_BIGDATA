# 01 | Camada Bronze (ingestão incremental)
# O Auto Loader lê apenas os arquivos novos do Volume, evitando duplicidade nas reexecuções.

from pyspark.sql import functions as F

CATALOGO, SCHEMA, VOLUME = "workspace", "luz_segura", "chegada"
S = f"{CATALOGO}.{SCHEMA}"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

def carregar_bronze(fonte, formato, tabela, opcoes=None):
    leitor = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}"))
    for k, v in (opcoes or {}).items():
        leitor = leitor.option(k, v)
    q = (leitor.load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"{S}.{tabela}"))
    q.awaitTermination()

carregar_bronze("pontos",   "csv",  "bronze_pontos_osm", {"header": "true"})
carregar_bronze("clima",    "json", "bronze_clima")
carregar_bronze("sensor",   "csv",  "bronze_sensor",     {"header": "true"})
carregar_bronze("relatos",  "csv",  "bronze_relatos",    {"header": "true"})
carregar_bronze("chamados", "csv",  "bronze_chamados",   {"header": "true"})

# Carga completa da base estática de bairros
(spark.read.option("header", "true").csv(f"{BASE}/referencia/bairros_referencia.csv")
    .write.mode("overwrite").saveAsTable(f"{S}.bronze_bairros_referencia"))

# Registro de volumetria por carga
def registrar_contagem(momento):
    tabelas = ["bronze_pontos_osm", "bronze_clima", "bronze_sensor", "bronze_relatos", "bronze_chamados"]
    linhas = [(momento, t, spark.table(f"{S}.{t}").count()) for t in tabelas]
    (spark.createDataFrame(linhas, ["momento", "tabela", "linhas"])
        .withColumn("registrado_em", F.current_timestamp())
        .write.mode("append").saveAsTable(f"{S}.evidencia_lotes"))
    resumo = (spark.table(f"{S}.evidencia_lotes")
        .groupBy("tabela").pivot("momento").agg(F.max("linhas")))
    display(resumo)

registrar_contagem("5_relatos_app") # 1_lote1, 2_lote2, 3_lote3, 4_reexecucao, 5_relatos_app)