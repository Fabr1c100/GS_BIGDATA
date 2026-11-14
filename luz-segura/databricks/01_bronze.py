from pyspark.sql import functions as F
base = "/Volumes/workspace/gs/chegada"

def carregar_bronze(fonte, formato, tabela, opcoes=None):
    leitor = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{base}/_controle/schema_{fonte}"))
    for k, v in (opcoes or {}).items():
        leitor = leitor.option(k, v)
    q = (leitor.load(f"{base}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{base}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"workspace.gs.{tabela}"))
    q.awaitTermination()

carregar_bronze("clima",    "json", "bronze_clima")
carregar_bronze("sensor",   "csv",  "bronze_sensor",   {"header": "true"})
carregar_bronze("relatos",  "csv",  "bronze_relatos",  {"header": "true"})
carregar_bronze("chamados", "csv",  "bronze_chamados", {"header": "true"})