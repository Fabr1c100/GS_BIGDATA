# 00 | Preparação do ambiente (Luz Segura)
# Cria o schema, o volume e as pastas de entrada. Só precisa rodar uma vez.

CATALOGO, SCHEMA, VOLUME = "workspace", "luz_segura", "chegada"
S = f"{CATALOGO}.{SCHEMA}"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
PASTAS = ["pontos", "clima", "sensor", "relatos", "chamados", "referencia"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {S}.{VOLUME}")
for p in PASTAS:
    dbutils.fs.mkdirs(f"{BASE}/{p}")
display(dbutils.fs.ls(BASE))

def reiniciar_ambiente():
    for t in spark.catalog.listTables(S):
        spark.sql(f"DROP TABLE IF EXISTS {S}.{t.name}")
    dbutils.fs.rm(f"{BASE}/_controle", True)
    for p in PASTAS:
        for f in dbutils.fs.ls(f"{BASE}/{p}"):
            dbutils.fs.rm(f.path)