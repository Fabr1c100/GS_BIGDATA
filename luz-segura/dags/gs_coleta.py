from pathlib import Path
import pendulum, requests
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from geradores.simulacao import gerar_sensor, gerar_relatos, gerar_chamados

CHEGADA = Path("/opt/airflow/chegada")

def coletar_clima(ds, **_):
    r = requests.get("https://archive-api.open-meteo.com/v1/archive",
        params={"latitude": -23.53, "longitude": -46.79, "start_date": ds, "end_date": ds,
                "hourly": "cloud_cover,precipitation", "timezone": "America/Sao_Paulo"},
        timeout=30)
    r.raise_for_status()
    destino = CHEGADA / "clima" / f"clima_{ds}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(r.text, encoding="utf-8")

def conferir_chegada(ds, **_):
    esperados = [CHEGADA / "clima" / f"clima_{ds}.json",
                 CHEGADA / "sensor" / f"sensor_{ds}.csv",
                 CHEGADA / "relatos" / f"relatos_{ds}.csv",
                 CHEGADA / "chamados" / f"chamados_{ds}.csv"]
    faltando = [p.name for p in esperados if not p.exists()]
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes: {faltando}")

with DAG(dag_id="gs_coleta", schedule="@daily",
         start_date=pendulum.datetime(2026, 11, 3),
         catchup=True, max_active_runs=1) as dag:
    inicio = EmptyOperator(task_id="inicio")
    t_clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    t_sensor = PythonOperator(task_id="gerar_sensor", python_callable=gerar_sensor)
    t_relatos = PythonOperator(task_id="gerar_relatos", python_callable=gerar_relatos)
    t_chamados = PythonOperator(task_id="gerar_chamados", python_callable=gerar_chamados)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)
    inicio >> [t_clima, t_sensor, t_relatos, t_chamados] >> conferir