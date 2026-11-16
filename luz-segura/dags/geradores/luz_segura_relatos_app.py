import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from geradores.app_relatos import exportar_relatos_app

# DAG disparada manualmente (botão do MVP ou interface do Airflow)
with DAG(dag_id="luz_segura_relatos_app", schedule=None,
         start_date=pendulum.datetime(2026, 11, 3), catchup=False) as dag:
    PythonOperator(task_id="exportar_relatos_app", python_callable=exportar_relatos_app)