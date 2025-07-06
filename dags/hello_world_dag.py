from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 수동 실행
    catchup=False,
    tags=["example"]
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world,
    )
