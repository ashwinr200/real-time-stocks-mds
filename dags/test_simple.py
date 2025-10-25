from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def simple_task():
    print("✅ Simple task is working!")
    return "Success"

with DAG(
    "test_simple_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="test_task",
        python_callable=simple_task,
    )
