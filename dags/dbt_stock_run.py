from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    'dbt_stock_run',
    default_args=default_args,
    description='Run dbt stock models',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'stocks'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="cd /opt/airflow/dags/dbt_stock && dbt run",
    )
