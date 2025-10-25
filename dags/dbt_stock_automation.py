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
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'dbt_stock_automation',
    default_args=default_args,
    description='Automate dbt stock models',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'stocks', 'automation'],
) as dag:

    # Use environment variables to redirect logs and target
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="cd /opt/airflow/dags/dbt_stock && "
                     "DBT_LOG_PATH=/tmp/dbt_logs "
                     "DBT_TARGET_PATH=/tmp/dbt_target "
                     "dbt run --profiles-dir .",
    )
